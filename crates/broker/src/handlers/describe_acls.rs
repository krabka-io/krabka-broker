//! `DescribeAcls` handler (`api_key` 29).
//!
//! Authorizes `Describe` on `Cluster`, then projects every ACL in the metadata
//! image that matches the request filter. The filter follows Kafka's
//! `AclBindingFilter`, which [`super::acl_wire::binding_filter`] implements.
//!
//! A cluster with no authorizer configured has no ACLs to project, and says
//! so with `SECURITY_DISABLED` rather than an empty listing.

use {
    super::acl_wire::{
        NO_AUTHORIZER_MESSAGE, PatternTypeCode, ResourceTypeCode,
        binding_filter::{AclBindingFilter, UnknownElement, WireAclBindingFilter},
        operation_to_wire, pattern_type_to_wire, permission_to_wire, resource_type_to_wire,
    },
    crate::{
        broker::Broker,
        codes,
        error::BrokerError,
        handlers::{ErrorResponse as _, RequestContext, cluster_describe_denied},
    },
    krabka_metadata::AclEntry,
    krabka_protocol::{
        ProtocolError,
        owned::{
            describe_acls_request::DescribeAclsRequest,
            describe_acls_response::{AclDescription, DescribeAclsResource, DescribeAclsResponse},
        },
    },
    std::collections::HashMap,
};

/// The message of a cluster-describe refusal. Kafka's `AuthHelper` writes
/// "Request <request> needs DESCRIBE permission.", where `<request>` is the JVM
/// `toString` of the channel request; krabka names the API in its place.
const CLUSTER_DESCRIBE_DENIED_MESSAGE: &str = "Request DescribeAcls needs DESCRIBE permission.";

fn acl_description(entry: &AclEntry) -> AclDescription {
    AclDescription {
        principal: entry.principal.clone(),
        host: entry.host.clone(),
        operation: operation_to_wire(entry.operation),
        permission_type: permission_to_wire(entry.permission_type),
        ..Default::default()
    }
}

fn describe_acls_resource(
    resource_type: ResourceTypeCode,
    resource_name: String,
    pattern_type: PatternTypeCode,
    acls: Vec<AclDescription>,
) -> DescribeAclsResource {
    DescribeAclsResource {
        resource_type,
        resource_name,
        pattern_type,
        acls,
        ..Default::default()
    }
}

/// A successful listing.
///
/// Kafka's `AclApis.handleDescribeAcls` sets only the throttle time and the
/// resources, so `ErrorMessage` keeps its generated default: the empty
/// string, not null.
fn describe_acls_response(resources: Vec<DescribeAclsResource>) -> DescribeAclsResponse {
    DescribeAclsResponse {
        error_code: codes::NONE,
        error_message: Some(String::new()),
        resources,
        ..Default::default()
    }
}

pub fn handle(
    broker: &Broker,
    req: &DescribeAclsRequest,
    _version: i16,
    ctx: &RequestContext<'_>,
) -> Result<DescribeAclsResponse, BrokerError> {
    // Kafka's `DescribeAclsRequest` constructor refuses an `UNKNOWN` element
    // while the request parses, before any authorization, and the broker
    // closes the connection. The error return is that close.
    let filter = build_filter(req).map_err(|UnknownElement| {
        ProtocolError::InvalidValue("DescribeAclsRequest contains UNKNOWN elements")
    })?;

    let image = broker.controller.current_image();
    if cluster_describe_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        return Ok(DescribeAclsResponse::error(
            codes::CLUSTER_AUTHORIZATION_FAILED,
            Some(CLUSTER_DESCRIBE_DENIED_MESSAGE.into()),
        ));
    }

    // No authorizer: there is nothing to describe, and Kafka says so rather
    // than answering an empty listing a tool would read as "no ACLs exist".
    // `KafkaApis.handleDescribeAcls` runs the cluster-describe check first
    // and only then matches on `authorizer.isEmpty`, which is the order here.
    if !broker.config.authorizer.is_configured() {
        return Ok(DescribeAclsResponse::error(
            codes::SECURITY_DISABLED,
            Some(NO_AUTHORIZER_MESSAGE.into()),
        ));
    }

    Ok(describe_acls_response(matching_resources(
        image.all_acls(),
        &filter,
    )))
}

/// The resource pattern that `DescribeAcls` groups its bindings under.
#[derive(PartialEq, Eq, Hash)]
struct ResourcePatternKey {
    resource_type: ResourceTypeCode,
    resource_name: String,
    pattern_type: PatternTypeCode,
}

/// Groups the ACLs `filter` matches by resource pattern, the nested shape
/// Kafka's `DescribeAclsResponse.aclsResources` builds.
fn matching_resources<'a>(
    acls: impl Iterator<Item = &'a AclEntry>,
    filter: &AclBindingFilter,
) -> Vec<DescribeAclsResource> {
    let mut by_resource: HashMap<ResourcePatternKey, Vec<AclDescription>> = HashMap::new();
    for entry in acls.filter(|entry| filter.matches(entry)) {
        let key = ResourcePatternKey {
            resource_type: resource_type_to_wire(entry.resource_type),
            resource_name: entry.resource_name.clone(),
            pattern_type: pattern_type_to_wire(entry.pattern_type),
        };
        by_resource
            .entry(key)
            .or_default()
            .push(acl_description(entry));
    }
    by_resource
        .into_iter()
        .map(|(key, acls)| {
            describe_acls_resource(key.resource_type, key.resource_name, key.pattern_type, acls)
        })
        .collect()
}

fn build_filter(req: &DescribeAclsRequest) -> Result<AclBindingFilter, UnknownElement> {
    AclBindingFilter::from_wire(WireAclBindingFilter {
        resource_type: req.resource_type_filter,
        resource_name: req.resource_name_filter.as_deref(),
        pattern_type: req.pattern_type_filter,
        principal: req.principal_filter.as_deref(),
        host: req.host_filter.as_deref(),
        operation: req.operation,
        permission_type: req.permission_type,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            broker::Broker,
            handlers::{
                RequestContext,
                acl_test_support::{
                    self as acl_fixtures, OPERATION_ANY, OPERATION_READ, OPERATION_WRITE,
                    PATTERN_TYPE_ANY, PATTERN_TYPE_LITERAL, PATTERN_TYPE_MATCH,
                    PATTERN_TYPE_PREFIXED, PERMISSION_ALLOW, PERMISSION_ANY, RESOURCE_TYPE_TOPIC,
                    expected_empty_match_filter,
                },
                test_support::StartedBroker,
            },
        },
        assert2::{assert, check},
        krabka_metadata::{AclOperation, PatternType},
    };
    const VERSION: i16 = 3;

    /// A request corruption applied to one axis of the filter, labelled by that axis.
    struct CorruptRequestCase {
        axis: &'static str,
        corrupt: fn(&mut DescribeAclsRequest, i8),
    }

    /// A request edit labelled by what it sets.
    struct EditCase {
        name: &'static str,
        edit: fn(&mut DescribeAclsRequest),
    }

    /// A principal and host filter, with the number of resources it must list.
    struct NullableFilterCase {
        principal_filter: Option<&'static str>,
        host_filter: Option<&'static str>,
        want: usize,
    }

    use crate::test_support::allow_acl as acl;

    fn request(
        resource_name: Option<&str>,
        principal: Option<&str>,
        operation: i8,
    ) -> DescribeAclsRequest {
        crate::handlers::acl_test_support::acl_request_filter!(
            DescribeAclsRequest,
            resource_name,
            principal,
            operation
        )
    }

    krabka_macros::wire_codec_helpers! {
        test_context ["admin-client"];
    }

    use crate::{
        error::BrokerError, test_support::start_broker_with_authorizer_no_audit as start_broker,
    };

    #[test]
    fn build_filter_keeps_empty_strings_and_decodes_axes() {
        let req = krabka_macros::empty_match_acl_filter!(DescribeAclsRequest);

        let built = build_filter(&req).expect("filter");

        let expected = expected_empty_match_filter();
        assert!(built == expected);
    }

    /// Kafka's `DescribeAclsRequest.normalizeAndValidate` refuses only the
    /// `UNKNOWN` (0) byte; any other byte it does not define parses as
    /// `UNKNOWN` and matches nothing.
    #[test]
    fn build_filter_refuses_only_the_unknown_byte() {
        let cases: [CorruptRequestCase; 4] = [
            CorruptRequestCase {
                axis: "resource_type_filter",
                corrupt: |r, b| r.resource_type_filter = b,
            },
            CorruptRequestCase {
                axis: "pattern_type_filter",
                corrupt: |r, b| r.pattern_type_filter = b,
            },
            CorruptRequestCase {
                axis: "operation",
                corrupt: |r, b| r.operation = b,
            },
            CorruptRequestCase {
                axis: "permission_type",
                corrupt: |r, b| r.permission_type = b,
            },
        ];
        for CorruptRequestCase { axis, corrupt } in cases {
            let mut unknown = request(Some("orders"), Some("User:alice"), OPERATION_READ);
            corrupt(&mut unknown, 0);
            check!(build_filter(&unknown) == Err(UnknownElement), "axis {axis}");

            let mut undefined = request(Some("orders"), Some("User:alice"), OPERATION_READ);
            corrupt(&mut undefined, 99);
            check!(build_filter(&undefined).is_ok(), "axis {axis}");
        }
    }

    #[test]
    fn response_helpers_preserve_error_resource_and_acl_fields() {
        let err = DescribeAclsResponse::error(
            codes::SECURITY_DISABLED,
            Some(NO_AUTHORIZER_MESSAGE.into()),
        );
        let expected_err = krabka_macros::unthrottled_wire!(DescribeAclsResponse {
            error_code: codes::SECURITY_DISABLED,
            error_message: Some("No Authorizer is configured on the broker".into()),
            resources: Vec::new(),
        });
        assert!(err == expected_err);

        let desc = acl_description(&acl().call());
        let expected_desc = krabka_macros::tagged_wire!(AclDescription {
            principal: "User:alice".into(),
            host: "*".into(),
            operation: OPERATION_READ,
            permission_type: PERMISSION_ALLOW,
        });
        assert!(desc == expected_desc);

        let resource = describe_acls_resource(
            RESOURCE_TYPE_TOPIC,
            "orders".into(),
            PATTERN_TYPE_LITERAL,
            vec![desc.clone()],
        );
        let expected_resource = krabka_macros::tagged_wire!(DescribeAclsResource {
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "orders".into(),
            pattern_type: PATTERN_TYPE_LITERAL,
            acls: vec![desc],
        });
        assert!(resource == expected_resource);

        let resp = describe_acls_response(vec![resource.clone()]);
        let expected_resp = krabka_macros::unthrottled_wire!(DescribeAclsResponse {
            error_code: codes::NONE,
            error_message: Some(String::new()),
            resources: vec![resource],
        });
        assert!(resp == expected_resp);
    }

    fn describe_alice_orders(broker: &Broker, ctx: &RequestContext<'_>) -> DescribeAclsResponse {
        handle(
            broker,
            &request(Some("orders"), Some("User:alice"), OPERATION_READ),
            VERSION,
            ctx,
        )
        .expect("handle")
    }

    #[tokio::test]
    async fn handle_denies_cluster_describe() {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, ctx);
            boot [start_broker] deny_all;
            user ["alice"];
        };

        let resp = describe_alice_orders(&broker, &ctx);

        let expected = DescribeAclsResponse::error(
            codes::CLUSTER_AUTHORIZATION_FAILED,
            Some("Request DescribeAcls needs DESCRIBE permission.".into()),
        );
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_answers_security_disabled_when_no_authorizer_is_configured() {
        krabka_macros::broker_test_setup! {
        bind(broker_handle, _dir, broker, ctx);
        boot [acl_fixtures::start_security_disabled_acls()];
        user ["admin"];
        };

        let resp = describe_alice_orders(&broker, &ctx);

        let expected = DescribeAclsResponse::error(
            codes::SECURITY_DISABLED,
            Some("No Authorizer is configured on the broker".into()),
        );
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// Kafka refuses an `UNKNOWN` element while the request parses, which is
    /// before the cluster-describe check, and closes the connection. A
    /// principal that would be denied gets the same close.
    #[tokio::test]
    async fn handle_closes_the_connection_on_an_unknown_element() {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, ctx);
            boot [start_broker] deny_all;
            user ["alice"];
        };
        let mut req = request(Some("orders"), Some("User:alice"), OPERATION_READ);
        req.operation = 0;

        let result = handle(&broker, &req, VERSION, &ctx);

        assert!(
            let Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "DescribeAclsRequest contains UNKNOWN elements"
            ))) = result
        );
        broker_handle.shutdown().await;
    }

    /// A byte Kafka does not define parses as `UNKNOWN`, and Kafka's
    /// `AclBindingFilter.matches` matches no binding against it. The KIP-373
    /// `USER` resource type and `CREATE_TOKENS` and `DESCRIBE_TOKENS`
    /// operations are valid, and no stored binding carries them.
    #[tokio::test]
    async fn handle_answers_an_empty_listing_for_values_no_binding_carries() {
        krabka_macros::broker_test_setup! {
        bind(broker_handle, _dir, broker, ctx);
        boot [acl_fixtures::start_orders_acl()];
        user ["admin"];
        };
        let any = DescribeAclsRequest {
            resource_type_filter: 1,
            resource_name_filter: None,
            pattern_type_filter: PATTERN_TYPE_ANY,
            principal_filter: None,
            host_filter: None,
            operation: OPERATION_ANY,
            permission_type: PERMISSION_ANY,
            ..Default::default()
        };
        let cases: [EditCase; 7] = [
            EditCase {
                name: "undefined resource type",
                edit: |r| r.resource_type_filter = 99,
            },
            EditCase {
                name: "undefined pattern type",
                edit: |r| r.pattern_type_filter = 99,
            },
            EditCase {
                name: "undefined operation",
                edit: |r| r.operation = 99,
            },
            EditCase {
                name: "undefined permission",
                edit: |r| r.permission_type = 99,
            },
            EditCase {
                name: "USER resource type",
                edit: |r| r.resource_type_filter = 7,
            },
            EditCase {
                name: "CREATE_TOKENS operation",
                edit: |r| r.operation = 13,
            },
            EditCase {
                name: "DESCRIBE_TOKENS operation",
                edit: |r| r.operation = 14,
            },
        ];
        let expected = krabka_macros::unthrottled_wire!(DescribeAclsResponse {
            error_code: codes::NONE,
            error_message: Some(String::new()),
            resources: Vec::new(),
        });
        for EditCase { name, edit } in cases {
            let mut req = any.clone();
            edit(&mut req);
            let resp = handle(&broker, &req, VERSION, &ctx).expect("handle");
            check!(resp == expected, "{name}");
        }
        broker_handle.shutdown().await;
    }

    #[derive(Default)]
    struct AclPatternListingScenario {
        started: Option<StartedBroker>,
        request: Option<DescribeAclsRequest>,
        response: Option<DescribeAclsResponse>,
    }
    #[rstest::fixture]
    fn acl_pattern_listing() -> AclPatternListingScenario {
        let started = None;
        AclPatternListingScenario {
            started,
            ..Default::default()
        }
    }
    fn described_acl_pattern_code(pattern: &str) -> i8 {
        match pattern {
            "any" => PATTERN_TYPE_ANY,
            "match" => PATTERN_TYPE_MATCH,
            "literal" => PATTERN_TYPE_LITERAL,
            "prefixed" => PATTERN_TYPE_PREFIXED,
            _ => panic!("unknown ACL pattern filter: {pattern}"),
        }
    }
    #[rstest_bdd_macros::given("topic read ACLs for User:alice contain these resource patterns")]
    async fn stored_acl_patterns(
        acl_pattern_listing: &mut AclPatternListingScenario,
        datatable: Vec<Vec<String>>,
    ) {
        let entries = datatable
            .into_iter()
            .skip(1)
            .map(|row| {
                let [pattern, name] = row.as_slice() else {
                    panic!("ACL pattern type and name required")
                };
                let pattern_type = match pattern.as_str() {
                    "literal" => PatternType::Literal,
                    "prefixed" => PatternType::Prefixed,
                    _ => panic!("concrete ACL pattern required"),
                };
                AclEntry {
                    pattern_type,
                    ..acl().resource_name(name.as_str()).call()
                }
            })
            .collect();
        acl_pattern_listing.started = Some(
            acl_fixtures::start_configured_acls()
                .entries(move || entries)
                .call()
                .await,
        );
    }
    #[rstest_bdd_macros::given(
        "DescribeAcls filters pattern type {pattern} and resource name {name}"
    )]
    fn requested_acl_pattern_listing(
        acl_pattern_listing: &mut AclPatternListingScenario,
        pattern: String,
        name: String,
    ) {
        acl_pattern_listing.request = Some(DescribeAclsRequest {
            resource_type_filter: RESOURCE_TYPE_TOPIC,
            resource_name_filter: match name.as_str() {
                "null" => None,
                "empty" => Some(String::new()),
                _ => Some(name),
            },
            pattern_type_filter: described_acl_pattern_code(&pattern),
            principal_filter: None,
            host_filter: None,
            operation: OPERATION_ANY,
            permission_type: PERMISSION_ANY,
            ..Default::default()
        });
    }
    #[rstest_bdd_macros::when(
        "an admin describes matching ACLs and the result is sorted by resource name"
    )]
    fn describe_matching_acl_patterns(acl_pattern_listing: &mut AclPatternListingScenario) {
        let broker = acl_pattern_listing
            .started
            .as_ref()
            .unwrap()
            .broker
            .broker_arc_for_test();
        krabka_macros::request_test_identity! {bind(ctx);user ["admin"];};
        let mut response = handle(
            &broker,
            acl_pattern_listing.request.as_ref().unwrap(),
            VERSION,
            &ctx,
        )
        .expect("handle");
        response
            .resources
            .sort_by(|a, b| a.resource_name.cmp(&b.resource_name));
        acl_pattern_listing.response = Some(response);
    }
    #[rstest_bdd_macros::then("the complete ACL description contains patterns {listed}")]
    async fn check_described_acl_patterns(
        acl_pattern_listing: &mut AclPatternListingScenario,
        listed: String,
    ) {
        let resources = if listed == "none" {
            Vec::new()
        } else {
            listed
                .split(';')
                .map(|row| {
                    let (pattern, name) = row.split_once(':').unwrap();
                    krabka_macros::tagged_wire!(DescribeAclsResource {
                        resource_type: RESOURCE_TYPE_TOPIC,
                        resource_name: name.into(),
                        pattern_type: described_acl_pattern_code(pattern),
                        acls: vec![krabka_macros::tagged_wire!(AclDescription {
                            principal: "User:alice".into(),
                            host: "*".into(),
                            operation: OPERATION_READ,
                            permission_type: PERMISSION_ALLOW
                        })]
                    })
                })
                .collect()
        };
        let expected = krabka_macros::unthrottled_wire!(DescribeAclsResponse {
            error_code: codes::NONE,
            error_message: Some(String::new()),
            resources
        });
        check!(acl_pattern_listing.response.as_ref().unwrap() == &expected);
        acl_pattern_listing
            .started
            .take()
            .unwrap()
            .broker
            .shutdown()
            .await;
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_describe_acl_patterns/patterns.feature",
        name = "Pattern filters distinguish null names exact names and matching prefixes"
    )]
    #[tokio::test]
    async fn handle_matches_patterns_the_way_kafka_does(
        acl_pattern_listing: AclPatternListingScenario,
        pattern: String,
        name: String,
        listed: String,
    ) {
        let _ = (acl_pattern_listing, pattern, name, listed);
    }

    /// Only a null principal or host is a wildcard. An empty string matches
    /// only an empty field.
    #[tokio::test]
    async fn handle_takes_only_null_principal_and_host_as_any() {
        krabka_macros::broker_test_setup! {
        bind(broker_handle, _dir, broker, ctx);
        boot [acl_fixtures::start_orders_acl()];
        user ["admin"];
        };

        let cases: [NullableFilterCase; 4] = [
            NullableFilterCase {
                principal_filter: None,
                host_filter: None,
                want: 1,
            },
            NullableFilterCase {
                principal_filter: Some(""),
                host_filter: None,
                want: 0,
            },
            NullableFilterCase {
                principal_filter: None,
                host_filter: Some(""),
                want: 0,
            },
            NullableFilterCase {
                principal_filter: Some("User:alice"),
                host_filter: Some("*"),
                want: 1,
            },
        ];
        for NullableFilterCase {
            principal_filter,
            host_filter,
            want,
        } in cases
        {
            let req = DescribeAclsRequest {
                resource_type_filter: RESOURCE_TYPE_TOPIC,
                resource_name_filter: None,
                pattern_type_filter: PATTERN_TYPE_ANY,
                principal_filter: principal_filter.map(Into::into),
                host_filter: host_filter.map(Into::into),
                operation: OPERATION_ANY,
                permission_type: PERMISSION_ANY,
                ..Default::default()
            };
            let resp = handle(&broker, &req, VERSION, &ctx).expect("handle");
            check!(
                resp.resources.len() == want,
                "principal {principal_filter:?} host {host_filter:?}"
            );
        }
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_returns_only_matching_acl_fields() {
        krabka_macros::broker_test_setup! {
        bind(broker_handle, _dir, broker, ctx);
        boot [acl_fixtures::start_orders_payments_acls()];
        user ["admin"];
        };

        let resp = describe_alice_orders(&broker, &ctx);

        let expected = krabka_macros::unthrottled_wire!(DescribeAclsResponse {
            error_code: codes::NONE,
            error_message: Some(String::new()),
            resources: vec![krabka_macros::tagged_wire!(DescribeAclsResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: "orders".into(),
                pattern_type: PATTERN_TYPE_LITERAL,
                acls: vec![krabka_macros::tagged_wire!(AclDescription {
                    principal: "User:alice".into(),
                    host: "*".into(),
                    operation: OPERATION_READ,
                    permission_type: PERMISSION_ALLOW,
                })],
            })],
        });
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[test]
    fn acl_description_preserves_non_read_operations() {
        let desc = acl_description(
            &acl()
                .resource_name("payments")
                .principal("User:bob")
                .operation(AclOperation::Write)
                .call(),
        );

        assert!(desc.principal == "User:bob");
        assert!(desc.operation == OPERATION_WRITE);
    }
}
