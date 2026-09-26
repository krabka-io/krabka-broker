//! `DescribeAcls` handler (`api_key` 29).
//!
//! Authorizes `Describe` on `Cluster`, then projects every ACL in the metadata
//! image that matches the request filter. The filter follows Kafka's
//! `AclBindingFilter`, which [`super::acl_wire::binding_filter`] implements.
//!
//! A cluster with no authorizer configured has no ACLs to project, and says
//! so with `SECURITY_DISABLED` rather than an empty listing.

use bytes::Bytes;
use krabka_metadata::AclEntry;
use krabka_protocol::{
    Encode, ProtocolError,
    owned::{
        describe_acls_request::DescribeAclsRequest,
        describe_acls_response::{AclDescription, DescribeAclsResource, DescribeAclsResponse},
    },
};

use super::acl_wire::{
    CLUSTER_RESOURCE_NAME, NO_AUTHORIZER_MESSAGE, PatternTypeCode, ResourceTypeCode,
    binding_filter::{AclBindingFilter, UnknownElement, WireAclBindingFilter},
    operation_to_wire, pattern_type_to_wire, permission_to_wire, resource_type_to_wire,
};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
};

/// The message of a cluster-describe refusal. Kafka's `AuthHelper` writes
/// "Request <request> needs DESCRIBE permission.", where `<request>` is the JVM
/// `toString` of the channel request; krabka names the API in its place.
const CLUSTER_DESCRIBE_DENIED_MESSAGE: &str = "Request DescribeAcls needs DESCRIBE permission.";

fn describe_acls_error_response(
    error_code: i16,
    error_message: &'static str,
) -> DescribeAclsResponse {
    DescribeAclsResponse {
        error_code,
        error_message: Some(error_message.into()),
        ..Default::default()
    }
}

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

fn describe_acls_response(resources: Vec<DescribeAclsResource>) -> DescribeAclsResponse {
    DescribeAclsResponse {
        resources,
        ..Default::default()
    }
}

// `async` for symmetry with the other ACL wire handlers (CreateAcls /
// DeleteAcls awaits `controller.submit_change`; read-only
// DescribeAcls itself never suspends.
#[tracing::instrument(
    name = "handle_describe_acls",
    level = "info",
    skip_all,
    fields(api = "DescribeAcls"),
    err
)]
pub(crate) fn handle(
    broker: &Broker,
    req: DescribeAclsRequest,
    ctx: &crate::handlers::RequestContext<'_>,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    // Kafka's `DescribeAclsRequest` constructor refuses an `UNKNOWN` element
    // while the request parses, before any authorization, and the broker
    // closes the connection. The error return is that close.
    let filter = build_filter(&req).map_err(|UnknownElement| {
        ProtocolError::InvalidValue("DescribeAclsRequest contains UNKNOWN elements")
    })?;

    let image = broker.controller.current_image();
    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: krabka_metadata::ResourceType::Cluster,
            resource_name: CLUSTER_RESOURCE_NAME,
            operation: krabka_metadata::AclOperation::Describe,
        },
    );
    if allow == AuthorizationResult::Deny {
        let resp = describe_acls_error_response(
            codes::CLUSTER_AUTHORIZATION_FAILED,
            CLUSTER_DESCRIBE_DENIED_MESSAGE,
        );
        return encode_response(&resp, api_version);
    }

    // No authorizer: there is nothing to describe, and Kafka says so rather
    // than answering an empty listing a tool would read as "no ACLs exist".
    // `KafkaApis.handleDescribeAcls` runs the cluster-describe check first
    // and only then matches on `authorizer.isEmpty`, which is the order here.
    if !broker.config.authorizer.is_configured() {
        let resp = describe_acls_error_response(codes::SECURITY_DISABLED, NO_AUTHORIZER_MESSAGE);
        return encode_response(&resp, api_version);
    }

    let resp = describe_acls_response(matching_resources(image.all_acls(), &filter));
    encode_response(&resp, api_version)
}

/// Groups the ACLs `filter` matches by resource pattern, the nested shape
/// Kafka's `DescribeAclsResponse.aclsResources` builds.
fn matching_resources<'a>(
    acls: impl Iterator<Item = &'a AclEntry>,
    filter: &AclBindingFilter,
) -> Vec<DescribeAclsResource> {
    let mut by_resource: std::collections::HashMap<
        (ResourceTypeCode, String, PatternTypeCode),
        Vec<AclDescription>,
    > = std::collections::HashMap::new();
    for entry in acls.filter(|entry| filter.matches(entry)) {
        let key = (
            resource_type_to_wire(entry.resource_type),
            entry.resource_name.clone(),
            pattern_type_to_wire(entry.pattern_type),
        );
        by_resource
            .entry(key)
            .or_default()
            .push(acl_description(entry));
    }
    by_resource
        .into_iter()
        .map(|((rt, rn, pt), acls)| describe_acls_resource(rt, rn, pt, acls))
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

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    crate::handlers::encode_response_with_context(resp, api_version, "encode DescribeAcls")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_metadata::{
        AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
    };
    use krabka_protocol::UnknownTaggedFields;

    use super::*;
    use crate::{
        broker::BrokerHandle,
        handlers::acl_wire::binding_filter::{AxisFilter, PatternTypeFilter},
        test_support::{DenyAll, peer, principal},
    };

    const VERSION: i16 = 3;
    const RESOURCE_TYPE_TOPIC: i8 = 2;
    const PATTERN_TYPE_ANY: i8 = 1;
    const PATTERN_TYPE_MATCH: i8 = 2;
    const PATTERN_TYPE_LITERAL: i8 = 3;
    const PATTERN_TYPE_PREFIXED: i8 = 4;
    const OPERATION_ANY: i8 = 1;
    const OPERATION_READ: i8 = 3;
    const OPERATION_WRITE: i8 = 4;
    const PERMISSION_ANY: i8 = 1;
    const PERMISSION_ALLOW: i8 = 3;

    /// A describe-table row: the filter pattern type and name, and the
    /// (resource name, pattern type) pairs the listing must hold.
    type PatternRow<'a> = (i8, Option<&'a str>, &'a [(&'a str, i8)]);

    fn acl(resource_name: &str, principal: &str, operation: AclOperation) -> AclEntry {
        AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: resource_name.into(),
            pattern_type: PatternType::Literal,
            principal: principal.into(),
            host: "*".into(),
            operation,
            permission_type: PermissionType::Allow,
        }
    }

    fn request(
        resource_name: Option<&str>,
        principal: Option<&str>,
        operation: i8,
    ) -> DescribeAclsRequest {
        DescribeAclsRequest {
            resource_type_filter: RESOURCE_TYPE_TOPIC,
            resource_name_filter: resource_name.map(Into::into),
            pattern_type_filter: PATTERN_TYPE_LITERAL,
            principal_filter: principal.map(Into::into),
            host_filter: Some("*".into()),
            operation,
            permission_type: PERMISSION_ALLOW,
            ..Default::default()
        }
    }

    crate::test_support::response_helpers!(
        DescribeAclsResponse,
        version = VERSION,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    /// An authorizer an operator actually configured, which lets the `admin`
    /// test principal through as a super user.
    ///
    /// The ACL RPCs answer `SECURITY_DISABLED` under the default
    /// `AllowAllAuthorizer`, so every case about the describing path needs a
    /// broker that has an authorizer at all.
    fn configured_authorizer() -> Arc<dyn crate::authorizer::Authorizer> {
        Arc::new(crate::authorizer::SimpleAclAuthorizer::new(
            std::iter::once("admin".to_owned()).collect(),
        ))
    }

    async fn seed_acls(handle: &BrokerHandle, entries: Vec<AclEntry>) {
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(
                entries
                    .into_iter()
                    .map(MetadataRecord::V1AccessControlEntry)
                    .collect(),
            )
            .await
            .expect("seed ACLs");
    }

    #[test]
    fn build_filter_keeps_empty_strings_and_decodes_axes() {
        let req = DescribeAclsRequest {
            resource_type_filter: RESOURCE_TYPE_TOPIC,
            resource_name_filter: Some(String::new()),
            pattern_type_filter: PATTERN_TYPE_MATCH,
            principal_filter: Some(String::new()),
            host_filter: None,
            operation: OPERATION_ANY,
            permission_type: PERMISSION_ANY,
            ..Default::default()
        };

        let built = build_filter(&req).expect("filter");

        let expected = AclBindingFilter {
            resource_type: AxisFilter::Exact(ResourceType::Topic),
            resource_name: Some(String::new()),
            pattern_type: PatternTypeFilter::Match,
            principal: Some(String::new()),
            host: None,
            operation: AxisFilter::Any,
            permission_type: AxisFilter::Any,
        };
        assert!(built == expected);
    }

    /// Kafka's `DescribeAclsRequest.normalizeAndValidate` refuses only the
    /// `UNKNOWN` (0) byte; any other byte it does not define parses as
    /// `UNKNOWN` and matches nothing.
    #[test]
    fn build_filter_refuses_only_the_unknown_byte() {
        type CorruptRequest = fn(&mut DescribeAclsRequest, i8);
        let cases: [(&str, CorruptRequest); 4] = [
            ("resource_type_filter", |r, b| r.resource_type_filter = b),
            ("pattern_type_filter", |r, b| r.pattern_type_filter = b),
            ("operation", |r, b| r.operation = b),
            ("permission_type", |r, b| r.permission_type = b),
        ];
        for (axis, corrupt) in cases {
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
        let err = describe_acls_error_response(codes::SECURITY_DISABLED, NO_AUTHORIZER_MESSAGE);
        let expected_err = DescribeAclsResponse {
            throttle_time_ms: 0,
            error_code: codes::SECURITY_DISABLED,
            error_message: Some("No Authorizer is configured on the broker".into()),
            resources: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(err == expected_err);

        let desc = acl_description(&acl("orders", "User:alice", AclOperation::Read));
        let expected_desc = AclDescription {
            principal: "User:alice".into(),
            host: "*".into(),
            operation: OPERATION_READ,
            permission_type: PERMISSION_ALLOW,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(desc == expected_desc);

        let resource = describe_acls_resource(
            RESOURCE_TYPE_TOPIC,
            "orders".into(),
            PATTERN_TYPE_LITERAL,
            vec![desc.clone()],
        );
        let expected_resource = DescribeAclsResource {
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "orders".into(),
            pattern_type: PATTERN_TYPE_LITERAL,
            acls: vec![desc],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resource == expected_resource);

        let resp = describe_acls_response(vec![resource.clone()]);
        let expected_resp = DescribeAclsResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            error_message: None,
            resources: vec![resource],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected_resp);
    }

    #[tokio::test]
    async fn handle_denies_cluster_describe() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let resp = handle(
            &broker,
            request(Some("orders"), Some("User:alice"), OPERATION_READ),
            &ctx,
            VERSION,
        )
        .expect("handle");
        let resp = decode_response(&resp);

        let expected = DescribeAclsResponse {
            throttle_time_ms: 0,
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("Request DescribeAcls needs DESCRIBE permission.".into()),
            resources: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_answers_security_disabled_when_no_authorizer_is_configured() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_acls(
            &broker_handle,
            vec![acl("orders", "User:alice", AclOperation::Read)],
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let resp = handle(
            &broker,
            request(Some("orders"), Some("User:alice"), OPERATION_READ),
            &ctx,
            VERSION,
        )
        .expect("handle");
        let resp = decode_response(&resp);

        let expected = DescribeAclsResponse {
            throttle_time_ms: 0,
            error_code: codes::SECURITY_DISABLED,
            error_message: Some("No Authorizer is configured on the broker".into()),
            resources: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// Kafka refuses an `UNKNOWN` element while the request parses, which is
    /// before the cluster-describe check, and closes the connection. A
    /// principal that would be denied gets the same close.
    #[tokio::test]
    async fn handle_closes_the_connection_on_an_unknown_element() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let ctx = test_context(&p, &peer);
        let mut req = request(Some("orders"), Some("User:alice"), OPERATION_READ);
        req.operation = 0;

        let result = handle(&broker, req, &ctx, VERSION);

        assert!(
            let Err(crate::error::BrokerError::Protocol(ProtocolError::InvalidValue(
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
        type Edit = fn(&mut DescribeAclsRequest);

        let (broker_handle, _dir) = start_broker(configured_authorizer()).await;
        seed_acls(
            &broker_handle,
            vec![acl("orders", "User:alice", AclOperation::Read)],
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);
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
        let cases: [(&str, Edit); 7] = [
            ("undefined resource type", |r| r.resource_type_filter = 99),
            ("undefined pattern type", |r| r.pattern_type_filter = 99),
            ("undefined operation", |r| r.operation = 99),
            ("undefined permission", |r| r.permission_type = 99),
            ("USER resource type", |r| r.resource_type_filter = 7),
            ("CREATE_TOKENS operation", |r| r.operation = 13),
            ("DESCRIBE_TOKENS operation", |r| r.operation = 14),
        ];
        let expected = DescribeAclsResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            error_message: None,
            resources: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        for (name, edit) in cases {
            let mut req = any.clone();
            edit(&mut req);
            let resp = handle(&broker, req, &ctx, VERSION).expect("handle");
            check!(decode_response(&resp) == expected, "{name}");
        }
        broker_handle.shutdown().await;
    }

    /// The table from issue #770: `MATCH` lists every binding that applies to
    /// the named resource, and only a null name is a wildcard.
    #[tokio::test]
    async fn handle_matches_patterns_the_way_kafka_does() {
        let (broker_handle, _dir) = start_broker(configured_authorizer()).await;
        let seeded = [
            ("foo", PatternType::Literal),
            ("*", PatternType::Literal),
            ("f", PatternType::Prefixed),
            ("fo", PatternType::Prefixed),
            ("bar", PatternType::Prefixed),
            ("food", PatternType::Literal),
        ];
        seed_acls(
            &broker_handle,
            seeded
                .iter()
                .map(|(name, pattern_type)| AclEntry {
                    pattern_type: *pattern_type,
                    ..acl(name, "User:alice", AclOperation::Read)
                })
                .collect(),
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let all: &[(&str, i8)] = &[
            ("*", PATTERN_TYPE_LITERAL),
            ("bar", PATTERN_TYPE_PREFIXED),
            ("f", PATTERN_TYPE_PREFIXED),
            ("fo", PATTERN_TYPE_PREFIXED),
            ("foo", PATTERN_TYPE_LITERAL),
            ("food", PATTERN_TYPE_LITERAL),
        ];
        let cases: [PatternRow<'_>; 12] = [
            (PATTERN_TYPE_ANY, None, all),
            (PATTERN_TYPE_ANY, Some(""), &[]),
            (
                PATTERN_TYPE_ANY,
                Some("foo"),
                &[("foo", PATTERN_TYPE_LITERAL)],
            ),
            (PATTERN_TYPE_MATCH, None, all),
            (PATTERN_TYPE_MATCH, Some(""), &[("*", PATTERN_TYPE_LITERAL)]),
            (
                PATTERN_TYPE_MATCH,
                Some("foo"),
                &[
                    ("*", PATTERN_TYPE_LITERAL),
                    ("f", PATTERN_TYPE_PREFIXED),
                    ("fo", PATTERN_TYPE_PREFIXED),
                    ("foo", PATTERN_TYPE_LITERAL),
                ],
            ),
            (
                PATTERN_TYPE_LITERAL,
                None,
                &[
                    ("*", PATTERN_TYPE_LITERAL),
                    ("foo", PATTERN_TYPE_LITERAL),
                    ("food", PATTERN_TYPE_LITERAL),
                ],
            ),
            (PATTERN_TYPE_LITERAL, Some(""), &[]),
            (
                PATTERN_TYPE_LITERAL,
                Some("foo"),
                &[("foo", PATTERN_TYPE_LITERAL)],
            ),
            (
                PATTERN_TYPE_PREFIXED,
                None,
                &[
                    ("bar", PATTERN_TYPE_PREFIXED),
                    ("f", PATTERN_TYPE_PREFIXED),
                    ("fo", PATTERN_TYPE_PREFIXED),
                ],
            ),
            (PATTERN_TYPE_PREFIXED, Some(""), &[]),
            (PATTERN_TYPE_PREFIXED, Some("foo"), &[]),
        ];
        for (pattern_type, name, want) in cases {
            let req = DescribeAclsRequest {
                resource_type_filter: RESOURCE_TYPE_TOPIC,
                resource_name_filter: name.map(Into::into),
                pattern_type_filter: pattern_type,
                principal_filter: None,
                host_filter: None,
                operation: OPERATION_ANY,
                permission_type: PERMISSION_ANY,
                ..Default::default()
            };
            let resp = handle(&broker, req, &ctx, VERSION).expect("handle");
            let mut resp = decode_response(&resp);
            resp.resources
                .sort_by(|a, b| a.resource_name.cmp(&b.resource_name));

            let expected = DescribeAclsResponse {
                throttle_time_ms: 0,
                error_code: codes::NONE,
                error_message: None,
                resources: want
                    .iter()
                    .map(|(resource_name, pattern_type)| DescribeAclsResource {
                        resource_type: RESOURCE_TYPE_TOPIC,
                        resource_name: (*resource_name).into(),
                        pattern_type: *pattern_type,
                        acls: vec![AclDescription {
                            principal: "User:alice".into(),
                            host: "*".into(),
                            operation: OPERATION_READ,
                            permission_type: PERMISSION_ALLOW,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        }],
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    })
                    .collect(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            };
            check!(resp == expected, "pattern {pattern_type} name {name:?}");
        }
        broker_handle.shutdown().await;
    }

    /// Only a null principal or host is a wildcard. An empty string matches
    /// only an empty field.
    #[tokio::test]
    async fn handle_takes_only_null_principal_and_host_as_any() {
        let (broker_handle, _dir) = start_broker(configured_authorizer()).await;
        seed_acls(
            &broker_handle,
            vec![acl("orders", "User:alice", AclOperation::Read)],
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let cases: [(Option<&str>, Option<&str>, usize); 4] = [
            (None, None, 1),
            (Some(""), None, 0),
            (None, Some(""), 0),
            (Some("User:alice"), Some("*"), 1),
        ];
        for (principal_filter, host_filter, want) in cases {
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
            let resp = handle(&broker, req, &ctx, VERSION).expect("handle");
            check!(
                decode_response(&resp).resources.len() == want,
                "principal {principal_filter:?} host {host_filter:?}"
            );
        }
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_returns_only_matching_acl_fields() {
        let (broker_handle, _dir) = start_broker(configured_authorizer()).await;
        seed_acls(
            &broker_handle,
            vec![
                acl("orders", "User:alice", AclOperation::Read),
                acl("payments", "User:bob", AclOperation::Write),
            ],
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let resp = handle(
            &broker,
            request(Some("orders"), Some("User:alice"), OPERATION_READ),
            &ctx,
            VERSION,
        )
        .expect("handle");
        let resp = decode_response(&resp);

        let expected = DescribeAclsResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            error_message: None,
            resources: vec![DescribeAclsResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: "orders".into(),
                pattern_type: PATTERN_TYPE_LITERAL,
                acls: vec![AclDescription {
                    principal: "User:alice".into(),
                    host: "*".into(),
                    operation: OPERATION_READ,
                    permission_type: PERMISSION_ALLOW,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[test]
    fn acl_description_preserves_non_read_operations() {
        let desc = acl_description(&acl("payments", "User:bob", AclOperation::Write));

        assert!(desc.principal == "User:bob");
        assert!(desc.operation == OPERATION_WRITE);
    }
}
