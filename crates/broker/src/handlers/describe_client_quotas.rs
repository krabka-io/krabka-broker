//! `DescribeClientQuotas` (`api_key` 48, KIP-13/124).

use bytes::Bytes;
use krabka_metadata::{EntityKey, ResourceType};
use krabka_protocol::{
    Encode,
    owned::{
        describe_client_quotas_request::{ComponentData, DescribeClientQuotasRequest},
        describe_client_quotas_response::{
            DescribeClientQuotasResponse, EntityData, EntryData, ValueData,
        },
    },
};

use super::acl_wire::CLUSTER_RESOURCE_NAME;
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes::{CLUSTER_AUTHORIZATION_FAILED, INVALID_REQUEST, NONE, UNSUPPORTED_VERSION},
};

/// Wire `match_type`: entity name must equal `match_` exactly (KIP-546 `EXACT`).
const MATCH_TYPE_EXACT: i8 = 0;
/// Wire `match_type`: only the default (unnamed) entity matches (KIP-546 `DEFAULT`).
const MATCH_TYPE_DEFAULT: i8 = 1;
/// Wire `match_type`: any entity of the given type matches (KIP-546 `ANY`).
const MATCH_TYPE_ANY: i8 = 2;

/// The entity types a filter component may name.
const USER: &str = "user";
const CLIENT_ID: &str = "client-id";
const IP: &str = "ip";

/// A filter rejection: the top-level error code and message Kafka sends.
#[derive(Debug, PartialEq, Eq)]
struct FilterError {
    code: i16,
    message: String,
}

fn invalid(message: impl Into<String>) -> FilterError {
    FilterError {
        code: INVALID_REQUEST,
        message: message.into(),
    }
}

/// Checks the filter components as Kafka's `ClientQuotasImage.describe`
/// does, in the same order, before any match runs.
///
/// # Errors
///
/// Returns the first rule the filter breaks, with Kafka's error code and
/// message.
fn validate_filter(components: &[ComponentData]) -> Result<(), FilterError> {
    let mut seen: Vec<&str> = Vec::with_capacity(components.len());
    for comp in components {
        let entity_type = comp.entity_type.as_str();
        if entity_type.is_empty() {
            return Err(invalid("Invalid empty entity type."));
        }
        if seen.contains(&entity_type) {
            return Err(invalid(format!(
                "Entity type {entity_type} cannot appear more than once in the filter."
            )));
        }
        if ![IP, USER, CLIENT_ID].contains(&entity_type) {
            return Err(FilterError {
                code: UNSUPPORTED_VERSION,
                message: format!("Unsupported entity type {entity_type}"),
            });
        }
        match (comp.match_type, comp.match_.is_some()) {
            (MATCH_TYPE_EXACT, false) => {
                return Err(invalid(
                    "Request specified MATCH_TYPE_EXACT, but set match string to null.",
                ));
            }
            (MATCH_TYPE_DEFAULT, true) => {
                return Err(invalid(
                    "Request specified MATCH_TYPE_DEFAULT, but also specified a match string.",
                ));
            }
            (MATCH_TYPE_ANY, true) => {
                return Err(invalid(
                    "Request specified MATCH_TYPE_SPECIFIED, but also specified a match string.",
                ));
            }
            (MATCH_TYPE_EXACT | MATCH_TYPE_DEFAULT | MATCH_TYPE_ANY, _) => {}
            (other, _) => return Err(invalid(format!("Unknown match type {other}"))),
        }
        seen.push(entity_type);
    }
    if seen.contains(&IP) && (seen.contains(&USER) || seen.contains(&CLIENT_ID)) {
        return Err(invalid(
            "Invalid entity filter component combination. IP filter component should not be \
             used with user or clientId filter component.",
        ));
    }
    Ok(())
}

#[tracing::instrument(
    name = "handle_describe_client_quotas",
    level = "info",
    skip_all,
    fields(api = "DescribeClientQuotas"),
    err
)]
pub(crate) fn handle(
    broker: &Broker,
    req: DescribeClientQuotasRequest,
    ctx: &crate::handlers::RequestContext<'_>,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let image = broker.controller.current_image();
    // Kafka's `KafkaApis.handleDescribeClientQuotasRequest` authorizes
    // `DescribeConfigs` on the cluster. A denial goes through
    // `ApiError.fromThrowable`, which drops the default message, so the
    // response carries a null `error_message`.
    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Cluster,
            resource_name: CLUSTER_RESOURCE_NAME,
            operation: krabka_metadata::AclOperation::DescribeConfigs,
        },
    );
    if matches!(allow, AuthorizationResult::Deny) {
        let resp = DescribeClientQuotasResponse {
            throttle_time_ms: 0,
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: None,
            entries: None,
            ..Default::default()
        };
        return encode_response(&resp, api_version);
    }

    // Kafka's `ClientQuotasImage.describe` throws on a bad filter, and
    // `KafkaApis.handleError` answers with `entries = null`.
    if let Err(err) = validate_filter(&req.components) {
        let resp = DescribeClientQuotasResponse {
            throttle_time_ms: 0,
            error_code: err.code,
            error_message: Some(err.message),
            entries: None,
            ..Default::default()
        };
        return encode_response(&resp, api_version);
    }

    let mut entries: Vec<EntryData> = Vec::new();
    for (stored_key, configs) in image.client_quotas() {
        if !entity_matches_filter(stored_key, &req.components, req.strict) {
            continue;
        }
        entries.push(EntryData {
            entity: stored_key
                .iter()
                .map(|(t, n)| EntityData {
                    entity_type: t.clone(),
                    entity_name: n.clone(),
                    ..Default::default()
                })
                .collect(),
            values: configs
                .iter()
                .map(|(k, v)| ValueData {
                    key: k.clone(),
                    value: *v,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        });
    }

    let resp = DescribeClientQuotasResponse {
        throttle_time_ms: 0,
        error_code: NONE,
        error_message: None,
        entries: Some(entries),
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

pub(crate) fn entity_matches_filter(
    stored: &EntityKey,
    components: &[ComponentData],
    strict: bool,
) -> bool {
    if strict && stored.len() != components.len() {
        return false;
    }
    for comp in components {
        let Some(stored_entity) = stored.iter().find(|(t, _)| t == &comp.entity_type) else {
            return false;
        };
        let ok = match comp.match_type {
            MATCH_TYPE_EXACT => stored_entity.1.as_deref() == comp.match_.as_deref(),
            MATCH_TYPE_DEFAULT => stored_entity.1.is_none(),
            MATCH_TYPE_ANY => true,
            _ => false,
        };
        if !ok {
            return false;
        }
    }
    true
}

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    crate::handlers::encode_response_with_context(resp, api_version, "encode DescribeClientQuotas")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_metadata::{ClientQuotaRecord, MetadataRecord, QuotaEntity};

    use super::*;
    use crate::{
        broker::BrokerHandle,
        test_support::{DenyAll, peer, principal},
    };

    const VERSION: i16 = 1;

    fn comp(entity_type: &str, match_type: i8, m: Option<&str>) -> ComponentData {
        ComponentData {
            entity_type: entity_type.into(),
            match_type,
            match_: m.map(Into::into),
            ..Default::default()
        }
    }

    fn key(parts: Vec<(&str, Option<&str>)>) -> EntityKey {
        parts
            .into_iter()
            .map(|(t, n)| (t.into(), n.map(Into::into)))
            .collect()
    }

    fn request(components: Vec<ComponentData>, strict: bool) -> DescribeClientQuotasRequest {
        DescribeClientQuotasRequest {
            components,
            strict,
            ..Default::default()
        }
    }

    crate::test_support::response_helpers!(
        DescribeClientQuotasResponse,
        version = VERSION,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    async fn seed_quota(
        handle: &BrokerHandle,
        entity: Vec<(&str, Option<&str>)>,
        key: &str,
        value: f64,
    ) {
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(vec![MetadataRecord::V1ClientQuota(ClientQuotaRecord {
                entity: entity
                    .into_iter()
                    .map(|(entity_type, entity_name)| QuotaEntity {
                        entity_type: entity_type.into(),
                        entity_name: entity_name.map(Into::into),
                    })
                    .collect(),
                config_key: key.into(),
                config_value: Some(value),
            })])
            .await
            .expect("seed quota");
    }

    #[test]
    fn strict_exact_match_filters_correctly() {
        let stored = key(vec![("user", Some("alice"))]);
        let filter = vec![comp("user", MATCH_TYPE_EXACT, Some("alice"))];
        assert!(entity_matches_filter(&stored, &filter, true));
        assert!(!entity_matches_filter(&stored, &filter[..0], true)); // strict: type-count mismatch
    }

    #[test]
    fn non_strict_filter_returns_supersets() {
        // Stored has (user, client-id); filter only mentions user.
        let stored = key(vec![("client-id", Some("app1")), ("user", Some("alice"))]);
        let filter = vec![comp("user", MATCH_TYPE_EXACT, Some("alice"))];
        assert!(entity_matches_filter(&stored, &filter, false));
        assert!(!entity_matches_filter(&stored, &filter, true)); // strict rejects superset
    }

    #[test]
    fn default_match_type_filters_by_none_entity_name() {
        let stored_default = key(vec![("user", None)]);
        let stored_named = key(vec![("user", Some("alice"))]);
        let filter = vec![comp("user", MATCH_TYPE_DEFAULT, None)];
        assert!(entity_matches_filter(&stored_default, &filter, true));
        assert!(!entity_matches_filter(&stored_named, &filter, true));
    }

    #[test]
    fn any_match_type_returns_all_names_of_type() {
        let stored1 = key(vec![("user", Some("alice"))]);
        let stored2 = key(vec![("user", None)]);
        let filter = vec![comp("user", MATCH_TYPE_ANY, None)];
        assert!(entity_matches_filter(&stored1, &filter, true));
        assert!(entity_matches_filter(&stored2, &filter, true));
    }

    #[tokio::test]
    async fn denied_response_preserves_error_fields() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let bytes = handle(
            &broker,
            request(vec![comp("user", MATCH_TYPE_EXACT, Some("alice"))], true),
            &ctx,
            VERSION,
        )
        .expect("handle");
        let resp = decode_response(&bytes);

        let expected = DescribeClientQuotasResponse {
            throttle_time_ms: 0,
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: None,
            entries: None,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected, "{resp:?}");
        broker_handle.shutdown().await;
    }

    /// Kafka's `KafkaApis.handleDescribeClientQuotasRequest` authorizes
    /// `DescribeConfigs` on the cluster (#663). `AlterConfigs` and `All`
    /// imply it. `Describe`, and `Alter` (which implies `Describe`), do not.
    /// A denial carries the default message, which Kafka sends as null.
    #[tokio::test]
    async fn cluster_describe_configs_gates_the_quota_read() {
        let (broker_handle, _dir) = start_broker(Arc::new(
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new()),
        ))
        .await;
        seed_quota(
            &broker_handle,
            vec![("user", Some("alice"))],
            "producer_byte_rate",
            1024.0,
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let allowed = DescribeClientQuotasResponse {
            throttle_time_ms: 0,
            error_code: NONE,
            error_message: None,
            entries: Some(vec![EntryData {
                entity: vec![EntityData {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                    ..Default::default()
                }],
                values: vec![ValueData {
                    key: "producer_byte_rate".into(),
                    value: 1024.0,
                    ..Default::default()
                }],
                ..Default::default()
            }]),
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        let denied = DescribeClientQuotasResponse {
            throttle_time_ms: 0,
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: None,
            entries: None,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };

        for (user, grant, expected) in [
            ("no-grant", None, &denied),
            (
                "describe",
                Some(krabka_metadata::AclOperation::Describe),
                &denied,
            ),
            ("alter", Some(krabka_metadata::AclOperation::Alter), &denied),
            (
                "describe-configs",
                Some(krabka_metadata::AclOperation::DescribeConfigs),
                &allowed,
            ),
            (
                "alter-configs",
                Some(krabka_metadata::AclOperation::AlterConfigs),
                &allowed,
            ),
            ("all", Some(krabka_metadata::AclOperation::All), &allowed),
        ] {
            if let Some(operation) = grant {
                crate::test_support::grant_cluster_operation(&broker_handle, user, operation).await;
            }
            let p = principal(user);
            let peer = peer();
            let ctx = test_context(&p, &peer);

            let bytes = handle(
                &broker,
                request(vec![comp("user", MATCH_TYPE_ANY, None)], false),
                &ctx,
                VERSION,
            )
            .expect("handle");
            let resp = decode_response(&bytes);

            check!(resp == *expected, "user {user} with grant {grant:?}");
        }
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_returns_matching_quota_entry_fields() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_quota(
            &broker_handle,
            vec![("client-id", Some("app-1")), ("user", Some("alice"))],
            "producer_byte_rate",
            2048.0,
        )
        .await;
        seed_quota(
            &broker_handle,
            vec![("user", Some("bob"))],
            "consumer_byte_rate",
            512.0,
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let bytes = handle(
            &broker,
            request(vec![comp("user", MATCH_TYPE_EXACT, Some("alice"))], false),
            &ctx,
            VERSION,
        )
        .expect("handle");
        let resp = decode_response(&bytes);

        check!(resp.throttle_time_ms == 0, "{resp:?}");
        check!(resp.error_code == 0, "{resp:?}");
        check!(resp.error_message == None, "{resp:?}");
        let entries = resp.entries.expect("entries");
        assert!(entries.len() == 1, "{entries:?}");
        let entry = &entries[0];
        assert!(entry.entity.len() == 2, "{entry:?}");
        let by_type: std::collections::HashMap<_, _> = entry
            .entity
            .iter()
            .map(|e| (e.entity_type.as_str(), e.entity_name.as_deref()))
            .collect();
        check!(
            by_type.get("client-id") == Some(&Some("app-1")),
            "{entry:?}"
        );
        check!(by_type.get("user") == Some(&Some("alice")), "{entry:?}");
        check!(entry.values.len() == 1, "{entry:?}");
        check!(
            entry.values[0].key.as_str() == "producer_byte_rate",
            "{entry:?}"
        );
        check!(
            (entry.values[0].value - 2048.0).abs() < f64::EPSILON,
            "{entry:?}"
        );
        broker_handle.shutdown().await;
    }

    /// Kafka's `ClientQuotasImage.describe` rejects a bad filter before any
    /// match, with `entries = null` (#674). Valid filters still match.
    #[tokio::test]
    async fn invalid_filters_answer_kafkas_top_level_error() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_quota(
            &broker_handle,
            vec![("user", Some("alice"))],
            "producer_byte_rate",
            1024.0,
        )
        .await;
        seed_quota(
            &broker_handle,
            vec![("user", None)],
            "producer_byte_rate",
            2048.0,
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let error = |code: i16, message: &str| DescribeClientQuotasResponse {
            throttle_time_ms: 0,
            error_code: code,
            error_message: Some(message.into()),
            entries: None,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        let found = |name: Option<&str>, value: f64| DescribeClientQuotasResponse {
            throttle_time_ms: 0,
            error_code: NONE,
            error_message: None,
            entries: Some(vec![EntryData {
                entity: vec![EntityData {
                    entity_type: "user".into(),
                    entity_name: name.map(Into::into),
                    ..Default::default()
                }],
                values: vec![ValueData {
                    key: "producer_byte_rate".into(),
                    value,
                    ..Default::default()
                }],
                ..Default::default()
            }]),
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };

        let rows: Vec<(&str, Vec<ComponentData>, DescribeClientQuotasResponse)> = vec![
            (
                "empty entity type",
                vec![comp("", MATCH_TYPE_ANY, None)],
                error(INVALID_REQUEST, "Invalid empty entity type."),
            ),
            (
                "repeated entity type",
                vec![
                    comp("user", MATCH_TYPE_EXACT, Some("alice")),
                    comp("user", MATCH_TYPE_ANY, None),
                ],
                error(
                    INVALID_REQUEST,
                    "Entity type user cannot appear more than once in the filter.",
                ),
            ),
            (
                "unknown entity type",
                vec![comp("group", MATCH_TYPE_ANY, None)],
                error(UNSUPPORTED_VERSION, "Unsupported entity type group"),
            ),
            (
                "exact with null match",
                vec![comp("user", MATCH_TYPE_EXACT, None)],
                error(
                    INVALID_REQUEST,
                    "Request specified MATCH_TYPE_EXACT, but set match string to null.",
                ),
            ),
            (
                "default with a match string",
                vec![comp("user", MATCH_TYPE_DEFAULT, Some("alice"))],
                error(
                    INVALID_REQUEST,
                    "Request specified MATCH_TYPE_DEFAULT, but also specified a match string.",
                ),
            ),
            (
                "specified with a match string",
                vec![comp("user", MATCH_TYPE_ANY, Some("alice"))],
                error(
                    INVALID_REQUEST,
                    "Request specified MATCH_TYPE_SPECIFIED, but also specified a match string.",
                ),
            ),
            (
                "unknown match type",
                vec![comp("user", 7, None)],
                error(INVALID_REQUEST, "Unknown match type 7"),
            ),
            (
                "ip with user",
                vec![
                    comp("ip", MATCH_TYPE_ANY, None),
                    comp("user", MATCH_TYPE_ANY, None),
                ],
                error(
                    INVALID_REQUEST,
                    "Invalid entity filter component combination. IP filter component should \
                     not be used with user or clientId filter component.",
                ),
            ),
            (
                "client-id with ip",
                vec![
                    comp("client-id", MATCH_TYPE_DEFAULT, None),
                    comp("ip", MATCH_TYPE_EXACT, Some("1.2.3.4")),
                ],
                error(
                    INVALID_REQUEST,
                    "Invalid entity filter component combination. IP filter component should \
                     not be used with user or clientId filter component.",
                ),
            ),
            (
                "valid exact",
                vec![comp("user", MATCH_TYPE_EXACT, Some("alice"))],
                found(Some("alice"), 1024.0),
            ),
            (
                "valid default",
                vec![comp("user", MATCH_TYPE_DEFAULT, None)],
                found(None, 2048.0),
            ),
        ];
        for (name, components, expected) in rows {
            let bytes = handle(&broker, request(components, true), &ctx, VERSION).expect("handle");
            let resp = decode_response(&bytes);
            check!(resp == expected, "row {name}");
        }

        // A valid SPECIFIED filter matches every user entity, named or default.
        let bytes = handle(
            &broker,
            request(vec![comp("user", MATCH_TYPE_ANY, None)], true),
            &ctx,
            VERSION,
        )
        .expect("handle");
        let resp = decode_response(&bytes);
        let mut names: Vec<Option<String>> = resp
            .entries
            .expect("entries")
            .into_iter()
            .flat_map(|e| e.entity.into_iter().map(|d| d.entity_name))
            .collect();
        names.sort();
        check!(names == vec![None, Some("alice".to_owned())]);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn successful_empty_match_uses_some_empty_entries() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let bytes = handle(
            &broker,
            request(vec![comp("user", MATCH_TYPE_EXACT, Some("missing"))], true),
            &ctx,
            VERSION,
        )
        .expect("handle");
        let resp = decode_response(&bytes);

        let expected = DescribeClientQuotasResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            entries: Some(Vec::new()),
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
