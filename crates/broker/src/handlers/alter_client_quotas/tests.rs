//! End-to-end tests for the `AlterClientQuotas` handler: the cluster
//! authorization preamble, the per-entry results a mixed request returns, and
//! the response the handler returns.
//!
//! Most of them drive a live broker, so they are kept out of the module root.

use std::sync::Arc;

use assert2::assert;
use krabka_protocol::owned::alter_client_quotas_response::{
    EntityData as RespEntity, EntryData as RespEntry,
};

use super::{
    test_support::{entry, request},
    *,
};
use crate::{
    broker::BrokerHandle,
    codes::INVALID_REQUEST,
    test_support::{DenyAll, peer, start_broker_with_authorizer as start_broker, test_ctx},
};

crate::test_support::context_helper!(client_id = "admin-client");

fn quota_value(handle: &BrokerHandle, user: &str, quota_key: &str) -> Option<f64> {
    let key: krabka_metadata::EntityKey = vec![("user".into(), Some(user.into()))];
    handle
        .controller_image_for_test()
        .client_quotas()
        .get(&key)
        .and_then(|configs| configs.get(quota_key).copied())
}

#[test]
fn whole_request_error_answers_every_entry() {
    let req = request(
        vec![
            entry(vec![("user", Some("alice"))], vec![]),
            entry(vec![("client-id", Some("app"))], vec![]),
        ],
        false,
    );

    let resp = whole_request_error(&req, CLUSTER_AUTHORIZATION_FAILED, "denied");

    let expected = AlterClientQuotasResponse {
        throttle_time_ms: 0,
        entries: vec![
            RespEntry {
                error_code: CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some("denied".into()),
                entity: vec![RespEntity {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            RespEntry {
                error_code: CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some("denied".into()),
                entity: vec![RespEntity {
                    entity_type: "client-id".into(),
                    entity_name: Some("app".into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        ],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
}

#[tokio::test]
async fn handle_denies_cluster_alter_for_each_entry() {
    let version = 1;
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    test_ctx!(ctx, "alice");
    let req = request(
        vec![entry(
            vec![("user", Some("alice"))],
            vec![("producer_byte_rate", 1024.0, false)],
        )],
        false,
    );

    let resp = handle(&broker, req, version, &ctx).await.expect("handle");

    let expected = AlterClientQuotasResponse {
        throttle_time_ms: 0,
        entries: vec![RespEntry {
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("Cluster authorization failed.".into()),
            entity: vec![RespEntity {
                entity_type: "user".into(),
                entity_name: Some("alice".into()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    assert!(quota_value(&broker_handle, "alice", "producer_byte_rate") == None);
    broker_handle.shutdown().await;
}

/// Kafka's `ControllerApis.handleAlterClientQuotas` authorizes `AlterConfigs`
/// on the cluster (#664). `All` implies it. `Alter` does not, so a principal
/// that can reassign partitions cannot change quotas.
#[tokio::test]
async fn cluster_alter_configs_gates_the_quota_write() {
    let version = 1;
    let (broker_handle, _dir) = start_broker(Arc::new(
        crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new()),
    ))
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let peer = peer();

    for (user, grant, allowed) in [
        ("no-grant", None, false),
        ("alter", Some(AclOperation::Alter), false),
        (
            "describe-configs",
            Some(AclOperation::DescribeConfigs),
            false,
        ),
        ("alter-configs", Some(AclOperation::AlterConfigs), true),
        ("all", Some(AclOperation::All), true),
    ] {
        if let Some(operation) = grant {
            crate::test_support::grant_cluster_operation(&broker_handle, user, operation).await;
        }
        let principal = crate::test_support::principal(user);
        let ctx = test_context(&principal, &peer);
        let req = request(
            vec![entry(
                vec![("user", Some(user))],
                vec![("producer_byte_rate", 1024.0, false)],
            )],
            false,
        );

        let resp = handle(&broker, req, version, &ctx).await.expect("handle");

        let (error_code, error_message) = if allowed {
            (0, None)
        } else {
            (
                CLUSTER_AUTHORIZATION_FAILED,
                Some("Cluster authorization failed.".to_string()),
            )
        };
        let expected = AlterClientQuotasResponse {
            throttle_time_ms: 0,
            entries: vec![RespEntry {
                error_code,
                error_message,
                entity: vec![RespEntity {
                    entity_type: "user".into(),
                    entity_name: Some(user.into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert2::check!(resp == expected, "user {user} with grant {grant:?}");
        let stored = allowed.then_some(1024.0);
        assert2::check!(
            quota_value(&broker_handle, user, "producer_byte_rate") == stored,
            "user {user} with grant {grant:?}"
        );
    }
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_returns_entry_results_and_submits_valid_changes() {
    let version = 1;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    test_ctx!(ctx, "admin");
    let req = request(
        vec![
            entry(
                vec![("user", Some("alice"))],
                vec![("producer_byte_rate", 1024.0, false)],
            ),
            entry(
                vec![("user", Some("bob"))],
                vec![("unknown_quota_key", 1.0, false)],
            ),
        ],
        false,
    );

    let resp = handle(&broker, req, version, &ctx).await.expect("handle");

    let expected = AlterClientQuotasResponse {
        throttle_time_ms: 0,
        entries: vec![
            RespEntry {
                error_code: 0,
                error_message: None,
                entity: vec![RespEntity {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            RespEntry {
                error_code: INVALID_REQUEST,
                error_message: Some("Invalid configuration key unknown_quota_key".into()),
                entity: vec![RespEntity {
                    entity_type: "user".into(),
                    entity_name: Some("bob".into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        ],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    for (user, quota_key, want) in [
        ("alice", "producer_byte_rate", Some(1024.0)),
        ("bob", "unknown_quota_key", None),
    ] {
        assert!(
            quota_value(&broker_handle, user, quota_key) == want,
            "user {user}"
        );
    }
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_validate_only_reports_success_without_submitting() {
    let version = 1;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    test_ctx!(ctx, "admin");
    let req = request(
        vec![entry(
            vec![("user", Some("carol"))],
            vec![("producer_byte_rate", 2048.0, false)],
        )],
        true,
    );

    let resp = handle(&broker, req, version, &ctx).await.expect("handle");

    let expected = AlterClientQuotasResponse {
        throttle_time_ms: 0,
        entries: vec![RespEntry {
            error_code: 0,
            error_message: None,
            entity: vec![RespEntity {
                entity_type: "user".into(),
                entity_name: Some("carol".into()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    assert!(quota_value(&broker_handle, "carol", "producer_byte_rate") == None);
    broker_handle.shutdown().await;
}

/// Kafka keys the response by entity (#675): a repeated entity answers one
/// row with "Ignoring duplicate entity", and the first entry's change is
/// still written. `validate_only` gives the same rows and writes nothing.
#[tokio::test]
async fn repeated_entity_answers_one_row_with_and_without_validate_only() {
    let version = 1;
    test_ctx!(ctx, "admin");
    let row = |name: &str, code: i16, message: Option<&str>| RespEntry {
        error_code: code,
        error_message: message.map(Into::into),
        entity: vec![RespEntity {
            entity_type: "user".into(),
            entity_name: Some(name.into()),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let expected = AlterClientQuotasResponse {
        throttle_time_ms: 0,
        entries: vec![
            row(
                "dave",
                INVALID_REQUEST,
                Some("Ignoring duplicate entity ClientQuotaEntity(entries={user=dave})"),
            ),
            row("erin", 0, None),
        ],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };

    for (validate_only, stored) in [(true, [None, None]), (false, [Some(1024.0), Some(4.0)])] {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let req = request(
            vec![
                entry(
                    vec![("user", Some("dave"))],
                    vec![("producer_byte_rate", 1024.0, false)],
                ),
                entry(
                    vec![("user", Some("erin"))],
                    vec![("request_percentage", 4.0, false)],
                ),
                entry(
                    vec![("user", Some("dave"))],
                    vec![("consumer_byte_rate", 2048.0, false)],
                ),
            ],
            validate_only,
        );

        let resp = handle(&broker, req, version, &ctx).await.expect("handle");

        assert2::check!(resp == expected, "validate_only {validate_only}");
        assert2::check!(
            [
                quota_value(&broker_handle, "dave", "producer_byte_rate"),
                quota_value(&broker_handle, "erin", "request_percentage"),
            ] == stored,
            "validate_only {validate_only}"
        );
        assert2::check!(
            quota_value(&broker_handle, "dave", "consumer_byte_rate") == None,
            "validate_only {validate_only}"
        );
        broker_handle.shutdown().await;
    }
}
