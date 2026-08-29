//! End-to-end tests for the `AlterClientQuotas` handler: the cluster
//! authorization preamble, the per-entry results a mixed request returns, and
//! the encoded response body the handler writes.
//!
//! Most of them drive a live broker, so they are kept out of the module root.

use std::{net::SocketAddr, sync::Arc};

use assert2::assert;
use krabka_protocol::owned::{
    alter_client_quotas_request::EntityData,
    alter_client_quotas_response::{EntityData as RespEntity, EntryData as RespEntry},
};
use krabka_security::{AuthMethod, Principal};

use super::{
    test_support::{entry, request},
    *,
};
use crate::{
    broker::BrokerHandle,
    codes::{INVALID_CONFIG, INVALID_REQUEST},
    test_support::{DenyAll, start_broker_with_authorizer as start_broker},
};

crate::test_support::response_helpers!(AlterClientQuotasResponse, client_id = "admin-client");

fn quota_value(handle: &BrokerHandle, user: &str, quota_key: &str) -> Option<f64> {
    let key: krabka_metadata::EntityKey = vec![("user".into(), Some(user.into()))];
    handle
        .controller_image_for_test()
        .client_quotas()
        .get(&key)
        .and_then(|configs| configs.get(quota_key).copied())
}

#[test]
fn whole_request_error_encodes_all_entries() {
    let version = 1;
    let req = request(
        vec![
            entry(vec![("user", Some("alice"))], vec![]),
            entry(vec![("client-id", Some("app"))], vec![]),
        ],
        false,
    );

    let bytes = encode_whole_request_error(&req, CLUSTER_AUTHORIZATION_FAILED, "denied", version)
        .expect("encode");
    let resp = decode_response(&bytes, version);

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

#[test]
fn encode_response_writes_decodable_body() {
    let version = 1;
    let resp = AlterClientQuotasResponse {
        throttle_time_ms: 123,
        entries: vec![err_entry(
            &[EntityData {
                entity_type: "user".into(),
                entity_name: Some("alice".into()),
                ..Default::default()
            }],
            INVALID_REQUEST,
            "bad request".into(),
        )],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };

    let bytes = encode_response(&resp, version).expect("encode");
    let decoded = decode_response(&bytes, version);

    let expected = AlterClientQuotasResponse {
        throttle_time_ms: 123,
        entries: vec![RespEntry {
            error_code: INVALID_REQUEST,
            error_message: Some("bad request".into()),
            entity: vec![RespEntity {
                entity_type: "user".into(),
                entity_name: Some("alice".into()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(decoded == expected);
}

#[tokio::test]
async fn handle_denies_cluster_alter_for_each_entry() {
    let version = 1;
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = Principal {
        name: "alice".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    };
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = test_context(&principal, &peer);
    let req = request(
        vec![entry(
            vec![("user", Some("alice"))],
            vec![("producer_byte_rate", 1024.0, false)],
        )],
        false,
    );

    let resp = handle(&broker, req, &ctx, version).await.expect("handle");
    let resp = decode_response(&resp, version);

    let expected = AlterClientQuotasResponse {
        throttle_time_ms: 0,
        entries: vec![RespEntry {
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("alter-client-quotas denied".into()),
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

#[tokio::test]
async fn handle_returns_entry_results_and_submits_valid_changes() {
    let version = 1;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = Principal {
        name: "admin".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    };
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = test_context(&principal, &peer);
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

    let resp = handle(&broker, req, &ctx, version).await.expect("handle");
    let resp = decode_response(&resp, version);

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
                error_code: INVALID_CONFIG,
                error_message: Some("unknown quota key \"unknown_quota_key\"".into()),
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
    let principal = Principal {
        name: "admin".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    };
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = test_context(&principal, &peer);
    let req = request(
        vec![entry(
            vec![("user", Some("carol"))],
            vec![("producer_byte_rate", 2048.0, false)],
        )],
        true,
    );

    let resp = handle(&broker, req, &ctx, version).await.expect("handle");
    let resp = decode_response(&resp, version);

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
