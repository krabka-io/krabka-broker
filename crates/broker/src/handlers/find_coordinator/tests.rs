//! Live-broker tests for the `FindCoordinator` handler.
//!
//! These drive `handle` against a running broker, which is what covers the
//! bootstrap-then-resolve path for the `__transaction_state` topic: the
//! configured partition count must shape the topic the handler creates and the
//! partition it routes a transactional id to.

use assert2::assert;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::owned::find_coordinator_response::FindCoordinatorResponse;

use super::*;
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
    test_support::{DenyAll, peer, principal, start_broker_with},
};

const KAFKA_TOPIC_ID: &str = "BQUFBQUFBQUFBQUFBQUFBQ";

#[derive(Debug)]
struct DenyOneTransaction;

impl Authorizer for DenyOneTransaction {
    fn authorize(
        &self,
        _source: &dyn krabka_authz::AclSource,
        request: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        if request.resource_type == ResourceType::TransactionalId
            && request.operation == AclOperation::Describe
            && request.resource_name == "denied"
        {
            AuthorizationResult::Deny
        } else {
            AuthorizationResult::Allow
        }
    }
}

#[tokio::test]
async fn configured_partition_count_controls_txn_topic_and_routing() {
    let (broker_handle, _dir) = start_broker_with(|config| {
        config.audit_enabled = false;
        config.transaction_state_num_partitions = 7;
        config.transaction_state_replication_factor = 1;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal("admin");
    let peer = peer();
    let context = crate::test_support::request_context(&principal, &peer, "admin-client");
    let version = krabka_protocol::owned::find_coordinator_response::MAX_VERSION;
    let tid = "my-tid"; // hashes to partition 43 with the old fixed count of 50
    let request = FindCoordinatorRequest {
        key_type: KEY_TYPE_TRANSACTION,
        coordinator_keys: vec![tid.to_string()],
        ..Default::default()
    };

    let response = handle(
        &broker,
        version,
        1,
        &crate::test_support::encode_request(&request, version),
        &context,
    )
    .await
    .expect("find transaction coordinator");
    let response: FindCoordinatorResponse =
        crate::test_support::decode_response(&response, version);

    let image = broker_handle.controller_image_for_test();
    let topic = image
        .topic(crate::txn::bootstrap::TOPIC)
        .expect("transaction-state topic");
    assert!(topic.partitions == 7);
    assert!(topic.replication_factor == 1);
    assert!(image.partitions_of(crate::txn::bootstrap::TOPIC).count() == 7);
    assert!(response.coordinators.len() == 1);
    assert!(response.coordinators[0].error_code == codes::NONE);
    assert!(response.coordinators[0].node_id == broker.config.broker_id);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn denied_share_key_does_not_bootstrap_or_expose_a_coordinator() {
    let (broker_handle, _dir) = start_broker_with(|config| {
        config.audit_enabled = false;
        config.authorizer = std::sync::Arc::new(DenyAll);
        config.share_coordinator.state_topic_replication_factor = 1;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal("alice");
    let peer = peer();
    let context = crate::test_support::request_context(&principal, &peer, "share-client");
    let version = krabka_protocol::owned::find_coordinator_response::MAX_VERSION;
    let key = format!("share-group:{KAFKA_TOPIC_ID}:0");
    let request = FindCoordinatorRequest {
        key_type: KEY_TYPE_SHARE,
        coordinator_keys: vec![key],
        ..Default::default()
    };

    let response = handle(
        &broker,
        version,
        1,
        &crate::test_support::encode_request(&request, version),
        &context,
    )
    .await
    .expect("deny share coordinator lookup");
    let response: FindCoordinatorResponse =
        crate::test_support::decode_response(&response, version);

    assert!(response.coordinators.len() == 1);
    let row = &response.coordinators[0];
    assert!(row.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
    assert!(row.node_id == -1);
    assert!(row.host.is_empty());
    assert!(row.port == -1);
    assert!(
        broker_handle
            .controller_image_for_test()
            .topic(crate::share_coordinator::bootstrap::TOPIC)
            .is_none()
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn share_key_type_before_v6_is_invalid_without_bootstrap() {
    let (broker_handle, _dir) = start_broker_with(|config| config.audit_enabled = false).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal("alice");
    let peer = peer();
    let context = crate::test_support::request_context(&principal, &peer, "share-client");
    let version = 5;
    let request = FindCoordinatorRequest {
        key_type: KEY_TYPE_SHARE,
        coordinator_keys: vec![format!("share-group:{KAFKA_TOPIC_ID}:0")],
        ..Default::default()
    };

    let response = handle(
        &broker,
        version,
        4,
        &crate::test_support::encode_request(&request, version),
        &context,
    )
    .await
    .expect("reject pre-v6 share coordinator lookup");
    let response: FindCoordinatorResponse =
        crate::test_support::decode_response(&response, version);

    assert!(response.coordinators.len() == 1);
    assert!(response.coordinators[0].error_code == codes::INVALID_REQUEST);
    assert!(
        broker_handle
            .controller_image_for_test()
            .topic(crate::share_coordinator::bootstrap::TOPIC)
            .is_none()
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn v4_empty_key_array_stays_empty_without_bootstrap() {
    let (broker_handle, _dir) = start_broker_with(|config| config.audit_enabled = false).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal("alice");
    let peer = peer();
    let context = crate::test_support::request_context(&principal, &peer, "txn-client");
    let version = 4;
    let request = FindCoordinatorRequest {
        key_type: KEY_TYPE_TRANSACTION,
        coordinator_keys: vec![],
        ..Default::default()
    };

    let response = handle(
        &broker,
        version,
        5,
        &crate::test_support::encode_request(&request, version),
        &context,
    )
    .await
    .expect("empty batched coordinator lookup");
    let response: FindCoordinatorResponse =
        crate::test_support::decode_response(&response, version);

    assert!(response.coordinators.is_empty());
    assert!(
        broker_handle
            .controller_image_for_test()
            .topic(crate::txn::bootstrap::TOPIC)
            .is_none()
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn mixed_rejection_and_resolution_preserve_key_order_and_errors() {
    let (broker_handle, _dir) = start_broker_with(|config| {
        config.audit_enabled = false;
        config.authorizer = std::sync::Arc::new(DenyOneTransaction);
        config.transaction_state_replication_factor = 1;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal("alice");
    let peer = peer();
    let context = crate::test_support::request_context(&principal, &peer, "txn-client");
    let version = krabka_protocol::owned::find_coordinator_response::MAX_VERSION;
    let request = FindCoordinatorRequest {
        key_type: KEY_TYPE_TRANSACTION,
        coordinator_keys: vec![
            "allowed-first".into(),
            "denied".into(),
            "allowed-last".into(),
        ],
        ..Default::default()
    };

    let response = handle(
        &broker,
        version,
        6,
        &crate::test_support::encode_request(&request, version),
        &context,
    )
    .await
    .expect("mixed coordinator lookup");
    let response: FindCoordinatorResponse =
        crate::test_support::decode_response(&response, version);

    assert!(
        response
            .coordinators
            .iter()
            .map(|row| (row.key.as_str(), row.error_code))
            .collect::<Vec<_>>()
            == vec![
                ("allowed-first", codes::NONE),
                ("denied", codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
                ("allowed-last", codes::NONE),
            ]
    );
    broker_handle.shutdown().await;
}

#[test]
fn bootstrap_failure_is_shaped_per_admitted_key_without_losing_rejections() {
    let slots = vec![
        KeySlot::Resolve("allowed-first".into()),
        KeySlot::Rejected(Coordinator {
            key: "denied".into(),
            error_code: codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
            node_id: -1,
            host: String::new(),
            port: -1,
            ..Default::default()
        }),
        KeySlot::Resolve("allowed-last".into()),
    ];
    let unavailable = unavailable_for_keys(
        vec!["allowed-first".into(), "allowed-last".into()],
        "topic bootstrap failed",
    );
    let response = merge_key_slots(slots, unavailable);

    assert!(
        response
            .iter()
            .map(|row| (row.key.as_str(), row.error_code))
            .collect::<Vec<_>>()
            == vec![
                ("allowed-first", codes::COORDINATOR_NOT_AVAILABLE),
                ("denied", codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
                ("allowed-last", codes::COORDINATOR_NOT_AVAILABLE),
            ]
    );
    assert!(response[0].node_id == -1);
    assert!(response[2].node_id == -1);
}

#[tokio::test]
async fn unknown_key_type_is_invalid_and_exposes_no_endpoint() {
    let (broker_handle, _dir) = start_broker_with(|config| config.audit_enabled = false).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal("alice");
    let peer = peer();
    let context = crate::test_support::request_context(&principal, &peer, "unknown-client");
    let version = krabka_protocol::owned::find_coordinator_response::MAX_VERSION;
    let request = FindCoordinatorRequest {
        key_type: i8::MAX,
        coordinator_keys: vec!["not-a-coordinator-kind".into()],
        ..Default::default()
    };

    let response = handle(
        &broker,
        version,
        2,
        &crate::test_support::encode_request(&request, version),
        &context,
    )
    .await
    .expect("reject unknown coordinator key type");
    let response: FindCoordinatorResponse =
        crate::test_support::decode_response(&response, version);

    assert!(response.coordinators.len() == 1);
    let row = &response.coordinators[0];
    assert!(row.error_code == codes::INVALID_REQUEST);
    assert!(row.node_id == -1);
    assert!(row.host.is_empty());
    assert!(row.port == -1);
    let image = broker_handle.controller_image_for_test();
    assert!(image.topic(crate::txn::bootstrap::TOPIC).is_none());
    assert!(
        image
            .topic(crate::share_coordinator::bootstrap::TOPIC)
            .is_none()
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn malformed_share_key_is_invalid_without_bootstrap() {
    let (broker_handle, _dir) = start_broker_with(|config| {
        config.audit_enabled = false;
        config.share_coordinator.state_topic_replication_factor = 1;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal("alice");
    let peer = peer();
    let context = crate::test_support::request_context(&principal, &peer, "share-client");
    let version = krabka_protocol::owned::find_coordinator_response::MAX_VERSION;
    let request = FindCoordinatorRequest {
        key_type: KEY_TYPE_SHARE,
        coordinator_keys: vec!["malformed".into()],
        ..Default::default()
    };

    let response = handle(
        &broker,
        version,
        3,
        &crate::test_support::encode_request(&request, version),
        &context,
    )
    .await
    .expect("reject malformed share coordinator key");
    let response: FindCoordinatorResponse =
        crate::test_support::decode_response(&response, version);

    assert!(response.coordinators.len() == 1);
    assert!(response.coordinators[0].error_code == codes::INVALID_REQUEST);
    assert!(
        broker_handle
            .controller_image_for_test()
            .topic(crate::share_coordinator::bootstrap::TOPIC)
            .is_none()
    );
    broker_handle.shutdown().await;
}
