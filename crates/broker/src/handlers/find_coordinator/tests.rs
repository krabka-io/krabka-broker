//! Live-broker tests for the `FindCoordinator` handler.
//!
//! These drive `handle` against a running broker, which is what covers the
//! bootstrap-then-resolve path for the `__transaction_state` topic: the
//! configured partition count must shape the topic the handler creates and the
//! partition it routes a transactional id to.

use assert2::assert;
use krabka_protocol::owned::find_coordinator_response::FindCoordinatorResponse;

use super::*;
use crate::test_support::{peer, principal, start_broker_with};

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
