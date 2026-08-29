//! `InitProducerId` with and without a transactional id.
//!
//! A plain call hands out a pooled producer id, and a transactional one needs
//! the `__transaction_state` topic to exist first, so `FindCoordinator` with
//! `key_type` 1 bootstraps it. A repeated transactional id bumps the epoch.

use assert2::{assert, check};
use krabka_protocol::owned::{
    find_coordinator_request::FindCoordinatorRequest,
    init_producer_id_request::InitProducerIdRequest,
};

use crate::support;

#[tokio::test]
async fn init_producer_id_returns_fresh_pid() {
    let p = support::start().await;
    let r = p
        .client
        .send(InitProducerIdRequest::default())
        .await
        .expect("InitProducerId");
    check!(r.error_code == 0);
    check!(r.producer_id == 0);
    check!(r.producer_epoch == 0);
    p.broker.shutdown().await;
}

#[tokio::test]
async fn init_producer_id_without_coordinator_bootstrap_returns_not_coordinator() {
    // Without a prior FindCoordinator(TRANSACTION) call, the broker has not
    // yet refreshed its leader_partitions set for __transaction_state, so it
    // cannot confirm it is the coordinator and returns NOT_COORDINATOR (16).
    let p = support::start().await;
    let r = p
        .client
        .send(InitProducerIdRequest {
            transactional_id: Some("tx-1".into()),
            ..Default::default()
        })
        .await
        .expect("InitProducerId");
    assert!(r.error_code == 16); // NOT_COORDINATOR
    p.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn find_coordinator_txn_creates_topic_and_returns_local_broker() {
    let p = support::start().await; // single-voter broker
    // Use coordinator_keys (v4+ style) so the transaction-id reaches the
    // broker on the wire. key_type=1 selects the TRANSACTION branch.
    let r = p
        .client
        .send(FindCoordinatorRequest {
            coordinator_keys: vec!["my-tid".into()],
            key_type: 1, // TRANSACTION
            ..Default::default()
        })
        .await
        .expect("FindCoordinator(TRANSACTION)");
    // The broker bootstraps __transaction_state on demand, resolves the
    // partition leader, and returns itself (the only broker in the cluster).
    assert!(r.error_code == 0, "top-level error_code");
    assert!(r.coordinators.len() == 1, "one coordinator entry");
    let c = &r.coordinators[0];
    check!(c.error_code == 0, "coordinator error_code");
    check!(c.node_id == 1, "node_id should be this single broker");
    check!(!c.host.is_empty(), "host should be non-empty");
    check!(c.port > 0, "port should be positive");
    p.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_producer_id_with_transactional_id_returns_real_pid() {
    let p = support::start().await;
    // Bootstrap __transaction_state via FindCoordinator (key_type=1).
    // Use coordinator_keys (v4+ wire format) so the transaction-id reaches
    // the broker and triggers topic creation + leader registration.
    let _ = p
        .client
        .send(FindCoordinatorRequest {
            coordinator_keys: vec!["my-tid".into()],
            key_type: 1, // TRANSACTION
            ..Default::default()
        })
        .await
        .expect("FindCoordinator");

    let r = p
        .client
        .send(InitProducerIdRequest {
            transactional_id: Some("my-tid".into()),
            transaction_timeout_ms: 60_000,
            ..Default::default()
        })
        .await
        .expect("InitProducerId");
    check!(r.error_code == 0, "error_code should be NONE");
    check!(
        r.producer_id >= 0,
        "producer_id should come from txn coordinator's pool"
    );
    check!(r.producer_epoch == 0, "first allocation → epoch 0");
    p.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_producer_id_with_same_tid_bumps_epoch() {
    let p = support::start().await;
    // Bootstrap __transaction_state for stable-tid.
    let _ = p
        .client
        .send(FindCoordinatorRequest {
            coordinator_keys: vec!["stable-tid".into()],
            key_type: 1, // TRANSACTION
            ..Default::default()
        })
        .await
        .expect("FindCoordinator");

    let r1 = p
        .client
        .send(InitProducerIdRequest {
            transactional_id: Some("stable-tid".into()),
            transaction_timeout_ms: 60_000,
            ..Default::default()
        })
        .await
        .expect("InitProducerId 1");
    assert!(r1.error_code == 0, "r1 error_code");

    let r2 = p
        .client
        .send(InitProducerIdRequest {
            transactional_id: Some("stable-tid".into()),
            transaction_timeout_ms: 60_000,
            ..Default::default()
        })
        .await
        .expect("InitProducerId 2");
    check!(r2.error_code == 0, "r2 error_code");
    check!(r1.producer_id == r2.producer_id, "same pid for same tid");
    check!(
        r2.producer_epoch == r1.producer_epoch + 1,
        "second call bumps epoch by 1"
    );
    p.broker.shutdown().await;
}
