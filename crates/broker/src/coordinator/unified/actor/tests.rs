//! Unit tests for the actor loop itself: what a mailbox failure does to the
//! task, and the one-actor-per-group guarantee the handle registry gives.

use std::sync::Arc;

use assert2::assert;
use krabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;

use super::{
    GroupActorMessage,
    test_support::{await_until, make_coordinator},
};
use crate::codes;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_exits_on_append_error() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_consumer("g");
    log.fail_next
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: String::new(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            client_id: "client-a".into(),
            client_host: String::new(),
            reply: tx,
        })
        .await;
    let resp = rx.await.unwrap();
    assert!(resp.error_code == codes::COORDINATOR_LOAD_IN_PROGRESS);

    // Wait for the actor to drain and drop its receiver.
    await_until("actor mpsc closed after exit", || handle.tx.is_closed()).await;
    assert!(
        handle.tx.is_closed(),
        "actor mpsc should be closed after exit"
    );

    // get_or_create should respawn a fresh actor.
    let fresh = coord.get_or_create_consumer("g");
    assert!(!fresh.tx.is_closed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_protocol_get_or_create_returns_the_one_actor() {
    // KIP-848 live migration: the registry no longer pins a group to its
    // spawn kind. Both getters return the SAME actor for an id; the per-group
    // kind lock now lives in the actor's message arms, not the registry.
    let (coord, _log) = make_coordinator();
    // Consumer owns "c" → a classic get-or-create returns that same actor.
    let c_consumer = coord.get_or_create_consumer("c");
    let c_classic = coord.get_or_create_classic("c");
    assert!(Arc::ptr_eq(&c_consumer, &c_classic));
    // Classic owns "k" → a consumer get-or-create returns that same actor.
    let k_classic = coord.get_or_create_classic("k");
    let k_consumer = coord.get_or_create_consumer("k");
    assert!(Arc::ptr_eq(&k_classic, &k_consumer));
}
