//! Tests for one liveness tick: the discovery step that starts a session for
//! a registered broker that never heartbeated, the follower that must track
//! nothing, and the first tick of a new term that seeds before it sweeps.

use assert2::assert;
use krabka_metadata::{LeaderEpoch, PartitionRecord};

use super::*;
use crate::{
    heartbeat::controller_state::TestClock,
    leader_election::test_support::{
        fake_source, img_with_partition, one_partition_change, recovery_handle_for_tests,
        register_brokers,
    },
};

#[tokio::test]
async fn tick_discovers_registered_broker_that_never_heartbeated_and_fails_it_over() {
    // Broker 1 leads t-0 and dies before its first heartbeat reaches this
    // controller. Brokers 2 and 3 heartbeat as usual.
    let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
    register_brokers(&mut img, &[1, 2, 3]);
    let source = fake_source(img, Some(NodeId(2)));
    let controller: Arc<dyn crate::metadata_source::MetadataSource> = source.clone();
    let clock = TestClock::new();
    let liveness = Arc::new(ControllerLivenessState::with_test_clock(
        std::time::Duration::from_millis(10),
        &clock,
    ));
    let metrics = crate::metrics::BrokerMetrics::new();
    let recovery = recovery_handle_for_tests();
    // This node has led for a while, so the tick does not seed a new
    // term: broker 1 is found by discovery alone.
    let mut state = LivenessTickState {
        was_leader: true,
        ..LivenessTickState::default()
    };
    liveness.record_heartbeat(2).await;
    liveness.record_heartbeat(3).await;

    // First tick: discovery starts broker 1's session, fenced until it
    // proves catch-up. Nothing expires and nothing is submitted.
    run_liveness_tick(
        &controller,
        NodeId(2),
        &liveness,
        &metrics,
        &recovery,
        &mut state,
    )
    .await;
    assert!(!liveness.is_alive(1).await);
    assert!(liveness.unavailable_snapshot().await.contains(&1));
    assert!(liveness.dead_snapshot().await.is_empty());
    assert!(source.submitted().is_empty());

    // One full window later brokers 2 and 3 heartbeated again. Broker 1
    // did not. The tick expires it and fails t-0 over to broker 2.
    clock.advance(std::time::Duration::from_millis(11));
    liveness.record_heartbeat(2).await;
    liveness.record_heartbeat(3).await;
    run_liveness_tick(
        &controller,
        NodeId(2),
        &liveness,
        &metrics,
        &recovery,
        &mut state,
    )
    .await;

    let batches = source.submitted();
    assert!(batches.len() == 1, "the edge submits once, got {batches:?}");
    let expected = PartitionRecord {
        topic: "t".into(),
        partition: 0,
        leader: NodeId(2),
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: vec![NodeId(2), NodeId(3)],
        leader_epoch: LeaderEpoch(6),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 1,
    };
    assert!(*one_partition_change(&batches[0]) == expected);

    // The test source never applies the change, so the image still shows
    // broker 1 as leader. That models a lost commit. The next tick's
    // sweep re-drives the same failover.
    run_liveness_tick(
        &controller,
        NodeId(2),
        &liveness,
        &metrics,
        &recovery,
        &mut state,
    )
    .await;
    let batches = source.submitted();
    assert!(batches.len() == 2, "the sweep retries, got {batches:?}");
    assert!(*one_partition_change(&batches[1]) == expected);
}

#[tokio::test]
async fn tick_on_a_follower_tracks_nothing_and_submits_nothing() {
    let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
    register_brokers(&mut img, &[1, 2, 3]);
    let source = fake_source(img, Some(NodeId(9)));
    let controller: Arc<dyn crate::metadata_source::MetadataSource> = source.clone();
    let clock = TestClock::new();
    let liveness = Arc::new(ControllerLivenessState::with_test_clock(
        std::time::Duration::from_millis(10),
        &clock,
    ));
    let metrics = crate::metrics::BrokerMetrics::new();
    let recovery = recovery_handle_for_tests();
    let mut state = LivenessTickState::default();

    run_liveness_tick(
        &controller,
        NodeId(2),
        &liveness,
        &metrics,
        &recovery,
        &mut state,
    )
    .await;
    clock.advance(std::time::Duration::from_millis(11));
    run_liveness_tick(
        &controller,
        NodeId(2),
        &liveness,
        &metrics,
        &recovery,
        &mut state,
    )
    .await;

    // A follower does not receive heartbeats, so it must not start
    // sessions from the image. Otherwise every broker would look dead.
    assert!(liveness.dead_snapshot().await.is_empty());
    assert!(!liveness.is_alive(1).await);
    assert!(source.submitted().is_empty());
}

#[tokio::test]
async fn first_tick_of_a_new_term_seeds_before_it_sweeps() {
    // While this node was a follower it received no heartbeats, so its
    // registry expired every session. When it takes the lead, the first
    // tick must seed those brokers alive before any sweep can read the
    // stale dead set and fail over partitions whose leaders are healthy.
    let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
    register_brokers(&mut img, &[1, 2, 3]);
    let source = fake_source(img, Some(NodeId(9)));
    let controller: Arc<dyn crate::metadata_source::MetadataSource> = source.clone();
    let clock = TestClock::new();
    let liveness = Arc::new(ControllerLivenessState::with_test_clock(
        std::time::Duration::from_millis(10),
        &clock,
    ));
    let metrics = crate::metrics::BrokerMetrics::new();
    let recovery = recovery_handle_for_tests();
    let mut state = LivenessTickState::default();

    // Sessions from the previous term expire while node 2 follows.
    for broker in [1, 2, 3] {
        liveness.record_heartbeat(broker).await;
    }
    clock.advance(std::time::Duration::from_millis(11));
    run_liveness_tick(
        &controller,
        NodeId(2),
        &liveness,
        &metrics,
        &recovery,
        &mut state,
    )
    .await;
    assert!(liveness.dead_snapshot().await == [1, 2, 3].into_iter().collect());

    // Node 2 takes the lead. The first tick of the term seeds every
    // registered broker alive and submits nothing.
    // `send_replace` does not need a live receiver: the tick subscribes
    // on demand and drops its receiver at once.
    source.set_leader(Some(NodeId(2)));
    run_liveness_tick(
        &controller,
        NodeId(2),
        &liveness,
        &metrics,
        &recovery,
        &mut state,
    )
    .await;
    assert!(liveness.dead_snapshot().await.is_empty());
    assert!(liveness.is_alive(1).await);
    assert!(source.submitted().is_empty());

    // The seeded window is a real one: a broker that stays silent for a
    // full window afterwards still expires and fails over.
    clock.advance(std::time::Duration::from_millis(11));
    liveness.record_heartbeat(2).await;
    liveness.record_heartbeat(3).await;
    run_liveness_tick(
        &controller,
        NodeId(2),
        &liveness,
        &metrics,
        &recovery,
        &mut state,
    )
    .await;
    assert!(liveness.dead_snapshot().await == [1].into_iter().collect());
    assert!(source.submitted().len() == 1);
}
