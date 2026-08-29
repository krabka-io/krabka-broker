//! Tests for the dead-broker failover itself: the edge that
//! `AliveToDead` fires, the bound on a stalled commit, and the
//! level-triggered sweep that re-drives a failover the edge could not
//! complete and then stops walking the image once nothing is stuck.

use assert2::assert;
use krabka_metadata::{LeaderEpoch, PartitionRecord};

use super::*;
use crate::{
    heartbeat::controller_state::{LivenessTransition, TestClock},
    leader_election::test_support::{
        TestMetadataSource, img_with_partition, liveness_with_alive, liveness_with_dead,
        one_partition_change, recovery_handle_for_tests, register_brokers,
    },
};

#[tokio::test]
async fn on_broker_dead_submits_failover_when_this_controller_is_leader() {
    let img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
    let source = Arc::new(TestMetadataSource::new(img, Some(NodeId(7))));
    let controller: Arc<dyn crate::metadata_source::MetadataSource> = source.clone();
    let liveness = liveness_with_alive(&[2, 3]).await;
    let recovery = recovery_handle_for_tests();

    on_broker_dead(
        &controller,
        NodeId(7),
        NodeId(1),
        &liveness,
        &crate::metrics::BrokerMetrics::new(),
        &recovery,
    )
    .await
    .expect("broker dead handling should submit");

    let batches = source.submitted_batches().await;
    assert!(batches.len() == 1);
    let pr = one_partition_change(&batches[0]);
    assert!(pr.leader == 2);
    assert!(pr.partition_epoch == 1);
}

#[tokio::test(start_paused = true)]
async fn on_broker_dead_bounds_a_stalled_commit() {
    let img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
    let source = Arc::new(TestMetadataSource::new_stalled(img, Some(NodeId(7))));
    let controller: Arc<dyn crate::metadata_source::MetadataSource> = source.clone();
    let liveness = liveness_with_alive(&[2, 3]).await;
    let recovery = recovery_handle_for_tests();

    // Paused time: the runtime auto-advances past the bound as soon as
    // the only pending future is the timeout itself.
    let result = on_broker_dead(
        &controller,
        NodeId(7),
        NodeId(1),
        &liveness,
        &crate::metrics::BrokerMetrics::new(),
        &recovery,
    )
    .await;

    let error = result.expect_err("a stalled commit must surface as an error");
    assert!(matches!(error, crate::error::BrokerError::Replication(_)));
    assert!(source.submitted_batches().await.is_empty());
}

#[tokio::test]
async fn sweep_resolves_death_edge_that_found_no_alive_isr_member() {
    // Partition t-0: leader 1, ISR {1, 2}. Replica 3 is out of the ISR.
    let img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2]);
    let source = Arc::new(TestMetadataSource::new(img, Some(NodeId(7))));
    let controller: Arc<dyn crate::metadata_source::MetadataSource> = source.clone();
    let clock = TestClock::new();
    let liveness = Arc::new(ControllerLivenessState::with_test_clock(
        std::time::Duration::from_millis(10),
        &clock,
    ));
    let metrics = crate::metrics::BrokerMetrics::new();
    let recovery = recovery_handle_for_tests();
    let mut state = LivenessTickState::default();

    // Broker 1 heartbeats once and then its session expires. Broker 3 is
    // alive but out of the ISR. Broker 2 has not heartbeated yet.
    liveness.record_heartbeat(1).await;
    clock.advance(std::time::Duration::from_millis(11));
    liveness.record_heartbeat(3).await;
    assert!(liveness.tick().await == vec![LivenessTransition::AliveToDead(1)]);

    // The edge finds no alive ISR replica. The partition stays
    // unavailable and the edge is consumed.
    on_broker_dead(
        &controller,
        NodeId(7),
        NodeId(1),
        &liveness,
        &metrics,
        &recovery,
    )
    .await
    .expect("edge handling");
    assert!(source.submitted_batches().await.is_empty());

    // The sweep sees the same liveness state and is also a no-op.
    sweep_dead_leaders(
        &controller,
        NodeId(7),
        &liveness,
        &metrics,
        &recovery,
        &mut state,
    )
    .await;
    assert!(source.submitted_batches().await.is_empty());

    // Broker 2 comes alive. No new edge fires for broker 1: it is
    // already dead.
    liveness.record_heartbeat(2).await;
    assert!(liveness.tick().await.is_empty());

    // The sweep re-drives the failover and elects broker 2.
    sweep_dead_leaders(
        &controller,
        NodeId(7),
        &liveness,
        &metrics,
        &recovery,
        &mut state,
    )
    .await;
    let batches = source.submitted_batches().await;
    assert!(batches.len() == 1);
    let expected = PartitionRecord {
        topic: "t".into(),
        partition: 0,
        leader: NodeId(2),
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: vec![NodeId(2)],
        leader_epoch: LeaderEpoch(6),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 1,
    };
    assert!(*one_partition_change(&batches[0]) == expected);
}

#[tokio::test]
async fn sweep_re_drives_only_dead_leaders_and_isr_members() {
    // (name, controller leader, partition leader, isr, dead, alive,
    //  expected submitted record)
    struct Case {
        name: &'static str,
        controller_leader: Option<NodeId>,
        leader: u64,
        isr: &'static [u64],
        dead: &'static [u64],
        alive: &'static [u64],
        expected: Option<PartitionRecord>,
    }
    let base = PartitionRecord {
        topic: "t".into(),
        partition: 0,
        leader: NodeId(1),
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: vec![],
        leader_epoch: LeaderEpoch(5),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 1,
    };
    let cases = [
        Case {
            name: "dead leader still leads: elect an alive ISR member",
            controller_leader: Some(NodeId(7)),
            leader: 1,
            isr: &[1, 2, 3],
            dead: &[1],
            alive: &[2, 3],
            expected: Some(PartitionRecord {
                leader: NodeId(2),
                isr: vec![NodeId(2), NodeId(3)],
                leader_epoch: LeaderEpoch(6),
                ..base.clone()
            }),
        },
        Case {
            name: "dead ISR member: shrink the ISR without an epoch bump",
            controller_leader: Some(NodeId(7)),
            leader: 1,
            isr: &[1, 2, 3],
            dead: &[2],
            alive: &[1, 3],
            expected: Some(PartitionRecord {
                isr: vec![NodeId(1), NodeId(3)],
                ..base.clone()
            }),
        },
        Case {
            name: "failover already done: dead broker is a plain replica",
            controller_leader: Some(NodeId(7)),
            leader: 2,
            isr: &[2, 3],
            dead: &[1],
            alive: &[2, 3],
            expected: None,
        },
        Case {
            name: "not the controller leader: no re-drive",
            controller_leader: Some(NodeId(8)),
            leader: 1,
            isr: &[1, 2, 3],
            dead: &[1],
            alive: &[2, 3],
            expected: None,
        },
    ];
    for case in cases {
        let img = img_with_partition("t", 0, case.leader, &[1, 2, 3], case.isr);
        let source = Arc::new(TestMetadataSource::new(img, case.controller_leader));
        let controller: Arc<dyn crate::metadata_source::MetadataSource> = source.clone();
        let liveness = liveness_with_dead(case.dead, case.alive).await;
        let metrics = crate::metrics::BrokerMetrics::new();
        let recovery = recovery_handle_for_tests();
        let mut state = LivenessTickState::default();

        sweep_dead_leaders(
            &controller,
            NodeId(7),
            &liveness,
            &metrics,
            &recovery,
            &mut state,
        )
        .await;

        let batches = source.submitted_batches().await;
        let submitted = batches
            .first()
            .map(|batch| one_partition_change(batch).clone());
        assert!(
            batches.len() <= 1 && submitted == case.expected,
            "{}: got {batches:?}",
            case.name
        );
    }
}

#[tokio::test]
async fn sweep_walks_the_image_once_per_change_while_a_dead_broker_stays_resolved() {
    // Broker 1 is dead and no longer leads or sits in an ISR. The sweep
    // walks the image once, records that nothing is stuck, and skips the
    // walk on later ticks until the image or the dead set changes.
    let mut img = img_with_partition("t", 0, /*leader*/ 2, &[1, 2, 3], &[2, 3]);
    register_brokers(&mut img, &[1, 2, 3]);
    let source = Arc::new(TestMetadataSource::new(img, Some(NodeId(7))));
    let controller: Arc<dyn crate::metadata_source::MetadataSource> = source.clone();
    let liveness = liveness_with_dead(&[1], &[2, 3]).await;
    let metrics = crate::metrics::BrokerMetrics::new();
    let recovery = recovery_handle_for_tests();
    let mut state = LivenessTickState::default();

    sweep_dead_leaders(
        &controller,
        NodeId(7),
        &liveness,
        &metrics,
        &recovery,
        &mut state,
    )
    .await;
    let memo = state
        .clean_sweep
        .as_ref()
        .expect("a clean sweep is remembered");
    assert!(Arc::ptr_eq(&memo.0, &controller.current_image()));
    assert!(memo.1 == [1].into_iter().collect());
    assert!(source.submitted_batches().await.is_empty());

    // Broker 1 comes back: the dead set changes and the memo is dropped.
    liveness.record_heartbeat(1).await;
    sweep_dead_leaders(
        &controller,
        NodeId(7),
        &liveness,
        &metrics,
        &recovery,
        &mut state,
    )
    .await;
    assert!(state.clean_sweep.is_none());
}
