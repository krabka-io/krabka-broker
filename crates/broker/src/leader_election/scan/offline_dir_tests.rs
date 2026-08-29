//! Tests for the offline-log-dir failover scan (KIP-112): the leader
//! election when the leader's directory fails, the plain ISR shrink for a
//! non-leader replica, idempotence after a completed failover, and the
//! empty-ISR branches that either defer to the recovery manager or fall
//! through to an unclean election.

use assert2::{assert, check};
use krabka_metadata::LeaderEpoch;

use super::*;
use crate::{
    config_keys::{UNCLEAN_LEADER_ELECTION_ENABLE, UNCLEAN_RECOVERY_STRATEGY},
    leader_election::test_support::{img_with_dirs, one_partition_change, set_topic_config},
};

#[tokio::test]
async fn offline_dir_elects_alive_isr_member_when_leader_dir_failed() {
    let bad = uuid::Uuid::from_u128(0xDEAD);
    let good = uuid::Uuid::from_u128(0x1);
    let img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2, 3], &[bad, good, good]);
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in [1u64, 2, 3] {
        l.record_heartbeat(n).await;
    }
    let offline: std::collections::HashSet<uuid::Uuid> = [bad].into_iter().collect();
    let plan = super::compute_offline_dir_failover_changes(
        &img,
        NodeId(1),
        &offline,
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;
    let MetadataRecord::V1Partition(pr) = &plan.changes[0] else {
        panic!()
    };
    let expected = PartitionRecord {
        topic: "t".into(),
        partition: 0,
        leader: NodeId(2),
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: vec![NodeId(2), NodeId(3)],
        leader_epoch: LeaderEpoch(6),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![bad, good, good],
        partition_epoch: 1,
    };
    assert!(*pr == expected);
}

#[tokio::test]
async fn offline_dir_leaves_healthy_dir_partition_untouched() {
    let bad = uuid::Uuid::from_u128(0xDEAD);
    let good = uuid::Uuid::from_u128(0x1);
    let img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2, 3], &[good, good, good]);
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in [1u64, 2, 3] {
        l.record_heartbeat(n).await;
    }
    let offline: std::collections::HashSet<uuid::Uuid> = [bad].into_iter().collect();
    let plan = super::compute_offline_dir_failover_changes(
        &img,
        NodeId(1),
        &offline,
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;
    assert!(plan.changes.is_empty());
}

#[tokio::test]
async fn offline_dir_shrinks_isr_for_non_leader_replica() {
    let bad = uuid::Uuid::from_u128(0xDEAD);
    let good = uuid::Uuid::from_u128(0x1);
    let img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2, 3], &[good, bad, good]);
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in [1u64, 2, 3] {
        l.record_heartbeat(n).await;
    }
    let offline: std::collections::HashSet<uuid::Uuid> = [bad].into_iter().collect();
    let plan = super::compute_offline_dir_failover_changes(
        &img,
        NodeId(2),
        &offline,
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;
    let MetadataRecord::V1Partition(pr) = &plan.changes[0] else {
        panic!()
    };
    let expected = PartitionRecord {
        topic: "t".into(),
        partition: 0,
        leader: NodeId(1),
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: vec![NodeId(1), NodeId(3)],
        leader_epoch: LeaderEpoch(5),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![good, bad, good],
        partition_epoch: 1,
    };
    assert!(*pr == expected);
}

#[tokio::test]
async fn offline_dir_idempotent_after_failover() {
    let bad = uuid::Uuid::from_u128(0xDEAD);
    let good = uuid::Uuid::from_u128(0x1);
    // After failover: broker 1's dir is bad but broker 1 is no longer
    // leader (broker 2 is), and broker 1 is not in ISR {2,3} either.
    let img = img_with_dirs("t", 2, &[1, 2, 3], &[2, 3], &[bad, good, good]);
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in [1u64, 2, 3] {
        l.record_heartbeat(n).await;
    }
    let offline: std::collections::HashSet<uuid::Uuid> = [bad].into_iter().collect();
    let plan = super::compute_offline_dir_failover_changes(
        &img,
        NodeId(1),
        &offline,
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;
    assert!(plan.changes.is_empty());
}

#[tokio::test]
async fn offline_dir_empty_isr_balanced_strategy_defers_to_urm() {
    // Broker 1 is leader, its replica is on the bad dir, and the only other
    // ISR member (broker 2) is NOT alive — alive_isr is empty.
    // Topic sets unclean.recovery.strategy=Balanced.
    // Expect: recoveries gets the entry, changes is empty.
    let bad = uuid::Uuid::from_u128(0xDEAD);
    let good = uuid::Uuid::from_u128(0x1);
    let mut img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2], &[bad, good, good]);
    set_topic_config(&mut img, "t", UNCLEAN_RECOVERY_STRATEGY, "Balanced");
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    // Only broker 3 alive but it's NOT in the ISR — alive_isr = empty.
    l.record_heartbeat(3).await;
    let offline: std::collections::HashSet<uuid::Uuid> = [bad].into_iter().collect();
    let plan = compute_offline_dir_failover_changes(
        &img,
        NodeId(1),
        &offline,
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;
    assert!(
        plan.changes.is_empty(),
        "Balanced strategy must not make an immediate change; got {:?}",
        plan.changes
    );
    assert!(
        plan.recoveries == vec![("t".to_string(), 0, RecoveryStrategy::Balanced)],
        "Balanced strategy must enqueue a recovery job; got {:?}",
        plan.recoveries
    );
}

#[tokio::test]
async fn offline_dir_empty_isr_aggressive_strategy_defers_to_urm() {
    // Same as above but with Aggressive strategy.
    let bad = uuid::Uuid::from_u128(0xDEAD);
    let good = uuid::Uuid::from_u128(0x1);
    let mut img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2], &[bad, good, good]);
    set_topic_config(&mut img, "t", UNCLEAN_RECOVERY_STRATEGY, "Aggressive");
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    // broker 2 is not alive, broker 3 is alive but not in ISR.
    l.record_heartbeat(3).await;
    let offline: std::collections::HashSet<uuid::Uuid> = [bad].into_iter().collect();
    let plan = compute_offline_dir_failover_changes(
        &img,
        NodeId(1),
        &offline,
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;
    assert!(plan.changes.is_empty());
    assert!(
        plan.recoveries == vec![("t".to_string(), 0, RecoveryStrategy::Aggressive)],
        "Aggressive strategy must enqueue a recovery job; got {:?}",
        plan.recoveries
    );
}

#[tokio::test]
async fn offline_dir_empty_isr_unclean_enabled_elects_out_of_isr_replica() {
    // Broker 1 is leader on bad dir, broker 2 (the only ISR peer) is dead,
    // broker 3 is alive and out-of-ISR.
    // unclean.leader.election.enable=true → elect broker 3, singleton ISR,
    // bump unclean_leader_elections_total.
    let bad = uuid::Uuid::from_u128(0xDEAD);
    let good = uuid::Uuid::from_u128(0x1);
    let mut img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2], &[bad, good, good]);
    set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "true");
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    // broker 3 alive, broker 2 dead (no heartbeat).
    l.record_heartbeat(3).await;
    let offline: std::collections::HashSet<uuid::Uuid> = [bad].into_iter().collect();
    let metrics = crate::metrics::BrokerMetrics::new();
    let plan = compute_offline_dir_failover_changes(&img, NodeId(1), &offline, &l, &metrics).await;
    assert!(plan.recoveries.is_empty());
    let pr = one_partition_change(&plan.changes);
    // Must elect broker 3 (only alive out-of-ISR) with a singleton
    // ISR (unclean election) and a bumped leader_epoch.
    let expected = PartitionRecord {
        topic: "t".into(),
        partition: 0,
        leader: NodeId(3),
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: vec![NodeId(3)],
        leader_epoch: LeaderEpoch(6),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![bad, good, good],
        partition_epoch: 1,
    };
    assert!(*pr == expected);
    assert!(
        metrics.unclean_leader_elections_total.get() == 1,
        "unclean counter must be bumped exactly once"
    );
}

#[tokio::test]
async fn offline_dir_empty_isr_no_unclean_leaves_partition_unavailable() {
    // Broker 1 is leader on bad dir, broker 2 dead, broker 3 alive but
    // not in ISR.  No recovery strategy, no unclean flag → no change.
    let bad = uuid::Uuid::from_u128(0xDEAD);
    let good = uuid::Uuid::from_u128(0x1);
    let img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2], &[bad, good, good]);
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    l.record_heartbeat(3).await; // only 3 alive, but not in ISR
    let offline: std::collections::HashSet<uuid::Uuid> = [bad].into_iter().collect();
    let plan = compute_offline_dir_failover_changes(
        &img,
        NodeId(1),
        &offline,
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;
    assert!(
        plan.changes.is_empty(),
        "default-off must not emit any change; got {:?}",
        plan.changes
    );
    assert!(plan.recoveries.is_empty());
}

#[tokio::test]
async fn offline_dir_empty_isr_unclean_enabled_no_alive_replica_stays_unavailable() {
    // Broker 1 is leader on bad dir, ALL brokers are dead.
    // unclean enabled but no alive replica → no change.
    let bad = uuid::Uuid::from_u128(0xDEAD);
    let good = uuid::Uuid::from_u128(0x1);
    let mut img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2], &[bad, good, good]);
    set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "true");
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    // No heartbeats — nobody alive.
    let offline: std::collections::HashSet<uuid::Uuid> = [bad].into_iter().collect();
    let metrics = crate::metrics::BrokerMetrics::new();
    let plan = compute_offline_dir_failover_changes(&img, NodeId(1), &offline, &l, &metrics).await;
    check!(
        plan.changes.is_empty(),
        "no alive replica → no election; got {:?}",
        plan.changes
    );
    check!(plan.recoveries.is_empty());
    check!(
        metrics.unclean_leader_elections_total.get() == 0,
        "no election means no counter bump"
    );
}
