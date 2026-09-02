//! Tests for the dead-broker failover scan: the clean ISR election, the
//! KIP-841 `unclean.leader.election.enable` fall-through, the KIP-966
//! offset-aware strategies that defer to the recovery manager, and the
//! witness role the scan reads out of the metadata image.

use assert2::assert;
use krabka_metadata::LeaderEpoch;

use super::*;
use crate::{
    config_keys::{
        ELIGIBLE_LEADER_REPLICAS, MIN_INSYNC_REPLICAS, UNCLEAN_LEADER_ELECTION_ENABLE,
        UNCLEAN_RECOVERY_STRATEGY,
    },
    leader_election::test_support::{
        elected_partition, img_with_partition, mark_witnesses_in_image, one_partition_change,
        set_cluster_default, set_topic_config, set_topic_configs,
    },
};

#[tokio::test]
async fn failover_picks_alive_isr_member_when_available() {
    // Leader 1 dies, ISR {1, 2, 3}, both 2 and 3 alive — pick 2.
    let img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in [2u64, 3] {
        l.record_heartbeat(n).await;
    }
    let plan = compute_failover_changes(
        &img,
        /*dead=*/ NodeId(1),
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;
    assert!(plan.recoveries.is_empty());
    let pr = one_partition_change(&plan.changes);
    // leader_epoch and partition_epoch must both bump on election.
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
    assert!(*pr == expected);
}

#[tokio::test]
async fn failover_processes_dead_replica_even_when_not_in_isr() {
    // Synthetic but valid during ISR churn: dead broker is the current
    // leader/replica, while the ISR already contains only surviving peers.
    let img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[2, 3]);
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in [2u64, 3] {
        l.record_heartbeat(n).await;
    }

    let plan = compute_failover_changes(
        &img,
        /*dead=*/ NodeId(1),
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;

    let pr = one_partition_change(&plan.changes);
    assert!(pr.leader == 2);
    assert!(pr.isr == vec![NodeId(2), NodeId(3)]);
    assert!(pr.leader_epoch == 6, "leader_epoch must bump on election");
}

#[tokio::test]
async fn failover_ignores_partition_when_dead_broker_is_unrelated() {
    // Broker 9 is neither a replica nor an ISR member. Even if some other
    // ISR member is dead, this scan must not rewrite the partition.
    let img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in [1u64, 3] {
        l.record_heartbeat(n).await;
    }

    let plan = compute_failover_changes(
        &img,
        /*dead=*/ NodeId(9),
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;

    assert!(plan.changes.is_empty());
    assert!(plan.recoveries.is_empty());
}

#[tokio::test]
async fn failover_leaves_partition_unavailable_when_unclean_disabled() {
    // ISR is just {1}, broker 1 dies, brokers 2/3 alive. With
    // `unclean.leader.election.enable=false` (the default) the
    // controller must not elect — partition stays unavailable.
    let img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in [2u64, 3] {
        l.record_heartbeat(n).await;
    }
    let plan = compute_failover_changes(
        &img,
        /*dead=*/ NodeId(1),
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;
    assert!(
        plan.changes.is_empty(),
        "default-off must not emit any change, got {:?}",
        plan.changes,
    );
    assert!(plan.recoveries.is_empty());
}

#[tokio::test]
async fn failover_elects_unclean_when_topic_opts_in() {
    // Same setup, but `unclean.leader.election.enable=true` on the
    // topic. Controller must elect the first alive out-of-ISR replica
    // (broker 2) as leader with singleton ISR.
    let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
    set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "true");
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in [2u64, 3] {
        l.record_heartbeat(n).await;
    }
    let metrics = crate::metrics::BrokerMetrics::new();
    let plan = compute_failover_changes(&img, /*dead=*/ NodeId(1), &l, &metrics).await;
    assert!(plan.recoveries.is_empty());
    let pr = one_partition_change(&plan.changes);
    // Must elect the first alive replica (broker 2) with a singleton
    // ISR (KIP-841) and a bumped leader_epoch.
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
    assert!(*pr == expected);
    // Each unclean election bumps the counter exactly once.
    assert!(metrics.unclean_leader_elections_total.get() == 1);
}

#[tokio::test]
async fn failover_clean_does_not_bump_unclean_counter() {
    // Clean failover (ISR non-empty with an alive member) must not
    // bump the unclean-election counter — the metric is reserved
    // for the KIP-841 data-loss footgun path.
    let img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in [2u64, 3] {
        l.record_heartbeat(n).await;
    }
    let metrics = crate::metrics::BrokerMetrics::new();
    let _ = compute_failover_changes(&img, /*dead=*/ NodeId(1), &l, &metrics).await;
    assert!(metrics.unclean_leader_elections_total.get() == 0);
}

#[tokio::test]
async fn failover_unclean_skips_when_no_alive_replica() {
    // Unclean opt-in but ALL replicas dead — no election possible.
    let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
    set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "true");
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    // No heartbeats — nobody alive.
    let plan = compute_failover_changes(
        &img,
        /*dead=*/ NodeId(1),
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;
    assert!(
        plan.changes.is_empty(),
        "no alive replica → no election, got {:?}",
        plan.changes,
    );
    assert!(plan.recoveries.is_empty());
}

#[tokio::test]
async fn failover_unclean_false_string_keeps_default_safe_behavior() {
    // Explicit `false` must behave the same as unset.
    let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
    set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "false");
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in [2u64, 3] {
        l.record_heartbeat(n).await;
    }
    let plan = compute_failover_changes(
        &img,
        /*dead=*/ NodeId(1),
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;
    assert!(
        plan.changes.is_empty(),
        "explicit `false` keeps safe default"
    );
    assert!(plan.recoveries.is_empty());
}

#[tokio::test]
async fn failover_unclean_does_not_pick_dead_broker_itself() {
    // Edge case: `dead` is in `replicas`. The unclean fallback must
    // skip it — otherwise we'd re-elect the dead broker.
    let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
    set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "true");
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    // Only broker 3 alive — broker 2 also dead.
    l.record_heartbeat(3).await;
    let plan = compute_failover_changes(
        &img,
        /*dead=*/ NodeId(1),
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;
    assert!(plan.recoveries.is_empty());
    let pr = one_partition_change(&plan.changes);
    assert!(pr.leader == 3);
    assert!(pr.isr == vec![NodeId(3)]);
}

#[tokio::test]
async fn failover_unclean_does_not_apply_when_isr_still_has_alive_member() {
    // Leader 1 dies. ISR {1, 2} but 2 is alive — clean path picks
    // broker 2 even if unclean is enabled. (The unclean branch only
    // fires when alive_isr is empty.)
    let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2]);
    set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "true");
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in [2u64, 3] {
        l.record_heartbeat(n).await;
    }
    let plan = compute_failover_changes(
        &img,
        /*dead=*/ NodeId(1),
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;
    assert!(plan.recoveries.is_empty());
    let pr = one_partition_change(&plan.changes);
    assert!(pr.leader == 2);
    assert!(
        pr.isr == vec![NodeId(2)],
        "clean ISR-only election keeps the surviving ISR member, not a singleton-of-some-other-replica"
    );
}

#[tokio::test]
async fn failover_shrinks_isr_for_partitions_where_dead_is_non_leader() {
    // Broker 2 dies; partition's leader is 1 (still alive). The
    // dead member must be dropped from ISR without bumping the
    // leader_epoch (the leader isn't changing).
    let img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in [1u64, 3] {
        l.record_heartbeat(n).await;
    }
    let plan = compute_failover_changes(
        &img,
        /*dead=*/ NodeId(2),
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;
    assert!(plan.recoveries.is_empty());
    let pr = one_partition_change(&plan.changes);
    // Leader unchanged; a non-leader-change must NOT bump leader_epoch
    // (stays 5) but does bump partition_epoch.
    let expected = PartitionRecord {
        topic: "t".into(),
        partition: 0,
        leader: NodeId(1),
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: vec![NodeId(1), NodeId(3)],
        leader_epoch: LeaderEpoch(5),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 1,
    };
    assert!(*pr == expected);
}

#[tokio::test]
async fn failover_balanced_strategy_requests_recovery_not_immediate_change() {
    // Leader 1 dies, ISR shrinks to empty after dropping it; the topic
    // opted into `unclean.recovery.strategy=Balanced`, so the failover
    // scan must NOT make a blind immediate change — it hands the
    // partition to the URM via `recoveries`.
    let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
    set_topic_config(&mut img, "t", UNCLEAN_RECOVERY_STRATEGY, "Balanced");
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in [2u64, 3] {
        l.record_heartbeat(n).await;
    }
    let plan = compute_failover_changes(
        &img,
        /*dead=*/ NodeId(1),
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;
    assert!(
        plan.changes.is_empty(),
        "Balanced strategy must defer to the URM, not elect immediately, got {:?}",
        plan.changes,
    );
    assert!(plan.recoveries == vec![("t".to_string(), 0, RecoveryStrategy::Balanced)]);
}

#[tokio::test]
async fn failover_uses_cluster_default_recovery_settings() {
    let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
    set_cluster_default(&mut img, UNCLEAN_RECOVERY_STRATEGY, "Balanced");
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in [2u64, 3] {
        l.record_heartbeat(n).await;
    }

    let plan =
        compute_failover_changes(&img, NodeId(1), &l, &crate::metrics::BrokerMetrics::new()).await;

    assert!(plan.changes.is_empty());
    assert!(plan.recoveries == vec![("t".to_string(), 0, RecoveryStrategy::Balanced)]);
}

#[tokio::test]
async fn topic_none_overrides_cluster_strategy_and_uses_cluster_legacy_flag() {
    let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
    set_cluster_default(&mut img, UNCLEAN_RECOVERY_STRATEGY, "Balanced");
    set_cluster_default(&mut img, UNCLEAN_LEADER_ELECTION_ENABLE, "true");
    set_topic_config(&mut img, "t", UNCLEAN_RECOVERY_STRATEGY, "None");
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in [2u64, 3] {
        l.record_heartbeat(n).await;
    }

    let plan =
        compute_failover_changes(&img, NodeId(1), &l, &crate::metrics::BrokerMetrics::new()).await;

    assert!(plan.recoveries.is_empty());
    let change = one_partition_change(&plan.changes);
    assert!(change.leader == 2);
    assert!(change.isr == vec![NodeId(2)]);
}

#[tokio::test]
async fn failover_strategy_none_still_uses_legacy_enable_flag() {
    // No recovery strategy set (defaults to None), but the legacy
    // `unclean.leader.election.enable=true` flag is on. The scan keeps
    // the KIP-841 behavior: blind pick of the first alive replica.
    let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
    set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "true");
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in [2u64, 3] {
        l.record_heartbeat(n).await;
    }
    let plan = compute_failover_changes(
        &img,
        /*dead=*/ NodeId(1),
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;
    assert!(
        plan.recoveries.is_empty(),
        "strategy None must not enqueue an offset-aware recovery",
    );
    let pr = one_partition_change(&plan.changes);
    assert!(pr.leader == 2, "legacy path picks first alive replica");
    assert!(pr.isr == vec![NodeId(2)]);
}

#[tokio::test]
async fn failover_scan_reads_the_witness_role_out_of_the_image() {
    // End-to-end through `compute_failover_changes`: the witness role
    // arrives as a per-broker config record, exactly as the broker
    // publishes it at registration.
    let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
    mark_witnesses_in_image(&mut img, &[2]);
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in [2u64, 3] {
        l.record_heartbeat(n).await;
    }
    let plan = compute_failover_changes(
        &img,
        /*dead=*/ NodeId(1),
        &l,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await;
    assert!(plan.recoveries.is_empty());
    let pr = one_partition_change(&plan.changes);
    let expected = PartitionRecord {
        topic: "t".into(),
        partition: 0,
        leader: NodeId(3),
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: vec![NodeId(2), NodeId(3)],
        leader_epoch: LeaderEpoch(6),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 1,
    };
    assert!(*pr == expected);
}

#[tokio::test]
async fn failover_scan_leaves_a_witness_only_survivor_unavailable() {
    let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
    set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "true");
    mark_witnesses_in_image(&mut img, &[2]);
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    l.record_heartbeat(2).await;
    let metrics = crate::metrics::BrokerMetrics::new();
    let plan = compute_failover_changes(&img, /*dead=*/ NodeId(1), &l, &metrics).await;
    assert!(plan.changes.is_empty(), "got {:?}", plan.changes);
    assert!(plan.recoveries.is_empty());
    assert!(plan.unavailable == vec![("t".to_string(), 0)]);
    assert!(metrics.unclean_leader_elections_total.get() == 0);
}

/// One row of the eligible-leader-replica table below.
struct ElrScanCase<'a> {
    label: &'a str,
    /// Extra topic-config overrides, on top of the published ELR.
    policy: &'a [(&'a str, &'a str)],
}

/// KIP-966 through the whole dead-broker scan: the emitted record, the
/// metric, and the two lists the plan carries.
///
/// Broker 1 leads and dies, and the ISR named it alone. Broker 3 comes first
/// in the assignment and is alive, so every pre-ELR path lands on it: the
/// KIP-841 election picks it and calls the result unclean, and the strategies
/// hand the partition to the URM. Only broker 2 is published as eligible, so
/// only broker 2 is known to hold every committed record -- and Kafka's
/// `electAnyLeader` elects it and reports the election clean, under every one
/// of these policies.
#[tokio::test]
async fn failover_elects_an_eligible_leader_replica_cleanly_under_every_policy() {
    let cases = [
        ElrScanCase {
            label: "no unclean election and no offset-aware strategy",
            policy: &[],
        },
        ElrScanCase {
            label: "unclean election enabled",
            policy: &[(UNCLEAN_LEADER_ELECTION_ENABLE, "true")],
        },
        ElrScanCase {
            label: "balanced offset-aware recovery",
            policy: &[(UNCLEAN_RECOVERY_STRATEGY, "Balanced")],
        },
        ElrScanCase {
            label: "aggressive recovery and unclean election together",
            policy: &[
                (UNCLEAN_RECOVERY_STRATEGY, "Aggressive"),
                (UNCLEAN_LEADER_ELECTION_ENABLE, "true"),
            ],
        },
    ];
    for case in cases {
        let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 3, 2], &[1]);
        let mut overrides: Vec<(&str, &str)> = vec![
            (ELIGIBLE_LEADER_REPLICAS, "0:2:"),
            (MIN_INSYNC_REPLICAS, "2"),
        ];
        overrides.extend_from_slice(case.policy);
        set_topic_configs(&mut img, "t", &overrides);
        let l = ControllerLivenessState::new(krabka_units::secs(10));
        for n in [2u64, 3] {
            l.record_heartbeat(n).await;
        }
        let metrics = crate::metrics::BrokerMetrics::new();

        let plan = compute_failover_changes(&img, /*dead=*/ NodeId(1), &l, &metrics).await;

        let expected = PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: NodeId(2),
            replicas: vec![NodeId(1), NodeId(3), NodeId(2)],
            isr: vec![NodeId(2)],
            leader_epoch: LeaderEpoch(6),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 1,
        };
        assert!(
            *elected_partition(&plan.changes) == expected,
            "{}",
            case.label
        );
        assert!(plan.recoveries.is_empty(), "{}", case.label);
        assert!(plan.unavailable.is_empty(), "{}", case.label);
        assert!(
            metrics.unclean_leader_elections_total.get() == 0,
            "{}: an ELR election loses nothing and must not meter as unclean",
            case.label
        );
    }
}

/// An eligible leader replica that is not alive fails Kafka's
/// `isAcceptableLeader`, so the decision falls back to the rung below. With
/// the KIP-841 toggle on that is the out-of-ISR election of broker 3, and it
/// is reported as the data loss it is.
#[tokio::test]
async fn a_dead_eligible_leader_replica_leaves_the_unclean_election_to_decide() {
    let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 3, 2], &[1]);
    set_topic_configs(
        &mut img,
        "t",
        &[
            (ELIGIBLE_LEADER_REPLICAS, "0:2:"),
            (MIN_INSYNC_REPLICAS, "2"),
            (UNCLEAN_LEADER_ELECTION_ENABLE, "true"),
        ],
    );
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    l.record_heartbeat(3).await;
    let metrics = crate::metrics::BrokerMetrics::new();

    let plan = compute_failover_changes(&img, /*dead=*/ NodeId(1), &l, &metrics).await;

    let pr = elected_partition(&plan.changes);
    assert!(pr.leader == NodeId(3));
    assert!(pr.isr == vec![NodeId(3)]);
    assert!(metrics.unclean_leader_elections_total.get() == 1);
}
