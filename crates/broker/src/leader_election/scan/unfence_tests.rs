//! Tests for the elections a broker's unfence runs: every partition that has
//! no leader takes the election ladder again with the unfencing broker as an
//! acceptable leader, which is Kafka's `handleBrokerUnfenced`.
//!
//! A partition with no leader is one whose last-known ELR names the leader its
//! record still holds (see `crate::elr::state::is_leaderless`). The two states
//! the rows start from are the two ways Kafka reaches it: a fence puts the
//! last leader in the ELR as well (`0:1,2,3:1`), and an unclean restart keeps
//! it out (`0::1`).

use assert2::assert;
use krabka_metadata::{
    LeaderEpoch, LeaderRecoveryState, PartitionRecoveryRecord, PartitionUpdateRecord,
};

use super::*;
use crate::{
    config_keys::{ELIGIBLE_LEADER_REPLICAS, MIN_INSYNC_REPLICAS},
    leader_election::test_support::{img_with_partition, set_topic_configs},
};

/// The image of a partition `t-0` with replicas `[1, 2, 3]`, led by broker 1
/// alone in its record, and with `published` as its ELR state.
fn leaderless_image(published: &str) -> MetadataImage {
    let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
    crate::test_support::finalize_elr_version(&mut img);
    set_topic_configs(
        &mut img,
        "t",
        &[
            (MIN_INSYNC_REPLICAS, "2"),
            (ELIGIBLE_LEADER_REPLICAS, published),
        ],
    );
    img
}

/// The records an unfence election writes: broker `leader` takes the
/// partition under a singleton ISR at the next leader epoch, with the ELR
/// state it leaves behind and, for an unclean election, the `RECOVERING`
/// marker.
///
/// The partition record's leader is broker 1, so an election of broker 1 keeps
/// it and only bumps the leader epoch. That cannot ride a
/// `V1PartitionUpdate`, which reaches the log as a `PartitionChangeRecord` that
/// bumps the epoch only when the leader changes, so the partition record stays
/// whole and the ELR and recovery records follow it. Any other election is the
/// one `V1PartitionUpdate` Kafka's `PartitionChangeRecord` is.
fn elected(leader: u64, eligible: &[u64], recovering: bool) -> Vec<MetadataRecord> {
    let partition = PartitionRecord {
        topic: "t".into(),
        partition: 0,
        leader: NodeId(leader),
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: vec![NodeId(leader)],
        leader_epoch: LeaderEpoch(6),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 1,
    };
    let eligible: Vec<NodeId> = eligible.iter().copied().map(NodeId).collect();
    let recovery = recovering.then_some(LeaderRecoveryState::Recovering);
    if leader != 1 {
        return vec![MetadataRecord::V1PartitionUpdate(PartitionUpdateRecord {
            partition,
            eligible_leader_replicas: Some(eligible),
            last_known_elr: Some(vec![]),
            recovery_state: recovery,
        })];
    }
    let mut records = vec![
        MetadataRecord::V1Partition(partition),
        MetadataRecord::V1PartitionElr(krabka_metadata::PartitionElrRecord {
            topic: "t".into(),
            partition: 0,
            eligible_leader_replicas: eligible,
            last_known_elr: vec![],
        }),
    ];
    records.extend(recovery.map(|state| {
        MetadataRecord::V1PartitionRecovery(PartitionRecoveryRecord {
            topic: "t".into(),
            partition: 0,
            state,
        })
    }));
    records
}

/// `PartitionChangeBuilderTest.testEligibleLeaderReplicas_ElectLastKnownLeader`
/// and `_ElrCanBeElected`, one broker unfencing at a time, as the heartbeat
/// handler runs the scan.
///
/// The first row is the case `canElectLastKnownLeader` exists for: the ELR is
/// empty, so the last leader comes back as an unclean leader -- singleton ISR,
/// `RECOVERING`, both sets cleared -- with no `unclean.leader.election.enable`
/// and no `unclean.recovery.strategy` in front of it. The second is the fence
/// case: the last leader is in the ELR, so its return is a clean ELR election
/// that leaves the rest eligible. The third is another broker unfencing while
/// the last leader is still down, which elects the ELR member that returned.
#[tokio::test]
async fn an_unfence_elects_for_a_partition_with_no_leader() {
    struct Case {
        label: &'static str,
        published: &'static str,
        unfenced: u64,
        alive: &'static [u64],
        expected: Vec<MetadataRecord>,
        unclean_elections: u64,
    }
    let cases = [
        Case {
            label: "the last leader returns after an unclean restart",
            published: "0::1",
            unfenced: 1,
            alive: &[2, 3],
            expected: elected(1, &[], true),
            unclean_elections: 1,
        },
        Case {
            label: "the last leader returns from the ELR after a fence",
            published: "0:1,2,3:1",
            unfenced: 1,
            alive: &[2, 3],
            expected: elected(1, &[2, 3], false),
            unclean_elections: 0,
        },
        Case {
            label: "an ELR member returns while the last leader is down",
            published: "0:1,2,3:1",
            unfenced: 2,
            alive: &[3],
            expected: elected(2, &[1, 3], false),
            unclean_elections: 0,
        },
    ];
    for case in cases {
        let img = leaderless_image(case.published);
        let l = ControllerLivenessState::new(krabka_units::secs(10));
        for &n in case.alive {
            l.record_heartbeat(n).await;
        }
        let metrics = crate::metrics::BrokerMetrics::new();

        let changes = compute_unfence_changes(&img, NodeId(case.unfenced), &l, &metrics).await;

        assert!(changes == case.expected, "{}", case.label);
        assert!(
            metrics.unclean_leader_elections_total.get() == case.unclean_elections,
            "{}",
            case.label
        );
    }
}

/// `testEligibleLeaderReplicas_ElectLastKnownLeaderShouldFail` and
/// `_NotEligibleLastKnownLeader`, plus the partitions the scan must not touch:
/// with nothing to elect the scan writes nothing, and a partition that has a
/// leader is never one of the partitions `partitionsWithNoLeader` walks.
#[tokio::test]
async fn an_unfence_leaves_alone_what_it_cannot_elect() {
    struct Case {
        label: &'static str,
        published: &'static str,
        unfenced: u64,
        alive: &'static [u64],
    }
    let cases = [
        Case {
            label: "the ELR is not empty and its member is down",
            published: "0:2:1",
            unfenced: 3,
            alive: &[],
        },
        Case {
            label: "the last known leader is still down",
            published: "0::1",
            unfenced: 2,
            alive: &[],
        },
        Case {
            label: "the partition has a leader",
            published: "0:2:",
            unfenced: 1,
            alive: &[2, 3],
        },
        Case {
            label: "the last-known set is not the last leader alone",
            published: "0::1,2",
            unfenced: 1,
            alive: &[2, 3],
        },
    ];
    for case in cases {
        let img = leaderless_image(case.published);
        let l = ControllerLivenessState::new(krabka_units::secs(10));
        for &n in case.alive {
            l.record_heartbeat(n).await;
        }

        let changes = compute_unfence_changes(
            &img,
            NodeId(case.unfenced),
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;

        assert!(changes.is_empty(), "{}: {changes:?}", case.label);
    }
}

/// What an election does to the state it starts from: applying the records
/// leaves a partition that has a leader, the last-known ELR is empty, and the
/// scan has nothing more to say.
#[tokio::test]
async fn an_unfence_election_leaves_a_partition_with_a_leader() {
    let mut img = leaderless_image("0::1");
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    let metrics = crate::metrics::BrokerMetrics::new();

    let changes = compute_unfence_changes(&img, NodeId(1), &l, &metrics).await;
    for record in &changes {
        img.apply(record);
    }

    let partition = img.partition("t", 0).expect("partition");
    assert!(!crate::elr::state::is_leaderless(&img, partition));
    assert!(partition.leader == NodeId(1));
    assert!(partition.leader_epoch == LeaderEpoch(6));
    assert!(img.leader_recovery_state("t", 0) == LeaderRecoveryState::Recovering);
    assert!(
        compute_unfence_changes(&img, NodeId(1), &l, &metrics)
            .await
            .is_empty()
    );
}
