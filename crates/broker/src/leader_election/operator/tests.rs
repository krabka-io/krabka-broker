//! Tests for the operator-triggered elections: the preferred-leader happy
//! path and every refusal it can report, the unclean election an operator
//! forces after the ISR is gone, the controlled-shutdown drain, and the
//! witness replica that none of them may give leadership to.

use assert2::assert;
use krabka_metadata::{LeaderEpoch, MetadataImage};
use uuid::Uuid;

use super::*;
use crate::leader_election::test_support::{
    img_with_partition, liveness_with_alive, no_witnesses, witnesses,
};

#[tokio::test]
async fn preferred_happy_path() {
    let img = img_with_partition("foo", 0, /*leader*/ 2, &[1, 2, 3], &[1, 2, 3]);
    let l = liveness_with_alive(&[1, 2, 3]).await;
    let new_pr = select_new_leader_for_partition(
        &img,
        &l,
        &no_witnesses(),
        "foo",
        0,
        ElectionType::Preferred,
    )
    .await
    .expect("should elect");
    let expected = PartitionRecord {
        topic: "foo".into(),
        partition: 0,
        leader: NodeId(1),
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: vec![NodeId(1), NodeId(2), NodeId(3)],
        leader_epoch: LeaderEpoch(6),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 1,
    };
    assert!(new_pr == expected);
}

#[tokio::test]
async fn preferred_election_rejects_exhausted_metadata_epochs() {
    for (partition_epoch, leader_epoch) in [(i32::MAX, 5), (0, i32::MAX)] {
        let mut img = img_with_partition("foo", 0, 2, &[1, 2, 3], &[1, 2, 3]);
        let mut record = img.partition("foo", 0).expect("seeded partition").clone();
        record.partition_epoch = partition_epoch;
        record.leader_epoch = LeaderEpoch(leader_epoch);
        img.apply(&krabka_metadata::MetadataRecord::V1Partition(record));
        let l = liveness_with_alive(&[1, 2, 3]).await;

        let error = select_new_leader_for_partition(
            &img,
            &l,
            &no_witnesses(),
            "foo",
            0,
            ElectionType::Preferred,
        )
        .await
        .expect_err("exhausted epoch must fail closed");

        assert!(error == ElectError::EpochExhausted);
    }
}

#[tokio::test]
async fn preferred_election_error_cases() {
    // Replicas are always [1, 2, 3]; the preferred leader is replica 1.
    // (current_leader, isr, alive, expected)
    let cases: [(u64, &[u64], &[u64], ElectError); 3] = [
        // Preferred replica 1 is already the leader.
        (
            1,
            &[1, 2, 3],
            &[1, 2, 3],
            ElectError::PreferredAlreadyLeader,
        ),
        // Preferred replica 1 is not in the ISR.
        (2, &[2, 3], &[1, 2, 3], ElectError::PreferredNotInIsr),
        // Preferred replica 1 is in the ISR but dead.
        (2, &[1, 2, 3], &[2, 3], ElectError::PreferredNotAlive),
    ];
    for (leader, isr, alive, expected) in cases {
        let img = img_with_partition("foo", 0, leader, &[1, 2, 3], isr);
        let l = liveness_with_alive(alive).await;
        let err = select_new_leader_for_partition(
            &img,
            &l,
            &no_witnesses(),
            "foo",
            0,
            ElectionType::Preferred,
        )
        .await
        .unwrap_err();
        assert!(
            err == expected,
            "leader {leader}, isr {isr:?}, alive {alive:?}"
        );
    }
}

#[tokio::test]
async fn unclean_happy_path() {
    // ISR is just {1}, broker 1 is dead, brokers 2/3 are alive.
    let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1]);
    let l = liveness_with_alive(&[2, 3]).await;
    let new_pr =
        select_new_leader_for_partition(&img, &l, &no_witnesses(), "foo", 0, ElectionType::Unclean)
            .await
            .expect("unclean should elect");
    let expected = PartitionRecord {
        topic: "foo".into(),
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
    assert!(new_pr == expected);
}

#[tokio::test]
async fn unclean_no_alive_replicas() {
    let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1]);
    let l = liveness_with_alive(&[]).await; // everyone dead
    let err =
        select_new_leader_for_partition(&img, &l, &no_witnesses(), "foo", 0, ElectionType::Unclean)
            .await
            .unwrap_err();
    assert!(err == ElectError::NoEligibleReplica);
}

#[tokio::test]
async fn unclean_isr_member_alive_returns_election_not_needed() {
    let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1, 2]);
    let l = liveness_with_alive(&[1, 2]).await; // ISR has live member
    let err =
        select_new_leader_for_partition(&img, &l, &no_witnesses(), "foo", 0, ElectionType::Unclean)
            .await
            .unwrap_err();
    assert!(err == ElectError::ElectionNotNeeded);
}

#[tokio::test]
async fn shutdown_replacement_picks_alive_isr_member() {
    // Broker 1 is leader and wants to shut down. ISR is {1,2,3}, all alive.
    let img = img_with_partition("foo", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
    let l = liveness_with_alive(&[1, 2, 3]).await;
    let new_pr = select_replacement_leader_for_shutdown(
        &img,
        &l,
        &no_witnesses(),
        "foo",
        0,
        /*shutting_down*/ NodeId(1),
    )
    .await
    .expect("should pick replacement");
    // ISR untouched — shutting-down broker stays in ISR until dead.
    let expected = PartitionRecord {
        topic: "foo".into(),
        partition: 0,
        leader: NodeId(2),
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: vec![NodeId(1), NodeId(2), NodeId(3)],
        leader_epoch: LeaderEpoch(6),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 1,
    };
    assert!(new_pr == expected);
}

#[tokio::test]
async fn shutdown_replacement_skips_dead_isr_members() {
    // Broker 1 (leader) wants to drain. ISR {1,2,3} but 2 is dead.
    // Replacement should be 3.
    let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1, 2, 3]);
    let l = liveness_with_alive(&[1, 3]).await;
    let new_pr =
        select_replacement_leader_for_shutdown(&img, &l, &no_witnesses(), "foo", 0, NodeId(1))
            .await
            .expect("should pick replacement");
    assert!(new_pr.leader == 3);
    assert!(new_pr.leader_epoch == 6);
}

#[tokio::test]
async fn shutdown_replacement_error_cases() {
    // Replicas are always [1, 2, 3]; leader is always broker 1.
    // (isr, alive, shutting_down, expected)
    let cases: [(&[u64], &[u64], u64, ElectError); 3] = [
        // Broker 5 wants to shut down, but leader is 1. No-op.
        (&[1, 2, 3], &[1, 2, 3, 5], 5, ElectError::ElectionNotNeeded),
        // Broker 1 wants to drain. ISR is {1} only (singleton). No
        // other broker eligible.
        (&[1], &[1, 2, 3], 1, ElectError::NoEligibleReplica),
        // Broker 1 wants to drain. ISR {1,2} but 2 is dead; 3 is alive
        // but not in ISR.
        (&[1, 2], &[1, 3], 1, ElectError::NoEligibleReplica),
    ];
    for (isr, alive, shutting_down, expected) in cases {
        let img = img_with_partition("foo", 0, 1, &[1, 2, 3], isr);
        let l = liveness_with_alive(alive).await;
        let err = select_replacement_leader_for_shutdown(
            &img,
            &l,
            &no_witnesses(),
            "foo",
            0,
            NodeId(shutting_down),
        )
        .await
        .unwrap_err();
        assert!(
            err == expected,
            "isr {isr:?}, alive {alive:?}, shutting_down {shutting_down}"
        );
    }
}

#[tokio::test]
async fn shutdown_replacement_unknown_partition() {
    let img = MetadataImage::new(Uuid::nil());
    let l = liveness_with_alive(&[1]).await;
    let err =
        select_replacement_leader_for_shutdown(&img, &l, &no_witnesses(), "ghost", 0, NodeId(1))
            .await
            .unwrap_err();
    assert!(err == ElectError::UnknownTopicOrPartition);
}

#[tokio::test]
async fn unknown_topic_returns_error() {
    let img = MetadataImage::new(Uuid::nil());
    let l = liveness_with_alive(&[]).await;
    let err = select_new_leader_for_partition(
        &img,
        &l,
        &no_witnesses(),
        "ghost",
        0,
        ElectionType::Preferred,
    )
    .await
    .unwrap_err();
    assert!(err == ElectError::UnknownTopicOrPartition);
}

#[tokio::test]
async fn preferred_election_refuses_a_witness_preferred_replica() {
    // Site-aware placement put the witness first in `replicas`, so the
    // preferred replica can never lead.
    let img = img_with_partition("foo", 0, /*leader*/ 2, &[1, 2, 3], &[1, 2, 3]);
    let l = liveness_with_alive(&[1, 2, 3]).await;
    let err = select_new_leader_for_partition(
        &img,
        &l,
        &witnesses(&[1]),
        "foo",
        0,
        ElectionType::Preferred,
    )
    .await
    .unwrap_err();
    assert!(err == ElectError::PreferredIsWitness);
}

#[tokio::test]
async fn operator_unclean_election_skips_a_witness_replica() {
    // Every data replica in the ISR is dead and the operator forces an
    // unclean election. The alive witness 2 must not take leadership, and
    // it must not report the election as unneeded either.
    let img = img_with_partition("foo", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2]);
    let l = liveness_with_alive(&[2, 3]).await;
    let new_pr = select_new_leader_for_partition(
        &img,
        &l,
        &witnesses(&[2]),
        "foo",
        0,
        ElectionType::Unclean,
    )
    .await
    .expect("unclean should elect the data replica");
    let expected = PartitionRecord {
        topic: "foo".into(),
        partition: 0,
        leader: NodeId(3),
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: vec![NodeId(3)],
        leader_epoch: LeaderEpoch(6),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 1,
    };
    assert!(new_pr == expected);
}

#[tokio::test]
async fn controlled_shutdown_never_drains_leadership_to_a_witness() {
    // Broker 1 leads and wants to drain. ISR is {1, 2, 3} with 2 the
    // witness, so the drain target is data replica 3.
    let img = img_with_partition("foo", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
    let l = liveness_with_alive(&[1, 2, 3]).await;
    let new_pr =
        select_replacement_leader_for_shutdown(&img, &l, &witnesses(&[2]), "foo", 0, NodeId(1))
            .await
            .expect("should pick the data replica");
    let expected = PartitionRecord {
        topic: "foo".into(),
        partition: 0,
        leader: NodeId(3),
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: vec![NodeId(1), NodeId(2), NodeId(3)],
        leader_epoch: LeaderEpoch(6),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 1,
    };
    assert!(new_pr == expected);
}

#[tokio::test]
async fn controlled_shutdown_reports_no_eligible_replica_when_only_a_witness_remains() {
    // ISR is {1, 2} with 2 the witness. Nothing can take leadership, so
    // the drain gate must not count this partition.
    let img = img_with_partition("foo", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2]);
    let l = liveness_with_alive(&[1, 2, 3]).await;
    let err =
        select_replacement_leader_for_shutdown(&img, &l, &witnesses(&[2]), "foo", 0, NodeId(1))
            .await
            .unwrap_err();
    assert!(err == ElectError::NoEligibleReplica);
}
