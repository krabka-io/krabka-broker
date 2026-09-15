//! Tests for the operator-triggered elections: the preferred-leader happy
//! path and every refusal it can report, the unclean election an operator
//! forces after the ISR is gone, and the witness replica that neither may give
//! leadership to.

use assert2::assert;
use krabka_metadata::{LeaderEpoch, MetadataImage};
use uuid::Uuid;

use super::*;
use crate::leader_election::test_support::{
    alive_set, img_with_partition, no_witnesses, witnesses,
};

#[tokio::test]
async fn preferred_happy_path() {
    let img = img_with_partition("foo", 0, /*leader*/ 2, &[1, 2, 3], &[1, 2, 3]);
    let l = alive_set(&[1, 2, 3]);
    let new_pr = select_new_leader_for_partition(
        &img,
        &l,
        &no_witnesses(),
        "foo",
        0,
        ElectionType::Preferred,
    )
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
        let l = alive_set(&[1, 2, 3]);

        let error = select_new_leader_for_partition(
            &img,
            &l,
            &no_witnesses(),
            "foo",
            0,
            ElectionType::Preferred,
        )
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
        let l = alive_set(alive);
        let err = select_new_leader_for_partition(
            &img,
            &l,
            &no_witnesses(),
            "foo",
            0,
            ElectionType::Preferred,
        )
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
    let l = alive_set(&[2, 3]);
    let new_pr =
        select_new_leader_for_partition(&img, &l, &no_witnesses(), "foo", 0, ElectionType::Unclean)
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
    let l = alive_set(&[]); // everyone dead
    let err =
        select_new_leader_for_partition(&img, &l, &no_witnesses(), "foo", 0, ElectionType::Unclean)
            .unwrap_err();
    assert!(err == ElectError::NoEligibleReplica);
}

#[tokio::test]
async fn unclean_isr_member_alive_returns_election_not_needed() {
    let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1, 2]);
    let l = alive_set(&[1, 2]); // ISR has live member
    let err =
        select_new_leader_for_partition(&img, &l, &no_witnesses(), "foo", 0, ElectionType::Unclean)
            .unwrap_err();
    assert!(err == ElectError::ElectionNotNeeded);
}

#[tokio::test]
async fn unknown_topic_returns_error() {
    let img = MetadataImage::new(Uuid::nil());
    let l = alive_set(&[]);
    let err = select_new_leader_for_partition(
        &img,
        &l,
        &no_witnesses(),
        "ghost",
        0,
        ElectionType::Preferred,
    )
    .unwrap_err();
    assert!(err == ElectError::UnknownTopicOrPartition);
}

#[tokio::test]
async fn preferred_election_refuses_a_witness_preferred_replica() {
    // Site-aware placement put the witness first in `replicas`, so the
    // preferred replica can never lead.
    let img = img_with_partition("foo", 0, /*leader*/ 2, &[1, 2, 3], &[1, 2, 3]);
    let l = alive_set(&[1, 2, 3]);
    let err = select_new_leader_for_partition(
        &img,
        &l,
        &witnesses(&[1]),
        "foo",
        0,
        ElectionType::Preferred,
    )
    .unwrap_err();
    assert!(err == ElectError::PreferredIsWitness);
}

#[tokio::test]
async fn operator_unclean_election_skips_a_witness_replica() {
    // Every data replica in the ISR is dead and the operator forces an
    // unclean election. The alive witness 2 must not take leadership, and
    // it must not report the election as unneeded either.
    let img = img_with_partition("foo", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2]);
    let l = alive_set(&[2, 3]);
    let new_pr = select_new_leader_for_partition(
        &img,
        &l,
        &witnesses(&[2]),
        "foo",
        0,
        ElectionType::Unclean,
    )
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
