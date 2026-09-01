//! The pure reassignment decision, with no I/O and no controller. Given a
//! partition record and the set of alive brokers it returns the next
//! `PartitionRecord`, which is either a leader handoff or a completion.
//!
//! The logic lives apart from the background task in [`super`] so that a unit
//! test and the `stateright` model checker can drive the policy on its own.

use krabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord};
use krabka_raft::NodeId;
use krabka_verified::reassignment::{ReassignmentAction, reassignment_action};

use crate::heartbeat::controller_state::ControllerLivenessState;

/// Remaps a partition's `directories` vector onto a new `replicas` order. A
/// KIP-455 reassignment changes both the replica membership and the order.
///
/// `directories` runs index-parallel to `replicas`. A verbatim clone after the
/// replica set changed would misalign the slots and break KIP-112 offline-dir
/// failover. A surviving replica keeps its dir UUID. A newly added replica
/// gets `Uuid::nil()`, which means UNASSIGNED, until it reports through
/// `AssignReplicasToDirs`.
pub(crate) fn remap_directories(
    old_replicas: &[NodeId],
    old_directories: &[uuid::Uuid],
    new_replicas: &[NodeId],
) -> Vec<uuid::Uuid> {
    let old: std::collections::HashMap<NodeId, uuid::Uuid> = old_replicas
        .iter()
        .copied()
        .zip(old_directories.iter().copied())
        .collect();
    new_replicas
        .iter()
        .map(|n| old.get(n).copied().unwrap_or_else(uuid::Uuid::nil))
        .collect()
}

/// The pure per-partition reassignment decision. From a partition's current
/// record and the alive set, it returns the next `PartitionRecord`, which is
/// either a leader handoff or a completion. It returns `None` to wait.
///
/// The function does no I/O. It is separate from
/// `compute_reassignment_progress` so that a unit test and a model checker can
/// drive the policy on its own.
pub(crate) fn reassign_one(
    pr: &PartitionRecord,
    alive: &std::collections::HashSet<NodeId>,
) -> Option<PartitionRecord> {
    let target: Vec<NodeId> = pr
        .replicas
        .iter()
        .filter(|r| !pr.removing_replicas.contains(r))
        .copied()
        .collect();
    let eligible_handoffs: Vec<bool> = target
        .iter()
        .map(|n| pr.isr.contains(n) && alive.contains(n))
        .collect();
    let action = reassignment_action(
        pr.adding_replicas.iter().all(|n| pr.isr.contains(n)),
        pr.removing_replicas.contains(&pr.leader),
        &eligible_handoffs,
    );
    if let ReassignmentAction::Handoff(index) = action {
        // The verified index maps to target ∩ ISR ∩ alive.
        let new_leader = target[index];
        let leader_epoch = crate::metadata_epoch::next_leader(pr.leader_epoch)?;
        let partition_epoch = crate::metadata_epoch::next_i32(pr.partition_epoch)?;
        return Some(PartitionRecord {
            topic: pr.topic.clone(),
            partition: pr.partition,
            leader: new_leader,
            leader_epoch,
            replicas: pr.replicas.clone(),
            isr: pr.isr.clone(),
            adding_replicas: pr.adding_replicas.clone(),
            removing_replicas: pr.removing_replicas.clone(),
            directories: pr.directories.clone(),
            partition_epoch,
        });
    }
    if action != ReassignmentAction::Complete {
        return None;
    }
    // Completion phase: switch to the target replica set.
    let new_isr: Vec<NodeId> = pr
        .isr
        .iter()
        .filter(|n| target.contains(n))
        .copied()
        .collect();
    let new_directories = remap_directories(&pr.replicas, &pr.directories, &target);
    let partition_epoch = crate::metadata_epoch::next_i32(pr.partition_epoch)?;
    Some(PartitionRecord {
        topic: pr.topic.clone(),
        partition: pr.partition,
        leader: pr.leader,
        leader_epoch: pr.leader_epoch, // unchanged: leader stays, only replica set changes
        replicas: target,
        isr: new_isr,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: new_directories,
        partition_epoch,
    })
}

/// Pure logic. It scans every in-flight reassignment, and produces a
/// completion record or a leader-handoff record for each one that is ready to
/// advance, followed by the KIP-966 eligible-leader state those records imply.
///
/// A completion narrows the ISR to the target replicas and drops the rest of
/// the replica set, so it is an ISR shrink and a replica-set change at once.
/// [`ElrPublisher`](crate::elr::ElrPublisher) rides the same batch, the way it
/// does on every other path that moves an ISR: without it a replica the
/// partition no longer has could stay in the ELR that
/// `DescribeTopicPartitions` reports.
pub(crate) async fn compute_reassignment_progress(
    image: &MetadataImage,
    liveness: &ControllerLivenessState,
) -> Vec<MetadataRecord> {
    let mut updates = Vec::new();
    // Snapshot the alive set once (single lock) instead of taking the
    // liveness lock per target replica in the leader-handoff branch.
    let alive: std::collections::HashSet<NodeId> = liveness
        .alive_snapshot()
        .await
        .into_iter()
        .map(NodeId)
        .collect();
    for pr in image.reassignments_in_flight() {
        if let Some(next) = reassign_one(pr, &alive) {
            updates.push(MetadataRecord::V1Partition(next));
        }
    }
    crate::elr::ElrPublisher::new(image).extend(&mut updates);
    updates
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_metadata::{BrokerRegistrationRecord, MetadataImage, MetadataRecord, TopicRecord};
    use uuid::Uuid;

    use super::*;
    use crate::reassignment::test_support::{first_partition, img, img_with_dirs, liveness};

    #[test]
    fn remap_directories_preserves_slot_alignment_on_replica_removal() {
        let da = uuid::Uuid::from_u128(0xA);
        let db = uuid::Uuid::from_u128(0xB);
        let dc = uuid::Uuid::from_u128(0xC);
        // replicas [1,2,3] dirs [dA,dB,dC]; reassignment removes broker 2.
        let new = remap_directories(
            &[NodeId(1), NodeId(2), NodeId(3)],
            &[da, db, dc],
            &[NodeId(1), NodeId(3)],
        );
        // broker 1 keeps dA at slot 0; broker 3 keeps dC at slot 1 (NOT dB).
        assert!(new == vec![da, dc]);
    }

    #[test]
    fn remap_directories_assigns_nil_to_new_replica() {
        let da = uuid::Uuid::from_u128(0xA);
        // replicas [1] dirs [dA]; add broker 2 (no dir yet).
        let new = remap_directories(&[NodeId(1)], &[da], &[NodeId(1), NodeId(2)]);
        assert!(new == vec![da, uuid::Uuid::nil()]);
    }

    #[tokio::test]
    async fn completion_preserves_directory_slot_alignment() {
        // replicas=[1,2,3], adding=[3], removing=[2], all in ISR.
        // directories=[dA, dB, dC] — slot 0→broker1, 1→broker2, 2→broker3.
        // After completion target=[1,3]; expected dirs=[dA, dC].
        let da = Uuid::from_u128(0xA);
        let db = Uuid::from_u128(0xB);
        let dc = Uuid::from_u128(0xC);
        let image = img_with_dirs(&[1, 2, 3], &[1, 2, 3], &[3], &[2], 1, &[da, db, dc]);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&image, &l).await;
        assert!(updates.len() == 1);
        let pr = first_partition(&updates[0]);
        // Slot 0 → broker 1 → dA; slot 1 → broker 3 → dC (NOT dB).
        check!(pr.replicas == vec![NodeId(1), NodeId(3)]);
        check!(pr.directories == vec![da, dc]);
        check!(pr.partition_epoch == 1);
    }

    #[tokio::test]
    async fn complete_when_adding_in_isr_writes_target() {
        let img = img(&[1, 2, 3], &[1, 2, 3], &[3], &[2], 1);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.len() == 1);
        let pr = first_partition(&updates[0]);
        // leader and leader_epoch are unchanged (leader didn't change).
        check!(pr.replicas == vec![NodeId(1), NodeId(3)]);
        check!(pr.adding_replicas == Vec::<NodeId>::new());
        check!(pr.removing_replicas == Vec::<NodeId>::new());
        check!(pr.isr == vec![NodeId(1), NodeId(3)]);
        check!(pr.leader == 1);
        check!(pr.leader_epoch == krabka_metadata::LeaderEpoch(5));
        check!(pr.partition_epoch == 1);
    }

    /// KIP-966: a completion is an ISR shrink and a replica-set change at
    /// once, so the eligible-leader state moves with it in the same batch.
    /// Without the publisher on this path a replica the partition no longer
    /// has would stay in the ELR `DescribeTopicPartitions` reports.
    #[tokio::test]
    async fn a_completion_republishes_the_eligible_leader_state() {
        for (label, isr, published, want) in [
            (
                "a completion that reaches min ISR tombstones the key",
                &[1u64, 2u64][..],
                "0:3:",
                None,
            ),
            (
                "a dropped replica leaves the ELR for the last-known set",
                &[1u64][..],
                "0:3:",
                Some("0::3"),
            ),
        ] {
            // replicas=[1,2,3], removing=[3]: the completion drops broker 3
            // from the replica set, and the published ELR still names it.
            let mut image = std::sync::Arc::try_unwrap(img(&[1, 2, 3], isr, &[], &[3], 1))
                .expect("the fixture holds the only reference");
            image.apply(&MetadataRecord::V1TopicConfig(
                krabka_metadata::TopicConfigRecord {
                    topic: "foo".into(),
                    overrides: [
                        (
                            crate::config_keys::MIN_INSYNC_REPLICAS.to_string(),
                            "3".to_string(),
                        ),
                        (
                            crate::config_keys::ELIGIBLE_LEADER_REPLICAS.to_string(),
                            published.to_string(),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                },
            ));
            let l = liveness(&[1, 2, 3]).await;

            let updates = compute_reassignment_progress(&image, &l).await;

            let mut overrides = std::collections::BTreeMap::from([(
                crate::config_keys::MIN_INSYNC_REPLICAS.to_string(),
                "3".to_string(),
            )]);
            if let Some(value) = want {
                overrides.insert(
                    crate::config_keys::ELIGIBLE_LEADER_REPLICAS.to_string(),
                    value.to_string(),
                );
            }
            check!(
                updates[1..]
                    == [MetadataRecord::V1TopicConfig(
                        krabka_metadata::TopicConfigRecord {
                            topic: "foo".into(),
                            overrides,
                        }
                    )],
                "{label}"
            );
        }
    }

    #[tokio::test]
    async fn no_update_emitted_when_waiting_idle_or_no_alive_target() {
        // (case, replicas, isr, adding, removing, leader, alive) — every case
        // should wait / stay idle: compute_reassignment_progress emits nothing.
        let cases = [
            // Adding replica 3 not yet in ISR → wait.
            (
                "adding_not_in_isr",
                vec![1, 2, 3],
                vec![1, 2],
                vec![3],
                vec![2],
                1,
                vec![1, 2, 3],
            ),
            // leader=2, removing=[2]; only target replicas {1,3} in isr but
            // none alive (only 2 alive) — wait.
            (
                "leader_handoff_no_alive_target_replica",
                vec![1, 2, 3],
                vec![1, 2, 3],
                vec![3],
                vec![2],
                2,
                vec![2],
            ),
            // No reassignment in flight → idle partition emits no update.
            (
                "idle_partition",
                vec![1, 2, 3],
                vec![1, 2, 3],
                vec![],
                vec![],
                1,
                vec![1, 2, 3],
            ),
        ];
        for (case, replicas, isr, adding, removing, leader, alive) in cases {
            let img = img(&replicas, &isr, &adding, &removing, leader);
            let l = liveness(&alive).await;
            let updates = compute_reassignment_progress(&img, &l).await;
            assert!(
                updates.is_empty(),
                "case {case}: should wait; got {updates:?}"
            );
        }
    }

    #[tokio::test]
    async fn leader_handoff_when_leader_in_removing() {
        // leader=2, removing=[2]; new leader must come from target ∩ isr = {1,3} ∩ {1,2,3} = {1,3}.
        let img = img(&[1, 2, 3], &[1, 2, 3], &[3], &[2], 2);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.len() == 1);
        let pr = first_partition(&updates[0]);
        assert!(
            pr.leader == 1 || pr.leader == 3,
            "leader was {}",
            pr.leader.0
        );
        // leader_epoch bumped; replica set unchanged — completion happens
        // next tick.
        check!(pr.leader_epoch == krabka_metadata::LeaderEpoch(6));
        check!(pr.partition_epoch == 1);
        check!(pr.adding_replicas == vec![NodeId(3)]);
        check!(pr.removing_replicas == vec![NodeId(2)]);
    }

    #[test]
    fn exhausted_epochs_block_reassignment_transitions() {
        let image = img(&[1, 2, 3], &[1, 2, 3], &[3], &[2], 2);
        let mut record = image.partition("foo", 0).expect("seeded partition").clone();
        let alive = std::collections::HashSet::from([NodeId(1), NodeId(2), NodeId(3)]);

        record.partition_epoch = i32::MAX;
        assert!(reassign_one(&record, &alive).is_none());

        record.partition_epoch = 0;
        record.leader_epoch = krabka_metadata::LeaderEpoch(i32::MAX);
        assert!(reassign_one(&record, &alive).is_none());
    }

    #[tokio::test]
    async fn multiple_partitions_handled_independently() {
        let mut img_inner = MetadataImage::new(Uuid::nil());
        for n in 1..=6u64 {
            img_inner.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    node_id: NodeId(n),
                    broker_epoch: 0,
                    incarnation_id: Uuid::nil(),
                    host: String::new(),
                    port: 0,
                    rack: None,
                    log_dirs: vec![],
                    endpoints: vec![],
                    features: std::collections::BTreeMap::new(),
                },
            ));
        }
        for name in ["foo", "bar"] {
            img_inner.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: name.into(),
                topic_id: Uuid::nil(),
                partitions: 1,
                replication_factor: 3,
            }));
            img_inner.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: name.into(),
                partition: 0,
                leader: NodeId(1),
                replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
                isr: vec![NodeId(1), NodeId(2), NodeId(3)],
                leader_epoch: krabka_metadata::LeaderEpoch(5),
                adding_replicas: vec![NodeId(3)],
                removing_replicas: vec![NodeId(2)],
                directories: vec![],
                partition_epoch: 0,
            }));
        }
        let img = Arc::new(img_inner);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.len() == 2);
    }

    #[tokio::test]
    async fn target_includes_only_replicas_minus_removing() {
        // adding=[4,5], removing=[1,2], replicas=[1,2,3,4,5].
        // target = [3,4,5]. isr ⊇ adding required; isr=[1,2,3,4,5].
        let img = img(&[1, 2, 3, 4, 5], &[1, 2, 3, 4, 5], &[4, 5], &[1, 2], 3);
        let l = liveness(&[1, 2, 3, 4, 5]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.len() == 1);
        let pr = first_partition(&updates[0]);
        assert!(pr.replicas == vec![NodeId(3), NodeId(4), NodeId(5)]);
        assert!(pr.isr == vec![NodeId(3), NodeId(4), NodeId(5)]);
    }

    #[tokio::test]
    async fn isr_intersection_when_some_targets_not_in_isr() {
        // adding=[4], removing=[2]; isr=[1,2,3,4]; target=[1,3,4].
        // new_isr = isr ∩ target = [1,3,4].
        let img = img(&[1, 2, 3, 4], &[1, 2, 3, 4], &[4], &[2], 1);
        let l = liveness(&[1, 2, 3, 4]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.len() == 1);
        let pr = first_partition(&updates[0]);
        assert!(pr.isr == vec![NodeId(1), NodeId(3), NodeId(4)]);
    }
}
