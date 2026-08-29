//! Per-row planning for `AlterPartitionReassignments`: it turns one alter row
//! into the `PartitionRecord` the controller submits, or into a wire error
//! code.
//!
//! The logic here is pure. The start path validates the requested target and
//! writes the union of the current and the target replica sets, and the cancel
//! path reverts an in-flight reassignment to the replicas it started from.

use krabka_metadata::{MetadataImage, PartitionRecord};
use krabka_raft::NodeId;

use crate::codes::{
    ELIGIBLE_LEADERS_NOT_AVAILABLE, INVALID_REPLICA_ASSIGNMENT, NO_REASSIGNMENT_IN_PROGRESS,
    UNKNOWN_TOPIC_OR_PARTITION,
};

/// Per-row rejection: a Kafka wire error code and a readable message.
type RowError = (i16, String);

/// Process one (topic, partition, `target_opt`) row from an
/// `AlterPartitionReassignments` request.
///
/// The return values are:
///   - `Ok(Some(PartitionRecord))`: submit this intermediate record
///   - `Ok(None)`: do nothing, because the row is already at target or the
///     alter is empty
///   - `Err((wire_code, message))`: reject this row
pub(crate) fn process_one_partition(
    image: &MetadataImage,
    topic: &str,
    partition: i32,
    target: Option<&[i32]>,
    allow_rf_change: bool,
) -> Result<Option<PartitionRecord>, RowError> {
    let pr = image
        .partition(topic, partition)
        .ok_or((UNKNOWN_TOPIC_OR_PARTITION, "unknown partition".into()))?;

    match target {
        None => cancel_path(pr),
        Some(target_slice) => {
            validate_target(target_slice, image, allow_rf_change, pr)?;
            Ok(start_path(pr, target_slice))
        }
    }
}

fn validate_target(
    target: &[i32],
    image: &MetadataImage,
    allow_rf_change: bool,
    pr: &PartitionRecord,
) -> Result<(), RowError> {
    if target.is_empty() {
        return Err((INVALID_REPLICA_ASSIGNMENT, "empty target".into()));
    }
    // Duplicates.
    let mut seen: std::collections::HashSet<i32> = std::collections::HashSet::new();
    for &n in target {
        if !seen.insert(n) {
            return Err((INVALID_REPLICA_ASSIGNMENT, format!("duplicate replica {n}")));
        }
    }
    // Every node id must be a registered broker.
    for &n in target {
        let Ok(node_id) = u64::try_from(n) else {
            return Err((INVALID_REPLICA_ASSIGNMENT, format!("negative broker {n}")));
        };
        if image.broker(NodeId(node_id)).is_none() {
            return Err((INVALID_REPLICA_ASSIGNMENT, format!("unknown broker {n}")));
        }
    }
    // RF-change check.
    if !allow_rf_change {
        let current_target_len = pr
            .replicas
            .iter()
            .filter(|n| !pr.removing_replicas.contains(n))
            .count();
        if target.len() != current_target_len {
            return Err((
                INVALID_REPLICA_ASSIGNMENT,
                format!(
                    "rf change disallowed: target len {} != current target len {}",
                    target.len(),
                    current_target_len,
                ),
            ));
        }
    }
    Ok(())
}

fn cancel_path(pr: &PartitionRecord) -> Result<Option<PartitionRecord>, RowError> {
    if pr.adding_replicas.is_empty() && pr.removing_replicas.is_empty() {
        return Err((NO_REASSIGNMENT_IN_PROGRESS, "nothing to cancel".into()));
    }
    let reverted_replicas: Vec<NodeId> = pr
        .replicas
        .iter()
        .filter(|n| !pr.adding_replicas.contains(n))
        .copied()
        .collect();
    let reverted_isr: Vec<NodeId> = pr
        .isr
        .iter()
        .filter(|n| !pr.adding_replicas.contains(n))
        .copied()
        .collect();
    let (leader, epoch_bump) = if pr.adding_replicas.contains(&pr.leader) {
        // Leader was an adding replica; revert leadership.
        match reverted_replicas.iter().find(|n| reverted_isr.contains(n)) {
            Some(&n) => (n, 1),
            None => {
                return Err((
                    ELIGIBLE_LEADERS_NOT_AVAILABLE,
                    "no eligible leader after cancel".into(),
                ));
            }
        }
    } else {
        (pr.leader, 0)
    };
    let new_directories =
        crate::reassignment::remap_directories(&pr.replicas, &pr.directories, &reverted_replicas);
    Ok(Some(PartitionRecord {
        topic: pr.topic.clone(),
        partition: pr.partition,
        leader,
        replicas: reverted_replicas,
        isr: reverted_isr,
        leader_epoch: krabka_metadata::LeaderEpoch(pr.leader_epoch.0 + epoch_bump),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: new_directories,
        partition_epoch: pr.partition_epoch + 1,
    }))
}

fn start_path(pr: &PartitionRecord, target: &[i32]) -> Option<PartitionRecord> {
    let target_set: Vec<NodeId> = target
        .iter()
        .map(|&id| NodeId(u64::try_from(id).expect("target validated as non-negative")))
        .collect();
    let current_target: Vec<NodeId> = pr
        .replicas
        .iter()
        .filter(|n| !pr.removing_replicas.contains(n))
        .copied()
        .collect();
    let old: Vec<NodeId> = current_target
        .iter()
        .filter(|n| !target_set.contains(n))
        .copied()
        .collect();
    let new: Vec<NodeId> = target_set
        .iter()
        .filter(|n| !current_target.contains(n))
        .copied()
        .collect();
    if old.is_empty() && new.is_empty() {
        return None; // already at target — no-op
    }
    // replicas = current_target ∪ target (current_target first, then new).
    let mut new_replicas = current_target;
    for n in &new {
        new_replicas.push(*n);
    }
    let new_directories =
        crate::reassignment::remap_directories(&pr.replicas, &pr.directories, &new_replicas);
    Some(PartitionRecord {
        topic: pr.topic.clone(),
        partition: pr.partition,
        leader: pr.leader,
        replicas: new_replicas,
        isr: pr.isr.clone(),
        leader_epoch: pr.leader_epoch,
        adding_replicas: new,
        removing_replicas: old,
        directories: new_directories,
        partition_epoch: pr.partition_epoch + 1,
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{BrokerRegistrationRecord, LeaderEpoch, MetadataRecord, TopicRecord};
    use uuid::Uuid;

    use super::*;

    fn img_with(
        replicas: &[u64],
        isr: &[u64],
        adding: &[u64],
        removing: &[u64],
        leader: u64,
    ) -> MetadataImage {
        img_with_epoch(replicas, isr, adding, removing, leader, 0)
    }

    fn img_with_epoch(
        replicas: &[u64],
        isr: &[u64],
        adding: &[u64],
        removing: &[u64],
        leader: u64,
        partition_epoch: i32,
    ) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        // Register brokers 1..=6 so validate_target accepts target lists.
        for n in 1u64..=6 {
            img.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    node_id: NodeId(n),
                    broker_epoch: 0,
                    incarnation_id: uuid::Uuid::nil(),
                    host: "localhost".into(),
                    port: 9092,
                    rack: None,
                    log_dirs: vec![],
                    endpoints: vec![],
                    features: std::collections::BTreeMap::new(),
                },
            ));
        }
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "foo".into(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: i16::try_from(replicas.len()).expect("replication factor fits i16"),
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(leader),
            replicas: replicas.iter().copied().map(NodeId).collect(),
            isr: isr.iter().copied().map(NodeId).collect(),
            leader_epoch: krabka_metadata::LeaderEpoch(5),
            adding_replicas: adding.iter().copied().map(NodeId).collect(),
            removing_replicas: removing.iter().copied().map(NodeId).collect(),
            directories: vec![],
            partition_epoch,
        }));
        img
    }

    #[test]
    fn validate_target_rejects_negative_broker_id() {
        let image = img_with(&[1], &[1], &[], &[], 1);
        let partition = image.partition("foo", 0).expect("seeded partition");
        let error = validate_target(&[-1], &image, true, partition).expect_err("negative broker");
        assert!(error.0 == INVALID_REPLICA_ASSIGNMENT);
        assert!(error.1.contains("negative broker"));
    }

    #[test]
    fn noop_when_already_at_target() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 2, 3]), true).unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn start_writes_union_replicas() {
        let img = img_with_epoch(&[1, 2, 3], &[1, 2, 3], &[], &[], 1, 11);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 4]), true)
            .expect("ok")
            .expect("Some");
        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
            isr: vec![NodeId(1), NodeId(2), NodeId(3)],
            leader_epoch: LeaderEpoch(5), // unchanged on start
            adding_replicas: vec![NodeId(4)],
            removing_replicas: vec![NodeId(2), NodeId(3)],
            directories: vec![Uuid::nil(); 4],
            partition_epoch: 12,
        };
        assert!(res == expected);
    }

    #[test]
    fn replaces_existing_in_flight_reassignment() {
        // Currently in flight: replicas=[1,2,3,4], adding=[4], removing=[2,3].
        // current_target = [1,4]. New alter target = [5,6].
        // Expected: replicas=[1,4,5,6], adding=[5,6], removing=[1,4].
        let img = img_with(&[1, 2, 3, 4], &[1, 2, 3], &[4], &[2, 3], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[5, 6]), true)
            .expect("ok")
            .expect("Some");
        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(4), NodeId(5), NodeId(6)],
            isr: vec![NodeId(1), NodeId(2), NodeId(3)],
            leader_epoch: LeaderEpoch(5),
            adding_replicas: vec![NodeId(5), NodeId(6)],
            removing_replicas: vec![NodeId(1), NodeId(4)],
            directories: vec![Uuid::nil(); 4],
            partition_epoch: 1,
        };
        assert!(res == expected);
    }

    #[test]
    fn rf_change_rejected_when_disabled() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let err = process_one_partition(&img, "foo", 0, Some(&[1, 2]), false).unwrap_err();
        assert!(err.0 == INVALID_REPLICA_ASSIGNMENT);
    }

    #[test]
    fn rf_change_allowed_when_enabled() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 2]), true)
            .expect("ok")
            .expect("Some");
        assert!(res.removing_replicas == vec![NodeId(3)]);
    }

    #[test]
    fn rf_check_counts_current_target_without_removing_replicas() {
        let img = img_with(&[1, 2, 3, 4], &[1, 3, 4], &[4], &[2], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 3, 4]), false).expect("ok");

        assert!(res.is_none());
    }

    #[test]
    fn cancel_with_leader_in_adding_reverts_leader() {
        // After a successful leader handoff during reassignment, leader=4 (an adding replica).
        // Cancel: leader should revert to whoever in reverted replicas ∩ isr.
        // replicas=[1,2,3,4], adding=[4], removing=[2,3], leader=4, isr=[1,4].
        let img = img_with_epoch(&[1, 2, 3, 4], &[1, 4], &[4], &[2, 3], 4, 11);
        let res = process_one_partition(&img, "foo", 0, None, true)
            .expect("ok")
            .expect("Some");
        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(1), // reverted replicas ∩ isr = [1]
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(1)],
            leader_epoch: LeaderEpoch(6), // bumped
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![Uuid::nil(); 3],
            partition_epoch: 12,
        };
        assert!(res == expected);
    }

    #[test]
    fn cancel_with_only_removing_replicas_is_valid() {
        let img = img_with_epoch(&[1, 2, 3], &[1, 2, 3], &[], &[3], 1, 11);
        let res = process_one_partition(&img, "foo", 0, None, true)
            .expect("ok")
            .expect("Some");

        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(1), NodeId(2), NodeId(3)],
            leader_epoch: LeaderEpoch(5),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![Uuid::nil(); 3],
            partition_epoch: 12,
        };
        assert!(res == expected);
    }

    #[test]
    fn empty_target_rejected() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let err = process_one_partition(&img, "foo", 0, Some(&[]), true).unwrap_err();
        assert!(err.0 == INVALID_REPLICA_ASSIGNMENT);
    }
}
