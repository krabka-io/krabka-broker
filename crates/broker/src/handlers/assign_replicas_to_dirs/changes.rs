//! The pure metadata delta that turns a reported directory into a
//! `PartitionDirAssignment` record.
//!
//! Nothing here touches the controller or the network, so the whole
//! request-to-record mapping is unit-testable against a hand-built
//! `MetadataImage`.

use krabka_metadata::{MetadataImage, MetadataRecord, PartitionDirAssignmentRecord};
use krabka_protocol::owned::assign_replicas_to_dirs_request::AssignReplicasToDirsRequest;

/// Collects every `MetadataRecord` change from the directories, topics, and
/// partitions in `req`. It calls `assignment_changes` for each partition
/// entry. The function is pure and does no I/O.
pub(crate) fn collect_assignment_changes(
    image: &MetadataImage,
    broker_id: u64,
    req: &AssignReplicasToDirsRequest,
) -> Vec<MetadataRecord> {
    let mut changes: Vec<MetadataRecord> = Vec::new();
    for dir in &req.directories {
        let dir_uuid = uuid::Uuid::from_bytes(dir.id.0);
        for t in &dir.topics {
            let topic_uuid = uuid::Uuid::from_bytes(t.topic_id.0);
            for p in &t.partitions {
                changes.extend(assignment_changes(
                    image,
                    broker_id,
                    topic_uuid,
                    p.partition_index,
                    dir_uuid,
                ));
            }
        }
    }
    changes
}

/// Computes the directory-assignment delta, of 0 or 1 records, that records
/// the replica of `(topic_id, partition)` on `broker_id` as living on
/// `dir_uuid`. This function is pure.
///
/// The result is empty when the topic or partition is unknown, when the broker
/// is not a replica, or when the slot already holds `dir_uuid`. The function
/// is therefore idempotent and avoids churn.
///
/// It emits a [`MetadataRecord::V1PartitionDirAssignment`] DELTA instead of a
/// full `V1Partition`. On apply, the delta merges ONLY the one replica's slot
/// in `directories`, and never touches leader, isr, replicas, adding, or
/// removing. A full read-modify-write here, built from a slightly stale image
/// read, would race a concurrent `AlterPartitionReassignments` and revert
/// `adding_replicas`. The delta does not depend on order (KIP-858).
fn assignment_changes(
    image: &MetadataImage,
    broker_id: u64,
    topic_id: uuid::Uuid,
    partition: i32,
    dir_uuid: uuid::Uuid,
) -> Vec<MetadataRecord> {
    let Some(topic_name) = image
        .topics()
        .find(|tr| tr.topic_id == topic_id)
        .map(|tr| tr.name.clone())
    else {
        return Vec::new();
    };
    let Some(pr) = image.partition(&topic_name, partition) else {
        return Vec::new();
    };
    let replica_slot = pr.replicas.iter().position(|n| n.0 == broker_id);
    let already_assigned =
        replica_slot.and_then(|slot| pr.directories.get(slot)) == Some(&dir_uuid);
    let slot = match krabka_verified::directory_assignment_decision(replica_slot, already_assigned)
    {
        krabka_verified::DirectoryAssignmentDecision::Ignore
        | krabka_verified::DirectoryAssignmentDecision::NoOp => return Vec::new(),
        krabka_verified::DirectoryAssignmentDecision::Assign(slot) => slot,
    };
    let Some(&replica) = pr.replicas.get(slot) else {
        return Vec::new();
    };
    vec![MetadataRecord::V1PartitionDirAssignment(
        PartitionDirAssignmentRecord {
            topic: topic_name,
            partition,
            replica,
            directory: dir_uuid,
        },
    )]
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
    use krabka_protocol::{
        owned::assign_replicas_to_dirs_request::{
            DirectoryData as ReqDirData, PartitionData as ReqPartData, TopicData as ReqTopicData,
        },
        primitives::uuid::Uuid as ProtocolUuid,
    };

    use super::*;

    #[test]
    fn sets_reporting_brokers_directory_slot() {
        let topic_id = uuid::Uuid::from_u128(0x7);
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id,
            partitions: 1,
            replication_factor: 2,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: krabka_audit::NodeId(1),
            replicas: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            isr: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![uuid::Uuid::nil(), uuid::Uuid::nil()],
            partition_epoch: 0,
        }));
        let dir = uuid::Uuid::from_u128(0xAA);
        let changes = assignment_changes(&image, 2, topic_id, 0, dir);
        let MetadataRecord::V1PartitionDirAssignment(r) = &changes[0] else {
            panic!("expected V1PartitionDirAssignment")
        };
        let expected = PartitionDirAssignmentRecord {
            topic: "t".into(),
            partition: 0,
            replica: krabka_audit::NodeId(2),
            directory: dir,
        };
        assert!(*r == expected);
    }

    #[test]
    fn idempotent_when_slot_already_set() {
        let topic_id = uuid::Uuid::from_u128(0x7);
        let dir = uuid::Uuid::from_u128(0xAA);
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id,
            partitions: 1,
            replication_factor: 2,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: krabka_audit::NodeId(1),
            replicas: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            isr: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![uuid::Uuid::nil(), dir],
            partition_epoch: 0,
        }));
        assert!(assignment_changes(&image, 2, topic_id, 0, dir).is_empty());
    }

    #[test]
    fn delta_preserves_replica_order_and_only_changes_the_reporting_slot() {
        let (mut image, topic_id) = make_image_with_broker2_replica();
        let dir = uuid::Uuid::from_u128(0xAA);
        let changes = assignment_changes(&image, 2, topic_id, 0, dir);
        assert!(changes.len() == 1);
        image.apply(&changes[0]);

        let partition = image.partition("t", 0).expect("updated partition");
        assert!(partition.replicas == vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)]);
        assert!(partition.directories == vec![uuid::Uuid::nil(), dir]);
        assert!(assignment_changes(&image, 2, topic_id, 0, dir).is_empty());
    }

    #[test]
    fn empty_when_broker_not_a_replica() {
        let topic_id = uuid::Uuid::from_u128(0x7);
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id,
            partitions: 1,
            replication_factor: 2,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: krabka_audit::NodeId(1),
            replicas: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            isr: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![uuid::Uuid::nil(), uuid::Uuid::nil()],
            partition_epoch: 0,
        }));
        assert!(
            assignment_changes(&image, 99, topic_id, 0, uuid::Uuid::from_u128(0xAA)).is_empty()
        );
    }

    // ── collect_assignment_changes ────────────────────────────────────────────

    /// Builds a minimal image with one topic and one partition, where broker
    /// 2 is a replica. It returns the topic UUID, so callers can put it in the
    /// request.
    fn make_image_with_broker2_replica() -> (MetadataImage, uuid::Uuid) {
        let topic_id = uuid::Uuid::from_u128(0x42);
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id,
            partitions: 1,
            replication_factor: 2,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: krabka_audit::NodeId(1),
            replicas: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            isr: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![uuid::Uuid::nil(), uuid::Uuid::nil()],
            partition_epoch: 0,
        }));
        (image, topic_id)
    }

    #[test]
    fn collect_assignment_changes_produces_one_change_for_known_partition() {
        let (image, topic_id) = make_image_with_broker2_replica();
        let dir_uuid = uuid::Uuid::from_u128(0xAA);

        // Build a request where broker 2 reports partition 0 on dir 0xAA.
        let req = AssignReplicasToDirsRequest {
            broker_id: 2,
            broker_epoch: -1,
            directories: vec![ReqDirData {
                id: ProtocolUuid(dir_uuid.into_bytes()),
                topics: vec![ReqTopicData {
                    topic_id: ProtocolUuid(topic_id.into_bytes()),
                    partitions: vec![ReqPartData {
                        partition_index: 0,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let changes = collect_assignment_changes(&image, 2, &req);
        assert!(
            changes.len() == 1,
            "expected one change, got {}",
            changes.len()
        );
        let MetadataRecord::V1PartitionDirAssignment(r) = &changes[0] else {
            panic!("expected V1PartitionDirAssignment");
        };
        // The delta names broker 2's replica of (t, 0) on dir_uuid; on apply it
        // merges only slot 1, leaving slot 0 (broker 1) untouched.
        let expected = PartitionDirAssignmentRecord {
            topic: "t".into(),
            partition: 0,
            replica: krabka_audit::NodeId(2),
            directory: dir_uuid,
        };
        assert!(*r == expected);
    }

    #[test]
    fn collect_assignment_changes_empty_for_unknown_partition() {
        let (image, topic_id) = make_image_with_broker2_replica();
        let dir_uuid = uuid::Uuid::from_u128(0xAA);

        // Request a partition index that doesn't exist (partition 99).
        let req = AssignReplicasToDirsRequest {
            broker_id: 2,
            broker_epoch: -1,
            directories: vec![ReqDirData {
                id: ProtocolUuid(dir_uuid.into_bytes()),
                topics: vec![ReqTopicData {
                    topic_id: ProtocolUuid(topic_id.into_bytes()),
                    partitions: vec![ReqPartData {
                        partition_index: 99,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let changes = collect_assignment_changes(&image, 2, &req);
        assert!(
            changes.is_empty(),
            "unknown partition must yield no changes"
        );
    }
}
