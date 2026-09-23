//! The leadership gate `ListOffsets` applies before resolving any offset:
//! whether this node may answer for the partition from what it holds
//! locally, or has to send the client elsewhere.
//!
//! Kafka's `ReplicaManager.fetchOffsetForTimestamp` computes
//! `fetchOnlyFromLeader = replicaId != DEBUGGING_REPLICA_ID`
//! (`ReplicaManager.scala` ~1481) and resolves through
//! `getPartitionOrException`, whose `Partition.localLogWithEpochOrThrow`
//! (`Partition.scala` ~1435-1441) raises `NotLeaderOrFollowerException` when
//! the metadata cache knows the partition but this node does not lead it --
//! unless the request is `DEBUGGING_REPLICA_ID` (-2), which is allowed to
//! read a follower's own (possibly lagging) local log -- and
//! `UnknownTopicOrPartitionException` when the cache does not know the
//! partition at all (`ReplicaManager.scala` ~1533-1545). Once past that
//! check, an offline log directory raises `KafkaStorageException`.
//!
//! Without this gate a broker holding the partition only as a follower
//! answered a consumer with its own lagging log end offset instead of
//! sending it to the leader, and a broker that did not hold the partition at
//! all answered `UNKNOWN_TOPIC_OR_PARTITION` even when the topic existed and
//! was led elsewhere.

use std::sync::Arc;

use crate::{codes, partition::Partition, partition_registry::PartitionRegistry};

/// Kafka's `ListOffsetsRequest.DEBUGGING_REPLICA_ID`: the one `replica_id`
/// allowed to read a follower's own local log instead of being redirected to
/// the leader. No follower sends it; it is reserved for offline debugging
/// tools.
pub(super) const DEBUGGING_REPLICA_ID: i32 = -2;

/// Decide whether this node may answer `ListOffsets` for one partition, and
/// hand back the local [`Partition`] to resolve it against when it may.
///
/// # Errors
///
/// Returns the Kafka error code the caller should answer that partition row
/// with: `UNKNOWN_TOPIC_OR_PARTITION` when the metadata image does not know
/// the partition at all, `NOT_LEADER_OR_FOLLOWER` when the image knows it but
/// this node does not lead it and the request is not the debugging sentinel,
/// and `KAFKA_STORAGE_ERROR` when the partition's log directory is offline.
pub(super) fn resolve_leadership(
    topic_name: &str,
    partition_index: i32,
    replica_id: i32,
    partitions: &PartitionRegistry,
    log_dir_status: &crate::log_dir_status::LogDirRegistry,
    image: &krabka_metadata::MetadataImage,
    node_id: krabka_metadata::NodeId,
) -> Result<Arc<Partition>, i16> {
    let Some(record) = image.partition(topic_name, partition_index) else {
        return Err(codes::UNKNOWN_TOPIC_OR_PARTITION);
    };
    if record.leader != node_id && replica_id != DEBUGGING_REPLICA_ID {
        return Err(codes::NOT_LEADER_OR_FOLLOWER);
    }
    let Some(partition) = partitions.get(topic_name, krabka_ids::PartitionIndex(partition_index))
    else {
        return Err(codes::UNKNOWN_TOPIC_OR_PARTITION);
    };
    if log_dir_status.is_offline(&partition.log_dir.load()) {
        return Err(codes::KAFKA_STORAGE_ERROR);
    }
    Ok(partition)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
    use uuid::Uuid;

    use super::*;
    use crate::log_dir_status::LogDirRegistry;

    fn image_with_topic(topic: &str, leader: u64) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: topic.into(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: 2,
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.into(),
            partition: 0,
            leader: krabka_audit::NodeId(leader),
            replicas: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            isr: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
        img
    }

    fn local_partition(dir: &std::path::Path) -> Arc<Partition> {
        let log = krabka_log::Log::open(
            crate::log_dir::partition_dir(dir, "orders", 0),
            krabka_log::LogConfig::default(),
        )
        .expect("open log");
        crate::broker::spawn_partition(
            "orders".into(),
            krabka_ids::PartitionIndex(0),
            dir.to_path_buf(),
            log,
            LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            false,
        )
    }

    #[test]
    fn image_without_the_partition_is_always_unknown_topic_or_partition() {
        let empty = MetadataImage::new(Uuid::nil());
        let partitions = PartitionRegistry::new();
        let log_dir_status = LogDirRegistry::default();
        for replica_id in [-1, -2, 3] {
            let got = resolve_leadership(
                "orders",
                0,
                replica_id,
                &partitions,
                &log_dir_status,
                &empty,
                krabka_audit::NodeId(1),
            );
            assert!(
                got.err() == Some(codes::UNKNOWN_TOPIC_OR_PARTITION),
                "{replica_id}"
            );
        }
    }

    #[tokio::test]
    async fn known_partition_not_held_locally_reflects_the_image_leader_unless_debugging() {
        let image = image_with_topic("orders", 2);
        let partitions = PartitionRegistry::new();
        let log_dir_status = LogDirRegistry::default();
        let node_id = krabka_audit::NodeId(1);

        for (replica_id, want) in [
            (-1, Err(codes::NOT_LEADER_OR_FOLLOWER)),
            (3, Err(codes::NOT_LEADER_OR_FOLLOWER)),
            // The debugging sentinel still needs a local replica to read; this
            // node holds none, so it is UNKNOWN_TOPIC_OR_PARTITION rather than
            // NOT_LEADER_OR_FOLLOWER.
            (-2, Err(codes::UNKNOWN_TOPIC_OR_PARTITION)),
        ] {
            let got = resolve_leadership(
                "orders",
                0,
                replica_id,
                &partitions,
                &log_dir_status,
                &image,
                node_id,
            )
            .map(|_| ());
            assert!(got == want, "replica_id={replica_id}");
        }
    }

    #[tokio::test]
    async fn following_locally_is_not_leader_or_follower_unless_debugging() {
        let dir = tempfile::tempdir().expect("tempdir");
        let image = image_with_topic("orders", 2);
        let partitions = PartitionRegistry::new();
        partitions.insert(
            "orders".into(),
            krabka_ids::PartitionIndex(0),
            local_partition(dir.path()),
        );
        let log_dir_status = LogDirRegistry::default();
        let node_id = krabka_audit::NodeId(1);

        let refused = resolve_leadership(
            "orders",
            0,
            -1,
            &partitions,
            &log_dir_status,
            &image,
            node_id,
        )
        .map(|_| ());
        assert!(refused == Err(codes::NOT_LEADER_OR_FOLLOWER));

        let debugging = resolve_leadership(
            "orders",
            0,
            DEBUGGING_REPLICA_ID,
            &partitions,
            &log_dir_status,
            &image,
            node_id,
        );
        assert!(debugging.is_ok());
    }

    #[tokio::test]
    async fn leading_locally_resolves_and_an_offline_log_dir_is_reported() {
        let dir = tempfile::tempdir().expect("tempdir");
        let image = image_with_topic("orders", 1);
        let partitions = PartitionRegistry::new();
        let partition = local_partition(dir.path());
        partitions.insert(
            "orders".into(),
            krabka_ids::PartitionIndex(0),
            partition.clone(),
        );
        let node_id = krabka_audit::NodeId(1);

        let log_dir_status = LogDirRegistry::default();
        let resolved = resolve_leadership(
            "orders",
            0,
            -1,
            &partitions,
            &log_dir_status,
            &image,
            node_id,
        );
        assert!(resolved.is_ok());

        log_dir_status.mark_offline(&partition.log_dir.load(), "test");
        let offline = resolve_leadership(
            "orders",
            0,
            -1,
            &partitions,
            &log_dir_status,
            &image,
            node_id,
        )
        .map(|_| ());
        assert!(offline == Err(codes::KAFKA_STORAGE_ERROR));
    }
}
