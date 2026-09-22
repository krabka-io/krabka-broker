//! The remote-tier half of a topic deletion: which partitions have archived
//! segments, and the detached cascade that clears them.
//!
//! The snapshot has to be taken before the local tear-down, because after it
//! the `Partition` is gone and with it both the `remote.storage.enable` flag
//! and the topic id. The cascade itself runs afterwards, so the two steps are
//! separate functions that the handler calls on either side of the commit.

use krabka_ids::PartitionIndex;
use krabka_remote_storage::TopicIdPartition;

use crate::{broker::Broker, partition_registry::PartitionRegistry};

/// Snapshots the `(topic_id, partition_id)` of every tiered partition of
/// `topic_name` BEFORE the controller commits the delete and the broker tears
/// down in-memory state.
///
/// After teardown the `Partition` is gone and the broker loses the
/// `remote.storage.enable` flag plus the topic id; this snapshot is the sole
/// record that drives the remote-tier partition-delete cascade.
pub(super) fn tiered_partitions(
    broker: &Broker,
    partitions: &PartitionRegistry,
    image: &krabka_metadata::MetadataImage,
    topic_name: &str,
    local_partitions: &[PartitionIndex],
) -> Vec<TopicIdPartition> {
    if broker.remote_reader.is_none() {
        return Vec::new();
    }
    let Some(topic_id) = image.topic(topic_name).map(|topic| topic.topic_id) else {
        return Vec::new();
    };
    local_partitions
        .iter()
        .copied()
        .filter(|&index| {
            partitions.get(topic_name, index).is_some_and(|partition| {
                partition
                    .log
                    .lock()
                    .is_ok_and(|log| log.config_snapshot().remote_storage_enable)
            })
        })
        .map(|index| TopicIdPartition::new(topic_id, topic_name.to_string(), index.get()))
        .collect()
}

/// Fires off the detached tasks that walk each tiered partition's remote
/// segments through `DeletePartitionMarked` → `DeletePartitionStarted` →
/// per-segment lifecycle → `DeletePartitionFinished`.
///
/// The response returns immediately; failures inside the cascade log at WARN.
/// A write-once archive keeps every archived byte: the cascade clears the
/// broker's metadata but deletes nothing. Deleting a topic must not erase a
/// compliance archive.
pub(super) fn spawn_remote_cascades(broker: &Broker, tiered_to_cascade: Vec<TopicIdPartition>) {
    if let Some(reader) = broker.remote_reader.as_ref() {
        let broker_id = broker.config.broker_id;
        let archive = crate::remote_log_manager::ArchiveMode::from_worm(
            broker.config.remote_storage_worm.as_ref(),
        );
        for tp in tiered_to_cascade {
            let rsm = reader.rsm.clone();
            let rlmm = reader.rlmm.clone();
            let index_cache = reader.index_cache.clone();
            tokio::spawn(crate::remote_log_manager::cascade_remote_partition_delete(
                tp,
                broker_id,
                archive,
                rsm,
                rlmm,
                index_cache,
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::check;
    use krabka_ids::LeaderEpoch;
    use krabka_log::{Log, LogConfig};
    use krabka_remote_storage::{
        RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate,
        RemoteLogSegmentState,
    };
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn tiered_partitions_and_spawn_remote_cascades() {
        let dir = tempfile::tempdir().expect("tempdir");
        let remote_dir = tempfile::tempdir().expect("remote tempdir");
        let mut config = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
        config.remote_storage_backend = Some(crate::config::RemoteStorageBackend::Local {
            dir: remote_dir.path().to_path_buf(),
        });
        config.remote_log_metadata = crate::config::RlmmKind::InMemory;
        let handle = crate::broker::Broker::start(config)
            .await
            .expect("start broker");
        let broker = handle.broker_arc_for_test();

        let topic_id = Uuid::new_v4();
        let mut image = krabka_metadata::MetadataImage::new(topic_id);
        image.apply(&krabka_metadata::MetadataRecord::V1Topic(
            krabka_metadata::TopicRecord {
                name: "tiered_topic".into(),
                topic_id,
                partitions: 1,
                replication_factor: 1,
            },
        ));

        let part_dir = dir.path().join("tiered_topic-0");
        std::fs::create_dir_all(&part_dir).unwrap();
        let log = Log::open(
            &part_dir,
            LogConfig {
                remote_storage_enable: true,
                ..LogConfig::default()
            },
        )
        .unwrap();
        let part = crate::broker::spawn_partition(
            "tiered_topic".to_string(),
            PartitionIndex(0),
            dir.path().to_path_buf(),
            log,
            broker.log_dir_status.clone(),
            broker.producer_state.clone(),
            false,
        );
        broker.partitions.insert(
            std::sync::Arc::from("tiered_topic"),
            PartitionIndex(0),
            part,
        );

        let tiered = tiered_partitions(
            &broker,
            &broker.partitions,
            &image,
            "tiered_topic",
            &[PartitionIndex(0)],
        );
        check!(tiered.len() == 1);
        check!(tiered[0].topic_id == topic_id);
        check!(tiered[0].partition == 0);

        let reader = broker.remote_reader.as_ref().unwrap();
        let seg = RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(tiered[0].clone(), Uuid::from_u128(100)),
            0,
            10,
            100,
            1,
            100,
            krabka_remote_storage::RemoteLogSegmentDetails::new(
                1024,
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(LeaderEpoch(0), 0)]),
            ),
        )
        .unwrap();
        reader.rlmm.add_remote_log_segment_metadata(seg).unwrap();
        let upd = RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: RemoteLogSegmentId::new(tiered[0].clone(), Uuid::from_u128(100)),
            event_timestamp_ms: 101,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        };
        reader.rlmm.update_remote_log_segment_metadata(upd).unwrap();
        check!(
            !reader
                .rlmm
                .list_remote_log_segments(&tiered[0])
                .unwrap()
                .is_empty()
        );

        spawn_remote_cascades(&broker, tiered.clone());

        let mut cleared = false;
        for _ in 0..50 {
            if reader
                .rlmm
                .list_remote_log_segments(&tiered[0])
                .unwrap()
                .is_empty()
            {
                cleared = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        check!(cleared);

        handle.shutdown().await;
    }
}
