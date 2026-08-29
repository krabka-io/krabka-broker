//! Propagation of a topic's config overrides from the metadata image onto
//! every locally-hosted partition of that topic.

use std::collections::HashSet;

use krabka_ids::PartitionIndex;
use krabka_log::LogConfig;
use krabka_metadata::MetadataImage;
use tracing::warn;

use super::TopicPartition;
use crate::partition_registry::PartitionRegistry;

/// Push topic-config overrides onto every locally-hosted partition in
/// `desired`. The call is idempotent, because the same `LogConfig` sent twice
/// is a cheap noop write inside `Log::set_config`. Errors on individual
/// partitions log through `warn!` and do not propagate.
pub(crate) async fn push_topic_configs(
    desired: &HashSet<TopicPartition>,
    partitions: &PartitionRegistry,
    image: &MetadataImage,
    base: &LogConfig,
) {
    let empty: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    for (topic, partition) in desired {
        let Some(part) = partitions.get(topic, PartitionIndex(*partition)) else {
            continue;
        };
        let overrides = image.topic_config(topic).unwrap_or(&empty);
        if let Err(e) = part.apply_log_config_overrides(overrides, base).await {
            warn!(
                topic = %topic, partition = partition, error = %e,
                "supervisor: apply_log_config_overrides failed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_units::hours;

    use super::*;
    use crate::replicator_supervisor::{
        materialize::{MaterializePartitionConfig, materialize_partition},
        test_support::await_until,
    };

    #[tokio::test]
    async fn push_topic_configs_pushes_overrides_to_local_partition() {
        use std::collections::BTreeMap;

        use krabka_log::LogConfig;
        use krabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
        use tempfile::tempdir;
        use uuid::Uuid;

        // Build an image with a topic + partition record + V1TopicConfig.
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: krabka_audit::NodeId(1),
            replicas: vec![krabka_audit::NodeId(1)],
            isr: vec![krabka_audit::NodeId(1)],
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
        let mut overrides = BTreeMap::new();
        overrides.insert("retention.ms".to_string(), "60000".to_string());
        img.apply(&MetadataRecord::V1TopicConfig(
            krabka_metadata::TopicConfigRecord {
                topic: "t".into(),
                overrides,
            },
        ));

        // Materialize the partition on disk.
        let dir = tempdir().expect("tempdir");
        let partitions = Arc::new(PartitionRegistry::new());
        let base = LogConfig {
            segment_size: krabka_units::mebibytes(1),
            ..LogConfig::default()
        };
        materialize_partition(MaterializePartitionConfig {
            partitions: &partitions,
            topic: "t",
            topic_id: None,
            partition: 0,
            log_dirs: &[dir.path().to_path_buf()],
            log_config: &base,
            log_dir_status: &crate::log_dir_status::LogDirRegistry::default(),
            producer_state: &Arc::new(crate::producer_state::ProducerState::new()),
            producer_id_expiration: hours(24),
            max_produce_group: 1_024,
            partition_writer_queue_depth: 64,
            diskless_wal_local_replica_count: 3,
            diskless: false,
            hot_tail: None,
            wal_shards: None,
            sequencer: None,
        })
        .expect("materialize");

        // Call push_topic_configs directly.
        let mut desired = HashSet::new();
        desired.insert(("t".to_string(), 0));
        push_topic_configs(&desired, &partitions, &img, &base).await;

        // Wait until the writer actor applies the SetLogConfig message and the
        // partition's Log reports retention.ms=60s.
        let part = partitions
            .get("t", PartitionIndex(0))
            .expect("partition materialized");
        await_until("retention.ms=60s applied to partition log", || {
            part.log
                .lock()
                .expect("log lock")
                .config_snapshot()
                .retention
                == Some(krabka_units::minutes(1))
        })
        .await;
        let snap = part.log.lock().expect("log lock").config_snapshot();
        assert!(snap.retention == Some(krabka_units::minutes(1)));
        assert!(snap.segment_size == krabka_units::mebibytes(1));
    }

    #[tokio::test]
    async fn push_topic_configs_with_no_overrides_uses_defaults() {
        use krabka_log::LogConfig;
        use krabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
        use tempfile::tempdir;
        use uuid::Uuid;

        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: krabka_audit::NodeId(1),
            replicas: vec![krabka_audit::NodeId(1)],
            isr: vec![krabka_audit::NodeId(1)],
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));

        let dir = tempdir().expect("tempdir");
        let partitions = Arc::new(PartitionRegistry::new());
        materialize_partition(MaterializePartitionConfig {
            partitions: &partitions,
            topic: "t",
            topic_id: None,
            partition: 0,
            log_dirs: &[dir.path().to_path_buf()],
            log_config: &LogConfig::default(),
            log_dir_status: &crate::log_dir_status::LogDirRegistry::default(),
            producer_state: &Arc::new(crate::producer_state::ProducerState::new()),
            producer_id_expiration: hours(24),
            max_produce_group: 1_024,
            partition_writer_queue_depth: 64,
            diskless_wal_local_replica_count: 3,
            diskless: false,
            hot_tail: None,
            wal_shards: None,
            sequencer: None,
        })
        .expect("materialize");

        let mut desired = HashSet::new();
        desired.insert(("t".to_string(), 0));
        push_topic_configs(&desired, &partitions, &img, &LogConfig::default()).await;

        // No overrides → default retention applies. Wait until the writer actor
        // has processed the push (the log already carries the default, so this
        // resolves as soon as the config snapshot matches).
        let part = partitions.get("t", PartitionIndex(0)).expect("partition");
        await_until("default retention applied to partition log", || {
            part.log
                .lock()
                .expect("log lock")
                .config_snapshot()
                .retention
                == LogConfig::default().retention
        })
        .await;
        let snap = part.log.lock().expect("log lock").config_snapshot();
        assert!(snap.retention == LogConfig::default().retention);
    }
}
