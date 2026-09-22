//! Tearing down the local state of a topic whose deletion the metadata quorum
//! has already committed.
//!
//! Three things have to go, in order: the partition-registry entries, the
//! diskless WAL shards, and the on-disk log directories. JBOD means a
//! partition may live in any configured log dir, so each one is resolved
//! rather than assumed.

use std::path::PathBuf;

use krabka_ids::PartitionIndex;
use uuid::Uuid;

use crate::{broker::Broker, log_dir, partition_registry::PartitionRegistry};

/// Removes every local partition of a deleted topic from the registry, the WAL
/// shard registry, and disk.
pub(super) fn remove_local_partitions(
    broker: &Broker,
    partitions: &PartitionRegistry,
    log_dirs: &[PathBuf],
    name: &str,
    topic_id: Option<Uuid>,
    local_partitions: Vec<PartitionIndex>,
) {
    for idx in local_partitions {
        if let Some(partition) = partitions.remove(name, idx)
            && let Some(writer) = partition.take_writer_handle()
        {
            writer.abort();
        }
        if let Some(topic_id) = topic_id {
            broker.hot_tail.remove_partition(topic_id, idx);
        }
        // JBOD: the partition may live in any log dir; resolve
        // its actual location (existing-location wins).
        let dir = log_dir::place_partition_dir(log_dirs, name, idx.get());
        if let (Some(topic_id), Some(owning_dir)) = (topic_id, dir.parent())
            && let Err(error) = crate::wal::quorum::remove_shard(
                broker.wal_shards.as_ref(),
                owning_dir,
                name,
                topic_id,
                idx,
            )
        {
            tracing::warn!(
                topic = %name,
                partition = idx.get(),
                error = %error,
                "failed to remove deleted topic WAL shard"
            );
        }
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;

    use super::*;
    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    #[tokio::test]
    async fn remove_local_partitions_removes_partition_directory() {
        let (broker_handle, dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();

        let part_dir = dir.path().join("doomed_topic-0");
        std::fs::create_dir_all(&part_dir).unwrap();
        check!(part_dir.exists());

        remove_local_partitions(
            &broker,
            &broker.partitions,
            &[dir.path().to_path_buf()],
            "doomed_topic",
            Some(Uuid::new_v4()),
            vec![PartitionIndex(0)],
        );

        check!(!part_dir.exists());
        broker_handle.shutdown().await;
    }
}
