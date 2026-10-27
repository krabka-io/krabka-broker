//! Owned cleanup of a captured topic generation after its deletion commits.

use {
    crate::{
        broker::Broker,
        diskless::hot_tail::HotTailCache,
        log_dir,
        partition::Partition,
        partition_registry::PartitionRegistry,
        wal::quorum::{ShardRemoval, registry::WalShardRegistry, remove_shard},
    },
    krabka_ids::PartitionIndex,
    std::{fs, io::ErrorKind, path::PathBuf, sync::Arc},
    uuid::Uuid,
};

struct CapturedPartition {
    index: PartitionIndex,
    runtime: Arc<Partition>,
    owning_dir: PathBuf,
}

/// Resources that remain owned until an accepted deletion finishes cleanup.
pub(super) struct LocalDeletion {
    registry: Arc<PartitionRegistry>,
    hot_tail: Arc<HotTailCache>,
    wal_shards: Arc<WalShardRegistry>,
    name: String,
    topic_id: Uuid,
    captured: Vec<CapturedPartition>,
}

impl LocalDeletion {
    pub(super) fn snapshot(
        broker: &Broker,
        name: &str,
        topic_id: Uuid,
        indices: &[PartitionIndex],
    ) -> Self {
        Self {
            registry: Arc::clone(&broker.partitions),
            hot_tail: Arc::clone(&broker.hot_tail),
            wal_shards: Arc::clone(&broker.wal_shards),
            name: name.to_owned(),
            topic_id,
            captured: indices
                .iter()
                .filter_map(|&index| {
                    broker.partitions.get(name, index).map(|runtime| {
                        let owning_dir = runtime.log_dir.load_full().as_ref().clone();
                        CapturedPartition {
                            index,
                            runtime,
                            owning_dir,
                        }
                    })
                })
                .collect(),
        }
    }

    pub(super) fn remove(self) {
        for captured in self.captured {
            // UUID-scoped resources cannot belong to a recreated topic.
            self.hot_tail
                .remove_partition(self.topic_id, captured.index);
            if let Err(error) = remove_shard(
                self.wal_shards.as_ref(),
                &captured.owning_dir,
                &self.name,
                self.topic_id,
                captured.index,
                ShardRemoval::All,
            ) {
                tracing::warn!(topic = %self.name, partition = captured.index.get(), %error,
                    "failed to remove deleted topic WAL shard");
            }
            self.registry.remove_captured_with_cleanup(
                &self.name,
                captured.index,
                &captured.runtime,
                || {
                    if let Some(writer) = captured.runtime.retire() {
                        writer.abort();
                    }
                    let dir = log_dir::partition_dir(
                        &captured.owning_dir,
                        &self.name,
                        captured.index.get(),
                    );
                    if let Err(error) = fs::remove_dir_all(dir)
                        && error.kind() != ErrorKind::NotFound
                    {
                        tracing::warn!(topic = %self.name, partition = captured.index.get(), %error,
                        "failed to remove deleted topic partition directory");
                    }
                },
            );
        }
    }
}
