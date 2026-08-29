//! Teardown of the local runtime and on-disk state for partitions the metadata
//! image no longer assigns to this broker, either because the topic was deleted
//! or because the replica was reassigned elsewhere.

use std::collections::{HashMap, HashSet};

use krabka_metadata::MetadataImage;
use tracing::warn;

use super::{ReplicatorSupervisor, TopicPartition};

impl ReplicatorSupervisor {
    pub(super) fn prune_deleted_topic_partitions(&self, image: &MetadataImage) {
        let current_topic_ids = image
            .topics()
            .map(|topic| (topic.name.clone(), topic.topic_id))
            .collect::<HashMap<_, _>>();
        let obsolete_topics = {
            let mut known_topic_ids = self
                .known_topic_ids
                .lock()
                .expect("replicator supervisor topic identities poisoned");
            let obsolete = known_topic_ids
                .iter()
                .filter(|(name, id)| current_topic_ids.get(*name) != Some(*id))
                .map(|(name, id)| (name.clone(), *id))
                .collect::<HashMap<_, _>>();
            *known_topic_ids = current_topic_ids;
            obsolete
        };
        for partition in self.partitions.arcs() {
            let Some(&topic_id) = obsolete_topics.get(&partition.topic) else {
                continue;
            };
            self.prune_partition(&partition, topic_id, false);
        }
    }

    pub(super) fn prune_unassigned_partitions(
        &self,
        local_set: &HashSet<TopicPartition>,
        image: &MetadataImage,
    ) {
        for partition in self.partitions.arcs() {
            let key = (partition.topic.clone(), partition.index.get());
            let Some(topic_id) = image.topic(&key.0).map(|topic| topic.topic_id) else {
                continue;
            };
            if image.partition(&key.0, key.1).is_none() || local_set.contains(&key) {
                continue;
            }
            let shard = crate::wal::quorum::registry::ShardId {
                topic_id,
                partition: partition.index,
            };
            self.prune_partition(&partition, topic_id, self.wal_shards.local_is_voter(shard));
        }
    }

    fn prune_partition(
        &self,
        partition: &crate::partition::Partition,
        topic_id: uuid::Uuid,
        preserve_follower: bool,
    ) {
        let topic = &partition.topic;
        let index = partition.index;
        let Some(removed) = self.partitions.remove(topic, index) else {
            return;
        };
        if let Some(writer) = removed.take_writer_handle() {
            writer.abort();
        }
        self.reported_dirs.remove(&(topic.clone(), index.get()));
        let owning_dir = removed.log_dir.load_full();
        let remove = if preserve_follower {
            crate::wal::quorum::remove_leader_shard(
                self.wal_shards.as_ref(),
                &owning_dir,
                topic,
                topic_id,
                index,
            )
        } else {
            crate::wal::quorum::remove_shard(
                self.wal_shards.as_ref(),
                &owning_dir,
                topic,
                topic_id,
                index,
            )
        };
        if let Err(error) = remove {
            warn!(
                topic = %topic,
                partition = index.get(),
                error = %error,
                "failed to prune WAL shard"
            );
        }
        let partition_dir = crate::log_dir::partition_dir(&owning_dir, topic, index.get());
        if let Err(error) = remove_partition_dir(&partition_dir) {
            warn!(
                topic = %topic,
                partition = index.get(),
                path = %partition_dir.display(),
                error = %error,
                "failed to prune partition directory"
            );
        }
    }
}

fn remove_partition_dir(path: &std::path::Path) -> std::io::Result<()> {
    match std::fs::remove_dir_all(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_ids::PartitionIndex;
    use krabka_raft::NodeId;

    use super::*;
    use crate::replicator_supervisor::test_support::{
        image_with, partition_record, supervisor_fixture, topic_record,
    };

    #[tokio::test]
    async fn reconcile_prunes_deleted_topic_partitions_but_keeps_live_topics() {
        #[derive(Debug, PartialEq, Eq)]
        struct PartitionState {
            topic: &'static str,
            registered: bool,
            directory_exists: bool,
            runtime_reused: Option<bool>,
        }

        let live_topic = topic_record("live", 1);
        let live_partition = partition_record("live", 0, NodeId(2), vec![NodeId(2)], 0);
        let active = image_with(&[
            topic_record("deleted", 1),
            partition_record("deleted", 0, NodeId(2), vec![NodeId(2)], 0),
            live_topic.clone(),
            live_partition.clone(),
            topic_record("recreated", 1),
            partition_record("recreated", 0, NodeId(2), vec![NodeId(2)], 0),
        ]);
        let after_delete = image_with(&[
            live_topic,
            live_partition,
            topic_record("recreated", 1),
            partition_record("recreated", 0, NodeId(2), vec![NodeId(2)], 0),
        ]);
        let (supervisor, partitions, _reporter, dir) = supervisor_fixture(active.clone());
        supervisor
            .materialize_local_partition(&active, "startup-only", 0)
            .expect("startup-only partition");
        supervisor.reconcile(&active).await;
        let original = ["deleted", "live", "recreated", "startup-only"]
            .into_iter()
            .map(|topic| {
                (
                    topic,
                    partitions
                        .get(topic, PartitionIndex(0))
                        .expect("original partition"),
                )
            })
            .collect::<HashMap<_, _>>();
        let deleted_topic_id = active.topic("deleted").expect("deleted topic").topic_id;
        let deleted_shard = crate::wal::quorum::registry::ShardId {
            topic_id: deleted_topic_id,
            partition: PartitionIndex(0),
        };
        supervisor.wal_shards.insert(
            deleted_shard,
            Arc::new(crate::wal::quorum::engine::WalShardEngine::for_logs(
                maplit::btreemap! {NodeId(2) => original["deleted"].log.clone()},
            )),
        );
        let deleted_wal_dir = crate::wal::quorum::shard_dir(
            dir.path(),
            "deleted",
            Some(deleted_topic_id),
            PartitionIndex(0),
        );
        std::fs::create_dir_all(&deleted_wal_dir).expect("deleted WAL shard directory");

        supervisor.reconcile(&after_delete).await;

        assert!(supervisor.wal_shards.get(deleted_shard).is_none());
        assert!(!deleted_wal_dir.exists());

        let actual = ["deleted", "live", "recreated", "startup-only"]
            .into_iter()
            .map(|topic| PartitionState {
                topic,
                registered: partitions.contains(topic, PartitionIndex(0)),
                directory_exists: dir.path().join(format!("{topic}-0")).exists(),
                runtime_reused: partitions
                    .get(topic, PartitionIndex(0))
                    .map(|current| Arc::ptr_eq(&original[topic], &current)),
            })
            .collect::<Vec<_>>();
        let expected = vec![
            PartitionState {
                topic: "deleted",
                registered: false,
                directory_exists: false,
                runtime_reused: None,
            },
            PartitionState {
                topic: "live",
                registered: true,
                directory_exists: true,
                runtime_reused: Some(true),
            },
            PartitionState {
                topic: "recreated",
                registered: true,
                directory_exists: true,
                runtime_reused: Some(false),
            },
            PartitionState {
                topic: "startup-only",
                registered: true,
                directory_exists: true,
                runtime_reused: Some(true),
            },
        ];
        assert!(actual == expected);
    }

    #[tokio::test]
    async fn reconcile_prunes_partition_after_local_replica_is_reassigned() {
        let topic = topic_record("moved", 1);
        let assigned = image_with(&[
            topic.clone(),
            partition_record("moved", 0, NodeId(2), vec![NodeId(2)], 0),
        ]);
        let reassigned = image_with(&[
            topic,
            partition_record("moved", 0, NodeId(1), vec![NodeId(1)], 1),
        ]);
        let (supervisor, partitions, _reporter, dir) = supervisor_fixture(assigned.clone());
        supervisor.reconcile(&assigned).await;

        let original = partitions
            .get("moved", PartitionIndex(0))
            .expect("assigned partition");
        let topic_id = assigned.topic("moved").expect("topic").topic_id;
        let shard = crate::wal::quorum::registry::ShardId {
            topic_id,
            partition: PartitionIndex(0),
        };
        supervisor.wal_shards.insert(
            shard,
            Arc::new(crate::wal::quorum::engine::WalShardEngine::for_logs(
                maplit::btreemap! {NodeId(2) => original.log.clone()},
            )),
        );
        let wal_dir =
            crate::wal::quorum::shard_dir(dir.path(), "moved", Some(topic_id), PartitionIndex(0));
        std::fs::create_dir_all(&wal_dir).expect("WAL shard directory");

        supervisor.reconcile(&reassigned).await;

        assert!(!partitions.contains("moved", PartitionIndex(0)));
        assert!(!dir.path().join("moved-0").exists());
        assert!(supervisor.wal_shards.get(shard).is_none());
        assert!(!wal_dir.exists());
        assert!(original.take_writer_handle().is_none());
    }

    #[tokio::test]
    async fn reconcile_keeps_partition_until_its_metadata_record_arrives() {
        let image = image_with(&[topic_record("pending", 1)]);
        let (supervisor, partitions, _reporter, dir) = supervisor_fixture(image.clone());
        supervisor
            .materialize_local_partition(&image, "pending", 0)
            .expect("startup partition");
        let original = partitions
            .get("pending", PartitionIndex(0))
            .expect("startup partition");

        supervisor.reconcile(&image).await;

        let current = partitions
            .get("pending", PartitionIndex(0))
            .expect("pending metadata must not delete local storage");
        assert!(Arc::ptr_eq(&original, &current));
        assert!(dir.path().join("pending-0").exists());
    }

    #[test]
    fn remove_partition_dir_is_idempotent_but_reports_other_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("missing");
        remove_partition_dir(&missing).expect("missing directory is already removed");

        let file = dir.path().join("file");
        std::fs::write(&file, b"not a directory").expect("test file");
        assert!(remove_partition_dir(&file).is_err());
    }
}
