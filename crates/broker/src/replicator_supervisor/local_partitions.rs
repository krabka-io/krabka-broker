//! Materialization of the partitions this broker hosts, and the per-reconcile
//! sync of each one's cached leader and leader epoch, plus the ISR where this
//! broker is the leader.

use std::{collections::HashSet, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_metadata::MetadataImage;
use tracing::warn;

use super::{
    ReplicatorSupervisor, TopicPartition,
    materialize::{MaterializePartitionConfig, materialize_partition_with_replication_target},
};

impl ReplicatorSupervisor {
    pub(super) async fn reconcile_local_partitions(
        &self,
        local_set: &HashSet<TopicPartition>,
        image: &MetadataImage,
    ) {
        for key in local_set {
            if let Err(e) = self.materialize_local_partition(image, &key.0, key.1) {
                warn!(
                    topic = %key.0, partition = key.1, error = %e,
                    "failed to materialize local partition"
                );
                continue;
            }
            let Some(part_record) = image.partition(&key.0, key.1).cloned() else {
                continue;
            };
            let Some(part) = self.partitions.get(&key.0, PartitionIndex(key.1)) else {
                continue;
            };
            // Always sync the partition's cached leader + epoch.
            // `Partition::install_leader_change` is idempotent (atomic stores
            // no-op on equal writes).
            let topic_id = image.topic(&key.0).map(|topic| topic.topic_id);
            let promoting_diskless = part.diskless
                && part_record.leader == self.node_id
                && part
                    .current_leader
                    .load(std::sync::atomic::Ordering::Acquire)
                    != self.node_id.0;
            if promoting_diskless {
                let Some(topic_id) = topic_id else {
                    warn!(
                        topic = %key.0,
                        partition = key.1,
                        "cannot prepare diskless promotion without topic identity"
                    );
                    continue;
                };
                let shard = crate::wal::quorum::registry::ShardId {
                    topic_id,
                    partition: PartitionIndex(key.1),
                };
                let engine = self.wal_shards.get(shard);
                let result = part
                    .install_replication_target_after_log_prepare(
                        Some(topic_id),
                        part_record.leader.0,
                        part_record.leader_epoch.0,
                        |log| {
                            let durable = crate::wal::quorum::follower::hydrate_on_promotion(
                                &self.log_dirs,
                                &key.0,
                                shard,
                                self.node_id,
                                &self.log_config,
                                log,
                            )?;
                            if let (Some(durable), Some(engine)) = (durable, engine.as_ref()) {
                                engine.adopt_local_durable_prefix(
                                    durable,
                                    log.log_start_offset(),
                                    log.log_end_offset(),
                                );
                            }
                            Ok(log.producer_state_snapshot())
                        },
                        |snapshot| {
                            self.producer_state.rebuild_from_snapshot(
                                &key.0,
                                PartitionIndex(key.1),
                                snapshot,
                            )
                        },
                    )
                    .await;
                if let Err(error) = result {
                    warn!(
                        topic = %key.0,
                        partition = key.1,
                        error = %error,
                        "failed to prepare diskless promotion"
                    );
                    continue;
                }
            } else {
                part.install_replication_target(
                    topic_id,
                    part_record.leader.0,
                    part_record.leader_epoch.0,
                )
                .await;
            }
            if part_record.leader == self.node_id {
                // Install the *current* ISR from the metadata image (not the
                // full replica set) as ISR membership: using `replicas` would
                // undo any shrink applied via AlterPartition, so
                // isr_maintenance's shrink would never stick (and producers
                // with acks=-1 would stay blocked on lagging followers). The
                // replica set is passed separately so follower-progress
                // tracking survives across reconciles for replicas catching
                // up toward ISR re-admission.
                part.install_isr(&part_record.isr, &part_record.replicas, part_record.leader)
                    .await;
            }
        }
    }

    /// Open (or recover) the on-disk `Partition` for `(topic, partition)`
    /// and insert it into the broker's shared `partitions` map.
    /// Idempotent: a no-op if the partition is already present.
    pub(super) fn materialize_local_partition(
        &self,
        image: &MetadataImage,
        topic: &str,
        partition: i32,
    ) -> Result<(), String> {
        let diskless = crate::broker::diskless_topic_config(image.topic_config(topic));
        let topic_id = image.topic(topic).map(|topic| topic.topic_id);
        let initial_target = if diskless {
            None
        } else {
            image
                .partition(topic, partition)
                .map(|record| crate::partition::ReplicationTarget {
                    topic_id,
                    leader_node_id: record.leader,
                    leader_epoch: record.leader_epoch,
                })
        };
        materialize_partition_with_replication_target(
            MaterializePartitionConfig {
                partitions: &self.partitions,
                topic,
                topic_id,
                partition,
                log_dirs: &self.log_dirs,
                log_config: &self.log_config,
                log_dir_status: &self.log_dir_status,
                producer_state: &self.producer_state,
                producer_id_expiration: self.producer_id_expiration,
                max_produce_group: self.max_produce_group,
                partition_writer_queue_depth: self.partition_writer_queue_depth,
                diskless_wal_local_replica_count: self.diskless_wal_local_replica_count,
                diskless,
                hot_tail: Some(self.hot_tail.clone()),
                wal_shards: Some(self.wal_shards.clone()),
                sequencer: diskless.then(|| {
                    Arc::new(crate::wal::ControllerSequencer::new(
                        self.controller.clone(),
                    )) as Arc<dyn crate::wal::OffsetSequencer>
                }),
            },
            initial_target,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use assert2::assert;
    use krabka_metadata::{MetadataRecord, TopicRecord};
    use krabka_raft::NodeId;
    use uuid::Uuid;

    use super::*;
    use crate::replicator_supervisor::test_support::{
        image_with, partition_record, supervisor_fixture, topic_record,
    };

    #[tokio::test]
    async fn reconcile_materializes_leader_partition_and_installs_isr() {
        let img = image_with(&[
            topic_record("t", 1),
            partition_record("t", 0, NodeId(2), vec![NodeId(1), NodeId(2), NodeId(3)], 7),
        ]);
        let (supervisor, partitions, _reporter, _dir) = supervisor_fixture(img.clone());

        supervisor.reconcile(&img).await;

        let part = partitions
            .get("t", PartitionIndex(0))
            .expect("local leader materialized");
        assert!(
            part.current_leader
                .load(std::sync::atomic::Ordering::Acquire)
                == 2,
            "leader cache updated"
        );
        assert!(
            part.current_leader_epoch
                .load(std::sync::atomic::Ordering::Acquire)
                == 7,
            "leader epoch cache updated"
        );
        let state = part.replica_state.lock().await;
        assert!(state.isr == [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect());
    }

    #[tokio::test]
    async fn reconcile_does_not_treat_reserved_diskless_offset_as_durable() {
        use std::collections::BTreeMap;

        use krabka_metadata::PartitionOffsetAdvanceRecord;

        let mut overrides = BTreeMap::new();
        overrides.insert("krabka.diskless".into(), "true".into());
        let img = image_with(&[
            topic_record("diskless", 1),
            partition_record("diskless", 0, NodeId(2), vec![NodeId(2)], 0),
            MetadataRecord::V1TopicConfig(krabka_metadata::TopicConfigRecord {
                topic: "diskless".into(),
                overrides,
            }),
            MetadataRecord::V1PartitionOffsetAdvance(PartitionOffsetAdvanceRecord {
                topic: "diskless".into(),
                partition: 0,
                count: 7,
            }),
        ]);
        let (supervisor, partitions, _reporter, _dir) = supervisor_fixture(img.clone());

        supervisor.reconcile(&img).await;

        let partition = partitions
            .get("diskless", PartitionIndex(0))
            .expect("diskless leader materialized");
        assert!(partition.high_watermark().await == krabka_log::Offset(0));
    }

    #[tokio::test]
    async fn reconcile_materializes_follower_but_does_not_install_isr() {
        let img = image_with(&[
            topic_record("t", 1),
            partition_record("t", 0, NodeId(1), vec![NodeId(1), NodeId(2), NodeId(3)], 7),
        ]);
        let (supervisor, partitions, _reporter, _dir) = supervisor_fixture(img.clone());

        supervisor.reconcile(&img).await;

        let part = partitions
            .get("t", PartitionIndex(0))
            .expect("local follower materialized");
        let state = part.replica_state.lock().await;
        assert!(state.isr.is_empty());
    }

    #[tokio::test]
    async fn materialize_local_partition_inserts_partition() {
        let img = MetadataImage::new(Uuid::nil());
        let (supervisor, partitions, _reporter, _dir) = supervisor_fixture(img.clone());

        supervisor
            .materialize_local_partition(&img, "t", 0)
            .unwrap();

        assert!(partitions.contains("t", PartitionIndex(0)));
    }

    #[tokio::test]
    async fn non_diskless_materialization_installs_target_before_registry_visibility() {
        let topic_id = Uuid::new_v4();
        let img = image_with(&[
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id,
                partitions: 1,
                replication_factor: 1,
            }),
            partition_record("t", 0, NodeId(2), vec![NodeId(2)], 7),
        ]);
        let (supervisor, partitions, _reporter, _dir) = supervisor_fixture(img.clone());

        supervisor
            .materialize_local_partition(&img, "t", 0)
            .expect("materialize");

        let partition = partitions
            .get("t", PartitionIndex(0))
            .expect("registry-visible partition");
        let expected = crate::partition::ReplicationTarget {
            topic_id: Some(topic_id),
            leader_node_id: NodeId(2),
            leader_epoch: krabka_metadata::LeaderEpoch(7),
        };
        assert!(*partition.replication_target.read().await == expected);
        assert!(partition.current_leader.load(Ordering::Acquire) == 2);
        assert!(partition.current_leader_epoch.load(Ordering::Acquire) == 7);
        assert!(
            partition.replica_state.lock().await.current_leader_epoch == krabka_ids::LeaderEpoch(7)
        );
    }

    #[tokio::test]
    async fn diskless_materialization_keeps_leader_unpublished_until_hydration() {
        let topic_id = Uuid::new_v4();
        let img = image_with(&[
            MetadataRecord::V1Topic(TopicRecord {
                name: "diskless".into(),
                topic_id,
                partitions: 1,
                replication_factor: 1,
            }),
            partition_record("diskless", 0, NodeId(2), vec![NodeId(2)], 7),
            MetadataRecord::V1TopicConfig(krabka_metadata::TopicConfigRecord {
                topic: "diskless".into(),
                overrides: std::collections::BTreeMap::from([(
                    "krabka.diskless".into(),
                    "true".into(),
                )]),
            }),
        ]);
        let (supervisor, partitions, _reporter, _dir) = supervisor_fixture(img.clone());

        supervisor
            .materialize_local_partition(&img, "diskless", 0)
            .expect("materialize");

        let partition = partitions
            .get("diskless", PartitionIndex(0))
            .expect("registry-visible partition");
        assert!(partition.diskless);
        assert!(
            *partition.replication_target.read().await
                == crate::partition::ReplicationTarget {
                    topic_id: Some(topic_id),
                    leader_node_id: NodeId(0),
                    leader_epoch: krabka_metadata::LeaderEpoch(0),
                }
        );
        assert!(partition.current_leader.load(Ordering::Acquire) == 0);
        assert!(partition.current_leader_epoch.load(Ordering::Acquire) == 0);
    }
}
