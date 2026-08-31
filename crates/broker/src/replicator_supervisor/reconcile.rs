//! The reconcile pass the supervisor runs for one metadata image: it places
//! the WAL voters, prunes what left the image, materializes what joined it, and
//! retargets the follower replicator tasks.

use krabka_ids::PartitionIndex;
use krabka_metadata::MetadataImage;
use tracing::warn;

use super::{
    ReplicatorSupervisor, ReplicatorTask, ReplicatorTaskTarget, TopicPartition,
    desired_follower_set, desired_local_set, desired_wal_placements, push_topic_configs,
};
use crate::replicator;

impl ReplicatorSupervisor {
    pub(crate) async fn reconcile(&self, image: &MetadataImage) {
        let wal_placements = desired_wal_placements(image, self.diskless_wal_local_replica_count);
        for (shard, placement) in &wal_placements {
            let shortfall = self
                .diskless_wal_local_replica_count
                .saturating_sub(placement.voters.len());
            if let Some(shortfall) = std::num::NonZeroUsize::new(shortfall) {
                warn!(
                    topic_id = %shard.topic_id,
                    partition = shard.partition.0,
                    available = placement.voters.len(),
                    required = self.diskless_wal_local_replica_count,
                    shortfall = shortfall.get(),
                    "diskless WAL placement lacks enough distinct-rack registered brokers"
                );
            }
        }
        self.wal_shards.replace_placements(&wal_placements);
        let desired_wal_followers = self.desired_wal_followers(image, &wal_placements);

        // A promoted WAL voter must have exclusive access to its follower log
        // before the canonical partition adopts that durable prefix.
        self.stop_obsolete_wal_followers(&desired_wal_followers)
            .await;
        let wal_dirs_to_keep = image
            .all_partitions()
            .filter(|partition| {
                crate::config_keys::resolve_diskless(image.topic_config(&partition.topic))
            })
            .filter_map(|partition| {
                let topic_id = image.topic(&partition.topic)?.topic_id;
                let shard = crate::wal::quorum::registry::ShardId {
                    topic_id,
                    partition: PartitionIndex(partition.partition),
                };
                wal_placements
                    .get(&shard)
                    .is_some_and(|placement| placement.voters.contains(&self.node_id))
                    .then_some((partition.topic.clone(), shard))
            })
            .flat_map(|(topic, shard)| {
                self.log_dirs.iter().map(move |log_dir| {
                    crate::wal::quorum::shard_dir(
                        log_dir,
                        &topic,
                        Some(shard.topic_id),
                        shard.partition,
                    )
                })
            })
            .collect();
        if let Err(error) =
            crate::wal::quorum::prune_orphaned_shard_dirs(&self.log_dirs, &wal_dirs_to_keep)
        {
            warn!(error = %error, "failed to prune orphaned WAL shard directories");
        }

        let local_set = desired_local_set(self.node_id, image);

        // A DeleteTopics handler tears down its local partition immediately
        // after the metadata commit. A reconcile that already captured the
        // preceding image can race that teardown and materialize the deleted
        // partition again. Re-prune from the authoritative new image before
        // materializing its desired set so that stale-image resurrection is
        // idempotently repaired on the next watch delivery.
        self.prune_deleted_topic_partitions(image);

        // A reassignment can remove this broker from a live partition's
        // replica set without deleting the topic. Stop and remove that local
        // runtime before materializing the new desired set. Partitions that
        // are absent from metadata are left alone here: startup recovery can
        // discover an on-disk partition before its topic record arrives, and
        // `prune_deleted_topic_partitions` handles known topic tombstones.
        self.prune_unassigned_partitions(&local_set, image);

        // 0. Materialize the on-disk partition for every assignment where
        //    self is in `replicas`, regardless of leader/follower role.
        //    Additionally: sync the partition's cached leader + epoch
        //    (idempotent), and for partitions where self is leader,
        //    install the ISR into ReplicaState for HW computation.
        self.reconcile_local_partitions(&local_set, image).await;

        // Start WAL followers only after reassignment pruning completes. A
        // broker that just stopped hosting the ordinary partition deletes its
        // old shard directory during pruning; starting first would race that
        // deletion against the new follower log.
        self.spawn_wal_followers(image, desired_wal_followers);

        // Push topic-config overrides onto every locally-hosted partition.
        // Pushes are idempotent — sending the same `LogConfig` is a cheap
        // noop write inside `Log::set_config`. The metadata-watch reconcile
        // loop fires on every image change, so AlterConfigs propagation is
        // bounded to one reconcile tick.
        push_topic_configs(&local_set, &self.partitions, image, &self.log_config).await;

        let desired = desired_follower_set(self.node_id, image);

        // 1. Cancel removed, retargeted, or exited tasks. Topic identity is
        // part of the target: a delete/recreate with the same name, leader,
        // and epoch must not reuse the old topic's task.
        let current: Vec<TopicPartition> = self.tasks.iter().map(|e| e.key().clone()).collect();
        for k in current {
            let desired_target = desired.contains(&k).then(|| {
                let topic = image.topic(&k.0)?;
                let partition = image.partition(&k.0, k.1)?;
                Some(ReplicatorTaskTarget {
                    topic_id: topic.topic_id,
                    leader: partition.leader,
                    leader_epoch: partition.leader_epoch,
                })
            });
            let desired_target = desired_target.flatten();
            let keep = self.tasks.get(&k).is_some_and(|task| {
                !task.handle.is_finished() && Some(task.target) == desired_target
            });
            if !keep && let Some((_, task)) = self.tasks.remove(&k) {
                task.shutdown.cancel();
            }
        }

        // 2. Spawn new follower replicators.
        for k in desired {
            if self.tasks.contains_key(&k) {
                continue;
            }
            let part = image.partition(&k.0, k.1).cloned();
            let Some(part) = part else { continue };
            let leader = part.leader;
            let Some(broker) = image.broker(leader).cloned() else {
                warn!(
                    topic = %k.0, partition = k.1, leader = leader.0,
                    "leader broker not yet registered in MetadataImage; deferring"
                );
                continue;
            };
            // Resolve the topic's `topic_id` from the same image we're
            // reconciling against. The replicator needs it for the v13+
            // Fetch wire format; without it the leader's handler can't
            // resolve the topic name and returns UNKNOWN_TOPIC_OR_PARTITION.
            let Some(topic_rec) = image.topic(&k.0).cloned() else {
                warn!(
                    topic = %k.0, partition = k.1,
                    "topic record missing from MetadataImage; deferring"
                );
                continue;
            };
            let token = self.shutdown.child_token();
            let target = ReplicatorTaskTarget {
                topic_id: topic_rec.topic_id,
                leader,
                leader_epoch: part.leader_epoch,
            };
            let config = self.replicator_config(&k, &topic_rec, &part, &broker, token.clone());
            let handle = tokio::spawn(replicator::run(config));
            self.tasks.insert(
                k,
                ReplicatorTask {
                    shutdown: token,
                    target,
                    handle,
                },
            );
        }

        // 3. Refresh the txn coordinator's view of locally-led
        //    __transaction_state partitions. Cheap (Arc clone + lock).
        if let Some(coord) = &self.txn_coordinator {
            coord.refresh_leader_partitions(image).await;
        }

        // 3b. Refresh the share coordinator's view of locally-led
        //     __share_group_state partitions (KIP-932). Same shape as txn.
        if let Some(coord) = &self.share_coordinator {
            coord.refresh_leader_partitions(image).await;
        }

        // 4. KIP-858: report any (topic, partition) whose owning log-dir UUID
        //    has changed since the last successful report (first materialization
        //    or after a KIP-113 dir swap). Only sends if there is at least one
        //    change; on error the tracker is NOT updated so we retry next tick.
        //    The report submits a non-clobbering V1PartitionDirAssignment delta
        //    (merges one replica's `directories` slot), so it can no longer
        //    revert a concurrent reassignment.
        self.report_dir_assignments(&local_set, image).await;
    }
}
