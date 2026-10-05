//! Local materialization of a newly created topic. Once the metadata quorum
//! commits the records, this module creates the log directories and the
//! partition objects of every replica that this broker hosts, and installs
//! the initial leader and ISR state of each of them.

use krabka_units::Time;

use super::INITIAL_LEADER_EPOCH;
use crate::replicator_supervisor::materialize_partition;

fn should_materialize_locally(
    replicas: &[krabka_raft::NodeId],
    node_id: krabka_raft::NodeId,
) -> bool {
    replicas.contains(&node_id)
}

fn is_local_leader(leader: krabka_raft::NodeId, node_id: krabka_raft::NodeId) -> bool {
    leader == node_id
}

#[derive(Clone, Copy)]
pub(super) struct TopicMaterialization<'a> {
    pub(super) partitions: &'a std::sync::Arc<crate::partition_registry::PartitionRegistry>,
    pub(super) log_dirs: &'a [std::path::PathBuf],
    pub(super) log_config: &'a krabka_log::LogConfig,
    pub(super) log_dir_status: &'a crate::log_dir_status::LogDirRegistry,
    pub(super) producer_state: &'a std::sync::Arc<crate::producer_state::ProducerState>,
    pub(super) producer_id_expiration: Time,
    pub(super) max_produce_group: usize,
    pub(super) partition_writer_queue_depth: usize,
    pub(super) diskless_wal_local_replica_count: usize,
    pub(super) node_id: krabka_raft::NodeId,
    pub(super) diskless: bool,
    pub(super) topic_id: uuid::Uuid,
    pub(super) hot_tail: &'a std::sync::Arc<crate::diskless::hot_tail::HotTailCache>,
    pub(super) wal_shards: &'a std::sync::Arc<crate::wal::quorum::registry::WalShardRegistry>,
    pub(super) controller: &'a std::sync::Arc<dyn crate::metadata_source::MetadataSource>,
}

pub(super) async fn materialize_topic(
    context: TopicMaterialization<'_>,
    topic: &str,
    assignments: &[Vec<krabka_raft::NodeId>],
    leaderships: &[super::InitialLeadership],
) {
    for (index, (replicas, leadership)) in assignments.iter().zip(leaderships).enumerate() {
        if !should_materialize_locally(replicas, context.node_id) {
            continue;
        }
        let index = i32::try_from(index).unwrap_or(0);
        if let Err(error) =
            materialize_partition(crate::replicator_supervisor::MaterializePartitionConfig {
                partitions: context.partitions,
                topic,
                topic_id: Some(context.topic_id),
                partition: index,
                log_dirs: context.log_dirs,
                log_config: context.log_config,
                log_dir_status: context.log_dir_status,
                producer_state: context.producer_state,
                producer_id_expiration: context.producer_id_expiration,
                max_produce_group: context.max_produce_group,
                partition_writer_queue_depth: context.partition_writer_queue_depth,
                diskless_wal_local_replica_count: context.diskless_wal_local_replica_count,
                diskless: context.diskless,
                hot_tail: Some(context.hot_tail.clone()),
                wal_shards: Some(context.wal_shards.clone()),
                sequencer: context.diskless.then(|| {
                    std::sync::Arc::new(crate::wal::ControllerSequencer::new(
                        context.controller.clone(),
                    )) as std::sync::Arc<dyn crate::wal::OffsetSequencer>
                }),
            })
        {
            tracing::error!(topic, partition = index, error = %error,
                "CreateTopics: materialize after quorum commit failed");
            continue;
        }
        let Some(partition) = context
            .partitions
            .get(topic, krabka_ids::PartitionIndex(index))
        else {
            continue;
        };
        if let Err(error) = leadership
            .install(
                &partition,
                context.producer_state,
                context.topic_id,
                context.node_id,
                replicas,
                INITIAL_LEADER_EPOCH,
            )
            .await
        {
            tracing::error!(topic, partition = index, error = %error,
                "CreateTopics: failed to record the initial leader epoch");
        }
    }
}

impl super::InitialLeadership {
    /// Installs this leadership on a partition this broker has just
    /// materialized: the first leader at `leader_epoch` and, when this broker
    /// leads, the ISR.
    ///
    /// When this broker leads a disk-backed partition, the leader epoch is
    /// recorded at the log end before the role is published, as Kafka's
    /// `Partition.makeLeader` does through `maybeAssignEpochStartOffset` on
    /// the first `PartitionRecord`. A follower that fetches before the
    /// leader's first write can then be placed straight away, instead of
    /// waiting for the supervisor's next reconcile to record the epoch. A
    /// diskless partition's promotion belongs to the supervisor, which
    /// prepares its log from the WAL, so it and a follower replica only
    /// install the leader. The disk-backed leader also takes the producer
    /// state of its new log into `producer_state`, as every promotion does.
    ///
    /// `CreatePartitions` materializes its new partitions through this too.
    ///
    /// # Errors
    /// Returns the leader-epoch checkpoint's error when the epoch cannot be
    /// recorded. The role and the ISR are then not installed, and the
    /// supervisor's next reconcile retries the promotion, as it does for a
    /// promotion of its own that fails.
    pub(crate) async fn install(
        &self,
        partition: &crate::partition::Partition,
        producer_state: &crate::producer_state::ProducerState,
        topic_id: uuid::Uuid,
        node_id: krabka_raft::NodeId,
        replicas: &[krabka_raft::NodeId],
        leader_epoch: i32,
    ) -> Result<(), crate::error::BrokerError> {
        let leader = self.leader;
        if !is_local_leader(leader, node_id) {
            partition
                .install_leader_change(leader.0, leader_epoch)
                .await;
            return Ok(());
        }
        if partition.diskless {
            partition
                .install_leader_change(leader.0, leader_epoch)
                .await;
        } else {
            partition
                .install_local_leadership(producer_state, Some(topic_id), leader.0, leader_epoch)
                .await?;
        }
        partition.install_isr(&self.isr, replicas, leader).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::Ordering};

    use assert2::assert;
    use krabka_raft::NodeId;

    use super::{is_local_leader, should_materialize_locally};
    use crate::{handlers::create_topics::InitialLeadership, partition::Partition};

    /// What an installed leadership left on a partition.
    #[derive(Debug, PartialEq, Eq)]
    struct InstalledRole {
        /// The leader-epoch cache, as `(epoch, start_offset)` pairs.
        epoch_history: Vec<(i32, i64)>,
        leader: u64,
        leader_epoch: i32,
        /// The ISR, sorted by node id.
        isr: Vec<u64>,
    }

    async fn installed_role(partition: &Partition) -> InstalledRole {
        let epoch_history = partition
            .log
            .lock()
            .expect("log mutex")
            .epoch_checkpoint()
            .entries()
            .iter()
            .map(|entry| (entry.epoch.0, entry.start_offset.0))
            .collect();
        let mut isr: Vec<u64> = partition
            .replica_state
            .lock()
            .await
            .isr
            .iter()
            .map(|node| node.0)
            .collect();
        isr.sort_unstable();
        InstalledRole {
            epoch_history,
            leader: partition.current_leader.load(Ordering::Acquire),
            leader_epoch: partition.current_leader_epoch.load(Ordering::Acquire),
            isr,
        }
    }

    /// A leader-epoch checkpoint on a full disk.
    #[derive(Debug)]
    struct EpochCheckpointFull;

    impl krabka_log::LogIo for EpochCheckpointFull {
        fn write_at(
            &self,
            target: krabka_log::IoTarget,
            file: &std::fs::File,
            buf: &[u8],
        ) -> std::io::Result<usize> {
            use std::io::Write as _;
            if target == krabka_log::IoTarget::LeaderEpochCheckpoint {
                return Err(std::io::ErrorKind::StorageFull.into());
            }
            (&*file).write(buf)
        }
    }

    /// Kafka's `Partition.makeLeader` records the first leader epoch at the
    /// log end the moment a disk-backed partition's leader is created, so a
    /// fresh leader's epoch cache names its epoch before any write and before
    /// any supervisor reconcile. A follower replica and a diskless leader,
    /// whose promotion the supervisor prepares, record nothing here. A
    /// promotion whose epoch cannot be recorded publishes neither the role
    /// nor the ISR.
    #[tokio::test]
    async fn a_new_partition_leader_records_its_epoch_before_publishing_the_role() {
        const LOCAL: NodeId = NodeId(1);
        let leadership = |leader| InitialLeadership {
            leader,
            isr: vec![NodeId(1), NodeId(2)],
        };
        // (name, leader, diskless, checkpoint fails, install succeeds, role)
        let cases = [
            (
                "local disk-backed leader",
                LOCAL,
                false,
                false,
                true,
                InstalledRole {
                    epoch_history: vec![(4, 0)],
                    leader: 1,
                    leader_epoch: 4,
                    isr: vec![1, 2],
                },
            ),
            (
                "follower replica",
                NodeId(2),
                false,
                false,
                true,
                InstalledRole {
                    epoch_history: vec![],
                    leader: 2,
                    leader_epoch: 4,
                    isr: vec![],
                },
            ),
            (
                "local diskless leader",
                LOCAL,
                true,
                false,
                true,
                InstalledRole {
                    epoch_history: vec![],
                    leader: 1,
                    leader_epoch: 4,
                    isr: vec![1, 2],
                },
            ),
            (
                "epoch checkpoint on a full disk",
                LOCAL,
                false,
                true,
                false,
                InstalledRole {
                    epoch_history: vec![],
                    leader: 0,
                    leader_epoch: 0,
                    isr: vec![],
                },
            ),
        ];
        for (name, leader, diskless, checkpoint_fails, installs, expected) in cases {
            let (mut partition, _dir) = crate::partition::test_support::test_partition(Arc::new(
                tokio::sync::Notify::new(),
            ));
            partition.diskless = diskless;
            if checkpoint_fails {
                partition
                    .log
                    .lock()
                    .expect("log mutex")
                    .test_set_io(Arc::new(EpochCheckpointFull));
            }

            let result = leadership(leader)
                .install(
                    &partition,
                    &crate::producer_state::ProducerState::new(),
                    uuid::Uuid::from_u128(7),
                    LOCAL,
                    &[NodeId(1), NodeId(2)],
                    4,
                )
                .await;

            assert!(result.is_ok() == installs, "{name}");
            assert!(installed_role(&partition).await == expected, "{name}");
        }
    }

    #[test]
    fn local_materialization_predicates_track_replica_membership_and_leader() {
        let materialize_cases: [(&[krabka_raft::NodeId], krabka_raft::NodeId, bool); 3] = [
            (&[NodeId(1), NodeId(2)], NodeId(1), true),
            (&[NodeId(1), NodeId(2)], NodeId(2), true),
            (&[NodeId(1), NodeId(2)], NodeId(3), false),
        ];
        for (replicas, node_id, want) in materialize_cases {
            assert!(
                should_materialize_locally(replicas, node_id) == want,
                "replicas {replicas:?}, node {node_id}"
            );
        }

        let leader_cases: [(krabka_raft::NodeId, krabka_raft::NodeId, bool); 2] =
            [(NodeId(1), NodeId(1), true), (NodeId(2), NodeId(1), false)];
        for (leader, node_id, want) in leader_cases {
            assert!(
                is_local_leader(leader, node_id) == want,
                "leader {leader}, node {node_id}"
            );
        }
    }
}
