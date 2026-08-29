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
) {
    for (index, replicas) in assignments.iter().enumerate() {
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
        let leader = replicas[0];
        partition
            .install_leader_change(leader.0, INITIAL_LEADER_EPOCH)
            .await;
        if is_local_leader(leader, context.node_id) {
            partition.install_isr(replicas, replicas, leader).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_raft::NodeId;

    use super::{is_local_leader, should_materialize_locally};

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
