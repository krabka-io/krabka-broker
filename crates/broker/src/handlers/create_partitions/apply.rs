//! The apply stage of `CreatePartitions`: it builds the `PartitionRecord`
//! batch that the controller commits, and it materializes the committed
//! partitions on the brokers that host a replica of them.
//!
//! The two halves live together because they describe the same partition
//! from both sides of the quorum. The record batch says what the cluster
//! agrees the new partitions are, and the materialization installs that same
//! leader and ISR in the local partition registry.

use krabka_metadata::{MetadataRecord, PartitionRecord};
use krabka_raft::NodeId;
use krabka_units::Time;

use crate::replicator_supervisor::materialize_partition;

fn should_materialize_locally(replicas: &[NodeId], node_id: NodeId) -> bool {
    replicas.contains(&node_id)
}

fn is_local_leader(leader: NodeId, node_id: NodeId) -> bool {
    leader == node_id
}
pub(super) fn partition_records(
    topic: &str,
    indices: &[i32],
    assignments: &[Vec<NodeId>],
) -> Vec<MetadataRecord> {
    indices
        .iter()
        .zip(assignments)
        .map(|(index, replicas)| {
            MetadataRecord::V1Partition(PartitionRecord {
                topic: topic.to_string(),
                partition: *index,
                leader: replicas[0],
                replicas: replicas.clone(),
                isr: replicas.clone(),
                leader_epoch: krabka_metadata::LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            })
        })
        .collect()
}

#[derive(Clone, Copy)]
pub(super) struct MaterializeContext<'a> {
    pub(super) partitions: &'a std::sync::Arc<crate::partition_registry::PartitionRegistry>,
    pub(super) log_dirs: &'a [std::path::PathBuf],
    pub(super) log_config: &'a krabka_log::LogConfig,
    pub(super) log_dir_status: &'a crate::log_dir_status::LogDirRegistry,
    pub(super) producer_state: &'a std::sync::Arc<crate::producer_state::ProducerState>,
    pub(super) producer_id_expiration: Time,
    pub(super) max_produce_group: usize,
    pub(super) partition_writer_queue_depth: usize,
    pub(super) diskless_wal_local_replica_count: usize,
    pub(super) node_id: NodeId,
    pub(super) diskless: bool,
    pub(super) topic_id: uuid::Uuid,
    pub(super) hot_tail: &'a std::sync::Arc<crate::diskless::hot_tail::HotTailCache>,
    pub(super) wal_shards: &'a std::sync::Arc<crate::wal::quorum::registry::WalShardRegistry>,
    pub(super) controller: &'a std::sync::Arc<dyn crate::metadata_source::MetadataSource>,
}

pub(super) async fn materialize_new_partitions(
    context: MaterializeContext<'_>,
    topic: &str,
    indices: &[i32],
    assignments: &[Vec<NodeId>],
) {
    for (index, replicas) in indices.iter().zip(assignments) {
        if !should_materialize_locally(replicas, context.node_id) {
            continue;
        }
        if let Err(error) =
            materialize_partition(crate::replicator_supervisor::MaterializePartitionConfig {
                partitions: context.partitions,
                topic,
                topic_id: Some(context.topic_id),
                partition: *index,
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
            tracing::error!(topic, partition = *index, error = %error,
                "CreatePartitions: materialize after quorum commit failed");
            continue;
        }
        let Some(partition) = context
            .partitions
            .get(topic, krabka_ids::PartitionIndex(*index))
        else {
            continue;
        };
        let leader = replicas[0];
        partition.install_leader_change(leader.0, 0).await;
        if is_local_leader(leader, context.node_id) {
            partition.install_isr(replicas, replicas, leader).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn local_materialization_predicates_track_replica_membership_and_leader() {
        check!(should_materialize_locally(
            &[NodeId(1), NodeId(2)],
            NodeId(1)
        ));
        check!(should_materialize_locally(
            &[NodeId(1), NodeId(2)],
            NodeId(2)
        ));
        check!(!should_materialize_locally(
            &[NodeId(1), NodeId(2)],
            NodeId(3)
        ));
        check!(is_local_leader(NodeId(1), NodeId(1)));
        check!(!is_local_leader(NodeId(2), NodeId(1)));
    }
}
