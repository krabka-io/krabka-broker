//! The `PartitionRecord` batch that `CreatePartitions` commits.
//!
//! Local replicas open through [`crate::handlers::partition_materialization`],
//! which installs the same initial leader and ISR that these records name.

use krabka_metadata::{MetadataRecord, PartitionRecord};
use krabka_raft::NodeId;

use crate::handlers::create_topics::{INITIAL_LEADER_EPOCH, InitialLeadership};

pub(super) fn partition_records(
    topic: &str,
    indices: &[i32],
    assignments: &[Vec<NodeId>],
    leaderships: &[InitialLeadership],
) -> Vec<MetadataRecord> {
    // Kafka's `buildPartitionRegistration`: the leader is the first ISR
    // member, and the ISR holds only the replicas that were active.
    indices
        .iter()
        .zip(assignments.iter().zip(leaderships))
        .map(|(index, (replicas, leadership))| {
            MetadataRecord::V1Partition(PartitionRecord {
                topic: topic.to_string(),
                partition: *index,
                leader: leadership.leader,
                replicas: replicas.clone(),
                isr: leadership.isr.clone(),
                leader_epoch: krabka_metadata::LeaderEpoch(INITIAL_LEADER_EPOCH),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::handlers::partition_materialization::should_materialize_locally;

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
    }
}
