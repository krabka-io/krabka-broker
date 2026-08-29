//! Turning the topic and partition list of a `DescribeQuorum` request into
//! the per-partition quorum rows the response carries.
//!
//! This is the whole projection from [`krabka_raft::QuorumState`] onto the
//! Kafka wire shape: the leader, epoch and high watermark of the metadata
//! partition, the voter and observer rows built from openraft's replication
//! map, and the `INVALID_TOPIC_EXCEPTION` row every other topic gets. The
//! function is pure, so it is driven directly by unit tests with no
//! controller behind it.

use krabka_protocol::{
    owned::{
        common::describe_quorum_response::replica_state::ReplicaState,
        describe_quorum_response::{PartitionData, TopicData},
    },
    primitives::uuid::Uuid,
};
use krabka_raft::QuorumState;

use crate::codes;

#[cfg(test)]
mod tests;

/// JVM "Unknown" sentinel for a voter's `log_end_offset` when openraft does
/// not track peer progress. That happens when this node is a follower, because
/// openraft fills the `replication` map only on the leader.
const UNKNOWN_LOG_END_OFFSET: i64 = -1;

/// The one Kafka-side topic name that represents the `KRaft` metadata log. It
/// mirrors `org.apache.kafka.common.Topic.CLUSTER_METADATA_TOPIC_NAME`.
const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";

/// Builds one `TopicData` row per requested topic. The metadata raft topic
/// gets a full `PartitionData` for partition 0. Any other topic gets a
/// per-partition `INVALID_TOPIC_EXCEPTION` row. The function is pure, so a
/// test can drive it with a hand-built `QuorumState` and no controller.
pub(super) fn build_topic_responses(
    requested: &[krabka_protocol::owned::describe_quorum_request::TopicData],
    quorum: &QuorumState,
) -> Vec<TopicData> {
    let leader_id = quorum
        .current_leader
        .map_or(-1, |n| i32::try_from(n.0).unwrap_or(-1));
    let leader_epoch = i32::try_from(quorum.current_term).unwrap_or(i32::MAX);
    let high_watermark = i64::try_from(quorum.last_applied_index).unwrap_or(i64::MAX);

    requested
        .iter()
        .map(|t| {
            let partitions: Vec<PartitionData> = t
                .partitions
                .iter()
                .map(|p| {
                    if t.topic_name == CLUSTER_METADATA_TOPIC && p.partition_index == 0 {
                        PartitionData {
                            partition_index: 0,
                            error_code: codes::NONE,
                            error_message: None,
                            leader_id,
                            leader_epoch,
                            high_watermark,
                            current_voters: quorum
                                .voters
                                .iter()
                                .map(|&id| {
                                    let matched = quorum
                                        .per_voter_matched_index
                                        .get(&id)
                                        .map_or(UNKNOWN_LOG_END_OFFSET, |&idx| {
                                            i64::try_from(idx).unwrap_or(i64::MAX)
                                        });
                                    // KIP-853 (v2+): the voter's directory
                                    // id, read from the replicated
                                    // membership. Zero (`Uuid::ZERO`) when
                                    // unknown — and skipped entirely on
                                    // v0/v1 encode.
                                    let replica_directory_id = quorum
                                        .voter_nodes
                                        .get(&id)
                                        .map_or(Uuid::ZERO, |n| Uuid(*n.directory_id.as_bytes()));
                                    ReplicaState {
                                        replica_id: i32::try_from(id.0).unwrap_or(-1),
                                        replica_directory_id,
                                        log_end_offset: matched,
                                        last_fetch_timestamp: -1,
                                        last_caught_up_timestamp: -1,
                                        ..Default::default()
                                    }
                                })
                                .collect(),
                            observers: quorum
                                .per_voter_matched_index
                                .iter()
                                .filter(|(id, _)| !quorum.voters.contains(id))
                                .map(|(id, offset)| ReplicaState {
                                    replica_id: i32::try_from(id.0).unwrap_or(-1),
                                    replica_directory_id: Uuid::ZERO,
                                    log_end_offset: i64::try_from(*offset).unwrap_or(i64::MAX),
                                    last_fetch_timestamp: -1,
                                    last_caught_up_timestamp: -1,
                                    ..Default::default()
                                })
                                .collect(),
                            ..Default::default()
                        }
                    } else {
                        PartitionData {
                            partition_index: p.partition_index,
                            error_code: codes::INVALID_TOPIC_EXCEPTION,
                            error_message: Some(format!(
                                "DescribeQuorum supports only `{CLUSTER_METADATA_TOPIC}`",
                            )),
                            leader_id: -1,
                            leader_epoch: -1,
                            high_watermark: -1,
                            current_voters: Vec::new(),
                            observers: Vec::new(),
                            ..Default::default()
                        }
                    }
                })
                .collect();
            TopicData {
                topic_name: t.topic_name.clone(),
                partitions,
                ..Default::default()
            }
        })
        .collect()
}
