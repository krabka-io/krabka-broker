//! Committed consumer-group offsets: the one restore input that is not a file
//! on a node.
//!
//! `__consumer_offsets` is compacted and internal, so it is never tiered and a
//! KIP-405 archive does not hold it. A restore therefore rebuilds the log and
//! nothing that points into it, and every group starts again from
//! `auto.offset.reset`. What can be copied is the coordinator's answer: the
//! committed offset of every group, read with `ListGroups` and `OffsetFetch`
//! while the cluster is up, and written back with `OffsetCommit` once a
//! restored cluster is.
//!
//! The write-back is a simple-consumer commit: empty `member_id` and
//! generation `-1`, which is what Kafka's own `kafka-consumer-groups
//! --reset-offsets --execute` sends and what the coordinator accepts for a
//! group with no live members. A group with live members is fenced, which is
//! the correct answer: an offset must not move under a consumer that is
//! reading from it.

use std::collections::BTreeMap;

use krabka_protocol::owned::offset_commit_request::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use serde::{Deserialize, Serialize};

/// `OffsetCommitRequest.generationIdOrMemberEpoch` for a caller that is not a
/// group member. Kafka's admin path sends this, and the coordinator skips
/// membership fencing for it.
pub const SIMPLE_CONSUMER_GENERATION: i32 = -1;

/// `OffsetCommitRequest.committedLeaderEpoch` when the epoch is not known.
/// A restored partition's epoch history is rebuilt from the archived batches,
/// so the capture does not carry an epoch to assert here.
pub const UNKNOWN_LEADER_EPOCH: i32 = -1;

/// One group's committed offset in one partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedOffset {
    /// Topic the offset was committed for.
    pub topic: String,
    /// Partition index within that topic.
    pub partition: i32,
    /// The committed offset: the next offset the group would read.
    pub offset: i64,
}

/// One group, with every partition it has a committed offset for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupOffsets {
    /// The group id.
    pub group: String,
    /// Its committed offsets, in `(topic, partition)` order.
    pub offsets: Vec<CommittedOffset>,
}

/// The captured offsets of every group in one cluster.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupOffsetsFile {
    /// One entry per group that had at least one committed offset.
    pub groups: Vec<GroupOffsets>,
}

impl GroupOffsetsFile {
    /// How many `(group, topic, partition)` offsets the file holds.
    #[must_use]
    pub fn offset_count(&self) -> usize {
        self.groups.iter().map(|group| group.offsets.len()).sum()
    }
}

/// Turn one `OffsetFetch` answer into the captured shape.
///
/// The answer arrives as a map, so the ordering is already `(topic, partition)`
/// and the captured file is byte-stable across two captures of an unchanged
/// cluster. That is what lets an operator diff two captures.
#[must_use]
pub fn group_offsets(group: &str, fetched: &BTreeMap<(String, i32), i64>) -> GroupOffsets {
    GroupOffsets {
        group: group.to_owned(),
        offsets: fetched
            .iter()
            .map(|((topic, partition), offset)| CommittedOffset {
                topic: topic.clone(),
                partition: *partition,
                offset: *offset,
            })
            .collect(),
    }
}

/// Build the `OffsetCommit` that puts one group's captured offsets back.
///
/// `topic_ids` carries the restored cluster's topic ids. `OffsetCommit` v10
/// encodes the topic id instead of the name, so a request that names topics
/// only would commit nothing there; below v10 the name is what is encoded and
/// the id is dropped. Setting both makes one request correct at every version
/// the broker may negotiate.
///
/// # Panics
///
/// Panics if the topic list is empty after a topic was pushed onto it, which
/// the loop below cannot do.
#[must_use]
pub fn commit_request(
    offsets: &GroupOffsets,
    topic_ids: &BTreeMap<String, krabka_protocol::primitives::uuid::Uuid>,
) -> OffsetCommitRequest {
    let mut topics: Vec<OffsetCommitRequestTopic> = Vec::new();
    for offset in &offsets.offsets {
        if topics.last().map(|topic| topic.name.as_str()) != Some(offset.topic.as_str()) {
            topics.push(OffsetCommitRequestTopic {
                name: offset.topic.clone(),
                topic_id: topic_ids.get(&offset.topic).copied().unwrap_or_default(),
                ..OffsetCommitRequestTopic::default()
            });
        }
        topics
            .last_mut()
            .expect("a topic was pushed above for this offset")
            .partitions
            .push(OffsetCommitRequestPartition {
                partition_index: offset.partition,
                committed_offset: offset.offset,
                committed_leader_epoch: UNKNOWN_LEADER_EPOCH,
                ..OffsetCommitRequestPartition::default()
            });
    }
    OffsetCommitRequest {
        group_id: offsets.group.clone(),
        generation_id_or_member_epoch: SIMPLE_CONSUMER_GENERATION,
        member_id: String::new(),
        topics,
        ..OffsetCommitRequest::default()
    }
}

/// Every partition the coordinator refused, as `topic-partition: code`.
///
/// A commit is per-partition on the wire, so a partial refusal is possible and
/// silence about it would report a restore that put back fewer offsets than it
/// claims.
#[must_use]
pub fn commit_refusals(
    response: &krabka_protocol::owned::offset_commit_response::OffsetCommitResponse,
) -> Vec<String> {
    response
        .topics
        .iter()
        .flat_map(|topic| {
            topic
                .partitions
                .iter()
                .filter(|partition| partition.error_code != 0)
                .map(move |partition| {
                    format!(
                        "{}-{}: error code {}",
                        topic.name, partition.partition_index, partition.error_code
                    )
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::check;
    use krabka_protocol::{
        owned::{
            offset_commit_request::{
                OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
            },
            offset_commit_response::{
                OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
            },
        },
        primitives::uuid::Uuid as WireUuid,
    };

    use super::{
        CommittedOffset, GroupOffsets, GroupOffsetsFile, SIMPLE_CONSUMER_GENERATION,
        UNKNOWN_LEADER_EPOCH, commit_refusals, commit_request, group_offsets,
    };

    fn captured() -> GroupOffsets {
        GroupOffsets {
            group: "orders-consumers".to_owned(),
            offsets: vec![
                CommittedOffset {
                    topic: "orders".to_owned(),
                    partition: 0,
                    offset: 12,
                },
                CommittedOffset {
                    topic: "orders".to_owned(),
                    partition: 1,
                    offset: 7,
                },
                CommittedOffset {
                    topic: "payments".to_owned(),
                    partition: 0,
                    offset: 3,
                },
            ],
        }
    }

    #[test]
    fn a_fetched_map_becomes_offsets_in_topic_and_partition_order() {
        let fetched = BTreeMap::from([
            (("payments".to_owned(), 0), 3),
            (("orders".to_owned(), 1), 7),
            (("orders".to_owned(), 0), 12),
        ]);
        check!(group_offsets("orders-consumers", &fetched) == captured());
    }

    #[test]
    fn the_commit_names_every_partition_once_per_topic_at_any_version() {
        let orders = WireUuid([7_u8; 16]);
        let topic_ids = BTreeMap::from([("orders".to_owned(), orders)]);

        let request = commit_request(&captured(), &topic_ids);

        let partition = |index: i32, offset: i64| OffsetCommitRequestPartition {
            partition_index: index,
            committed_offset: offset,
            committed_leader_epoch: UNKNOWN_LEADER_EPOCH,
            ..OffsetCommitRequestPartition::default()
        };
        check!(
            request
                == OffsetCommitRequest {
                    group_id: "orders-consumers".to_owned(),
                    generation_id_or_member_epoch: SIMPLE_CONSUMER_GENERATION,
                    member_id: String::new(),
                    topics: vec![
                        OffsetCommitRequestTopic {
                            name: "orders".to_owned(),
                            topic_id: orders,
                            partitions: vec![partition(0, 12), partition(1, 7)],
                            ..OffsetCommitRequestTopic::default()
                        },
                        OffsetCommitRequestTopic {
                            name: "payments".to_owned(),
                            // The cluster did not name an id for this topic, so
                            // the zero uuid goes on the wire and only the name
                            // resolves it.
                            topic_id: WireUuid::default(),
                            partitions: vec![partition(0, 3)],
                            ..OffsetCommitRequestTopic::default()
                        },
                    ],
                    ..OffsetCommitRequest::default()
                }
        );
    }

    #[test]
    fn only_the_refused_partitions_are_reported() {
        let response = OffsetCommitResponse {
            topics: vec![OffsetCommitResponseTopic {
                name: "orders".to_owned(),
                partitions: vec![
                    OffsetCommitResponsePartition {
                        partition_index: 0,
                        error_code: 0,
                        ..OffsetCommitResponsePartition::default()
                    },
                    OffsetCommitResponsePartition {
                        partition_index: 1,
                        error_code: 25,
                        ..OffsetCommitResponsePartition::default()
                    },
                ],
                ..OffsetCommitResponseTopic::default()
            }],
            ..OffsetCommitResponse::default()
        };
        check!(commit_refusals(&response) == vec!["orders-1: error code 25".to_owned()]);
    }

    #[test]
    fn the_offsets_file_counts_every_partition_of_every_group() {
        let file = GroupOffsetsFile {
            groups: vec![
                captured(),
                GroupOffsets {
                    group: "empty".to_owned(),
                    offsets: Vec::new(),
                },
            ],
        };
        check!(file.offset_count() == 3);
    }
}
