//! KIP-1071 streams-group record types persisted in `__consumer_offsets`.
//!
//! The wire encoding mirrors
//! [`persistence_next_gen`](crate::coordinator::unified::persistence_next_gen),
//! the KIP-848 consumer next-gen codecs, and the KIP-932 share equivalent
//! ([`super::super::share::persistence`]). Keys carry a leading `i16`
//! key-version discriminator, and values carry an `i16(0)` version preamble.
//!
//! Streams records reuse the same length-prefixed array, nullable-string, and
//! uuid leaf encoders. They model *tasks* rather than topic partitions. A task
//! is a `(subtopology, partition)` pair, grouped by the active, standby, or
//! warmup role.
//!
//! Key versions 15 to 21 belong to streams. The earlier ranges are taken: 0
//! and 1 for offset-commit, 2 for the classic group, 3, 5, 6, 7, and 8 for
//! consumer next-gen, and 9 to 14 for share.
//!
//! This module is deliberately self-contained. It defines its own value
//! structs, and it represents the assignment by role as
//! `BTreeMap<String, Vec<i32>>`, which maps a subtopology id to partitions,
//! instead of importing the in-memory state model.

mod assignment;
mod codec;
mod epochs;
mod keys;
mod member;
mod partition_metadata;
mod pending;
mod topology;

#[cfg(test)]
mod test_support;

pub use self::{
    assignment::{
        StreamsGroupCurrentMemberAssignmentValue, StreamsGroupTargetAssignmentMemberValue,
    },
    epochs::{StreamsGroupMetadataValue, StreamsGroupTargetAssignmentMetadataValue},
    keys::{
        KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT, KEY_STREAMS_GROUP_METADATA,
        KEY_STREAMS_MEMBER_METADATA, KEY_STREAMS_PARTITION_METADATA,
        KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER, KEY_STREAMS_TARGET_ASSIGNMENT_METADATA,
        KEY_STREAMS_TOPOLOGY, StreamsGroupKey, encode_current_member_assignment_key,
        encode_group_metadata_key, encode_member_metadata_key, encode_partition_metadata_key,
        encode_streams_key, encode_target_assignment_member_key,
        encode_target_assignment_metadata_key, encode_topology_key, parse_streams_key,
    },
    member::{StreamsEndpoint, StreamsGroupMemberMetadataValue},
    partition_metadata::{StreamsGroupPartitionMetadataValue, StreamsTopicMeta},
    pending::PendingStreamsRecords,
    topology::{
        StoredCopartitionGroup, StoredSubtopology, StoredTopicInfo, StreamsGroupTopologyValue,
    },
};
