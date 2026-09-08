//! KIP-1071 streams-group record types persisted in `__consumer_offsets`.
//!
//! The wire encoding follows the Apache Kafka schemas at tag `4.3.1`, under
//! `group-coordinator/src/main/resources/common/message/`, as the KIP-848
//! ([`persistence_next_gen`](crate::coordinator::unified::persistence_next_gen))
//! and KIP-932 ([`super::super::share::persistence`]) families do. A key is
//! non-flexible and starts with the schema's `apiKey` as an `i16`; a value
//! starts with an `i16` schema version, 0 for every record here, and is
//! flexible: compact strings, compact arrays and a tagged-field trailer on the
//! message and on every nested struct.
//!
//! Streams records model *tasks* rather than topic partitions. A task is a
//! `(subtopology, partition)` pair, grouped by the active, standby, or warmup
//! role.
//!
//! Key versions 17 and 19 to 23 belong to streams, and 18 to the one record
//! Kafka no longer defines; see `keys` for the full mapping. The earlier
//! numbers are Kafka's: 0 and 1 for offset-commit, 2 for the classic group, 3
//! to 8 for the consumer next-gen family, and 10 to 15 for share.
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
        StreamsMemberWireState,
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
