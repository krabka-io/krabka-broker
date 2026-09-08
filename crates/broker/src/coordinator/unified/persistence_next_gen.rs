//! KIP-848 record types persisted in `__consumer_offsets`.
//!
//! The wire encoding matches the Apache Kafka schemas at tag `4.3.1`, under
//! `group-coordinator/src/main/resources/common/message/`. A key is
//! non-flexible (`"flexibleVersions": "none"`) and starts with the schema's
//! `apiKey` as an `i16`. A value starts with an `i16` schema version, which is
//! 0 for all five records here, and is flexible (`"flexibleVersions": "0+"`):
//! compact strings, compact arrays, and a tagged-field trailer on the message
//! and on every nested struct.
//!
//! This file is the module root. The key discriminator and its codec live in
//! `keys`, the two single-epoch records in `epochs`, the member metadata record
//! and its classic sub-state in `member`, and the target and current assignment
//! records with their shared topic-partition codec in `assignment`.

mod assignment;
mod epochs;
mod keys;
mod member;

pub use self::{
    assignment::{
        AssignedTopicPartitions, CurrentMemberAssignmentValue, MemberAssignmentState,
        TargetAssignmentMemberValue,
    },
    epochs::{GroupMetadataValue, TargetAssignmentMetadataValue},
    keys::{
        KEY_CURRENT_MEMBER_ASSIGNMENT, KEY_GROUP_METADATA, KEY_MEMBER_METADATA,
        KEY_TARGET_ASSIGNMENT_MEMBER, KEY_TARGET_ASSIGNMENT_METADATA, NextGenKey, encode_key,
        parse_key,
    },
    member::{ClassicMemberMetadata, MemberMetadataValue},
};
