//! KIP-848 record types persisted in `__consumer_offsets`. The wire encoding
//! matches the Apache Kafka reference implementation. Values carry a
//! version preamble.
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
