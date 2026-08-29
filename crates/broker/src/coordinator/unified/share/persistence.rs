//! KIP-932 share-group record types persisted in `__consumer_offsets`.
//!
//! The wire encoding mirrors `persistence_next_gen`, the KIP-848 consumer
//! next-gen codecs. Keys carry a leading `i16` key-version discriminator, and
//! values carry an `i16(0)` version preamble. Share-group records reuse the same
//! length-prefixed array and nullable-string leaf encoders. They drop the
//! consumer-only fields: `instance_id`, `server_assignor`,
//! `subscribed_topic_regex`, `rebalance_timeout_ms`, and the revocation and
//! pending-assignment machinery.
//!
//! Key versions 9-13 are free. The consumer next-gen keys use 3, 5, 6, 7, 8.
//!
//! This file is the module root. The key discriminator and its codec live in
//! `keys`, the two single-epoch records in `epochs`, the member metadata record
//! in `member`, the target and current assignment records with their shared
//! topic-partition codec in `assignment`, and the share-state partition
//! metadata record in `partition_metadata`.

mod assignment;
mod epochs;
mod keys;
mod member;
mod partition_metadata;

#[cfg(test)]
mod test_support;

pub use self::{
    assignment::{ShareGroupCurrentMemberAssignmentValue, ShareGroupTargetAssignmentMemberValue},
    epochs::{ShareGroupMetadataValue, ShareGroupTargetAssignmentMetadataValue},
    keys::{
        KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT, KEY_SHARE_GROUP_METADATA,
        KEY_SHARE_GROUP_STATE_PARTITION_METADATA, KEY_SHARE_MEMBER_METADATA,
        KEY_SHARE_TARGET_ASSIGNMENT_MEMBER, KEY_SHARE_TARGET_ASSIGNMENT_METADATA, ShareGroupKey,
        encode_share_key, parse_share_key,
    },
    member::ShareGroupMemberMetadataValue,
    partition_metadata::ShareGroupStatePartitionMetadataValue,
};
