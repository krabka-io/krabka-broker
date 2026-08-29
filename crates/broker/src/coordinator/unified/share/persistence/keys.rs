//! The record keys of the KIP-932 share-group records, and their codec.
//!
//! Every key starts with an `i16` key-version discriminator in the range 9 to
//! 14, followed by the group id and, for the per-member records, the member id.
//! [`ShareGroupKey`] is the parsed form that the `__consumer_offsets` replay
//! path dispatches on.

use bytes::{BufMut, Bytes, BytesMut};
use krabka_protocol::ProtocolError;

use crate::{
    coordinator::unified::persistence::{get_string, put_string},
    error::BrokerError,
};

pub const KEY_SHARE_GROUP_METADATA: i16 = 9;
pub const KEY_SHARE_MEMBER_METADATA: i16 = 10;
pub const KEY_SHARE_TARGET_ASSIGNMENT_METADATA: i16 = 11;
pub const KEY_SHARE_TARGET_ASSIGNMENT_MEMBER: i16 = 12;
pub const KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT: i16 = 13;
pub const KEY_SHARE_GROUP_STATE_PARTITION_METADATA: i16 = 14;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShareGroupKey {
    GroupMetadata { group_id: String },
    MemberMetadata { group_id: String, member_id: String },
    TargetAssignmentMetadata { group_id: String },
    TargetAssignmentMember { group_id: String, member_id: String },
    CurrentMemberAssignment { group_id: String, member_id: String },
    StatePartitionMetadata { group_id: String },
}

#[must_use]
pub fn encode_share_key(key: &ShareGroupKey) -> Bytes {
    let mut buf = BytesMut::new();
    match key {
        ShareGroupKey::GroupMetadata { group_id } => {
            buf.put_i16(KEY_SHARE_GROUP_METADATA);
            put_string(&mut buf, group_id);
        }
        ShareGroupKey::MemberMetadata {
            group_id,
            member_id,
        } => {
            buf.put_i16(KEY_SHARE_MEMBER_METADATA);
            put_string(&mut buf, group_id);
            put_string(&mut buf, member_id);
        }
        ShareGroupKey::TargetAssignmentMetadata { group_id } => {
            buf.put_i16(KEY_SHARE_TARGET_ASSIGNMENT_METADATA);
            put_string(&mut buf, group_id);
        }
        ShareGroupKey::TargetAssignmentMember {
            group_id,
            member_id,
        } => {
            buf.put_i16(KEY_SHARE_TARGET_ASSIGNMENT_MEMBER);
            put_string(&mut buf, group_id);
            put_string(&mut buf, member_id);
        }
        ShareGroupKey::CurrentMemberAssignment {
            group_id,
            member_id,
        } => {
            buf.put_i16(KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT);
            put_string(&mut buf, group_id);
            put_string(&mut buf, member_id);
        }
        ShareGroupKey::StatePartitionMetadata { group_id } => {
            buf.put_i16(KEY_SHARE_GROUP_STATE_PARTITION_METADATA);
            put_string(&mut buf, group_id);
        }
    }
    buf.freeze()
}

/// # Errors
/// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
pub fn parse_share_key(version: i16, mut buf: &[u8]) -> Result<ShareGroupKey, BrokerError> {
    let key = match version {
        KEY_SHARE_GROUP_METADATA => ShareGroupKey::GroupMetadata {
            group_id: get_string(&mut buf)?,
        },
        KEY_SHARE_MEMBER_METADATA => ShareGroupKey::MemberMetadata {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        KEY_SHARE_TARGET_ASSIGNMENT_METADATA => ShareGroupKey::TargetAssignmentMetadata {
            group_id: get_string(&mut buf)?,
        },
        KEY_SHARE_TARGET_ASSIGNMENT_MEMBER => ShareGroupKey::TargetAssignmentMember {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT => ShareGroupKey::CurrentMemberAssignment {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        KEY_SHARE_GROUP_STATE_PARTITION_METADATA => ShareGroupKey::StatePartitionMetadata {
            group_id: get_string(&mut buf)?,
        },
        _ => {
            return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "unknown share-group key version",
            )));
        }
    };
    Ok(key)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn unknown_key_version_rejected() {
        assert!(parse_share_key(99, &[]).is_err());
    }
}
