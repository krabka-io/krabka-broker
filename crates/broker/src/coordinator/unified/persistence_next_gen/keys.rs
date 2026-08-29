//! The record keys of the KIP-848 next-gen consumer-group records, and their
//! codec.
//!
//! Every key starts with an `i16` key-version discriminator drawn from 3, 5, 6,
//! 7, and 8, followed by the group id and, for the per-member records, the
//! member id. [`NextGenKey`] is the parsed form that the `__consumer_offsets`
//! replay path dispatches on.

use bytes::{BufMut, Bytes, BytesMut};
use krabka_protocol::ProtocolError;

use crate::{
    coordinator::unified::persistence::{get_string, put_string},
    error::BrokerError,
};

pub const KEY_GROUP_METADATA: i16 = 3;
pub const KEY_MEMBER_METADATA: i16 = 5;
pub const KEY_TARGET_ASSIGNMENT_METADATA: i16 = 6;
pub const KEY_TARGET_ASSIGNMENT_MEMBER: i16 = 7;
pub const KEY_CURRENT_MEMBER_ASSIGNMENT: i16 = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NextGenKey {
    GroupMetadata { group_id: String },
    MemberMetadata { group_id: String, member_id: String },
    TargetAssignmentMetadata { group_id: String },
    TargetAssignmentMember { group_id: String, member_id: String },
    CurrentMemberAssignment { group_id: String, member_id: String },
}

/// # Errors
/// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
pub fn parse_key(version: i16, mut buf: &[u8]) -> Result<NextGenKey, BrokerError> {
    let key = match version {
        KEY_GROUP_METADATA => NextGenKey::GroupMetadata {
            group_id: get_string(&mut buf)?,
        },
        KEY_MEMBER_METADATA => NextGenKey::MemberMetadata {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        KEY_TARGET_ASSIGNMENT_METADATA => NextGenKey::TargetAssignmentMetadata {
            group_id: get_string(&mut buf)?,
        },
        KEY_TARGET_ASSIGNMENT_MEMBER => NextGenKey::TargetAssignmentMember {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        KEY_CURRENT_MEMBER_ASSIGNMENT => NextGenKey::CurrentMemberAssignment {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        _ => {
            return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "unknown next-gen key version",
            )));
        }
    };
    Ok(key)
}

#[must_use]
pub fn encode_key(key: &NextGenKey) -> Bytes {
    let mut buf = BytesMut::new();
    match key {
        NextGenKey::GroupMetadata { group_id } => {
            buf.put_i16(KEY_GROUP_METADATA);
            put_string(&mut buf, group_id);
        }
        NextGenKey::MemberMetadata {
            group_id,
            member_id,
        } => {
            buf.put_i16(KEY_MEMBER_METADATA);
            put_string(&mut buf, group_id);
            put_string(&mut buf, member_id);
        }
        NextGenKey::TargetAssignmentMetadata { group_id } => {
            buf.put_i16(KEY_TARGET_ASSIGNMENT_METADATA);
            put_string(&mut buf, group_id);
        }
        NextGenKey::TargetAssignmentMember {
            group_id,
            member_id,
        } => {
            buf.put_i16(KEY_TARGET_ASSIGNMENT_MEMBER);
            put_string(&mut buf, group_id);
            put_string(&mut buf, member_id);
        }
        NextGenKey::CurrentMemberAssignment {
            group_id,
            member_id,
        } => {
            buf.put_i16(KEY_CURRENT_MEMBER_ASSIGNMENT);
            put_string(&mut buf, group_id);
            put_string(&mut buf, member_id);
        }
    }
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn unknown_key_version_rejected() {
        assert!(parse_key(99, &[]).is_err());
    }

    #[test]
    fn key_roundtrip_member_metadata() {
        let k = NextGenKey::MemberMetadata {
            group_id: "g".into(),
            member_id: "m".into(),
        };
        let kb = encode_key(&k);
        let mut r = &kb[..];
        let v = bytes::Buf::get_i16(&mut r);
        assert!(parse_key(v, r).unwrap() == k);
    }
}
