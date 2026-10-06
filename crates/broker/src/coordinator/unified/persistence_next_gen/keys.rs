//! The record keys of the KIP-848 next-gen consumer-group records, and their
//! codec.
//!
//! Every key starts with an `i16` key-version discriminator, the `apiKey` of
//! the matching Apache Kafka schema at tag `4.3.1`: 3 for
//! `ConsumerGroupMetadataKey`, 5 for `ConsumerGroupMemberMetadataKey`, 6 for
//! `ConsumerGroupTargetAssignmentMetadataKey`, 7 for
//! `ConsumerGroupTargetAssignmentMemberKey`, 8 for
//! `ConsumerGroupCurrentMemberAssignmentKey` and 16 for
//! `ConsumerGroupRegularExpressionKey`. The group id follows, and then the
//! member id for the per-member records and the regular expression for the
//! resolved-regular-expression record. [`NextGenKey`] is the parsed form that
//! the `__consumer_offsets` replay path dispatches on.
//!
//! Every `coordinator-key` schema declares `"flexibleVersions": "none"`, so a
//! key string keeps the legacy `i16` length prefix and a key carries no
//! tagged-field trailer. Only the values are flexible; see
//! [`persistence::flex`](crate::coordinator::unified::persistence::flex).
//!
//! `apiKey` 4 is Kafka's `ConsumerGroupPartitionMetadata`. The broker does not
//! write it, and the number is not reused.

use bytes::Bytes;
use krabka_protocol::ProtocolError;

use crate::{
    coordinator::unified::persistence::{encode_string_key, get_string},
    error::BrokerError,
};

pub const KEY_GROUP_METADATA: i16 = 3;
pub const KEY_MEMBER_METADATA: i16 = 5;
pub const KEY_TARGET_ASSIGNMENT_METADATA: i16 = 6;
pub const KEY_TARGET_ASSIGNMENT_MEMBER: i16 = 7;
pub const KEY_CURRENT_MEMBER_ASSIGNMENT: i16 = 8;
pub const KEY_REGULAR_EXPRESSION: i16 = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NextGenKey {
    GroupMetadata { group_id: String },
    MemberMetadata { group_id: String, member_id: String },
    TargetAssignmentMetadata { group_id: String },
    TargetAssignmentMember { group_id: String, member_id: String },
    CurrentMemberAssignment { group_id: String, member_id: String },
    RegularExpression { group_id: String, regex: String },
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
        KEY_REGULAR_EXPRESSION => NextGenKey::RegularExpression {
            group_id: get_string(&mut buf)?,
            regex: get_string(&mut buf)?,
        },
        _ => {
            return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "unknown next-gen key version",
            )));
        }
    };
    Ok(key)
}

/// Encodes a [`NextGenKey`] with its leading `i16` key version.
///
/// # Errors
///
/// Returns [`BrokerError::Protocol`] when a string of the key is longer than
/// 32767 bytes, which a non-flexible key string cannot carry.
pub fn encode_key(key: &NextGenKey) -> Result<Bytes, BrokerError> {
    match key {
        NextGenKey::GroupMetadata { group_id } => {
            encode_string_key(KEY_GROUP_METADATA, &[group_id])
        }
        NextGenKey::MemberMetadata {
            group_id,
            member_id,
        } => encode_string_key(KEY_MEMBER_METADATA, &[group_id, member_id]),
        NextGenKey::TargetAssignmentMetadata { group_id } => {
            encode_string_key(KEY_TARGET_ASSIGNMENT_METADATA, &[group_id])
        }
        NextGenKey::TargetAssignmentMember {
            group_id,
            member_id,
        } => encode_string_key(KEY_TARGET_ASSIGNMENT_MEMBER, &[group_id, member_id]),
        NextGenKey::CurrentMemberAssignment {
            group_id,
            member_id,
        } => encode_string_key(KEY_CURRENT_MEMBER_ASSIGNMENT, &[group_id, member_id]),
        NextGenKey::RegularExpression { group_id, regex } => {
            encode_string_key(KEY_REGULAR_EXPRESSION, &[group_id, regex])
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn unknown_key_version_rejected() {
        assert!(parse_key(99, &[]).is_err());
    }

    /// `ConsumerGroupRegularExpressionKey`: an `i16` key version of 16, then
    /// the group id and the regular expression as non-flexible strings.
    #[test]
    fn regular_expression_key_bytes_match_kafka_schema() {
        let key = NextGenKey::RegularExpression {
            group_id: "g".into(),
            regex: "a.*".into(),
        };
        assert!(&encode_key(&key).unwrap()[..] == b"\x00\x10\x00\x01g\x00\x03a.*");
        let encoded = encode_key(&key).unwrap();
        let mut r = &encoded[..];
        let v = bytes::Buf::get_i16(&mut r);
        assert!(parse_key(v, r).unwrap() == key);
    }

    #[test]
    fn key_roundtrip_member_metadata() {
        let k = NextGenKey::MemberMetadata {
            group_id: "g".into(),
            member_id: "m".into(),
        };
        let kb = encode_key(&k).unwrap();
        let mut r = &kb[..];
        let v = bytes::Buf::get_i16(&mut r);
        assert!(parse_key(v, r).unwrap() == k);
    }
}
