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

use crate::coordinator::unified::persistence::group_record_keys;

pub const KEY_GROUP_METADATA: i16 = 3;
pub const KEY_MEMBER_METADATA: i16 = 5;
pub const KEY_TARGET_ASSIGNMENT_METADATA: i16 = 6;
pub const KEY_TARGET_ASSIGNMENT_MEMBER: i16 = 7;
pub const KEY_CURRENT_MEMBER_ASSIGNMENT: i16 = 8;
pub const KEY_REGULAR_EXPRESSION: i16 = 16;

group_record_keys! {
    pub enum NextGenKey {
        GroupMetadata => KEY_GROUP_METADATA,
        MemberMetadata(member_id) => KEY_MEMBER_METADATA,
        TargetAssignmentMetadata => KEY_TARGET_ASSIGNMENT_METADATA,
        TargetAssignmentMember(member_id) => KEY_TARGET_ASSIGNMENT_MEMBER,
        CurrentMemberAssignment(member_id) => KEY_CURRENT_MEMBER_ASSIGNMENT,
        RegularExpression(regex) => KEY_REGULAR_EXPRESSION,
    }

    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    fn parse_key;

    /// Encodes a [`NextGenKey`] with its leading `i16` key version.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::BrokerError::Protocol`] when a string of the key is longer than
    /// 32767 bytes, which a non-flexible key string cannot carry.
    fn encode_key;

    invalid "unknown next-gen key version";
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
