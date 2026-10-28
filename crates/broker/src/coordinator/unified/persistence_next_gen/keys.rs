//! The record keys of the KIP-848 next-gen consumer-group records, and their
//! codec.
//!
//! Every key starts with an `i16` key-version discriminator, the `apiKey` of
//! the matching Apache Kafka schema at tag `4.3.1`: 3 for
//! `ConsumerGroupMetadataKey`, 4 for the deprecated
//! `ConsumerGroupPartitionMetadataKey`, 5 for `ConsumerGroupMemberMetadataKey`, 6 for
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
//! The broker writes the key-4 record only as a tombstone, as Kafka 4.3.1
//! does; see [`partition_metadata`](super::partition_metadata).

use {
    crate::{
        coordinator::unified::persistence::{encode_string_key, get_string},
        error::BrokerError,
    },
    bytes::Bytes,
    krabka_protocol::ProtocolError,
};

pub const KEY_GROUP_METADATA: i16 = 3;
pub const KEY_PARTITION_METADATA: i16 = 4;
pub const KEY_MEMBER_METADATA: i16 = 5;
pub const KEY_TARGET_ASSIGNMENT_METADATA: i16 = 6;
pub const KEY_TARGET_ASSIGNMENT_MEMBER: i16 = 7;
pub const KEY_CURRENT_MEMBER_ASSIGNMENT: i16 = 8;
pub const KEY_REGULAR_EXPRESSION: i16 = 16;

#[derive(Debug, Clone, PartialEq, Eq, krabka_macros::GroupRecordKey)]
#[group_key(invalid = "unknown next-gen key version")]
pub enum NextGenKey {
    #[group_key(version = KEY_GROUP_METADATA)]
    GroupMetadata { group_id: String },
    /// The deprecated `ConsumerGroupPartitionMetadataKey`.
    #[group_key(version = KEY_PARTITION_METADATA)]
    PartitionMetadata { group_id: String },
    #[group_key(version = KEY_MEMBER_METADATA)]
    MemberMetadata { group_id: String, member_id: String },
    #[group_key(version = KEY_TARGET_ASSIGNMENT_METADATA)]
    TargetAssignmentMetadata { group_id: String },
    #[group_key(version = KEY_TARGET_ASSIGNMENT_MEMBER)]
    TargetAssignmentMember { group_id: String, member_id: String },
    #[group_key(version = KEY_CURRENT_MEMBER_ASSIGNMENT)]
    CurrentMemberAssignment { group_id: String, member_id: String },
    #[group_key(version = KEY_REGULAR_EXPRESSION)]
    RegularExpression { group_id: String, regex: String },
}

/// # Errors
/// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
pub fn parse_key(version: i16, buf: &[u8]) -> Result<NextGenKey, BrokerError> {
    NextGenKey::parse(version, buf)
}

/// Encodes a [`NextGenKey`] with its leading `i16` key version.
///
/// # Errors
///
/// Returns [`crate::error::BrokerError::Protocol`] when a string of the key is longer than
/// 32767 bytes, which a non-flexible key string cannot carry.
pub fn encode_key(key: &NextGenKey) -> Result<Bytes, BrokerError> { key.encode() }

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    /// `ConsumerGroupPartitionMetadataKey`: an `i16` key version of 4, then
    /// the group id as a non-flexible string.
    #[test]
    fn unknown_key_version_rejected() {
        assert!(parse_key(99, &[]).is_err());
    }

    /// `ConsumerGroupRegularExpressionKey`: an `i16` key version of 16, then
    /// the group id and the regular expression as non-flexible strings.
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
    use {
        crate::coordinator::unified::test_support::wire_bytes,
        rstest_bdd_macros::{given, scenarios, then, when},
    };
    #[derive(Default)]
    struct ConsumerKeyEncoding {
        key: Option<NextGenKey>,
        encoded: Option<Bytes>,
        decoded: Option<NextGenKey>,
    }
    #[rstest::fixture]
    fn consumer_key() -> ConsumerKeyEncoding {
        // Start this scenario with fresh inputs and observations.
        ConsumerKeyEncoding::default()
    }
    #[given("a consumer {kind} key for group {group} and expression {expression}")]
    fn consumer_key_fields(
        consumer_key: &mut ConsumerKeyEncoding,
        kind: String,
        group: String,
        expression: String,
    ) {
        consumer_key.key = Some(match kind.as_str() {
            "partition-metadata" => NextGenKey::PartitionMetadata { group_id: group },
            "regular-expression" => NextGenKey::RegularExpression {
                group_id: group,
                regex: expression,
            },
            _ => panic!("unknown consumer key family"),
        });
    }
    #[when("the coordinator encodes the consumer key and parses its Kafka key version")]
    fn consumer_key_codec(consumer_key: &mut ConsumerKeyEncoding) {
        let encoded = encode_key(consumer_key.key.as_ref().expect("key fields prepared"))
            .expect("encode key");
        let mut body = &encoded[..];
        let version = bytes::Buf::get_i16(&mut body);
        consumer_key.decoded = Some(parse_key(version, body).expect("decode key"));
        consumer_key.encoded = Some(encoded);
    }
    #[then("the consumer key bytes are {hex} and all original key fields survive decoding")]
    fn consumer_key_golden(consumer_key: &ConsumerKeyEncoding, hex: String) {
        let want = wire_bytes(&[&hex]);
        assert!(&consumer_key.encoded.as_ref().expect("key encoded")[..] == &want[..]);
        assert!(consumer_key.decoded == consumer_key.key);
    }
    scenarios!("tests/features/migrated/src/coordinator/unified/persistence_next_gen/keys.feature",fixtures=[consumer_key:ConsumerKeyEncoding]);
}
