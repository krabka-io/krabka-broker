//! The record keys of the KIP-1071 streams-group records, and their codec.
//!
//! Every key starts with an `i16` key-version discriminator, the `apiKey` of
//! the matching Apache Kafka schema at tag `4.3.1`: 17 for
//! `StreamsGroupMetadataKey`, 19 for `StreamsGroupMemberMetadataKey`, 20 for
//! `StreamsGroupTargetAssignmentMetadataKey`, 21 for
//! `StreamsGroupTargetAssignmentMemberKey`, 22 for
//! `StreamsGroupCurrentMemberAssignmentKey` and 23 for
//! `StreamsGroupTopologyKey`. The group id follows, and then the member id for
//! the per-member records. [`StreamsGroupKey`] is the parsed form that the
//! `__consumer_offsets` replay path dispatches on.
//!
//! These numbers are not free choices. Kafka's `GroupCoordinatorRecordSerde`
//! looks the leading `i16` up in its own `CoordinatorRecordType` table, so a
//! record written under a number Kafka assigns to a different type is decoded
//! as that other type, and its tools die in the middle of the topic rather than
//! skip it.
//!
//! Kafka 4.3.1 assigns no record type 18: KIP-1101 replaced the streams
//! partition-metadata record with the `MetadataHash` of
//! `StreamsGroupMetadataValue`. The broker neither writes nor reads it, so a
//! record of type 18 replays as an unknown type and is skipped, as Kafka's
//! loader skips it.
//!
//! Every `coordinator-key` schema declares `"flexibleVersions": "none"`, so a
//! key string keeps the legacy `i16` length prefix and a key carries no
//! tagged-field trailer. Only the values are flexible.

use {
    crate::{
        coordinator::unified::persistence::{encode_string_key, get_string},
        error::BrokerError,
    },
    bytes::Bytes,
    krabka_protocol::ProtocolError,
};

pub const KEY_STREAMS_GROUP_METADATA: i16 = 17;
pub const KEY_STREAMS_MEMBER_METADATA: i16 = 19;
pub const KEY_STREAMS_TARGET_ASSIGNMENT_METADATA: i16 = 20;
pub const KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER: i16 = 21;
pub const KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT: i16 = 22;
pub const KEY_STREAMS_TOPOLOGY: i16 = 23;

#[derive(Debug, Clone, PartialEq, Eq, krabka_macros::GroupRecordKey)]
#[group_key(invalid = "unknown streams-group key version")]
pub enum StreamsGroupKey {
    #[group_key(version = KEY_STREAMS_GROUP_METADATA)]
    GroupMetadata { group_id: String },
    #[group_key(version = KEY_STREAMS_MEMBER_METADATA)]
    MemberMetadata { group_id: String, member_id: String },
    #[group_key(version = KEY_STREAMS_TOPOLOGY)]
    Topology { group_id: String },
    #[group_key(version = KEY_STREAMS_TARGET_ASSIGNMENT_METADATA)]
    TargetAssignmentMetadata { group_id: String },
    #[group_key(version = KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER)]
    TargetAssignmentMember { group_id: String, member_id: String },
    #[group_key(version = KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT)]
    CurrentMemberAssignment { group_id: String, member_id: String },
}

/// # Errors
/// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
pub fn parse_streams_key(version: i16, buf: &[u8]) -> Result<StreamsGroupKey, BrokerError> {
    StreamsGroupKey::parse(version, buf)
}

/// Encodes a [`StreamsGroupKey`] for dispatch, with a leading `i16` key
/// version.
///
/// # Errors
///
/// Returns [`crate::error::BrokerError::Protocol`] when a string of the key is longer than
/// 32767 bytes.
pub fn encode_streams_key(key: &StreamsGroupKey) -> Result<Bytes, BrokerError> { key.encode() }

/// Encodes the group-metadata key.
///
/// # Errors
///
/// Returns [`crate::error::BrokerError::Protocol`] when a string of the key is longer than
/// 32767 bytes.
pub fn encode_group_metadata_key(group_id: &str) -> Result<Bytes, BrokerError> {
    encode_string_key(KEY_STREAMS_GROUP_METADATA, &[group_id])
}

/// Encodes the member-metadata key.
///
/// # Errors
///
/// Returns [`crate::error::BrokerError::Protocol`] when a string of the key is longer than
/// 32767 bytes.
pub fn encode_member_metadata_key(group_id: &str, member_id: &str) -> Result<Bytes, BrokerError> {
    encode_string_key(KEY_STREAMS_MEMBER_METADATA, &[group_id, member_id])
}

/// Encodes the topology key.
///
/// # Errors
///
/// Returns [`crate::error::BrokerError::Protocol`] when a string of the key is longer than
/// 32767 bytes.
pub fn encode_topology_key(group_id: &str) -> Result<Bytes, BrokerError> {
    encode_string_key(KEY_STREAMS_TOPOLOGY, &[group_id])
}

/// Encodes the target-assignment-metadata key.
///
/// # Errors
///
/// Returns [`crate::error::BrokerError::Protocol`] when a string of the key is longer than
/// 32767 bytes.
pub fn encode_target_assignment_metadata_key(group_id: &str) -> Result<Bytes, BrokerError> {
    encode_string_key(KEY_STREAMS_TARGET_ASSIGNMENT_METADATA, &[group_id])
}

/// Encodes the target-assignment-member key.
///
/// # Errors
///
/// Returns [`crate::error::BrokerError::Protocol`] when a string of the key is longer than
/// 32767 bytes.
pub fn encode_target_assignment_member_key(
    group_id: &str,
    member_id: &str,
) -> Result<Bytes, BrokerError> {
    encode_string_key(KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER, &[group_id, member_id])
}

/// Encodes the current-member-assignment key.
///
/// # Errors
///
/// Returns [`crate::error::BrokerError::Protocol`] when a string of the key is longer than
/// 32767 bytes.
pub fn encode_current_member_assignment_key(
    group_id: &str,
    member_id: &str,
) -> Result<Bytes, BrokerError> {
    encode_string_key(KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT, &[
        group_id, member_id,
    ])
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn unknown_key_version_rejected() {
        assert!(parse_streams_key(99, &[]).is_err());
        assert!(parse_streams_key(18, b"\x00\x02g1").is_err());
    }

    use {
        crate::coordinator::unified::{
            codec_observations::KafkaMetadataKey, test_support::wire_bytes,
        },
        rstest_bdd_macros::{given, scenarios, then, when},
    };
    type StreamsMetadataKeyScenario = KafkaMetadataKey;
    #[rstest::fixture]
    fn input() -> StreamsMetadataKeyScenario {
        // Start this scenario with fresh inputs and observations.
        StreamsMetadataKeyScenario::default()
    }
    #[given("the Streams compacted log record kind is {kind}")]
    fn metadata_record_kind(input: &mut StreamsMetadataKeyScenario, kind: String) {
        input.kind = kind;
    }
    #[when("the Streams coordinator selects the record API key")]
    fn metadata_api_key(input: &mut StreamsMetadataKeyScenario) {
        input.version = Some(match input.kind.as_str() {
            "group-metadata" => KEY_STREAMS_GROUP_METADATA,
            "member-metadata" => KEY_STREAMS_MEMBER_METADATA,
            "target-assignment-metadata" => KEY_STREAMS_TARGET_ASSIGNMENT_METADATA,
            "target-assignment-member" => KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER,
            "current-member-assignment" => KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT,
            "topology" => KEY_STREAMS_TOPOLOGY,
            _ => panic!("unknown metadata record kind"),
        });
    }
    #[then("the Streams record API key is {version}")]
    fn metadata_version(input: &StreamsMetadataKeyScenario, version: i16) {
        assert!(input.version == Some(version));
    }
    #[given("a Streams {kind} metadata key for group {group} and member {member}")]
    fn metadata_key_input(
        input: &mut StreamsMetadataKeyScenario,
        kind: String,
        group: String,
        member: String,
    ) {
        input.set_key_fields(kind, group, member);
    }
    #[when("the Streams coordinator encodes the metadata key")]
    fn encode_metadata_key(input: &mut StreamsMetadataKeyScenario) {
        input.encoded = Some(
            match input.kind.as_str() {
                "group" => encode_group_metadata_key(&input.group),
                "member" => encode_member_metadata_key(
                    &input.group,
                    input.member.as_deref().expect("member identifier"),
                ),
                _ => panic!("unknown Streams key kind"),
            }
            .expect("encode Streams metadata key"),
        );
    }
    #[then("the Streams metadata key has exact Kafka bytes {hex}")]
    fn metadata_key_bytes(input: &StreamsMetadataKeyScenario, hex: String) {
        let want = wire_bytes(&[&hex]);
        input.assert_kafka_bytes(&want);
    }
    scenarios!("tests/features/migrated/src/coordinator/unified/streams/persistence/keys.feature",fixtures=[input:StreamsMetadataKeyScenario]);
}
