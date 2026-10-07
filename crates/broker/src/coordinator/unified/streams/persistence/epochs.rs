//! The two streams records whose value is a single epoch counter.
//!
//! The group metadata record at key version 17 carries the group epoch, and
//! the target-assignment metadata record at key version 20 carries the
//! assignment epoch.
//!
//! # Layout
//!
//! From `StreamsGroupMetadataValue.json` and
//! `StreamsGroupTargetAssignmentMetadataValue.json` at Apache Kafka tag
//! `4.3.1`. Both declare `"flexibleVersions": "0+"`.
//!
//! - `StreamsGroupMetadataValue`: `Epoch` (int32) and `MetadataHash` (int64),
//!   both plain fields, then the tagged `ValidatedTopologyEpoch` (int32, tag 0,
//!   default -1) and `LastAssignmentConfigs` (tag 1, nullable, default null).
//!   The broker keeps the hash, and it omits both tagged fields, which
//!   restores them at their defaults. Kafka trunk adds KIP-1331's
//!   `StoredDescriptionTopologyEpoch` (int32, tag 2, default -1) and
//!   `FailedDescriptionTopologyEpoch` (int32, tag 3, default -1), which the
//!   broker keeps as [`DescriptionEpochs`]. As Kafka's generated writer does,
//!   it writes each of the two only when it is not -1.
//! - `StreamsGroupTargetAssignmentMetadataValue`: `AssignmentEpoch` (int32),
//!   then the tagged `AssignmentTimestamp` (int64, tag 0, default 0) from
//!   KIP-1263, which Kafka's generated writer omits when it is 0.

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    coordinator::unified::persistence::{
        flex::{epoch_value, get_value_version, put_tagged_fields, read_tagged},
        get_i32, get_i64,
    },
    error::BrokerError,
};

/// The tag of `StoredDescriptionTopologyEpoch`.
const TAG_STORED_DESCRIPTION_TOPOLOGY_EPOCH: u32 = 2;
/// The tag of `FailedDescriptionTopologyEpoch`.
const TAG_FAILED_DESCRIPTION_TOPOLOGY_EPOCH: u32 = 3;

/// KIP-1331's record of what the topology description plugin holds for a
/// group: Kafka's `StreamsGroup.storedDescriptionTopologyEpoch` and
/// `failedDescriptionTopologyEpoch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_macros::FieldDefaults)]
pub struct DescriptionEpochs {
    /// The topology epoch whose description the plugin holds,
    /// [`Self::NONE`] when it holds none, or [`Self::UNCERTAIN`] when a plugin
    /// operation may not have completed.
    #[default(Self::NONE)]
    pub stored: i32,
    /// The topology epoch whose description the plugin rejected for good, or
    /// [`Self::NONE`].
    #[default(Self::NONE)]
    pub failed: i32,
}

impl DescriptionEpochs {
    /// Kafka's `STORED_TOPOLOGY_EPOCH_NONE`, the schema default of both
    /// fields.
    pub const NONE: i32 = -1;
    /// Kafka's `STORED_TOPOLOGY_EPOCH_UNCERTAIN`: the plugin may or may not
    /// hold a description.
    pub const UNCERTAIN: i32 = -2;

    /// Whether the plugin holds the description of `topology_epoch`: Kafka's
    /// `isReliablyStoredTopologyEpoch` and an equal epoch.
    #[must_use]
    pub fn holds(self, topology_epoch: i32) -> bool {
        self.stored >= 0 && self.stored == topology_epoch
    }
}

/// Key v17 value: the streams group epoch, the metadata hash that the group
/// last configured its topology against, and the KIP-1331 description epochs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamsGroupMetadataValue {
    pub epoch: i32,
    /// Kafka's `MetadataHash`: see
    /// [`metadata_hash`](crate::coordinator::unified::streams::topology::metadata_hash).
    pub metadata_hash: i64,
    pub description: DescriptionEpochs,
}

impl StreamsGroupMetadataValue {
    #[must_use]
    pub fn encode(self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.epoch);
        buf.put_i64(self.metadata_hash);
        let tags = [
            (
                TAG_STORED_DESCRIPTION_TOPOLOGY_EPOCH,
                self.description.stored,
            ),
            (
                TAG_FAILED_DESCRIPTION_TOPOLOGY_EPOCH,
                self.description.failed,
            ),
        ]
        .into_iter()
        .filter(|&(_, epoch)| epoch != DescriptionEpochs::NONE)
        .map(|(tag, epoch)| (tag, Bytes::copy_from_slice(&epoch.to_be_bytes())))
        .collect();
        put_tagged_fields(&mut buf, tags);
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when `buf` ends before a field, or when a tagged
    /// description epoch is not four bytes long.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        get_value_version(&mut buf, 0, "unknown StreamsGroupMetadataValue version")?;
        let epoch = get_i32(&mut buf)?;
        let metadata_hash = get_i64(&mut buf)?;
        let mut description = DescriptionEpochs::default();
        read_tagged(&mut buf, |tag, payload| {
            let field = match tag {
                TAG_STORED_DESCRIPTION_TOPOLOGY_EPOCH => &mut description.stored,
                TAG_FAILED_DESCRIPTION_TOPOLOGY_EPOCH => &mut description.failed,
                _ => return Ok(false),
            };
            *field = krabka_protocol::primitives::fixed::get_i32(payload)?;
            Ok(true)
        })?;
        Ok(Self {
            epoch,
            metadata_hash,
            description,
        })
    }
}

epoch_value!(
    /// Key v20 value: the target-assignment epoch and the KIP-1263
    /// `AssignmentTimestamp`.
    StreamsGroupTargetAssignmentMetadataValue("StreamsGroupTargetAssignmentMetadataValue") {
        assignment_epoch,
        /// Kafka's `AssignmentTimestamp`: the wall-clock time in milliseconds at
        /// which the assignment calculation finished, or 0 when it is unknown.
        assignment_timestamp_ms,
    }
);

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::{
        streams::persistence::{
            KEY_STREAMS_GROUP_METADATA, KEY_STREAMS_TARGET_ASSIGNMENT_METADATA, StreamsGroupKey,
            encode_group_metadata_key, encode_target_assignment_metadata_key, parse_streams_key,
        },
        test_support::peek_version,
    };

    /// Kafka's generated writer puts a tagged field only when it is not its
    /// default, in tag order. Each row is the whole value: version, epoch,
    /// hash, then the trailer. The last row is what Kafka writes with the
    /// `ValidatedTopologyEpoch` (tag 0) the broker does not keep, which a read
    /// skips.
    #[test]
    fn group_metadata_bytes_match_kafka_schema() {
        const HEAD: &[u8] = b"\x00\x00\x00\x00\x00\x07\x01\x02\x03\x04\x05\x06\x07\x08";
        let value = |stored, failed| StreamsGroupMetadataValue {
            epoch: 7,
            metadata_hash: 0x0102_0304_0506_0708,
            description: DescriptionEpochs { stored, failed },
        };
        let rows: [(&str, StreamsGroupMetadataValue, &[u8]); 4] = [
            ("no description", value(-1, -1), b"\x00"),
            (
                "a stored description",
                value(4, -1),
                b"\x01\x02\x04\x00\x00\x00\x04",
            ),
            (
                "an uncertain store and a failed epoch",
                value(-2, 3),
                b"\x02\x02\x04\xff\xff\xff\xfe\x03\x04\x00\x00\x00\x03",
            ),
            (
                "a failed epoch alone",
                value(-1, 0),
                b"\x01\x03\x04\x00\x00\x00\x00",
            ),
        ];
        for (row, value, trailer) in rows {
            let bytes = value.encode();
            assert!(bytes[..] == [HEAD, trailer].concat()[..], "{row}");
            assert!(
                StreamsGroupMetadataValue::decode(&bytes).unwrap() == value,
                "{row}"
            );
        }

        let with_validated_epoch = [
            HEAD,
            b"\x02\x00\x04\x00\x00\x00\x05\x02\x04\x00\x00\x00\x04",
        ]
        .concat();
        assert!(StreamsGroupMetadataValue::decode(&with_validated_epoch).unwrap() == value(4, -1));
    }

    #[test]
    fn group_metadata_round_trip() {
        let kb = encode_group_metadata_key("g1").unwrap();
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_GROUP_METADATA);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::GroupMetadata {
                    group_id: "g1".into()
                }
        );

        let v = StreamsGroupMetadataValue {
            epoch: 7,
            metadata_hash: -9,
            description: DescriptionEpochs::default(),
        };
        assert!(StreamsGroupMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    /// i16 version | i32 `AssignmentEpoch` | tagged-field trailer, which holds
    /// `AssignmentTimestamp` as tag 0 with an eight-byte payload unless it is
    /// the default 0.
    #[test]
    fn target_assignment_metadata_bytes_match_kafka_schema() {
        // (case, value, Kafka's bytes)
        let rows: [(&str, StreamsGroupTargetAssignmentMetadataValue, &[u8]); 2] = [
            (
                "the time is unknown",
                StreamsGroupTargetAssignmentMetadataValue {
                    assignment_epoch: 12,
                    assignment_timestamp_ms: 0,
                },
                b"\x00\x00\x00\x00\x00\x0c\x00",
            ),
            (
                "an assignment that finished at 2026-10-07T00:00:00Z",
                StreamsGroupTargetAssignmentMetadataValue {
                    assignment_epoch: 12,
                    assignment_timestamp_ms: 1_791_331_200_000,
                },
                b"\x00\x00\x00\x00\x00\x0c\x01\x00\x08\x00\x00\x01\xa1\x13\xa8\xec\x00",
            ),
        ];
        for (case, value, bytes) in rows {
            assert!(&value.encode()[..] == bytes, "{case}");
            assert!(
                StreamsGroupTargetAssignmentMetadataValue::decode(bytes).unwrap() == value,
                "{case}"
            );
        }
    }

    #[test]
    fn target_assignment_metadata_round_trip() {
        let kb = encode_target_assignment_metadata_key("g1").unwrap();
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_TARGET_ASSIGNMENT_METADATA);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::TargetAssignmentMetadata {
                    group_id: "g1".into()
                }
        );

        let v = StreamsGroupTargetAssignmentMetadataValue {
            assignment_epoch: 12,
            assignment_timestamp_ms: 7,
        };
        assert!(StreamsGroupTargetAssignmentMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn epoch_records_reject_a_missing_tagged_trailer() {
        let g = StreamsGroupMetadataValue {
            epoch: 1,
            metadata_hash: 0,
            description: DescriptionEpochs::default(),
        }
        .encode();
        assert!(StreamsGroupMetadataValue::decode(&g[..g.len() - 1]).is_err());
    }
}
