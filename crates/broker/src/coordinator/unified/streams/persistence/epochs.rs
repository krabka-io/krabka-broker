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
//!   The broker keeps none of the three, so it writes the hash as 0 and omits
//!   both tagged fields, which restores them at their defaults.
//! - `StreamsGroupTargetAssignmentMetadataValue`: `AssignmentEpoch` (int32),
//!   then the tagged `AssignmentTimestamp` (int64, tag 0, default 0) from
//!   KIP-1263, also omitted.

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    coordinator::unified::persistence::{
        flex::{put_empty_tagged_fields, skip_tagged_fields},
        get_i16, get_i32, get_i64,
    },
    error::BrokerError,
};

/// The `MetadataHash` the broker writes. It keeps no subscribed-topic hash of
/// its own.
const METADATA_HASH: i64 = 0;

/// Key v17 value: the streams group epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamsGroupMetadataValue {
    pub epoch: i32,
}

impl StreamsGroupMetadataValue {
    #[must_use]
    pub fn encode(self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.epoch);
        buf.put_i64(METADATA_HASH);
        put_empty_tagged_fields(&mut buf);
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let epoch = get_i32(&mut buf)?;
        let _metadata_hash = get_i64(&mut buf)?;
        skip_tagged_fields(&mut buf)?;
        Ok(Self { epoch })
    }
}

/// Key v20 value: the target-assignment epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamsGroupTargetAssignmentMetadataValue {
    pub assignment_epoch: i32,
}

impl StreamsGroupTargetAssignmentMetadataValue {
    #[must_use]
    pub fn encode(self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.assignment_epoch);
        put_empty_tagged_fields(&mut buf);
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let assignment_epoch = get_i32(&mut buf)?;
        skip_tagged_fields(&mut buf)?;
        Ok(Self { assignment_epoch })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::streams::persistence::{
        KEY_STREAMS_GROUP_METADATA, KEY_STREAMS_TARGET_ASSIGNMENT_METADATA, StreamsGroupKey,
        encode_group_metadata_key, encode_target_assignment_metadata_key, parse_streams_key,
        test_support::peek_version,
    };

    #[test]
    fn group_metadata_bytes_match_kafka_schema() {
        let v = StreamsGroupMetadataValue { epoch: 7 };
        assert!(&v.encode()[..] == b"\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00");
    }

    #[test]
    fn group_metadata_round_trip() {
        let kb = encode_group_metadata_key("g1");
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_GROUP_METADATA);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::GroupMetadata {
                    group_id: "g1".into()
                }
        );

        let v = StreamsGroupMetadataValue { epoch: 7 };
        assert!(StreamsGroupMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn target_assignment_metadata_bytes_match_kafka_schema() {
        let v = StreamsGroupTargetAssignmentMetadataValue {
            assignment_epoch: 12,
        };
        assert!(&v.encode()[..] == b"\x00\x00\x00\x00\x00\x0c\x00");
    }

    #[test]
    fn target_assignment_metadata_round_trip() {
        let kb = encode_target_assignment_metadata_key("g1");
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
        };
        assert!(StreamsGroupTargetAssignmentMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn epoch_records_reject_a_missing_tagged_trailer() {
        let g = StreamsGroupMetadataValue { epoch: 1 }.encode();
        assert!(StreamsGroupMetadataValue::decode(&g[..g.len() - 1]).is_err());
    }
}
