//! The two share-group records whose value is a single epoch counter.
//!
//! The group metadata record at key version 11 carries the group epoch, and
//! the target-assignment metadata record at key version 12 carries the
//! assignment epoch.
//!
//! # Layout
//!
//! From `ShareGroupMetadataValue.json` and
//! `ShareGroupTargetAssignmentMetadataValue.json` at Apache Kafka tag `4.3.1`.
//! Both declare `"flexibleVersions": "0+"`.
//!
//! - `ShareGroupMetadataValue`: `Epoch` (int32) and `MetadataHash` (int64).
//!   The hash is a plain field rather than a tagged one, so it is always on the
//!   wire. The broker tracks no such hash and writes 0, which is what Kafka
//!   writes for a group whose topics hash to nothing yet, and drops the field
//!   on decode.
//! - `ShareGroupTargetAssignmentMetadataValue`: `AssignmentEpoch` (int32), then
//!   the tagged `AssignmentTimestamp` (int64, tag 0, default 0) from KIP-1263,
//!   which the broker leaves at its default and therefore omits.
//!
//! Both records end with the message's tagged-field count.

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    coordinator::unified::persistence::{
        flex::{put_empty_tagged_fields, skip_tagged_fields},
        get_i16, get_i32, get_i64,
    },
    error::BrokerError,
};

/// The `MetadataHash` the broker writes. It keeps no subscribed-topic hash of
/// its own, and 0 is the value Kafka's own record carries before one is
/// computed.
const METADATA_HASH: i64 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShareGroupMetadataValue {
    pub epoch: i32,
}

impl ShareGroupMetadataValue {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShareGroupTargetAssignmentMetadataValue {
    pub assignment_epoch: i32,
}

impl ShareGroupTargetAssignmentMetadataValue {
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
    use crate::coordinator::unified::share::persistence::{
        KEY_SHARE_GROUP_METADATA, KEY_SHARE_TARGET_ASSIGNMENT_METADATA, ShareGroupKey,
        encode_share_key, parse_share_key, test_support::peek_version,
    };

    #[test]
    fn group_metadata_bytes_match_kafka_schema() {
        // i16 version | i32 Epoch | i64 MetadataHash | uvarint tagged count.
        let v = ShareGroupMetadataValue { epoch: 7 };
        assert!(&v.encode()[..] == b"\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00");
    }

    #[test]
    fn group_metadata_round_trip() {
        let key = ShareGroupKey::GroupMetadata {
            group_id: "g1".into(),
        };
        let bytes = encode_share_key(&key);
        let (ver, body) = peek_version(&bytes);
        assert!(ver == KEY_SHARE_GROUP_METADATA);
        assert!(parse_share_key(ver, body).unwrap() == key);

        let v = ShareGroupMetadataValue { epoch: 7 };
        assert!(ShareGroupMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn target_assignment_metadata_bytes_match_kafka_schema() {
        let v = ShareGroupTargetAssignmentMetadataValue {
            assignment_epoch: 12,
        };
        assert!(&v.encode()[..] == b"\x00\x00\x00\x00\x00\x0c\x00");
    }

    #[test]
    fn target_assignment_metadata_round_trip() {
        let key = ShareGroupKey::TargetAssignmentMetadata {
            group_id: "g1".into(),
        };
        let b = encode_share_key(&key);
        let (ver, body) = peek_version(&b);
        assert!(ver == KEY_SHARE_TARGET_ASSIGNMENT_METADATA);
        assert!(parse_share_key(ver, body).unwrap() == key);

        let v = ShareGroupTargetAssignmentMetadataValue {
            assignment_epoch: 12,
        };
        assert!(ShareGroupTargetAssignmentMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn epoch_records_reject_a_missing_tagged_trailer() {
        let g = ShareGroupMetadataValue { epoch: 1 }.encode();
        assert!(ShareGroupMetadataValue::decode(&g[..g.len() - 1]).is_err());
        let t = ShareGroupTargetAssignmentMetadataValue {
            assignment_epoch: 1,
        }
        .encode();
        assert!(ShareGroupTargetAssignmentMetadataValue::decode(&t[..t.len() - 1]).is_err());
    }
}
