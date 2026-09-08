//! The two next-gen records whose value is a single epoch counter.
//!
//! The group metadata record at key version 3 carries the group epoch, and the
//! target-assignment metadata record at key version 6 carries the assignment
//! epoch.
//!
//! # Layout
//!
//! Both come from the Apache Kafka schemas at tag `4.3.1`, and both declare
//! `"flexibleVersions": "0+"`:
//!
//! - `ConsumerGroupMetadataValue`: `Epoch` (int32), then the tagged
//!   `MetadataHash` (int64, tag 0, default 0), which KIP-1101 added for
//!   Kafka's own rebalance trigger.
//! - `ConsumerGroupTargetAssignmentMetadataValue`: `AssignmentEpoch` (int32),
//!   then the tagged `AssignmentTimestamp` (int64, tag 0, default 0), which
//!   KIP-1263 added.
//!
//! The broker keeps state for neither tagged field, so it writes each at its
//! default, which the wire format spells as "absent" and Kafka's reader
//! restores as the default. Both records therefore end in the empty
//! tagged-field trailer, the single `0` byte whose absence is what makes
//! Kafka's reader underflow.

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    coordinator::unified::persistence::{
        flex::{put_empty_tagged_fields, skip_tagged_fields},
        get_i16, get_i32,
    },
    error::BrokerError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupMetadataValue {
    pub epoch: i32,
}

impl GroupMetadataValue {
    #[must_use]
    pub fn encode(self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.epoch);
        put_empty_tagged_fields(&mut buf);
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let epoch = get_i32(&mut buf)?;
        skip_tagged_fields(&mut buf)?;
        Ok(Self { epoch })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetAssignmentMetadataValue {
    pub assignment_epoch: i32,
}

impl TargetAssignmentMetadataValue {
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

    #[test]
    fn group_metadata_value_bytes_match_kafka_schema() {
        // i16 version 0 | i32 Epoch | uvarint tagged-field count 0.
        let v = GroupMetadataValue { epoch: 7 };
        assert!(&v.encode()[..] == b"\x00\x00\x00\x00\x00\x07\x00");
    }

    #[test]
    fn group_metadata_value_roundtrip() {
        let v = GroupMetadataValue { epoch: 7 };
        assert!(GroupMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn group_metadata_value_rejects_a_missing_tagged_trailer() {
        // This is the byte whose absence made Kafka's generated reader throw
        // BufferUnderflowException over the whole topic.
        let full = GroupMetadataValue { epoch: 7 }.encode();
        let truncated = &full[..full.len() - 1];
        assert!(GroupMetadataValue::decode(truncated).is_err());
    }

    #[test]
    fn group_metadata_value_accepts_a_kafka_written_metadata_hash() {
        // Kafka writes the tagged MetadataHash when it is non-zero: one tagged
        // field, tag 0, eight bytes of payload.
        let bytes: &[u8] = b"\x00\x00\x00\x00\x00\x07\x01\x00\x08\x00\x00\x00\x00\x00\x00\x00\x2a";
        assert!(GroupMetadataValue::decode(bytes).unwrap() == GroupMetadataValue { epoch: 7 });
    }

    #[test]
    fn target_assignment_metadata_bytes_match_kafka_schema() {
        let v = TargetAssignmentMetadataValue {
            assignment_epoch: 12,
        };
        assert!(&v.encode()[..] == b"\x00\x00\x00\x00\x00\x0c\x00");
    }

    #[test]
    fn target_assignment_metadata_roundtrip() {
        let v = TargetAssignmentMetadataValue {
            assignment_epoch: 12,
        };
        assert!(TargetAssignmentMetadataValue::decode(&v.encode()).unwrap() == v);
    }
}
