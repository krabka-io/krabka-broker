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
//!   Kafka's own rebalance trigger. The group keeps it as Kafka does (see
//!   [`topic_hash`](crate::coordinator::unified::topic_hash)).
//! - `ConsumerGroupTargetAssignmentMetadataValue`: `AssignmentEpoch` (int32),
//!   then the tagged `AssignmentTimestamp` (int64, tag 0, default 0), which
//!   KIP-1263 added.
//!
//! Kafka's generated writer leaves a tagged field out when it holds its
//! default, and its reader restores the default for an absent one. A record
//! whose tagged fields all hold their defaults therefore ends in the empty
//! tagged-field trailer, the single `0` byte whose absence is what makes
//! Kafka's reader underflow.

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    coordinator::unified::persistence::{
        flex::{epoch_value, get_value_version, put_tagged_fields, read_tagged},
        get_i32,
    },
    error::BrokerError,
};

/// The tag of `ConsumerGroupMetadataValue.MetadataHash`.
const TAG_METADATA_HASH: u32 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupMetadataValue {
    pub epoch: i32,
    /// Kafka's `MetadataHash`: the hash of the subscribed topics' metadata
    /// when the epoch was written.
    pub metadata_hash: i64,
}

impl GroupMetadataValue {
    #[must_use]
    pub fn encode(self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.epoch);
        let tags = if self.metadata_hash == 0 {
            Vec::new()
        } else {
            vec![(
                TAG_METADATA_HASH,
                Bytes::copy_from_slice(&self.metadata_hash.to_be_bytes()),
            )]
        };
        put_tagged_fields(&mut buf, tags);
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when `buf` ends before a field, when the value version
    /// is not 0, or when the tagged `MetadataHash` is shorter than eight bytes.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        get_value_version(&mut buf, 0, "unknown ConsumerGroupMetadataValue version")?;
        let epoch = get_i32(&mut buf)?;
        let mut metadata_hash = 0;
        read_tagged(&mut buf, |tag, payload| {
            if tag != TAG_METADATA_HASH {
                return Ok(false);
            }
            metadata_hash = krabka_protocol::primitives::fixed::get_i64(payload)?;
            Ok(true)
        })?;
        Ok(Self {
            epoch,
            metadata_hash,
        })
    }
}

epoch_value!(TargetAssignmentMetadataValue("ConsumerGroupTargetAssignmentMetadataValue") {
    assignment_epoch
});

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    /// `ConsumerGroupMetadataValue` as Kafka 4.3.1's generated writer lays it
    /// out: i16 version 0, i32 `Epoch`, then the tagged-field trailer, which
    /// holds `MetadataHash` as tag 0 with an eight-byte payload unless it is
    /// the default 0.
    #[test]
    fn group_metadata_value_bytes_match_kafka_schema() {
        // (case, value, Kafka's bytes)
        let rows: [(&str, GroupMetadataValue, &[u8]); 3] = [
            (
                "no subscribed topic: the hash is the default and absent",
                GroupMetadataValue {
                    epoch: 7,
                    metadata_hash: 0,
                },
                b"\x00\x00\x00\x00\x00\x07\x00",
            ),
            (
                "a small hash",
                GroupMetadataValue {
                    epoch: 7,
                    metadata_hash: 0x2a,
                },
                b"\x00\x00\x00\x00\x00\x07\x01\x00\x08\x00\x00\x00\x00\x00\x00\x00\x2a",
            ),
            (
                "Kafka's group hash of the golden topics foo and bar",
                GroupMetadataValue {
                    epoch: 2,
                    metadata_hash: -556_879_919_459_959_918,
                },
                b"\x00\x00\x00\x00\x00\x02\x01\x00\x08\xf8\x45\x90\xa9\xea\x0a\x7b\x92",
            ),
        ];
        for (case, value, bytes) in rows {
            check!(&value.encode()[..] == bytes, "{case}");
            check!(
                GroupMetadataValue::decode(bytes).unwrap() == value,
                "{case}"
            );
        }
    }

    #[test]
    fn group_metadata_value_rejects_a_missing_tagged_trailer() {
        // This is the byte whose absence made Kafka's generated reader throw
        // BufferUnderflowException over the whole topic.
        let full = GroupMetadataValue {
            epoch: 7,
            metadata_hash: 0,
        }
        .encode();
        let truncated = &full[..full.len() - 1];
        assert!(GroupMetadataValue::decode(truncated).is_err());
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
