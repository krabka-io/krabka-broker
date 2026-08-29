//! The two streams records whose value is a single epoch counter.
//!
//! The group metadata record at key version 15 carries the group epoch, and
//! the target-assignment metadata record at key version 19 carries the
//! assignment epoch. Both encode as the `i16(0)` version preamble and one
//! `i32`.

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    coordinator::unified::persistence::{get_i16, get_i32},
    error::BrokerError,
};

/// Key v15 value: the streams group epoch.
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
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        Ok(Self {
            epoch: get_i32(&mut buf)?,
        })
    }
}

/// Key v19 value: the target-assignment epoch.
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
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        Ok(Self {
            assignment_epoch: get_i32(&mut buf)?,
        })
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
}
