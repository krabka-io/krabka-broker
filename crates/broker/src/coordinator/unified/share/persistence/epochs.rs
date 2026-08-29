//! The two share-group records whose value is a single epoch counter.
//!
//! The group metadata record at key version 9 carries the group epoch, and the
//! target-assignment metadata record at key version 11 carries the assignment
//! epoch. Both encode as the `i16(0)` version preamble followed by one `i32`.

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    coordinator::unified::persistence::{get_i16, get_i32},
    error::BrokerError,
};

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
    use crate::coordinator::unified::share::persistence::{
        KEY_SHARE_GROUP_METADATA, KEY_SHARE_TARGET_ASSIGNMENT_METADATA, ShareGroupKey,
        encode_share_key, parse_share_key, test_support::peek_version,
    };

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
}
