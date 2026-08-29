//! The two next-gen records whose value is a single epoch counter.
//!
//! The group metadata record at key version 3 carries the group epoch, and the
//! target-assignment metadata record at key version 6 carries the assignment
//! epoch. Both encode as the `i16(0)` version preamble followed by one `i32`.

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    coordinator::unified::persistence::{get_i16, get_i32},
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
pub struct TargetAssignmentMetadataValue {
    pub assignment_epoch: i32,
}

impl TargetAssignmentMetadataValue {
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

    #[test]
    fn group_metadata_value_roundtrip() {
        let v = GroupMetadataValue { epoch: 7 };
        assert!(GroupMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn target_assignment_metadata_roundtrip() {
        let v = TargetAssignmentMetadataValue {
            assignment_epoch: 12,
        };
        assert!(TargetAssignmentMetadataValue::decode(&v.encode()).unwrap() == v);
    }
}
