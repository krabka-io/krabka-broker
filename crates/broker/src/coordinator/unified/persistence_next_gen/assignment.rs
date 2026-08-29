//! The KIP-848 target and current assignment records, and the topic-partition
//! list codec they share.
//!
//! [`TargetAssignmentMemberValue`] at key version 7 holds the assignment the
//! coordinator computed for one member. [`CurrentMemberAssignmentValue`] at key
//! version 8 holds what that member has converged on, its
//! [`MemberAssignmentState`], and the partitions it still owes back.
//! [`AssignedTopicPartitions`] is the leaf record that both value types repeat.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use krabka_protocol::{ProtocolError, primitives::uuid::Uuid};

use crate::{
    coordinator::unified::persistence::{get_bytes, get_i16, get_i32, put_bytes},
    error::BrokerError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignedTopicPartitions {
    pub topic_id: Uuid,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TargetAssignmentMemberValue {
    pub topic_partitions: Vec<AssignedTopicPartitions>,
}

impl TargetAssignmentMemberValue {
    #[must_use]
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        let n = i32::try_from(self.topic_partitions.len()).expect("fits");
        buf.put_i32(n);
        for tp in &self.topic_partitions {
            put_bytes(&mut buf, &Bytes::copy_from_slice(&tp.topic_id.0));
            let pn = i32::try_from(tp.partitions.len()).expect("fits");
            buf.put_i32(pn);
            for p in &tp.partitions {
                buf.put_i32(*p);
            }
        }
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let n = get_i32(&mut buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative");
        let mut topic_partitions = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            let id_bytes = get_bytes(&mut buf)?;
            if id_bytes.len() != 16 {
                return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                    "topic_id not 16 bytes",
                )));
            }
            let mut arr = [0u8; 16];
            arr.copy_from_slice(&id_bytes);
            let topic_id = Uuid(arr);
            let pn = get_i32(&mut buf)?;
            let pcap = usize::try_from(pn.max(0)).expect("non-negative");
            let mut partitions = Vec::with_capacity(pcap);
            for _ in 0..pn.max(0) {
                partitions.push(get_i32(&mut buf)?);
            }
            topic_partitions.push(AssignedTopicPartitions {
                topic_id,
                partitions,
            });
        }
        Ok(Self { topic_partitions })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemberAssignmentState {
    Stable = 0,
    UnreleasedPartitions = 1,
    UnrevokedPartitions = 2,
}

impl MemberAssignmentState {
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn from_i8(v: i8) -> Result<Self, BrokerError> {
        match v {
            0 => Ok(Self::Stable),
            1 => Ok(Self::UnreleasedPartitions),
            2 => Ok(Self::UnrevokedPartitions),
            _ => Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "unknown MemberAssignmentState",
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentMemberAssignmentValue {
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub state: MemberAssignmentState,
    pub assigned_partitions: Vec<AssignedTopicPartitions>,
    pub partitions_pending_revocation: Vec<AssignedTopicPartitions>,
}

impl CurrentMemberAssignmentValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.member_epoch);
        buf.put_i32(self.previous_member_epoch);
        buf.put_i8(self.state as i8);
        encode_topic_partitions(&mut buf, &self.assigned_partitions);
        encode_topic_partitions(&mut buf, &self.partitions_pending_revocation);
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let member_epoch = get_i32(&mut buf)?;
        let previous_member_epoch = get_i32(&mut buf)?;
        if buf.remaining() < 1 {
            return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "missing state byte",
            )));
        }
        let state = MemberAssignmentState::from_i8(buf.get_i8())?;
        let assigned_partitions = decode_topic_partitions(&mut buf)?;
        let partitions_pending_revocation = decode_topic_partitions(&mut buf)?;
        Ok(Self {
            member_epoch,
            previous_member_epoch,
            state,
            assigned_partitions,
            partitions_pending_revocation,
        })
    }
}

fn encode_topic_partitions(buf: &mut BytesMut, items: &[AssignedTopicPartitions]) {
    let n = i32::try_from(items.len()).expect("fits");
    buf.put_i32(n);
    for tp in items {
        put_bytes(buf, &Bytes::copy_from_slice(&tp.topic_id.0));
        let pn = i32::try_from(tp.partitions.len()).expect("fits");
        buf.put_i32(pn);
        for p in &tp.partitions {
            buf.put_i32(*p);
        }
    }
}

fn decode_topic_partitions(buf: &mut &[u8]) -> Result<Vec<AssignedTopicPartitions>, BrokerError> {
    let n = get_i32(buf)?;
    let cap = usize::try_from(n.max(0)).expect("non-negative");
    let mut out = Vec::with_capacity(cap);
    for _ in 0..n.max(0) {
        let id_bytes = get_bytes(buf)?;
        if id_bytes.len() != 16 {
            return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "topic_id not 16 bytes",
            )));
        }
        let mut arr = [0u8; 16];
        arr.copy_from_slice(&id_bytes);
        let topic_id = Uuid(arr);
        let pn = get_i32(buf)?;
        let pcap = usize::try_from(pn.max(0)).expect("non-negative");
        let mut partitions = Vec::with_capacity(pcap);
        for _ in 0..pn.max(0) {
            partitions.push(get_i32(buf)?);
        }
        out.push(AssignedTopicPartitions {
            topic_id,
            partitions,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn target_assignment_member_roundtrip() {
        let v = TargetAssignmentMemberValue {
            topic_partitions: vec![AssignedTopicPartitions {
                topic_id: Uuid([1; 16]),
                partitions: vec![0, 1, 2],
            }],
        };
        assert!(TargetAssignmentMemberValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn current_member_assignment_roundtrip() {
        let v = CurrentMemberAssignmentValue {
            member_epoch: 5,
            previous_member_epoch: 4,
            state: MemberAssignmentState::Stable,
            assigned_partitions: vec![AssignedTopicPartitions {
                topic_id: Uuid([2; 16]),
                partitions: vec![0, 1],
            }],
            partitions_pending_revocation: vec![],
        };
        assert!(CurrentMemberAssignmentValue::decode(&v.encode()).unwrap() == v);
    }
}
