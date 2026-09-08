//! The KIP-848 target and current assignment records, and the topic-partition
//! list codec they share.
//!
//! [`TargetAssignmentMemberValue`] at key version 7 holds the assignment the
//! coordinator computed for one member. [`CurrentMemberAssignmentValue`] at key
//! version 8 holds what that member has converged on, its
//! [`MemberAssignmentState`], and the partitions it still owes back.
//! [`AssignedTopicPartitions`] is the leaf record that both value types repeat.
//!
//! # Layout
//!
//! From `ConsumerGroupTargetAssignmentMemberValue.json` and
//! `ConsumerGroupCurrentMemberAssignmentValue.json` at Apache Kafka tag
//! `4.3.1`. Both declare `"flexibleVersions": "0+"`.
//!
//! - Target: `TopicPartitions` (`[]TopicPartition{TopicId uuid, Partitions
//!   []int32}`).
//! - Current: `MemberEpoch` (int32), `PreviousMemberEpoch` (int32), `State`
//!   (int8), `AssignedPartitions` and `PartitionsPendingRevocation`, both
//!   `[]TopicPartitions{TopicId uuid, Partitions []int32}` with a tagged
//!   `AssignmentEpochs` (tag 0, nullable, default null) the broker does not
//!   set.
//!
//! A `uuid` is sixteen raw bytes with no length prefix, arrays are compact, and
//! each element struct as well as the message carries a tagged-field trailer.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use krabka_protocol::{ProtocolError, primitives::uuid::Uuid};

use crate::{
    coordinator::unified::persistence::{
        flex::{
            get_compact_array_len, get_i32_array, get_uuid, put_compact_array_len,
            put_empty_tagged_fields, put_i32_array, put_uuid, skip_tagged_fields,
        },
        get_i16, get_i32,
    },
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
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        encode_topic_partitions(&mut buf, &self.topic_partitions);
        put_empty_tagged_fields(&mut buf);
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let topic_partitions = decode_topic_partitions(&mut buf)?;
        skip_tagged_fields(&mut buf)?;
        Ok(Self { topic_partitions })
    }
}

/// The member's reconciliation state, with Kafka's discriminants from
/// `org.apache.kafka.coordinator.group.modern.MemberState`: `STABLE` is 0,
/// `UNREVOKED_PARTITIONS` is 1, and `UNRELEASED_PARTITIONS` is 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemberAssignmentState {
    Stable = 0,
    UnrevokedPartitions = 1,
    UnreleasedPartitions = 2,
}

impl MemberAssignmentState {
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn from_i8(v: i8) -> Result<Self, BrokerError> {
        match v {
            0 => Ok(Self::Stable),
            1 => Ok(Self::UnrevokedPartitions),
            2 => Ok(Self::UnreleasedPartitions),
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
        put_empty_tagged_fields(&mut buf);
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
        skip_tagged_fields(&mut buf)?;
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
    put_compact_array_len(buf, items.len());
    for tp in items {
        put_uuid(buf, tp.topic_id.0);
        put_i32_array(buf, &tp.partitions);
        put_empty_tagged_fields(buf);
    }
}

fn decode_topic_partitions(buf: &mut &[u8]) -> Result<Vec<AssignedTopicPartitions>, BrokerError> {
    let n = get_compact_array_len(buf)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_id = Uuid(get_uuid(buf)?);
        let partitions = get_i32_array(buf)?;
        skip_tagged_fields(buf)?;
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
    fn target_assignment_member_bytes_match_kafka_schema() {
        let v = TargetAssignmentMemberValue {
            topic_partitions: vec![AssignedTopicPartitions {
                topic_id: Uuid([1; 16]),
                partitions: vec![0, 1],
            }],
        };
        let mut want: Vec<u8> = Vec::new();
        want.extend_from_slice(b"\x00\x00"); // value version 0
        want.push(0x02); // one TopicPartition
        want.extend_from_slice(&[1u8; 16]); // TopicId, sixteen raw bytes
        want.push(0x03); // two partitions
        want.extend_from_slice(&0i32.to_be_bytes());
        want.extend_from_slice(&1i32.to_be_bytes());
        want.push(0x00); // TopicPartition tagged fields
        want.push(0x00); // message tagged fields
        assert!(&v.encode()[..] == &want[..]);
    }

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
    fn current_member_assignment_bytes_match_kafka_schema() {
        let v = CurrentMemberAssignmentValue {
            member_epoch: 5,
            previous_member_epoch: 4,
            state: MemberAssignmentState::UnrevokedPartitions,
            assigned_partitions: vec![AssignedTopicPartitions {
                topic_id: Uuid([2; 16]),
                partitions: vec![7],
            }],
            partitions_pending_revocation: vec![],
        };
        let mut want: Vec<u8> = Vec::new();
        want.extend_from_slice(b"\x00\x00");
        want.extend_from_slice(&5i32.to_be_bytes());
        want.extend_from_slice(&4i32.to_be_bytes());
        want.push(0x01); // MemberState.UNREVOKED_PARTITIONS
        want.push(0x02); // one assigned TopicPartitions
        want.extend_from_slice(&[2u8; 16]);
        want.push(0x02); // one partition
        want.extend_from_slice(&7i32.to_be_bytes());
        want.push(0x00); // TopicPartitions tagged fields
        want.push(0x01); // empty PartitionsPendingRevocation
        want.push(0x00); // message tagged fields
        assert!(&v.encode()[..] == &want[..]);
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

    #[test]
    fn member_state_discriminants_match_kafka() {
        // org.apache.kafka.coordinator.group.modern.MemberState.
        assert!(MemberAssignmentState::Stable as i8 == 0);
        assert!(MemberAssignmentState::UnrevokedPartitions as i8 == 1);
        assert!(MemberAssignmentState::UnreleasedPartitions as i8 == 2);
        assert!(
            MemberAssignmentState::from_i8(1).unwrap()
                == MemberAssignmentState::UnrevokedPartitions
        );
        assert!(MemberAssignmentState::from_i8(3).is_err());
    }

    #[test]
    fn assignment_records_reject_a_missing_tagged_trailer() {
        let t = TargetAssignmentMemberValue::default().encode();
        assert!(TargetAssignmentMemberValue::decode(&t[..t.len() - 1]).is_err());
    }
}
