//! The two per-member task-assignment records, at key versions 21 and 22.
//!
//! The target assignment holds the active, standby, and warmup tasks the
//! coordinator wants a member to own. The current member assignment holds what
//! the member owns now, together with its reconciliation epochs and state and
//! any active task that is pending revocation. Each role is a map from a
//! subtopology id to the partitions of that subtopology.
//!
//! # Layout
//!
//! From `StreamsGroupTargetAssignmentMemberValue.json` and
//! `StreamsGroupCurrentMemberAssignmentValue.json` at Apache Kafka tag `4.3.1`.
//! Both declare `"flexibleVersions": "0+"`.
//!
//! - Target: `ActiveTasks`, `StandbyTasks` and `WarmupTasks`, each a
//!   `[]TaskIds{SubtopologyId string, Partitions []int32}`.
//! - Current: `MemberEpoch` (int32), `PreviousMemberEpoch` (int32), `State`
//!   (int8), then six `[]TaskIds`: the three roles, and then the three
//!   pending-revocation lists in the same role order.
//!
//! The broker revokes active tasks only, so it writes the standby and warmup
//! revocation lists empty, and drops them again on decode.

use std::collections::BTreeMap;

use bytes::{BufMut, Bytes, BytesMut};
use krabka_protocol::ProtocolError;

use super::codec::{decode_task_map, encode_task_map};
use crate::{
    coordinator::unified::{
        persistence::{
            flex::{get_i8, put_empty_tagged_fields, skip_tagged_fields},
            get_i16, get_i32,
        },
        streams::state::StreamsMemberAssignmentState,
    },
    error::BrokerError,
};

/// The member state as `org.apache.kafka.coordinator.group.streams.MemberState`
/// numbers it. Kafka's streams enum does not start at zero the way the consumer
/// one does: `STABLE` is 1, `UNREVOKED_TASKS` is 2 and `UNRELEASED_TASKS` is 3.
/// The broker's own [`StreamsMemberAssignmentState`] counts from zero, so the
/// two meet here rather than anywhere a reader of the group state could confuse
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamsMemberWireState {
    Stable = 1,
    UnrevokedTasks = 2,
    UnreleasedTasks = 3,
}

impl From<StreamsMemberAssignmentState> for StreamsMemberWireState {
    fn from(v: StreamsMemberAssignmentState) -> Self {
        match v {
            StreamsMemberAssignmentState::Stable => Self::Stable,
            StreamsMemberAssignmentState::UnrevokedActiveTasks => Self::UnrevokedTasks,
            StreamsMemberAssignmentState::UnreleasedActiveTasks => Self::UnreleasedTasks,
        }
    }
}

impl From<StreamsMemberWireState> for StreamsMemberAssignmentState {
    fn from(v: StreamsMemberWireState) -> Self {
        match v {
            StreamsMemberWireState::Stable => Self::Stable,
            StreamsMemberWireState::UnrevokedTasks => Self::UnrevokedActiveTasks,
            StreamsMemberWireState::UnreleasedTasks => Self::UnreleasedActiveTasks,
        }
    }
}

impl StreamsMemberWireState {
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn from_i8(v: i8) -> Result<Self, BrokerError> {
        match v {
            1 => Ok(Self::Stable),
            2 => Ok(Self::UnrevokedTasks),
            3 => Ok(Self::UnreleasedTasks),
            _ => Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "unknown streams MemberState",
            ))),
        }
    }
}

/// Key v21 value: a member's target task assignment, by role. Each role maps a
/// subtopology id to the partitions of that subtopology that the member holds.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamsGroupTargetAssignmentMemberValue {
    pub active: BTreeMap<String, Vec<i32>>,
    pub standby: BTreeMap<String, Vec<i32>>,
    pub warmup: BTreeMap<String, Vec<i32>>,
}

impl StreamsGroupTargetAssignmentMemberValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        encode_task_map(&mut buf, &self.active);
        encode_task_map(&mut buf, &self.standby);
        encode_task_map(&mut buf, &self.warmup);
        put_empty_tagged_fields(&mut buf);
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let active = decode_task_map(&mut buf)?;
        let standby = decode_task_map(&mut buf)?;
        let warmup = decode_task_map(&mut buf)?;
        skip_tagged_fields(&mut buf)?;
        Ok(Self {
            active,
            standby,
            warmup,
        })
    }
}

/// Key v22 value: a member's current in-flight task assignment, with the
/// reconciliation epochs and state, and any active task pending revocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupCurrentMemberAssignmentValue {
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub state: StreamsMemberWireState,
    pub active: BTreeMap<String, Vec<i32>>,
    pub standby: BTreeMap<String, Vec<i32>>,
    pub warmup: BTreeMap<String, Vec<i32>>,
    pub active_pending_revocation: BTreeMap<String, Vec<i32>>,
}

impl Default for StreamsGroupCurrentMemberAssignmentValue {
    fn default() -> Self {
        Self {
            member_epoch: 0,
            previous_member_epoch: 0,
            state: StreamsMemberWireState::Stable,
            active: BTreeMap::new(),
            standby: BTreeMap::new(),
            warmup: BTreeMap::new(),
            active_pending_revocation: BTreeMap::new(),
        }
    }
}

impl StreamsGroupCurrentMemberAssignmentValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.member_epoch);
        buf.put_i32(self.previous_member_epoch);
        buf.put_i8(self.state as i8);
        encode_task_map(&mut buf, &self.active);
        encode_task_map(&mut buf, &self.standby);
        encode_task_map(&mut buf, &self.warmup);
        encode_task_map(&mut buf, &self.active_pending_revocation);
        encode_task_map(&mut buf, &BTreeMap::new());
        encode_task_map(&mut buf, &BTreeMap::new());
        put_empty_tagged_fields(&mut buf);
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let member_epoch = get_i32(&mut buf)?;
        let previous_member_epoch = get_i32(&mut buf)?;
        let state = StreamsMemberWireState::from_i8(get_i8(&mut buf)?)?;
        let active = decode_task_map(&mut buf)?;
        let standby = decode_task_map(&mut buf)?;
        let warmup = decode_task_map(&mut buf)?;
        let active_pending_revocation = decode_task_map(&mut buf)?;
        let _standby_pending_revocation = decode_task_map(&mut buf)?;
        let _warmup_pending_revocation = decode_task_map(&mut buf)?;
        skip_tagged_fields(&mut buf)?;
        Ok(Self {
            member_epoch,
            previous_member_epoch,
            state,
            active,
            standby,
            warmup,
            active_pending_revocation,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::streams::persistence::{
        KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT, KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER,
        StreamsGroupKey, encode_current_member_assignment_key, encode_target_assignment_member_key,
        parse_streams_key, test_support::peek_version,
    };

    #[test]
    fn target_assignment_member_bytes_match_kafka_schema() {
        let mut active = BTreeMap::new();
        active.insert("0".to_string(), vec![1]);
        let v = StreamsGroupTargetAssignmentMemberValue {
            active,
            standby: BTreeMap::new(),
            warmup: BTreeMap::new(),
        };
        let mut want: Vec<u8> = vec![0x00, 0x00];
        want.push(0x02); // one ActiveTasks entry
        want.extend_from_slice(b"\x020"); // SubtopologyId "0"
        want.push(0x02); // one partition
        want.extend_from_slice(&1i32.to_be_bytes());
        want.push(0x00); // TaskIds tagged fields
        want.push(0x01); // empty StandbyTasks
        want.push(0x01); // empty WarmupTasks
        want.push(0x00); // message tagged fields
        assert!(&v.encode()[..] == &want[..]);
    }

    #[test]
    fn target_assignment_member_round_trip() {
        let kb = encode_target_assignment_member_key("g1", "m1");
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::TargetAssignmentMember {
                    group_id: "g1".into(),
                    member_id: "m1".into(),
                }
        );

        let mut active = BTreeMap::new();
        active.insert("0".to_string(), vec![0, 1, 2]);
        active.insert("1".to_string(), vec![]);
        let mut standby = BTreeMap::new();
        standby.insert("0".to_string(), vec![3]);
        let v = StreamsGroupTargetAssignmentMemberValue {
            active,
            standby,
            warmup: BTreeMap::new(),
        };
        assert!(StreamsGroupTargetAssignmentMemberValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn current_member_assignment_bytes_match_kafka_schema() {
        let v = StreamsGroupCurrentMemberAssignmentValue {
            member_epoch: 5,
            previous_member_epoch: 4,
            state: StreamsMemberWireState::UnrevokedTasks,
            ..Default::default()
        };
        let mut want: Vec<u8> = vec![0x00, 0x00];
        want.extend_from_slice(&5i32.to_be_bytes());
        want.extend_from_slice(&4i32.to_be_bytes());
        want.push(0x02); // streams MemberState.UNREVOKED_TASKS
        want.extend_from_slice(&[0x01; 6]); // six empty task lists
        want.push(0x00); // message tagged fields
        assert!(&v.encode()[..] == &want[..]);
    }

    #[test]
    fn current_member_assignment_round_trip() {
        let kb = encode_current_member_assignment_key("g1", "m1");
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::CurrentMemberAssignment {
                    group_id: "g1".into(),
                    member_id: "m1".into(),
                }
        );

        let mut active = BTreeMap::new();
        active.insert("0".to_string(), vec![0, 1]);
        let mut pending = BTreeMap::new();
        pending.insert("0".to_string(), vec![2]);
        let v = StreamsGroupCurrentMemberAssignmentValue {
            member_epoch: 5,
            previous_member_epoch: 4,
            state: StreamsMemberWireState::UnrevokedTasks,
            active,
            standby: BTreeMap::new(),
            warmup: BTreeMap::new(),
            active_pending_revocation: pending,
        };
        assert!(StreamsGroupCurrentMemberAssignmentValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn wire_state_matches_kafkas_streams_member_state() {
        // org.apache.kafka.coordinator.group.streams.MemberState.
        assert!(StreamsMemberWireState::Stable as i8 == 1);
        assert!(StreamsMemberWireState::UnrevokedTasks as i8 == 2);
        assert!(StreamsMemberWireState::UnreleasedTasks as i8 == 3);
        assert!(StreamsMemberWireState::from_i8(0).is_err());
        for state in [
            StreamsMemberAssignmentState::Stable,
            StreamsMemberAssignmentState::UnrevokedActiveTasks,
            StreamsMemberAssignmentState::UnreleasedActiveTasks,
        ] {
            let wire = StreamsMemberWireState::from(state);
            assert!(StreamsMemberAssignmentState::from(wire) == state);
        }
    }

    #[test]
    fn task_map_multi_subtopology_empty_partitions_round_trip() {
        // A task map with several subtopologies, some carrying no partitions,
        // must survive encode/decode unchanged.
        let mut active = BTreeMap::new();
        active.insert("0".to_string(), vec![0, 1, 2, 3]);
        active.insert("1".to_string(), vec![]);
        active.insert("2".to_string(), vec![7]);
        let v = StreamsGroupTargetAssignmentMemberValue {
            active,
            standby: BTreeMap::new(),
            warmup: BTreeMap::new(),
        };
        let decoded = StreamsGroupTargetAssignmentMemberValue::decode(&v.encode()).unwrap();
        assert!(decoded == v);
    }

    #[test]
    fn assignment_records_reject_a_missing_tagged_trailer() {
        let t = StreamsGroupTargetAssignmentMemberValue::default().encode();
        assert!(StreamsGroupTargetAssignmentMemberValue::decode(&t[..t.len() - 1]).is_err());
        let c = StreamsGroupCurrentMemberAssignmentValue::default().encode();
        assert!(StreamsGroupCurrentMemberAssignmentValue::decode(&c[..c.len() - 1]).is_err());
    }
}
