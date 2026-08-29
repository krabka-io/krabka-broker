//! The two per-member task-assignment records, at key versions 20 and 21.
//!
//! The target assignment holds the active, standby, and warmup tasks the
//! coordinator wants a member to own. The current member assignment holds what
//! the member owns now, together with its reconciliation epochs and state and
//! any active task that is pending revocation. Each role is a map from a
//! subtopology id to the partitions of that subtopology.

use std::collections::BTreeMap;

use bytes::{BufMut, Bytes, BytesMut};

use super::codec::{decode_task_map, encode_task_map, get_i8};
use crate::{
    coordinator::unified::persistence::{get_i16, get_i32},
    error::BrokerError,
};

/// Key v20 value: a member's target task assignment, by role. Each role maps a
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
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let active = decode_task_map(&mut buf)?;
        let standby = decode_task_map(&mut buf)?;
        let warmup = decode_task_map(&mut buf)?;
        Ok(Self {
            active,
            standby,
            warmup,
        })
    }
}

/// Key v21 value: a member's current in-flight task assignment, with the
/// reconciliation epochs and state, and any active task pending revocation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamsGroupCurrentMemberAssignmentValue {
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub state: i8,
    pub active: BTreeMap<String, Vec<i32>>,
    pub standby: BTreeMap<String, Vec<i32>>,
    pub warmup: BTreeMap<String, Vec<i32>>,
    pub active_pending_revocation: BTreeMap<String, Vec<i32>>,
}

impl StreamsGroupCurrentMemberAssignmentValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.member_epoch);
        buf.put_i32(self.previous_member_epoch);
        buf.put_i8(self.state);
        encode_task_map(&mut buf, &self.active);
        encode_task_map(&mut buf, &self.standby);
        encode_task_map(&mut buf, &self.warmup);
        encode_task_map(&mut buf, &self.active_pending_revocation);
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let member_epoch = get_i32(&mut buf)?;
        let previous_member_epoch = get_i32(&mut buf)?;
        let state = get_i8(&mut buf)?;
        let active = decode_task_map(&mut buf)?;
        let standby = decode_task_map(&mut buf)?;
        let warmup = decode_task_map(&mut buf)?;
        let active_pending_revocation = decode_task_map(&mut buf)?;
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
            state: 1,
            active,
            standby: BTreeMap::new(),
            warmup: BTreeMap::new(),
            active_pending_revocation: pending,
        };
        assert!(StreamsGroupCurrentMemberAssignmentValue::decode(&v.encode()).unwrap() == v);
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
            active: active.clone(),
            standby: BTreeMap::new(),
            warmup: BTreeMap::new(),
        };
        let decoded = StreamsGroupTargetAssignmentMemberValue::decode(&v.encode()).unwrap();
        assert!(decoded == v);
    }
}
