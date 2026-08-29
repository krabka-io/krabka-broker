//! The KIP-932 target and current assignment records, and the topic-partition
//! list codec they share.
//!
//! [`ShareGroupTargetAssignmentMemberValue`] at key version 12 holds the
//! assignment the coordinator computed for one member, and
//! [`ShareGroupCurrentMemberAssignmentValue`] at key version 13 holds what that
//! member has converged on at its member epoch. Share groups never revoke, so
//! neither record carries a pending-revocation list or an assignment state.

use bytes::{BufMut, Bytes, BytesMut};
use krabka_protocol::{ProtocolError, primitives::uuid::Uuid};

use crate::{
    coordinator::unified::persistence::{get_bytes, get_i16, get_i32, put_bytes},
    error::BrokerError,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShareGroupTargetAssignmentMemberValue {
    pub topic_partitions: Vec<(Uuid, Vec<i32>)>,
}

impl ShareGroupTargetAssignmentMemberValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        encode_topic_partitions(&mut buf, &self.topic_partitions);
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let topic_partitions = decode_topic_partitions(&mut buf)?;
        Ok(Self { topic_partitions })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShareGroupCurrentMemberAssignmentValue {
    pub member_epoch: i32,
    pub assigned_partitions: Vec<(Uuid, Vec<i32>)>,
}

impl ShareGroupCurrentMemberAssignmentValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.member_epoch);
        encode_topic_partitions(&mut buf, &self.assigned_partitions);
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let member_epoch = get_i32(&mut buf)?;
        let assigned_partitions = decode_topic_partitions(&mut buf)?;
        Ok(Self {
            member_epoch,
            assigned_partitions,
        })
    }
}

fn encode_topic_partitions(buf: &mut BytesMut, items: &[(Uuid, Vec<i32>)]) {
    let n = i32::try_from(items.len()).expect("fits");
    buf.put_i32(n);
    for (topic_id, partitions) in items {
        put_bytes(buf, &Bytes::copy_from_slice(&topic_id.0));
        let pn = i32::try_from(partitions.len()).expect("fits");
        buf.put_i32(pn);
        for p in partitions {
            buf.put_i32(*p);
        }
    }
}

fn decode_topic_partitions(buf: &mut &[u8]) -> Result<Vec<(Uuid, Vec<i32>)>, BrokerError> {
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
        out.push((topic_id, partitions));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::share::persistence::{
        KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT, KEY_SHARE_TARGET_ASSIGNMENT_MEMBER, ShareGroupKey,
        encode_share_key, parse_share_key, test_support::peek_version,
    };

    #[test]
    fn target_assignment_member_round_trip() {
        let key = ShareGroupKey::TargetAssignmentMember {
            group_id: "g1".into(),
            member_id: "m1".into(),
        };
        let b = encode_share_key(&key);
        let (ver, body) = peek_version(&b);
        assert!(ver == KEY_SHARE_TARGET_ASSIGNMENT_MEMBER);
        assert!(parse_share_key(ver, body).unwrap() == key);

        let v = ShareGroupTargetAssignmentMemberValue {
            topic_partitions: vec![(Uuid([1; 16]), vec![0, 1, 2]), (Uuid([2; 16]), vec![])],
        };
        assert!(ShareGroupTargetAssignmentMemberValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn current_member_assignment_round_trip() {
        let key = ShareGroupKey::CurrentMemberAssignment {
            group_id: "g1".into(),
            member_id: "m1".into(),
        };
        let b = encode_share_key(&key);
        let (ver, body) = peek_version(&b);
        assert!(ver == KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT);
        assert!(parse_share_key(ver, body).unwrap() == key);

        let v = ShareGroupCurrentMemberAssignmentValue {
            member_epoch: 5,
            assigned_partitions: vec![(Uuid([3; 16]), vec![0, 1])],
        };
        assert!(ShareGroupCurrentMemberAssignmentValue::decode(&v.encode()).unwrap() == v);
    }
}
