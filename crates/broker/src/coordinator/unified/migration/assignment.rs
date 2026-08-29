//! Translation from a member's server-side target assignment to the classic
//! `ConsumerProtocolAssignment` wire blob.
//!
//! Both migration directions and every classic RPC an upgraded group serves
//! read the same translation, so it lives in one place.

use std::collections::HashMap;

use bytes::{BufMut, Bytes, BytesMut};
use krabka_protocol::{
    Encode,
    owned::consumer_protocol_assignment::{ConsumerProtocolAssignment, TopicPartition},
    primitives::uuid::Uuid,
};

use crate::coordinator::unified::{
    consumer_state::GroupState as ConsumerState, reconciler::ReconcileInput,
};

/// Translates a member's server-side target, which maps topic ID to
/// partitions, into a classic `ConsumerProtocolAssignment` wire blob, which
/// maps topic name to partitions. The blob carries the leading `i16` version
/// prefix that a classic client expects in the `SyncGroup` assignment field.
///
/// This function drops a topic ID that the metadata image does not hold,
/// because the topic was deleted. The output order is deterministic, by topic
/// name.
pub(crate) fn target_to_consumer_assignment(
    target: &HashMap<Uuid, Vec<i32>>,
    image: &ReconcileInput,
) -> Bytes {
    let id_to_name: HashMap<Uuid, &str> = image
        .topic_id_by_name
        .iter()
        .map(|(name, id)| (*id, name.as_str()))
        .collect();
    let mut assigned: Vec<TopicPartition> = target
        .iter()
        .filter_map(|(tid, parts)| {
            id_to_name.get(tid).map(|name| {
                let mut p = parts.clone();
                p.sort_unstable();
                TopicPartition {
                    topic: (*name).to_string(),
                    partitions: p,
                    ..Default::default()
                }
            })
        })
        .collect();
    assigned.sort_by(|a, b| a.topic.cmp(&b.topic));
    let assignment = ConsumerProtocolAssignment {
        assigned_partitions: assigned,
        ..Default::default()
    };
    let mut out = BytesMut::new();
    out.put_i16(0); // "consumer" embedded-protocol version-negotiation prefix
    assignment
        .encode(&mut out, 0)
        .expect("ConsumerProtocolAssignment encode is infallible into BytesMut");
    out.freeze()
}

/// Translates a member's server-side TARGET into a
/// `ConsumerProtocolAssignment` blob. The target is the source of truth for
/// what the member should own, and it mirrors the native heartbeat response.
///
/// In the next-gen model a member's `assigned_partitions` fills in only as the
/// client acknowledges the target. A hosted classic member has no such
/// acknowledgement loop, so the target is what it must sync.
pub(super) fn member_target_assignment(
    state: &ConsumerState,
    member_id: &str,
    image: &ReconcileInput,
) -> Bytes {
    let target = state
        .target
        .per_member
        .get(member_id)
        .cloned()
        .unwrap_or_default();
    target_to_consumer_assignment(&target, image)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Buf;
    use krabka_protocol::Decode;

    use super::*;

    #[test]
    fn target_translates_to_consumer_assignment_blob() {
        let t1 = Uuid([1; 16]);
        let t2 = Uuid([2; 16]);
        let image = ReconcileInput {
            topic_id_by_name: [("orders".to_string(), t1), ("events".to_string(), t2)].into(),
            ..Default::default()
        };
        let target: std::collections::HashMap<Uuid, Vec<i32>> =
            [(t1, vec![2, 0, 1]), (t2, vec![5])].into();

        let blob = target_to_consumer_assignment(&target, &image);
        // Strip the version prefix and decode back.
        let mut cur = &blob[..];
        let version = cur.get_i16();
        assert!(version == 0);
        let decoded = ConsumerProtocolAssignment::decode(&mut cur, version).unwrap();
        // Deterministic order by topic name: events, orders.
        let names: Vec<&str> = decoded
            .assigned_partitions
            .iter()
            .map(|tp| tp.topic.as_str())
            .collect();
        assert!(names == vec!["events", "orders"]);
        let orders = decoded
            .assigned_partitions
            .iter()
            .find(|tp| tp.topic == "orders")
            .unwrap();
        // Partitions sorted.
        assert!(orders.partitions == vec![0, 1, 2]);
    }

    #[test]
    fn target_drops_unknown_topic_ids() {
        let known = Uuid([1; 16]);
        let ghost = Uuid([9; 16]);
        let image = ReconcileInput {
            topic_id_by_name: [("orders".to_string(), known)].into(),
            ..Default::default()
        };
        let target: std::collections::HashMap<Uuid, Vec<i32>> =
            [(known, vec![0]), (ghost, vec![0])].into();
        let blob = target_to_consumer_assignment(&target, &image);
        let mut cur = &blob[..];
        let _ = cur.get_i16();
        let decoded = ConsumerProtocolAssignment::decode(&mut cur, 0).unwrap();
        assert!(decoded.assigned_partitions.len() == 1);
        assert!(decoded.assigned_partitions[0].topic == "orders");
    }
}
