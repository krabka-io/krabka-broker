//! The two wire-shaped helpers: the `ConsumerGroupHeartbeat` request a modeled
//! client sends, and the assignment the coordinator advertised back in the
//! step it returned.
//!
//! They are the only place the model touches protocol types, so the rest of
//! the model works in plain partition vectors.

use std::collections::BTreeSet;

use krabka_protocol::owned::consumer_group_heartbeat_request::{
    ConsumerGroupHeartbeatRequest, TopicPartitions,
};

use super::{TOPIC, TOPIC_NAME};
use crate::coordinator::unified::actor::HeartbeatStep;

pub(super) fn hb_request(
    member_id: &str,
    member_epoch: i32,
    owned: &BTreeSet<i32>,
) -> ConsumerGroupHeartbeatRequest {
    ConsumerGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        subscribed_topic_names: Some(vec![TOPIC_NAME.into()]),
        rebalance_timeout_ms: 60_000,
        topic_partitions: Some(vec![TopicPartitions {
            topic_id: TOPIC,
            partitions: owned.iter().copied().collect(),
            ..Default::default()
        }]),
        ..Default::default()
    }
}

/// The steady-state heartbeat of the Java client, which sends only what
/// changed: no subscription, no rebalance timeout and no owned partitions. The
/// coordinator reads the absent owned set as "unchanged" (Kafka's
/// `ownsRevokedPartitions(null)`), and answers with an assignment only when the
/// member's assignment changed.
pub(super) fn keepalive_request(
    member_id: &str,
    member_epoch: i32,
) -> ConsumerGroupHeartbeatRequest {
    ConsumerGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        rebalance_timeout_ms: -1,
        ..Default::default()
    }
}

/// The partitions that the coordinator advertised to a member in the response
/// from `step`, or `None` when the response carries no assignment and the
/// member keeps the one it has.
pub(super) fn advertised_of(step: &HeartbeatStep) -> Option<Vec<i32>> {
    let assignment = step.response.assignment.as_ref()?;
    let mut v: Vec<i32> = assignment
        .topic_partitions
        .iter()
        .filter(|tp| tp.topic_id == TOPIC)
        .flat_map(|tp| tp.partitions.iter().copied())
        .collect();
    v.sort_unstable();
    Some(v)
}
