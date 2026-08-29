//! The two wire-shaped helpers: the `ConsumerGroupHeartbeat` request a modeled
//! client sends, and the assignment the coordinator advertised back in the step
//! it returned.
//!
//! They are the only place the model touches protocol types, so the rest of the
//! model works in plain partition vectors.

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

/// The partitions that the coordinator advertised to a member in the response
/// from `step`.
pub(super) fn advertised_of(step: &HeartbeatStep) -> Vec<i32> {
    let mut v: Vec<i32> = step
        .response
        .assignment
        .as_ref()
        .map(|a| {
            a.topic_partitions
                .iter()
                .filter(|tp| tp.topic_id == TOPIC)
                .flat_map(|tp| tp.partitions.iter().copied())
                .collect()
        })
        .unwrap_or_default();
    v.sort_unstable();
    v
}
