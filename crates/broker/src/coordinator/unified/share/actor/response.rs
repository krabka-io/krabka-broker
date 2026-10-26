//! Construction of the `ShareGroupHeartbeat` response. It keeps the wire
//! shaping of the success, error, and assignment replies out of the request
//! handlers that decide which of them to send.

use {
    crate::{
        codes,
        coordinator::unified::share::{config::ShareGroupConfig, state::ShareGroupState},
    },
    krabka_protocol::{
        owned::{
            common::share_group_heartbeat_response::topic_partitions::TopicPartitions,
            share_group_heartbeat_response::{
                Assignment as RespAssignment, ShareGroupHeartbeatResponse,
            },
        },
        primitives::uuid::Uuid,
    },
    std::collections::HashMap,
};

pub(super) fn base_resp(
    error_code: i16,
    member_epoch: i32,
    config: &ShareGroupConfig,
) -> ShareGroupHeartbeatResponse {
    ShareGroupHeartbeatResponse {
        error_code,
        member_epoch,
        heartbeat_interval_ms: i32::try_from(config.heartbeat_interval.as_millis())
            .unwrap_or(5_000),
        ..Default::default()
    }
}

pub(super) fn error_resp(
    error_code: i16,
    config: &ShareGroupConfig,
) -> ShareGroupHeartbeatResponse {
    base_resp(error_code, 0, config)
}

/// The success response for `member_id`. It carries the member's assignment
/// only when `with_assignment` is set; Kafka leaves it null otherwise.
pub(super) fn build_assignment_resp(
    state: &ShareGroupState,
    member_id: &str,
    config: &ShareGroupConfig,
    with_assignment: bool,
) -> ShareGroupHeartbeatResponse {
    let Some(m) = state.members.get(member_id) else {
        return error_resp(codes::UNKNOWN_MEMBER_ID, config);
    };
    let assignment = with_assignment.then(|| RespAssignment {
        topic_partitions: topic_partitions(&m.assigned_partitions),
        ..Default::default()
    });
    ShareGroupHeartbeatResponse {
        error_code: 0,
        member_id: Some(member_id.into()),
        member_epoch: m.member_epoch,
        heartbeat_interval_ms: i32::try_from(config.heartbeat_interval.as_millis())
            .unwrap_or(5_000),
        assignment,
        ..Default::default()
    }
}

/// The wire rows of a member's assigned partitions, one per topic.
fn topic_partitions(assigned: &HashMap<Uuid, Vec<i32>>) -> Vec<TopicPartitions> {
    assigned
        .iter()
        .map(|(&topic_id, partitions)| TopicPartitions {
            topic_id,
            partitions: partitions.clone(),
            ..Default::default()
        })
        .collect()
}
