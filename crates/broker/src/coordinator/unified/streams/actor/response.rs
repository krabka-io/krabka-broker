//! Builders for the `StreamsGroupHeartbeat` responses and for the
//! `StreamsGroupDescribe` projection.
//!
//! Every heartbeat reply, whether it carries an error code or an assignment,
//! repeats the group's timing configuration, so the builders here share one
//! base response. They also render the in-memory `subtopology -> partitions`
//! task maps back into the wire `TaskIds` shape.

use std::collections::BTreeMap;

use krabka_protocol::owned::{
    common::streams_group_heartbeat_response::{status::Status, task_ids::TaskIds as RespTaskIds},
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};

use super::{StreamsDescribeMember, StreamsDescribeView};
use crate::{
    codes,
    coordinator::unified::streams::{
        config::StreamsGroupConfig, persistence::StreamsGroupTopologyValue,
        state::StreamsGroupState,
    },
};

/// Renders a `subtopology -> partitions` task map as a response
/// `Vec<TaskIds>`.
fn map_to_task_ids(map: &BTreeMap<String, Vec<i32>>) -> Vec<RespTaskIds> {
    map.iter()
        .map(|(sub, parts)| RespTaskIds {
            subtopology_id: sub.clone(),
            partitions: parts.clone(),
            ..Default::default()
        })
        .collect()
}

pub(super) fn base_resp(
    error_code: i16,
    member_epoch: i32,
    config: &StreamsGroupConfig,
) -> StreamsGroupHeartbeatResponse {
    StreamsGroupHeartbeatResponse {
        error_code,
        member_epoch,
        heartbeat_interval_ms: duration_ms(config.heartbeat_interval, 5_000),
        acceptable_recovery_lag: i32::try_from(config.acceptable_recovery_lag).unwrap_or(i32::MAX),
        task_offset_interval_ms: duration_ms(config.task_offset_interval, 30_000),
        ..Default::default()
    }
}

pub(super) fn error_resp(
    error_code: i16,
    config: &StreamsGroupConfig,
) -> StreamsGroupHeartbeatResponse {
    base_resp(error_code, 0, config)
}

pub(super) fn build_assignment_resp(
    state: &StreamsGroupState,
    member_id: &str,
    config: &StreamsGroupConfig,
) -> StreamsGroupHeartbeatResponse {
    let m = state
        .members
        .get(member_id)
        .expect("member exists at build_assignment_resp");
    let status = if state.status.is_empty() {
        None
    } else {
        Some(
            state
                .status
                .iter()
                .map(|(code, detail)| Status {
                    status_code: *code,
                    status_detail: detail.clone(),
                    ..Default::default()
                })
                .collect(),
        )
    };
    StreamsGroupHeartbeatResponse {
        error_code: codes::NONE,
        member_id: member_id.to_string(),
        member_epoch: m.member_epoch,
        heartbeat_interval_ms: duration_ms(config.heartbeat_interval, 5_000),
        acceptable_recovery_lag: i32::try_from(config.acceptable_recovery_lag).unwrap_or(i32::MAX),
        task_offset_interval_ms: duration_ms(config.task_offset_interval, 30_000),
        status,
        active_tasks: Some(map_to_task_ids(&m.active)),
        standby_tasks: Some(map_to_task_ids(&m.standby)),
        warmup_tasks: Some(map_to_task_ids(&m.warmup)),
        ..Default::default()
    }
}

pub(super) fn build_describe(
    state: &StreamsGroupState,
    topology: Option<&StreamsGroupTopologyValue>,
) -> StreamsDescribeView {
    StreamsDescribeView {
        group_id: state.group_id.clone(),
        group_epoch: state.group_epoch,
        assignment_epoch: state.target.epoch,
        topology_epoch: state.topology_epoch,
        group_state: state.phase.as_str().to_string(),
        topology: topology.cloned(),
        members: state
            .members
            .values()
            .map(|m| StreamsDescribeMember {
                member_id: m.member_id.clone(),
                member_epoch: m.member_epoch,
                instance_id: m.instance_id.clone(),
                rack_id: m.rack_id.clone(),
                client_id: m.client_id.clone(),
                client_host: m.client_host.clone(),
                process_id: m.process_id.clone(),
                active: m.active.clone(),
                standby: m.standby.clone(),
                warmup: m.warmup.clone(),
            })
            .collect(),
    }
}

fn duration_ms(d: std::time::Duration, fallback: i32) -> i32 {
    i32::try_from(d.as_millis()).unwrap_or(fallback)
}
