//! Builders for the `StreamsGroupHeartbeat` responses and for the
//! `StreamsGroupDescribe` projection.
//!
//! A heartbeat that the group accepts gets the group's timing configuration,
//! its status list and, when they changed, its tasks and the endpoint
//! information of the group. A refused heartbeat gets only the error code and
//! message, as Kafka's `GroupCoordinatorService` builds it. The builders also
//! render the in-memory `subtopology -> partitions` task maps back into the
//! wire `TaskIds` shape.

use std::collections::{BTreeMap, BTreeSet};

use krabka_metadata::MetadataImage;
use krabka_protocol::owned::{
    common::streams_group_heartbeat_response::{
        endpoint::Endpoint, status::Status, task_ids::TaskIds as RespTaskIds,
        topic_partition::TopicPartition,
    },
    streams_group_heartbeat_response::{EndpointToPartitions, StreamsGroupHeartbeatResponse},
};

use super::{StreamsDescribeMember, StreamsDescribeView};
use crate::{
    codes,
    coordinator::unified::streams::{
        config::StreamsGroupConfig,
        persistence::StreamsGroupTopologyValue,
        state::{StreamsGroupState, StreamsMemberState},
        topology::{ConfiguredSubtopology, status as topo_status},
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

/// The response to a refused heartbeat: the error code and message, with every
/// other field at the default of Kafka's generated
/// `StreamsGroupHeartbeatResponseData`, where the status list is empty.
pub(crate) fn error_resp(
    error_code: i16,
    error_message: Option<String>,
) -> StreamsGroupHeartbeatResponse {
    StreamsGroupHeartbeatResponse {
        error_code,
        error_message,
        status: Some(Vec::new()),
        ..Default::default()
    }
}

/// What an accepted heartbeat sends beyond the member epoch and the status.
#[derive(Debug, Default)]
pub(super) struct ResponseDelta {
    /// Send the three task lists: the member joined or its assignment
    /// changed. Kafka sends null lists otherwise.
    pub send_tasks: bool,
    pub endpoint_information_epoch: i32,
    pub partitions_by_user_endpoint: Option<Vec<EndpointToPartitions>>,
}

pub(super) fn build_assignment_resp(
    state: &StreamsGroupState,
    member_id: &str,
    config: &StreamsGroupConfig,
    delta: ResponseDelta,
) -> StreamsGroupHeartbeatResponse {
    let m = state
        .members
        .get(member_id)
        .expect("member exists at build_assignment_resp");
    // Kafka builds the list on every heartbeat, in this order, and sends it
    // also when it is empty: the Streams client keeps the last list it saw
    // when the list is null.
    let status_entry = |status_code: i8, status_detail: String| Status {
        status_code,
        status_detail,
        ..Default::default()
    };
    let mut status = Vec::new();
    if state.topology.is_some() && m.topology_epoch < state.topology_epoch {
        status.push(status_entry(
            topo_status::STALE_TOPOLOGY,
            format!(
                "The member's topology epoch {} is behind the group's topology epoch {}.",
                m.topology_epoch, state.topology_epoch
            ),
        ));
    }
    if let Some((code, detail)) = &state.status {
        status.push(status_entry(*code, detail.clone()));
    }
    if let Some(requester) = &state.shutdown_request_member_id {
        status.push(status_entry(
            topo_status::SHUTDOWN_APPLICATION,
            format!(
                "Streams group member {requester} encountered a fatal error and requested a \
                 shutdown for the entire application."
            ),
        ));
    }
    StreamsGroupHeartbeatResponse {
        member_id: member_id.to_string(),
        status: Some(status),
        active_tasks: delta.send_tasks.then(|| map_to_task_ids(&m.active)),
        standby_tasks: delta.send_tasks.then(|| map_to_task_ids(&m.standby)),
        warmup_tasks: delta.send_tasks.then(|| map_to_task_ids(&m.warmup)),
        endpoint_information_epoch: delta.endpoint_information_epoch,
        partitions_by_user_endpoint: delta.partitions_by_user_endpoint,
        ..base_resp(codes::NONE, m.member_epoch, config)
    }
}

/// Kafka's `StreamsGroup.buildEndpointToPartitions`: one entry for each member
/// with a user endpoint, the other members in id order and `member_id` last.
///
/// An entry lists the source and repartition source topic partitions of the
/// member's active tasks, and of its standby and warmup tasks together, as
/// `EndpointToPartitionsManager` does. A task partition that a topic does not
/// have is left out. Kafka throws when the topology is not configured; this
/// builder lists no partitions instead, because a member of such a group has
/// no tasks.
pub(super) fn endpoint_to_partitions(
    state: &StreamsGroupState,
    member_id: &str,
    subtopologies: Option<&BTreeMap<String, ConfiguredSubtopology>>,
    image: Option<&MetadataImage>,
) -> Vec<EndpointToPartitions> {
    let mut members: Vec<&StreamsMemberState> = state
        .members
        .values()
        .filter(|member| member.member_id != member_id)
        .collect();
    members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    members.extend(state.members.get(member_id));
    members
        .into_iter()
        .filter_map(|member| {
            let (host, port) = member.user_endpoint.as_ref()?;
            let mut standby_and_warmup = member.standby.clone();
            for (subtopology, partitions) in &member.warmup {
                standby_and_warmup
                    .entry(subtopology.clone())
                    .or_default()
                    .extend(partitions.iter().copied());
            }
            Some(EndpointToPartitions {
                user_endpoint: Endpoint {
                    host: host.clone(),
                    port: *port,
                    ..Default::default()
                },
                active_partitions: topic_partitions(&member.active, subtopologies, image),
                standby_partitions: topic_partitions(&standby_and_warmup, subtopologies, image),
                ..Default::default()
            })
        })
        .collect()
}

/// Kafka's `EndpointToPartitionsManager.topicPartitions`.
fn topic_partitions(
    tasks: &BTreeMap<String, Vec<i32>>,
    subtopologies: Option<&BTreeMap<String, ConfiguredSubtopology>>,
    image: Option<&MetadataImage>,
) -> Vec<TopicPartition> {
    let (Some(subtopologies), Some(image)) = (subtopologies, image) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (subtopology_id, task_partitions) in tasks {
        let Some(subtopology) = subtopologies.get(subtopology_id) else {
            continue;
        };
        let topics: BTreeSet<&str> = subtopology
            .source_topics
            .iter()
            .chain(subtopology.repartition_source_topics.keys())
            .map(String::as_str)
            .collect();
        for topic in topics {
            if image.topic(topic).is_none() {
                continue;
            }
            let partition_count = image.topic_partition_count(topic);
            let mut partitions: Vec<i32> = task_partitions
                .iter()
                .copied()
                .filter(|partition| *partition < partition_count)
                .collect();
            partitions.sort_unstable();
            partitions.dedup();
            if !partitions.is_empty() {
                out.push(TopicPartition {
                    topic: topic.to_string(),
                    partitions,
                    ..Default::default()
                });
            }
        }
    }
    out
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
