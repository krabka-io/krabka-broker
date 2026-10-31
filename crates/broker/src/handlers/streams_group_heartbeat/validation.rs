//! The request checks that Kafka's `GroupCoordinatorService` runs on a
//! `StreamsGroupHeartbeat` before the group coordinator sees it.
//!
//! A refused request changes no group: Kafka answers it with the error code
//! and message and never schedules the write operation.

use {
    crate::{
        api_catalog::UnstableApiVersions, codes,
        coordinator::unified::replay_policy::LEAVE_GROUP_STATIC_MEMBER_EPOCH,
        handlers::coordinator_routing::java_blank,
    },
    krabka_protocol::owned::streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
};

/// Kafka's `throwIfStreamsGroupHeartbeatRequestIsUsingUnsupportedFeatures`
/// and then `throwIfStreamsGroupHeartbeatRequestIsInvalid`, in Kafka's order.
/// Returns the error code and message of the first check that the request
/// fails.
///
/// Kafka 4.3.1 refuses static membership, task offsets and warmup tasks, and
/// Kafka trunk drops those three checks, so they hold only while
/// `unstable.api.versions.enable` is off. Both refuse source topic regexes.
/// The error code and message that a refused request is answered with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RequestError {
    pub(super) code: i16,
    pub(super) message: String,
}

pub(super) fn request_error(
    req: &StreamsGroupHeartbeatRequest,
    unstable: UnstableApiVersions,
) -> Option<RequestError> {
    let invalid = |message: &str| {
        Some(RequestError {
            code: codes::INVALID_REQUEST,
            message: message.to_owned(),
        })
    };

    if unstable == UnstableApiVersions::Disabled {
        if req.instance_id.is_some() {
            return invalid("Static membership is not yet supported.");
        }
        if req.task_offsets.is_some() {
            return invalid("TaskOffsets are not supported yet.");
        }
        if req.task_end_offsets.is_some() {
            return invalid("TaskEndOffsets are not supported yet.");
        }
        if req
            .warmup_tasks
            .as_ref()
            .is_some_and(|tasks| !tasks.is_empty())
        {
            return invalid("WarmupTasks are not supported yet.");
        }
    }
    if req
        .topology
        .iter()
        .flat_map(|topology| topology.subtopologies.iter())
        .any(|subtopology| !subtopology.source_topic_regex.is_empty())
    {
        return invalid("Regular expressions for source topics are not supported yet.");
    }

    if java_blank(&req.member_id) {
        return invalid("MemberId can't be empty.");
    }
    if java_blank(&req.group_id) {
        return invalid("GroupId can't be empty.");
    }
    if req.instance_id.as_deref().is_some_and(java_blank) {
        return invalid("InstanceId can't be empty.");
    }
    if req.rack_id.as_deref().is_some_and(java_blank) {
        return invalid("RackId can't be empty.");
    }

    if req.member_epoch == 0 {
        if req.rebalance_timeout_ms == -1 {
            return invalid("RebalanceTimeoutMs must be provided in first request.");
        }
        // Kafka's `throwIfNotEmptyCollection` refuses a null list too.
        for (tasks, name) in [
            (req.active_tasks.as_deref(), "ActiveTasks"),
            (req.standby_tasks.as_deref(), "StandbyTasks"),
            (req.warmup_tasks.as_deref(), "WarmupTasks"),
        ] {
            if tasks.is_none_or(|tasks| !tasks.is_empty()) {
                return invalid(&format!("{name} must be empty when (re-)joining."));
            }
        }
        let Some(topology) = &req.topology else {
            return invalid("Topology must be non-null when (re-)joining.");
        };
        // Kafka's `throwIfInvalidTopology`.
        if let Some(topic) = topology
            .subtopologies
            .iter()
            .flat_map(|subtopology| subtopology.state_changelog_topics.iter())
            .find(|topic| topic.partitions != 0)
        {
            return Some(RequestError {
                code: codes::STREAMS_INVALID_TOPOLOGY,
                message: format!(
                    "Changelog topic {} must have an undefined partition count, but it is set to \
                     {}.",
                    topic.name, topic.partitions
                ),
            });
        }
    } else if req.member_epoch == LEAVE_GROUP_STATIC_MEMBER_EPOCH {
        if req.instance_id.is_none() {
            return invalid("InstanceId can't be null.");
        }
    } else if req.member_epoch < LEAVE_GROUP_STATIC_MEMBER_EPOCH {
        return invalid(&format!(
            "MemberEpoch is {}, but must be greater than or equal to -2.",
            req.member_epoch
        ));
    }

    let lists = [
        req.active_tasks.is_some(),
        req.standby_tasks.is_some(),
        req.warmup_tasks.is_some(),
    ];
    if lists.contains(&true) && lists.contains(&false) {
        return invalid("If one task-type is non-null, all must be non-null.");
    }

    if req.member_epoch != 0 && req.topology.is_some() {
        return invalid("Topology can only be provided when (re-)joining.");
    }
    None
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::handlers::group_heartbeat_test_support::streams_request,
        assert2::check,
        krabka_protocol::owned::{
            common::streams_group_heartbeat_request::{task_ids::TaskIds, topic_info::TopicInfo},
            streams_group_heartbeat_request::{Subtopology, Topology},
        },
    };

    fn topology() -> Topology {
        Topology {
            epoch: 1,
            subtopologies: vec![Subtopology {
                subtopology_id: "0".into(),
                source_topics: vec!["in".into()],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// A valid join: a member id, a rebalance timeout, three empty task lists
    /// and a topology.
    fn join() -> StreamsGroupHeartbeatRequest { streams_request("g", "m1") }

    fn heartbeat(member_epoch: i32) -> StreamsGroupHeartbeatRequest {
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch,
            ..Default::default()
        }
    }

    fn task() -> Vec<TaskIds> {
        vec![TaskIds {
            subtopology_id: "0".into(),
            partitions: vec![0],
            ..Default::default()
        }]
    }

    fn invalid(message: &str) -> RequestError {
        RequestError {
            code: codes::INVALID_REQUEST,
            message: message.to_owned(),
        }
    }

    #[derive(Default)]
    struct StreamsRequestValidation {
        request: Option<StreamsGroupHeartbeatRequest>,
        observations: Vec<Option<RequestError>>,
    }
    #[rstest::fixture]
    fn streams_validation() -> StreamsRequestValidation {
        let request = None;
        StreamsRequestValidation {
            request,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given("a streams heartbeat at member epoch {epoch}")]
    fn streams_member_epoch(streams_validation: &mut StreamsRequestValidation, epoch: i32) {
        streams_validation.request = Some(if epoch == 0 { join() } else { heartbeat(epoch) });
    }
    #[rstest_bdd_macros::given("the streams request contains these fields")]
    fn streams_request_fields(
        streams_validation: &mut StreamsRequestValidation,
        datatable: Vec<Vec<String>>,
    ) {
        let request = streams_validation.request.as_mut().expect("request epoch");
        for cells in datatable.into_iter().skip(1) {
            let value = cells[1].as_str();
            match cells[0].as_str() {
                "member_id" => request.member_id = value.to_owned(),
                "group_id" => {
                    request.group_id = if value == "space-tab" {
                        " \t".to_owned()
                    } else {
                        value.to_owned()
                    }
                }
                "instance_id" => request.instance_id = Some(value.to_owned()),
                "rack_id" => request.rack_id = Some(value.to_owned()),
                "rebalance_timeout_ms" => {
                    request.rebalance_timeout_ms = value.parse().expect("rebalance timeout");
                }
                "active_tasks" | "standby_tasks" | "warmup_tasks" => {
                    let tasks = match value {
                        "null" => None,
                        "empty" => Some(Vec::new()),
                        "subtopology 0 partition 0" => Some(task()),
                        other => panic!("unknown tasks {other}"),
                    };
                    match cells[0].as_str() {
                        "active_tasks" => request.active_tasks = tasks,
                        "standby_tasks" => request.standby_tasks = tasks,
                        _ => request.warmup_tasks = tasks,
                    }
                }
                "topology" => {
                    request.topology = match value {
                        "null" => None,
                        "epoch 1 source in" => Some(topology()),
                        other => panic!("unknown topology {other}"),
                    }
                }
                "changelog_partitions" => {
                    request
                        .topology
                        .as_mut()
                        .expect("join topology")
                        .subtopologies[0]
                        .state_changelog_topics = vec![TopicInfo {
                        name: "store-changelog".into(),
                        partitions: value.parse().expect("partition count"),
                        ..Default::default()
                    }];
                }
                "source_topic_regex" => {
                    request
                        .topology
                        .as_mut()
                        .expect("join topology")
                        .subtopologies[0]
                        .source_topic_regex = vec![value.to_owned()];
                }
                other => panic!("unknown request field {other}"),
            }
        }
    }
    #[rstest_bdd_macros::when("Kafka trunk streams heartbeat validation runs")]
    fn validate_streams_request(streams_validation: &mut StreamsRequestValidation) {
        streams_validation.observations.push(request_error(
            streams_validation.request.as_ref().expect("request"),
            UnstableApiVersions::Enabled,
        ));
    }
    #[rstest_bdd_macros::then("the streams request is accepted")]
    fn accepted_streams_request(streams_validation: &StreamsRequestValidation) {
        check!(streams_validation.observations.as_slice() == [None]);
    }
    #[rstest_bdd_macros::then(
        "the streams request is refused with code {code} and message \"{message}\""
    )]
    fn refused_streams_request(
        streams_validation: &StreamsRequestValidation,
        code: i16,
        message: String,
    ) {
        check!(
            streams_validation.observations.as_slice() == [Some(RequestError { code, message })]
        );
    }
    rstest_bdd_macros::scenarios!("tests/features/handler_streams_request_validation.feature", fixtures = [streams_validation: StreamsRequestValidation]);

    /// Kafka 4.3.1's `throwIfStreamsGroupHeartbeatRequestIsUsingUnsupportedFeatures`
    /// refuses, in this order, an instance id, task offsets, task end offsets,
    /// non-empty warmup tasks and a source topic regex, before it checks
    /// anything else. Kafka trunk keeps only the regex check.
    #[test]
    fn version_4_3_1_refuses_static_membership_offsets_and_warmup_tasks() {
        use krabka_protocol::owned::common::streams_group_heartbeat_request::task_offset::TaskOffset;

        let regex_topology = Topology {
            subtopologies: vec![Subtopology {
                source_topic_regex: vec!["in-.*".into()],
                ..topology().subtopologies[0].clone()
            }],
            ..topology()
        };
        let with_regex = |request: StreamsGroupHeartbeatRequest| StreamsGroupHeartbeatRequest {
            topology: Some(regex_topology.clone()),
            ..request
        };
        let offsets = || {
            Some(vec![TaskOffset {
                subtopology_id: "0".into(),
                partition: 0,
                offset: 5,
                ..Default::default()
            }])
        };
        let instance = Some("i1".to_owned());
        // (name, request, the answer of 4.3.1 and the answer of trunk)
        let rows = [
            (
                "an instance id",
                StreamsGroupHeartbeatRequest {
                    instance_id: instance.clone(),
                    ..join()
                },
                Some(invalid("Static membership is not yet supported.")),
                None,
            ),
            (
                "a static member leaving",
                StreamsGroupHeartbeatRequest {
                    instance_id: instance.clone(),
                    ..heartbeat(-2)
                },
                Some(invalid("Static membership is not yet supported.")),
                None,
            ),
            (
                "a static member leaving for good",
                StreamsGroupHeartbeatRequest {
                    instance_id: instance.clone(),
                    ..heartbeat(-1)
                },
                Some(invalid("Static membership is not yet supported.")),
                None,
            ),
            (
                "an empty instance id",
                StreamsGroupHeartbeatRequest {
                    instance_id: Some(String::new()),
                    ..join()
                },
                Some(invalid("Static membership is not yet supported.")),
                Some(invalid("InstanceId can't be empty.")),
            ),
            (
                "task offsets",
                StreamsGroupHeartbeatRequest {
                    task_offsets: offsets(),
                    ..heartbeat(3)
                },
                Some(invalid("TaskOffsets are not supported yet.")),
                None,
            ),
            (
                "empty task offsets",
                StreamsGroupHeartbeatRequest {
                    task_offsets: Some(vec![]),
                    ..heartbeat(3)
                },
                Some(invalid("TaskOffsets are not supported yet.")),
                None,
            ),
            (
                "task end offsets",
                StreamsGroupHeartbeatRequest {
                    task_end_offsets: offsets(),
                    ..heartbeat(3)
                },
                Some(invalid("TaskEndOffsets are not supported yet.")),
                None,
            ),
            (
                "warmup tasks",
                StreamsGroupHeartbeatRequest {
                    active_tasks: Some(vec![]),
                    standby_tasks: Some(vec![]),
                    warmup_tasks: Some(task()),
                    ..heartbeat(3)
                },
                Some(invalid("WarmupTasks are not supported yet.")),
                None,
            ),
            (
                "an empty warmup task list",
                StreamsGroupHeartbeatRequest {
                    active_tasks: Some(vec![]),
                    standby_tasks: Some(vec![]),
                    warmup_tasks: Some(vec![]),
                    ..heartbeat(3)
                },
                None,
                None,
            ),
            (
                "an instance id before task offsets",
                StreamsGroupHeartbeatRequest {
                    instance_id: instance.clone(),
                    task_offsets: offsets(),
                    task_end_offsets: offsets(),
                    ..heartbeat(3)
                },
                Some(invalid("Static membership is not yet supported.")),
                None,
            ),
            (
                "task offsets before task end offsets",
                StreamsGroupHeartbeatRequest {
                    task_offsets: offsets(),
                    task_end_offsets: offsets(),
                    ..heartbeat(3)
                },
                Some(invalid("TaskOffsets are not supported yet.")),
                None,
            ),
            (
                "an unsupported feature before an invalid request",
                StreamsGroupHeartbeatRequest {
                    instance_id: instance.clone(),
                    member_id: String::new(),
                    ..join()
                },
                Some(invalid("Static membership is not yet supported.")),
                Some(invalid("MemberId can't be empty.")),
            ),
            (
                "an unsupported feature before a source topic regex",
                with_regex(StreamsGroupHeartbeatRequest {
                    instance_id: instance,
                    ..join()
                }),
                Some(invalid("Static membership is not yet supported.")),
                Some(invalid(
                    "Regular expressions for source topics are not supported yet.",
                )),
            ),
            (
                "a source topic regex",
                with_regex(join()),
                Some(invalid(
                    "Regular expressions for source topics are not supported yet.",
                )),
                Some(invalid(
                    "Regular expressions for source topics are not supported yet.",
                )),
            ),
        ];
        for (name, request, released, trunk) in rows {
            check!(
                request_error(&request, UnstableApiVersions::Disabled) == released,
                "{name}: 4.3.1"
            );
            check!(
                request_error(&request, UnstableApiVersions::Enabled) == trunk,
                "{name}: trunk"
            );
        }
    }
}
