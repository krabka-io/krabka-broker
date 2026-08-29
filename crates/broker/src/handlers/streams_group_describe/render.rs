//! Projecting the streams coordinator's describe view onto the
//! `StreamsGroupDescribe` response types.
//!
//! The streams actor answers a `Describe` message with a `StreamsDescribeView`,
//! which is the coordinator's own shape rather than the wire's. This module is
//! the only place that turns that view into a `DescribedGroup`, its members,
//! and the resolved topology, so the field-for-field correspondence the JVM
//! `DescribeStreamsGroupsHandler` expects is decided in one file.

use std::collections::BTreeMap;

use krabka_protocol::owned::{
    common::streams_group_describe_response::{
        assignment::Assignment, key_value::KeyValue, task_ids::TaskIds, topic_info::TopicInfo,
    },
    streams_group_describe_response::{DescribedGroup, Member, Subtopology, Topology},
};

use crate::coordinator::unified::streams::{
    actor::{StreamsDescribeMember, StreamsDescribeView},
    persistence::{StoredSubtopology, StoredTopicInfo, StreamsGroupTopologyValue},
};

/// Map a [`StreamsDescribeView`] into a wire `DescribedGroup`.
///
/// [`StreamsDescribeView`]: crate::coordinator::unified::streams::actor::StreamsDescribeView
pub(super) fn render_group(view: StreamsDescribeView) -> DescribedGroup {
    DescribedGroup {
        group_id: view.group_id,
        group_state: view.group_state,
        group_epoch: view.group_epoch,
        assignment_epoch: view.assignment_epoch,
        // The resolved topology (subtopologies + their topics). The real JVM
        // `DescribeStreamsGroupsHandler` errors on a response with no topology,
        // so render it whenever the group has one.
        topology: view.topology.map(render_topology),
        members: view.members.into_iter().map(render_member).collect(),
        // Per-group authorized-operations bitfield is not computed here, so
        // leave the wire default (INT32_MIN sentinel = "not set").
        ..Default::default()
    }
}

/// Map a describe-view member into a wire `Member`. The view carries the
/// current in-flight active, standby, and warmup task ownership. The view does
/// not project `target_assignment`, so that field renders empty.
fn render_member(m: StreamsDescribeMember) -> Member {
    Member {
        member_id: m.member_id,
        member_epoch: m.member_epoch,
        instance_id: m.instance_id,
        rack_id: m.rack_id,
        client_id: m.client_id,
        client_host: m.client_host,
        process_id: m.process_id,
        assignment: Assignment {
            active_tasks: task_map_to_ids(&m.active),
            standby_tasks: task_map_to_ids(&m.standby),
            warmup_tasks: task_map_to_ids(&m.warmup),
            ..Default::default()
        },
        // The view does not project the target (next) assignment, so render empty.
        ..Default::default()
    }
}

/// Map the stored `StreamsGroupTopologyValue` into the wire describe `Topology`.
/// The describe `Subtopology` omits the request-only `source_topic_regex` and
/// `copartition_groups`. Everything else maps across field-for-field.
fn render_topology(t: StreamsGroupTopologyValue) -> Topology {
    fn topic_info(ti: StoredTopicInfo) -> TopicInfo {
        TopicInfo {
            name: ti.name,
            partitions: ti.partitions,
            replication_factor: ti.replication_factor,
            topic_configs: ti
                .topic_configs
                .into_iter()
                .map(|(key, value)| KeyValue {
                    key,
                    value,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
    fn subtopology(s: StoredSubtopology) -> Subtopology {
        Subtopology {
            subtopology_id: s.subtopology_id,
            source_topics: s.source_topics,
            repartition_sink_topics: s.repartition_sink_topics,
            state_changelog_topics: s
                .state_changelog_topics
                .into_iter()
                .map(topic_info)
                .collect(),
            repartition_source_topics: s
                .repartition_source_topics
                .into_iter()
                .map(topic_info)
                .collect(),
            ..Default::default()
        }
    }
    Topology {
        epoch: t.epoch,
        subtopologies: Some(t.subtopologies.into_iter().map(subtopology).collect()),
        ..Default::default()
    }
}

/// Render a `subtopology -> partitions` task map as the response `Vec<TaskIds>`.
fn task_map_to_ids(map: &BTreeMap<String, Vec<i32>>) -> Vec<TaskIds> {
    map.iter()
        .map(|(sub, parts)| TaskIds {
            subtopology_id: sub.clone(),
            partitions: parts.clone(),
            ..Default::default()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::UnknownTaggedFields;

    use super::*;
    use crate::{
        codes,
        handlers::streams_group_describe::test_support::{
            describe_member, expected_rendered_topology, expected_task_ids, task_map,
            topology_value,
        },
    };

    #[test]
    fn render_group_preserves_group_member_and_topology_fields() {
        let rendered = render_group(StreamsDescribeView {
            group_id: "streams-app".into(),
            group_epoch: 11,
            assignment_epoch: 10,
            topology_epoch: 9,
            group_state: "Stable".into(),
            topology: Some(topology_value()),
            members: vec![describe_member()],
        });

        let empty_assignment = Assignment {
            active_tasks: Vec::new(),
            standby_tasks: Vec::new(),
            warmup_tasks: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        let expected = DescribedGroup {
            error_code: codes::NONE,
            error_message: None,
            group_id: "streams-app".into(),
            group_state: "Stable".into(),
            group_epoch: 11,
            assignment_epoch: 10,
            topology: Some(expected_rendered_topology()),
            members: vec![Member {
                member_id: "member-1".into(),
                member_epoch: 7,
                instance_id: Some("instance-a".into()),
                rack_id: Some("rack-a".into()),
                client_id: "client-a".into(),
                client_host: "/127.0.0.1".into(),
                // Not projected by the describe view — wire default.
                topology_epoch: 0,
                process_id: "process-a".into(),
                user_endpoint: None,
                client_tags: Vec::new(),
                task_offsets: Vec::new(),
                task_end_offsets: Vec::new(),
                assignment: Assignment {
                    active_tasks: vec![expected_task_ids("sub-a", vec![0, 2])],
                    standby_tasks: vec![expected_task_ids("sub-a", vec![1])],
                    warmup_tasks: vec![expected_task_ids("sub-b", vec![3, 4])],
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
                // The view does not project the target (next) assignment.
                target_assignment: empty_assignment,
                is_classic: false,
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }],
            // Not computed here — wire default (INT32_MIN sentinel = "not set").
            authorized_operations: i32::MIN,
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(rendered == expected);
    }

    #[test]
    fn render_topology_preserves_subtopology_and_topic_info_fields() {
        let topology = render_topology(topology_value());

        assert!(topology == expected_rendered_topology());
    }

    #[test]
    fn task_map_to_ids_preserves_sorted_task_maps() {
        let tasks = task_map_to_ids(&task_map(&[("z", vec![9]), ("a", vec![1, 2])]));

        let expected = vec![
            expected_task_ids("a", vec![1, 2]),
            expected_task_ids("z", vec![9]),
        ];
        assert!(tasks == expected);
    }
}
