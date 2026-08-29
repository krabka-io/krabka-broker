//! Fixtures shared by the `StreamsGroupDescribe` test modules.
//!
//! The describe-view inputs and the fully-pinned wire values they are expected
//! to render into are used both by the `render` unit tests and by the
//! live-broker handler tests, so they are built here instead of in either
//! module. The broker-side helpers -- starting a broker with streams enabled,
//! finalizing `streams.version`, and driving one describe round trip over the
//! wire encoding -- live here for the same reason.

use std::{collections::BTreeMap, time::Duration};

use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
use krabka_protocol::{
    UnknownTaggedFields,
    owned::{
        common::streams_group_describe_response::{
            key_value::KeyValue, task_ids::TaskIds, topic_info::TopicInfo,
        },
        streams_group_describe_request::StreamsGroupDescribeRequest,
        streams_group_describe_response::{
            self as response_mod, DescribedGroup, StreamsGroupDescribeResponse, Subtopology,
            Topology,
        },
    },
};

use super::handle;
use crate::{
    broker::Broker,
    coordinator::unified::streams::{
        actor::StreamsDescribeMember,
        persistence::{StoredSubtopology, StoredTopicInfo, StreamsGroupTopologyValue},
    },
};

fn request(group_ids: &[&str]) -> StreamsGroupDescribeRequest {
    StreamsGroupDescribeRequest {
        group_ids: group_ids.iter().map(|gid| (*gid).into()).collect(),
        ..Default::default()
    }
}

crate::test_support::codec_helpers!(
    StreamsGroupDescribeRequest,
    StreamsGroupDescribeResponse,
    version = response_mod::MAX_VERSION
);

pub(super) async fn start_broker(
    streams_enabled: bool,
) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
    crate::test_support::start_broker_with(|cfg| {
        cfg.streams_group.enable = streams_enabled;
    })
    .await
}

pub(super) async fn finalize_streams_version(broker: &Broker) {
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: crate::features::STREAMS_VERSION.into(),
            level: 1,
        })])
        .await
        .expect("submit streams.version");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if broker
                .controller
                .current_image()
                .finalized_feature(crate::features::STREAMS_VERSION)
                == Some(1)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("streams.version visible");
}

pub(super) async fn describe(broker: &Broker, group_ids: &[&str]) -> StreamsGroupDescribeResponse {
    let version = response_mod::MAX_VERSION;
    let req_bytes = encode_request(&request(group_ids));
    let principal = crate::test_support::principal("admin");
    let peer = crate::test_support::peer();
    let ctx = crate::test_support::request_context(&principal, &peer, "admin-client");
    let resp = handle(broker, version, 1, &req_bytes, &ctx)
        .await
        .expect("handle describe");
    decode_response(&resp)
}

pub(super) fn task_map(entries: &[(&str, Vec<i32>)]) -> BTreeMap<String, Vec<i32>> {
    entries
        .iter()
        .map(|(subtopology_id, partitions)| ((*subtopology_id).into(), partitions.clone()))
        .collect()
}

pub(super) fn topology_value() -> StreamsGroupTopologyValue {
    StreamsGroupTopologyValue {
        epoch: 9,
        subtopologies: vec![StoredSubtopology {
            subtopology_id: "sub-a".into(),
            source_topics: vec!["input-a".into(), "input-b".into()],
            source_topic_regex: vec!["ignored-.*".into()],
            repartition_sink_topics: vec!["sink-a".into()],
            state_changelog_topics: vec![StoredTopicInfo {
                name: "store-a-changelog".into(),
                partitions: 3,
                replication_factor: 2,
                topic_configs: vec![("cleanup.policy".into(), "compact".into())],
            }],
            repartition_source_topics: vec![StoredTopicInfo {
                name: "source-repartition".into(),
                partitions: 4,
                replication_factor: 1,
                topic_configs: vec![("retention.ms".into(), "1000".into())],
            }],
            copartition_groups: Vec::new(),
        }],
    }
}

pub(super) fn describe_member() -> StreamsDescribeMember {
    StreamsDescribeMember {
        member_id: "member-1".into(),
        member_epoch: 7,
        instance_id: Some("instance-a".into()),
        rack_id: Some("rack-a".into()),
        client_id: "client-a".into(),
        client_host: "/127.0.0.1".into(),
        process_id: "process-a".into(),
        active: task_map(&[("sub-a", vec![0, 2])]),
        standby: task_map(&[("sub-a", vec![1])]),
        warmup: task_map(&[("sub-b", vec![3, 4])]),
    }
}

pub(super) fn expected_task_ids(subtopology_id: &str, partitions: Vec<i32>) -> TaskIds {
    TaskIds {
        subtopology_id: subtopology_id.into(),
        partitions,
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    }
}

fn expected_key_value(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: value.into(),
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    }
}

/// A fully-pinned error row as the handler renders it. Only `group_id` and
/// `error_code` are set, and every other field holds its wire default.
pub(super) fn error_group(group_id: &str, error_code: i16) -> DescribedGroup {
    DescribedGroup {
        error_code,
        error_message: None,
        group_id: group_id.into(),
        group_state: String::new(),
        group_epoch: 0,
        assignment_epoch: 0,
        topology: None,
        members: Vec::new(),
        // Wire default (INT32_MIN sentinel = "not set").
        authorized_operations: i32::MIN,
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    }
}

/// The wire `Topology` that [`render_topology`] must produce from
/// [`topology_value`], with every field pinned.
///
/// [`render_topology`]: super::render::render_topology
pub(super) fn expected_rendered_topology() -> Topology {
    Topology {
        epoch: 9,
        subtopologies: Some(vec![Subtopology {
            subtopology_id: "sub-a".into(),
            source_topics: vec!["input-a".into(), "input-b".into()],
            repartition_sink_topics: vec!["sink-a".into()],
            state_changelog_topics: vec![TopicInfo {
                name: "store-a-changelog".into(),
                partitions: 3,
                replication_factor: 2,
                topic_configs: vec![expected_key_value("cleanup.policy", "compact")],
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }],
            repartition_source_topics: vec![TopicInfo {
                name: "source-repartition".into(),
                partitions: 4,
                replication_factor: 1,
                topic_configs: vec![expected_key_value("retention.ms", "1000")],
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        }]),
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    }
}
