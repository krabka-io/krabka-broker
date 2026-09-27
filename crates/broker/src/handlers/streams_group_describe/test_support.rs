//! Fixtures shared by the `StreamsGroupDescribe` test modules.
//!
//! The describe-view inputs and the fully-pinned wire values they are expected
//! to render into are used both by the `render` unit tests and by the
//! live-broker handler tests, so they are built here instead of in either
//! module. The broker-side helpers -- starting a broker with streams enabled,
//! finalizing `streams.version`, and driving one describe round trip over the
//! wire encoding -- live here for the same reason.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

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
use krabka_security::Principal;

use super::handle;
use crate::{
    authorizer::Authorizer,
    broker::Broker,
    coordinator::unified::{
        StreamsGroupSeed,
        streams::{
            actor::{StreamsDescribeMember, StreamsGroupActorMessage},
            persistence::{StoredSubtopology, StoredTopicInfo, StreamsGroupTopologyValue},
        },
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

/// A broker with streams enabled and `authorizer` installed, for the tests
/// that drive a specific ACL gate (group `Describe` or topic `Describe`)
/// rather than allow everything.
pub(super) async fn start_broker_with_authorizer(
    authorizer: Arc<dyn Authorizer>,
) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
    crate::test_support::start_broker_with(|cfg| {
        cfg.streams_group.enable = true;
        cfg.authorizer = authorizer;
    })
    .await
}

/// Seed a live streams-group actor with `topology` and no members, the way
/// the bootstrap replayer hydrates one from `__consumer_offsets` records.
/// This is enough for `StreamsGroupDescribe` to render a topology without
/// driving a real `StreamsGroupHeartbeat` join (which would need real source
/// topics with real partitions).
pub(super) async fn seed_streams_group_topology(
    broker: &Broker,
    group_id: &str,
    topology: StreamsGroupTopologyValue,
) {
    broker.group_coordinator.mark_streams(group_id);
    let handle = broker.group_coordinator.get_or_create_streams(group_id);
    handle
        .tx
        .send(StreamsGroupActorMessage::Seed(StreamsGroupSeed {
            topology: Some(topology),
            ..Default::default()
        }))
        .await
        .expect("seed streams group topology");
}

/// A minimal topology naming exactly the one given source topic, for the
/// topic-Describe-authorization tests.
pub(super) fn topology_with_source_topic(topic: &str) -> StreamsGroupTopologyValue {
    StreamsGroupTopologyValue {
        epoch: 1,
        subtopologies: vec![StoredSubtopology {
            subtopology_id: "0".into(),
            source_topics: vec![topic.into()],
            source_topic_regex: Vec::new(),
            repartition_sink_topics: Vec::new(),
            state_changelog_topics: Vec::new(),
            repartition_source_topics: Vec::new(),
            copartition_groups: Vec::new(),
        }],
    }
}

/// Finalizes `streams.version` 1, the level that turns the streams protocol
/// on.
pub(super) async fn finalize_streams_version(broker: &Broker) {
    set_streams_version(broker, 1).await;
}

/// Removes the finalized `streams.version`, turning the streams protocol off.
///
/// A broker bootstrapped at `4.2-IV1` or later finalizes `streams.version` 1
/// by default, as Kafka's `StreamsVersion.SV_1` does, so a test that needs the
/// protocol off has to take it away.
pub(super) async fn unfinalize_streams_version(broker: &Broker) {
    set_streams_version(broker, 0).await;
}

/// Writes a `streams.version` `FeatureLevelRecord` at `level` and waits until
/// the broker's image shows it. Level 0 removes the feature, as it does in
/// Kafka.
async fn set_streams_version(broker: &Broker, level: i16) {
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: crate::features::STREAMS_VERSION.into(),
            level,
        })])
        .await
        .expect("submit streams.version");

    let want = (level != 0).then_some(level);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if broker
                .controller
                .current_image()
                .finalized_feature(crate::features::STREAMS_VERSION)
                == want
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
    let principal = crate::test_support::principal("admin");
    describe_as(broker, &principal, group_ids, false).await
}

/// Like [`describe`], but with an explicit principal and
/// `include_authorized_operations` flag, for the ACL-gate and KIP-430
/// bitfield tests.
pub(super) async fn describe_as(
    broker: &Broker,
    principal: &Principal,
    group_ids: &[&str],
    include_authorized_operations: bool,
) -> StreamsGroupDescribeResponse {
    let version = response_mod::MAX_VERSION;
    let req = StreamsGroupDescribeRequest {
        include_authorized_operations,
        ..request(group_ids)
    };
    let req_bytes = encode_request(&req);
    let peer = crate::test_support::peer();
    let ctx = crate::test_support::request_context(principal, &peer, "admin-client");
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
        topology_epoch: 9,
        user_endpoint: Some(("host-a".into(), 8080)),
        client_tags: vec![("zone".into(), "z1".into())],
        task_offsets: [(("sub-a".to_string(), 0), 5)].into(),
        task_end_offsets: [(("sub-a".to_string(), 0), 10)].into(),
        active: task_map(&[("sub-a", vec![0, 2])]),
        standby: task_map(&[("sub-a", vec![1])]),
        warmup: task_map(&[("sub-b", vec![3, 4])]),
        target_active: task_map(&[("sub-a", vec![0])]),
        target_standby: task_map(&[("sub-a", vec![1, 2])]),
        target_warmup: task_map(&[]),
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

/// A fully-pinned error row as the handler renders it. Only `group_id`,
/// `error_code` and, for `GROUP_ID_NOT_FOUND`, Kafka's message are set, and
/// every other field holds its wire default.
pub(super) fn error_group(group_id: &str, error_code: i16) -> DescribedGroup {
    DescribedGroup {
        error_code,
        error_message: (error_code == crate::codes::GROUP_ID_NOT_FOUND)
            .then(|| format!("Streams group {group_id} not found.")),
        group_id: group_id.into(),
        group_state: String::new(),
        group_epoch: 0,
        assignment_epoch: 0,
        topology: None,
        members: Vec::new(),
        // Wire default (INT32_MIN sentinel = "not set").
        authorized_operations: i32::MIN,
        topology_description: None,
        topology_description_status: 0,
        assignor_name: None,
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    }
}

/// Creates a topic with `partitions` partitions on the one-broker test
/// cluster.
pub(super) async fn create_topic(broker: &Broker, name: &str, partitions: i32) {
    use krabka_metadata::{LeaderEpoch, PartitionRecord, TopicRecord};

    let node_id = krabka_audit::NodeId(broker.config.node_id.0);
    let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
        name: name.into(),
        topic_id: uuid::Uuid::new_v4(),
        partitions,
        replication_factor: 1,
    })];
    records.extend((0..partitions).map(|partition| {
        MetadataRecord::V1Partition(PartitionRecord {
            topic: name.into(),
            partition,
            leader: node_id,
            replicas: vec![node_id],
            isr: vec![node_id],
            leader_epoch: LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        })
    }));
    broker
        .controller
        .submit_change(records)
        .await
        .expect("create the topic");
}

/// Sends one `StreamsGroupHeartbeat` through its handler and returns the
/// decoded response.
pub(super) async fn heartbeat(
    broker: &Broker,
    req: &krabka_protocol::owned::streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
) -> krabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse {
    use krabka_protocol::owned::streams_group_heartbeat_response::MAX_VERSION;

    let principal = crate::test_support::principal("admin");
    let peer = crate::test_support::peer();
    let ctx = crate::test_support::request_context(&principal, &peer, "streams-client");
    let bytes = crate::handlers::streams_group_heartbeat::handle(
        broker,
        MAX_VERSION,
        1,
        &crate::test_support::encode_request(req, MAX_VERSION),
        &ctx,
    )
    .await
    .expect("handle heartbeat");
    crate::test_support::decode_response(&bytes, MAX_VERSION)
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
