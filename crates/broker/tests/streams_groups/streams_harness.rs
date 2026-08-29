//! Broker boot, feature-finalization and heartbeat helpers shared by the
//! KIP-1071 streams-group scenarios in this suite.
//!
//! Every scenario boots a single-node broker, finalizes `streams.version` to
//! level 1 so the streams handlers stop returning `UNSUPPORTED_VERSION`, and
//! then drives `StreamsGroupHeartbeat` until the coordinator hands out active
//! tasks. Those steps, and the small accessors that read an assignment out of a
//! heartbeat response, live here so each scenario module holds only its own
//! assertions.

use std::{sync::Arc, time::Duration};

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig};
use krabka_client_core::Client;
use krabka_protocol::owned::{
    common::streams_group_heartbeat_request::{
        task_ids::TaskIds as ReqTaskIds, topic_info::TopicInfo,
    },
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    streams_group_describe_request::StreamsGroupDescribeRequest,
    streams_group_heartbeat_request::{StreamsGroupHeartbeatRequest, Subtopology, Topology},
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
};

pub async fn boot() -> (krabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

pub async fn connect(bootstrap: &str) -> Arc<Client> {
    Arc::new(
        Client::builder()
            .bootstrap(bootstrap)
            .client_id("c1")
            .build()
            .await
            .unwrap(),
    )
}

pub async fn create_topic(client: &Client, topic: &str, partitions: i32) {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "topic create failed: {resp:?}"
    );
}

/// Finalize `streams.version` to level 1 so the heartbeat and describe handlers
/// stop returning `UNSUPPORTED_VERSION`. `upgrade_type: 1` is UPGRADE.
pub async fn finalize_streams_version(client: &Client) {
    let resp = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: "streams.version".into(),
                max_version_level: 1,
                upgrade_type: 1,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures");
    assert!(
        resp.error_code == 0,
        "streams.version finalize failed: {resp:?}"
    );
}

/// A single-subtopology topology that subscribes to one source topic, with the
/// supplied changelog topics. An empty list means stateless.
pub fn topology(source_topic: &str, changelogs: Vec<TopicInfo>) -> Topology {
    Topology {
        epoch: 0,
        subtopologies: vec![Subtopology {
            subtopology_id: "0".into(),
            source_topics: vec![source_topic.into()],
            state_changelog_topics: changelogs,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// First-join heartbeat. It sends an empty member id, so the server mints one,
/// epoch 0, a process id, a rebalance timeout, and the supplied topology.
pub fn first_join(group: &str, topo: Topology) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: group.into(),
        member_id: String::new(),
        member_epoch: 0,
        process_id: Some("p1".into()),
        rebalance_timeout_ms: 30_000,
        topology: Some(topo),
        ..Default::default()
    }
}

/// Follow-up heartbeat. It sends a known member id and its current epoch, and
/// it echoes back the owned active tasks, as a steady-state member does.
pub fn follow_up(
    group: &str,
    member_id: &str,
    epoch: i32,
    active: Option<Vec<ReqTaskIds>>,
) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: group.into(),
        member_id: member_id.into(),
        member_epoch: epoch,
        active_tasks: active,
        ..Default::default()
    }
}

/// Sum of all active-task partitions in a heartbeat response.
pub fn active_partition_count(resp: &StreamsGroupHeartbeatResponse) -> usize {
    resp.active_tasks
        .as_ref()
        .map_or(0, |v| v.iter().map(|t| t.partitions.len()).sum())
}

/// Active-task partitions for a given subtopology id, sorted.
pub fn active_partitions_for(resp: &StreamsGroupHeartbeatResponse, sub: &str) -> Vec<i32> {
    let mut parts: Vec<i32> = resp
        .active_tasks
        .as_ref()
        .map(|v| {
            v.iter()
                .filter(|t| t.subtopology_id == sub)
                .flat_map(|t| t.partitions.clone())
                .collect()
        })
        .unwrap_or_default();
    parts.sort_unstable();
    parts
}

/// The response status codes. In the KIP-1071 status enum, 3 is
/// `MISSING_INTERNAL_TOPICS`.
pub fn status_codes(resp: &StreamsGroupHeartbeatResponse) -> Vec<i8> {
    resp.status
        .as_ref()
        .map(|v| v.iter().map(|s| s.status_code).collect())
        .unwrap_or_default()
}

pub async fn describe(
    client: &Client,
    group: &str,
) -> krabka_protocol::owned::streams_group_describe_response::StreamsGroupDescribeResponse {
    client
        .send(StreamsGroupDescribeRequest {
            group_ids: vec![group.into()],
            include_authorized_operations: false,
            ..Default::default()
        })
        .await
        .expect("StreamsGroupDescribe")
}

/// Drive a single member to its first join, then re-heartbeat until convergence
/// returns. The returned tuple is `(member_id, last_response)`.
pub async fn join_and_converge(
    client: &Client,
    group: &str,
    topo: Topology,
    want_active: usize,
    tries: usize,
) -> (String, StreamsGroupHeartbeatResponse) {
    // First join. Tolerate a transient coordinator-load on the very first call.
    let mut resp = client
        .send(first_join(group, topo))
        .await
        .expect("first heartbeat");
    let mut member_id = resp.member_id.clone();

    for _ in 0..tries {
        // COORDINATOR_LOAD_IN_PROGRESS (14): retry the first join.
        if resp.error_code == 14 {
            resp = client
                .send(first_join(group, topology("", vec![])))
                .await
                .expect("retry first heartbeat");
            member_id = resp.member_id.clone();
            continue;
        }
        assert!(resp.error_code == 0, "heartbeat error: {resp:?}");
        if active_partition_count(&resp) >= want_active {
            break;
        }
        // intentional: backoff between heartbeats while polling the RPC response
        // for active-task-assignment convergence. The assignment is coordinator-
        // local state that is not reflected in the metadata image and exposes no
        // metric/awaiter, so a bounded re-heartbeat loop is the only observer.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let active = resp.active_tasks.clone().map(|v| {
            v.into_iter()
                .map(|t| ReqTaskIds {
                    subtopology_id: t.subtopology_id,
                    partitions: t.partitions,
                    ..Default::default()
                })
                .collect()
        });
        resp = client
            .send(follow_up(group, &member_id, resp.member_epoch, active))
            .await
            .expect("follow-up heartbeat");
        member_id = resp.member_id.clone();
    }
    (member_id, resp)
}
