//! Canonical streams topology and heartbeat drivers for integration suites.
use std::time::Duration;

use assert2::assert;
use krabka_client_core::Client;
use krabka_protocol::owned::{
    common::streams_group_heartbeat_request::{
        task_ids::TaskIds as ReqTaskIds, topic_info::TopicInfo,
    },
    streams_group_heartbeat_request::{StreamsGroupHeartbeatRequest, Subtopology, Topology},
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    update_features_request::UpdateFeaturesRequest,
};

use crate::support::configs::feature_update;
pub async fn finalize_streams_version(client: &Client) {
    let resp = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![feature_update("streams.version", 1, 1)],
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures");
    assert!(
        resp.error_code == 0,
        "streams.version finalize failed: {resp:?}"
    );
}

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

pub fn first_join(group: &str, topo: Topology) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: group.into(),
        member_id: uuid::Uuid::new_v4().to_string(),
        member_epoch: 0,
        process_id: Some("p1".into()),
        rebalance_timeout_ms: 30_000,
        active_tasks: Some(Vec::new()),
        standby_tasks: Some(Vec::new()),
        warmup_tasks: Some(Vec::new()),
        topology: Some(topo),
        ..Default::default()
    }
}

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
        standby_tasks: active.as_ref().map(|_| Vec::new()),
        warmup_tasks: active.as_ref().map(|_| Vec::new()),
        active_tasks: active,
        ..Default::default()
    }
}

pub fn active_partition_count(resp: &StreamsGroupHeartbeatResponse) -> usize {
    resp.active_tasks
        .as_ref()
        .map_or(0, |v| v.iter().map(|t| t.partitions.len()).sum())
}
pub async fn streams_join_and_converge(
    client: &Client,
    group: &str,
    topo: Topology,
    want_active: usize,
    tries: usize,
    require_success: bool,
) -> (String, StreamsGroupHeartbeatResponse) {
    let mut resp = client
        .send(first_join(group, topo.clone()))
        .await
        .expect("first streams heartbeat");
    let mut member_id = resp.member_id.clone();

    for _ in 0..tries {
        if resp.error_code == 14 {
            resp = client
                .send(first_join(group, topo.clone()))
                .await
                .expect("retry streams heartbeat");
            member_id = resp.member_id.clone();
            continue;
        }
        if require_success {
            assert!(resp.error_code == 0, "heartbeat error: {resp:?}");
        } else if resp.error_code != 0 {
            break;
        }
        if active_partition_count(&resp) >= want_active {
            break;
        }
        // intentional: retry/backoff between bounded streams-heartbeat RPC polls;
        // task-assignment convergence is streams-coordinator-local state, not in
        // the metadata image and exposed by no metric — no awaiter can observe it.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let active = request_active_tasks(&resp);
        resp = client
            .send(follow_up(group, &member_id, resp.member_epoch, active))
            .await
            .expect("follow-up streams heartbeat");
        member_id = resp.member_id.clone();
    }
    (member_id, resp)
}
pub async fn boot(
    wait_electable: bool,
) -> (krabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let (dir, broker) = crate::support::standalone_broker().await;
    // A streams or classic group needs `__consumer_offsets`. No broker creates
    // it at startup, so create it as a client's first lookup does.
    broker.wait_until_group_coordinator_ready().await;
    if wait_electable {
        broker.wait_until_broker_electable(broker.node_id()).await;
    }
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

/// Converge a member while preserving the upgrade suites' return-on-error policy.
pub async fn join_until_assigned(
    client: &Client,
    group: &str,
    topology: Topology,
    want_active: usize,
    tries: usize,
) -> (String, StreamsGroupHeartbeatResponse) {
    streams_join_and_converge(client, group, topology, want_active, tries, false).await
}

/// Echo assigned active tasks into a request, preserving absent tasks and row order.
pub fn request_active_tasks(response: &StreamsGroupHeartbeatResponse) -> Option<Vec<ReqTaskIds>> {
    response.active_tasks.clone().map(|tasks| {
        tasks
            .into_iter()
            .map(|task| ReqTaskIds {
                subtopology_id: task.subtopology_id,
                partitions: task.partitions,
                ..Default::default()
            })
            .collect()
    })
}
