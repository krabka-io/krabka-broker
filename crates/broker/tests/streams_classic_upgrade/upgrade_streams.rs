//! Client-side drivers for the streams (`StreamsGroupHeartbeat`) side of the
//! upgrade scenarios.
//!
//! Both scenarios send a streams heartbeat at the group id a classic group
//! already owns, so the topology builder and the bounded convergence loop are
//! kept here apart from the classic-side `JoinGroup` drivers.

use std::time::Duration;

use krabka_client_core::Client;
use krabka_protocol::owned::{
    common::streams_group_heartbeat_request::task_ids::TaskIds as ReqTaskIds,
    streams_group_heartbeat_request::{StreamsGroupHeartbeatRequest, Subtopology, Topology},
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};

use crate::upgrade_harness::ERR_NONE;

pub fn topology(source_topic: &str) -> Topology {
    Topology {
        epoch: 0,
        subtopologies: vec![Subtopology {
            subtopology_id: "0".into(),
            source_topics: vec![source_topic.into()],
            state_changelog_topics: vec![],
            ..Default::default()
        }],
        ..Default::default()
    }
}

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

/// Drive a single streams member to convergence (at least `want_active`
/// active-task partitions). Returns `(member_id, last_response)`.
pub async fn streams_join_and_converge(
    client: &Client,
    group: &str,
    topo: Topology,
    want_active: usize,
    tries: usize,
) -> (String, StreamsGroupHeartbeatResponse) {
    let mut resp = client
        .send(first_join(group, topo))
        .await
        .expect("first streams heartbeat");
    let mut member_id = resp.member_id.clone();

    for _ in 0..tries {
        if resp.error_code == 14 {
            resp = client
                .send(first_join(group, topology("")))
                .await
                .expect("retry streams heartbeat");
            member_id = resp.member_id.clone();
            continue;
        }
        if resp.error_code != ERR_NONE {
            break;
        }
        let total: usize = resp
            .active_tasks
            .as_ref()
            .map_or(0, |v| v.iter().map(|t| t.partitions.len()).sum());
        if total >= want_active {
            break;
        }
        // intentional: retry/backoff between bounded streams-heartbeat RPC polls;
        // task-assignment convergence is streams-coordinator-local state, not in
        // the metadata image and exposed by no metric — no awaiter can observe it.
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
            .expect("follow-up streams heartbeat");
        member_id = resp.member_id.clone();
    }
    (member_id, resp)
}
