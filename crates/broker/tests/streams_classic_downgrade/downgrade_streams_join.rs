//! The streams-consumer side of the type flip: the topology and
//! `StreamsGroupHeartbeat` request builders, the loop that drives one member to
//! a converged task assignment, and the leave heartbeat that drains the group
//! before a classic `JoinGroup` may convert it.

use std::time::Duration;

use krabka_client_core::Client;
use krabka_protocol::owned::{
    common::streams_group_heartbeat_request::task_ids::TaskIds as ReqTaskIds,
    streams_group_heartbeat_request::{StreamsGroupHeartbeatRequest, Subtopology, Topology},
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};

use crate::{ERR_COORDINATOR_LOAD_IN_PROGRESS, ERR_NONE};

pub(crate) fn topology(source_topic: &str) -> Topology {
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

fn first_join(group: &str, topo: Topology) -> StreamsGroupHeartbeatRequest {
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

fn follow_up(
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

/// Drives one streams member to convergence, which means at least
/// `want_active` active-task partitions. It returns
/// `(member_id, last_response)`.
pub(crate) async fn streams_join_and_converge(
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
        if resp.error_code == ERR_COORDINATOR_LOAD_IN_PROGRESS {
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
        // intentional: backoff between streams heartbeat rounds while the
        // coordinator computes task assignment. Streams task assignment is not
        // in the metadata image and exposes no awaiter/metric to poll on.
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

/// Sends a streams `LeaveGroup`, with `member_epoch` -1, so that the group
/// drains.
pub(crate) async fn streams_leave(client: &Client, group: &str, member_id: &str) {
    let _ = client
        .send(StreamsGroupHeartbeatRequest {
            group_id: group.into(),
            member_id: member_id.into(),
            member_epoch: -1,
            ..Default::default()
        })
        .await
        .expect("streams leave heartbeat");
}
