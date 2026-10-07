//! Typed request helpers for the unit tests: each one sends a single
//! [`GroupActorMessage`] to a live actor and awaits the reply, so a test reads
//! as the RPC it exercises rather than as channel plumbing.

use std::collections::HashMap;

use krabka_protocol::{
    owned::{
        consumer_group_heartbeat_request::{ConsumerGroupHeartbeatRequest, TopicPartitions},
        consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
        heartbeat_request::HeartbeatRequest,
        join_group_request::JoinGroupRequest,
        leave_group_request::LeaveGroupRequest,
        leave_group_response::MemberResponse,
        sync_group_request::SyncGroupRequest,
    },
    primitives::uuid::Uuid,
};

use super::subscription_blob;
use crate::{
    coordinator::unified::{
        actor::{
            ClassicView, CommitFence, CommitRequest, DescribeView, GroupActorHandle,
            GroupActorMessage, JoinResult, LeaveResult, SyncResult,
        },
        classic_state::OffsetEntry,
    },
    task_util::ask,
};

pub async fn describe(handle: &GroupActorHandle) -> DescribeView {
    ask(&handle.tx, |reply| GroupActorMessage::Describe { reply })
        .await
        .unwrap()
}

pub async fn classic_leave_request(
    handle: &GroupActorHandle,
    req: LeaveGroupRequest,
    version: i16,
) -> LeaveResult {
    ask(&handle.tx, |reply| GroupActorMessage::ClassicLeave {
        req,
        version,
        reply,
    })
    .await
    .unwrap()
}

pub async fn classic_leave_member(handle: &GroupActorHandle, member_id: &str) -> LeaveResult {
    classic_leave_request(
        handle,
        LeaveGroupRequest {
            group_id: "g".into(),
            members: vec![
                krabka_protocol::owned::leave_group_request::MemberIdentity {
                    member_id: member_id.into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
        3,
    )
    .await
}

pub async fn classic_join(handle: &GroupActorHandle, member_id: &str, topic: &str) -> JoinResult {
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::ClassicJoin {
            req: JoinGroupRequest {
                group_id: "g".into(),
                member_id: member_id.into(),
                protocol_type: "consumer".into(),
                protocols: vec![
                    krabka_protocol::owned::join_group_request::JoinGroupRequestProtocol {
                        name: "range".into(),
                        metadata: subscription_blob(&[topic]),
                        ..Default::default()
                    },
                ],
                session_timeout_ms: 30_000,
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            version: 4,
            client_id: "client-a".into(),
            client_host: "127.0.0.1".into(),
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap()
}

pub async fn classic_sync(
    handle: &GroupActorHandle,
    member_id: &str,
    generation: i32,
) -> SyncResult {
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::ClassicSync {
            req: SyncGroupRequest {
                group_id: "g".into(),
                member_id: member_id.into(),
                generation_id: generation,
                ..Default::default()
            },
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap()
}

pub async fn classic_heartbeat(handle: &GroupActorHandle, member_id: &str) -> i16 {
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::ClassicHeartbeat {
            req: HeartbeatRequest {
                group_id: "g".into(),
                member_id: member_id.into(),
                generation_id: 0,
                ..Default::default()
            },
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap()
}

/// Sends a native consumer `Heartbeat` and returns the response. A
/// `member_id` of `""` with epoch 0 is a first-join. A first-join triggers
/// an upgrade when the group is a convertible classic group under a policy
/// that allows the upgrade.
pub async fn consumer_heartbeat(
    handle: &GroupActorHandle,
    member_id: &str,
    member_epoch: i32,
    topic: Option<&str>,
) -> ConsumerGroupHeartbeatResponse {
    consumer_request(
        handle,
        ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: member_id.into(),
            member_epoch,
            subscribed_topic_names: topic.map(|t| vec![t.into()]),
            rebalance_timeout_ms: 60_000,
            ..Default::default()
        },
    )
    .await
}

pub async fn consumer_request(
    handle: &GroupActorHandle,
    request: ConsumerGroupHeartbeatRequest,
) -> ConsumerGroupHeartbeatResponse {
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request,
            client_id: "client-a".into(),
            client_host: String::new(),
            regex_resolver: crate::coordinator::unified::regex_resolver::no_topic_regex_resolver(),
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap()
}

/// A native consumer heartbeat subscribed to `t` that reports `owned` as the
/// partitions the member holds.
pub async fn consumer_heartbeat_owning(
    handle: &GroupActorHandle,
    member_id: &str,
    member_epoch: i32,
    owned: &[(Uuid, &[i32])],
) -> ConsumerGroupHeartbeatResponse {
    consumer_request(
        handle,
        ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: member_id.into(),
            member_epoch,
            subscribed_topic_names: Some(vec!["t".into()]),
            rebalance_timeout_ms: 60_000,
            topic_partitions: Some(
                owned
                    .iter()
                    .map(|(topic_id, partitions)| TopicPartitions {
                        topic_id: *topic_id,
                        partitions: partitions.to_vec(),
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        },
    )
    .await
}

/// Reads the live `ClassicInspect` view. Only a classic-kind group
/// replies.
pub async fn classic_inspect(handle: &GroupActorHandle) -> ClassicView {
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::ClassicInspect { reply: tx })
        .await
        .unwrap();
    rx.await.unwrap()
}

/// A classic member leaves the group (v3 single-member leave list).
pub async fn classic_leave(handle: &GroupActorHandle, member_id: &str) -> Vec<MemberResponse> {
    classic_leave_member(handle, member_id).await.members
}

/// Validates an offset commit of `partitions` against the group's LIVE kind,
/// through the single `ValidateCommit` message.
pub async fn validate_commit(
    handle: &GroupActorHandle,
    member_id: &str,
    generation_or_epoch: i32,
    fence: CommitFence,
    partitions: &[(Uuid, i32)],
) -> Result<(), i16> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::ValidateCommit {
            commit: CommitRequest {
                member_id: member_id.into(),
                group_instance_id: None,
                generation_or_epoch,
                fence,
                partitions: partitions.to_vec(),
            },
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap()
}

/// Round-trips the kind-agnostic committed-offset store.
pub async fn fetch_committed(handle: &GroupActorHandle) -> HashMap<(String, i32), OffsetEntry> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::FetchOffsets { reply: tx })
        .await
        .unwrap();
    rx.await.unwrap().committed
}
