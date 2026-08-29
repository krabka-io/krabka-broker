//! Typed request helpers for the unit tests: each one sends a single
//! [`GroupActorMessage`] to a live actor and awaits the reply, so a test reads
//! as the RPC it exercises rather than as channel plumbing.

use std::collections::HashMap;

use krabka_protocol::owned::{
    consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
    consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
    heartbeat_request::HeartbeatRequest, join_group_request::JoinGroupRequest,
    leave_group_request::LeaveGroupRequest, leave_group_response::MemberResponse,
    sync_group_request::SyncGroupRequest,
};

use super::subscription_blob;
use crate::coordinator::unified::{
    actor::{ClassicView, GroupActorHandle, GroupActorMessage, JoinResult, SyncResult},
    classic_state::OffsetEntry,
};

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
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: member_id.into(),
                member_epoch,
                subscribed_topic_names: topic.map(|t| vec![t.into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            client_id: "client-a".into(),
            client_host: String::new(),
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap()
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
    use krabka_protocol::owned::leave_group_request::MemberIdentity;
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::ClassicLeave {
            req: LeaveGroupRequest {
                group_id: "g".into(),
                members: vec![MemberIdentity {
                    member_id: member_id.into(),
                    group_instance_id: None,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version: 3,
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap().members
}

/// Validates an offset commit against the group's LIVE kind, through the
/// single `ValidateCommit` message.
pub async fn validate_commit(
    handle: &GroupActorHandle,
    member_id: &str,
    generation_or_epoch: i32,
) -> Result<(), i16> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::ValidateCommit {
            member_id: member_id.into(),
            group_instance_id: None,
            generation_or_epoch,
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
        .send(GroupActorMessage::FetchCommitted { reply: tx })
        .await
        .unwrap();
    rx.await.unwrap()
}
