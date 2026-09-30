//! `LeaveGroup` (`api_key=13`). It removes one or more members inside the
//! group's actor. It then opens a rebalance again, if the group is `Stable` or
//! `CompletingRebalance` and members remain.
//!
//! The checks run in Kafka's order: `Read` on the group in
//! `KafkaApis.handleLeaveGroupRequest`, then the empty group id in
//! `GroupCoordinatorService.leaveGroup`, then the coordinator routing. A group
//! the coordinator does not know is Kafka's `UnknownMemberIdException`, which
//! the service answers with one `UNKNOWN_MEMBER_ID` row per requested member
//! and a top-level `NONE`; `LeaveGroupResponse` folds that row into the
//! top-level code at v0-v2.

use bytes::Bytes;
use krabka_protocol::owned::{
    leave_group_request::LeaveGroupRequest,
    leave_group_response::{LeaveGroupResponse, MemberResponse},
};
use tokio::sync::oneshot;

use crate::{
    broker::Broker,
    codes,
    coordinator::unified::actor::{GroupActorMessage, LeaveResult},
    error::BrokerError,
    handlers::group_read_denied,
};

#[cfg(test)]
mod tests;

#[tracing::instrument(
    name = "handle_leave_group",
    level = "info",
    skip_all,
    fields(api = "LeaveGroup", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let coordinator = broker.group_coordinator.clone();
    let mut cur: &[u8] = req_bytes;
    let req: LeaveGroupRequest = crate::handlers::decode_group_request(&mut cur, version)?;

    // ── ACL preamble ────────────────────────────────────────────────
    // `Read` on `Group(group_id)`. On Deny → whole-response
    // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
    let image = broker.controller.current_image();
    if group_read_denied(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        &req.group_id,
    ) {
        return encode_top_level(codes::GROUP_AUTHORIZATION_FAILED, version);
    }

    // `GroupCoordinatorService.leaveGroup`'s `isGroupIdNotEmpty` check runs
    // before the request is routed to a coordinator shard.
    if req.group_id.is_empty() {
        return encode_top_level(codes::INVALID_GROUP_ID, version);
    }

    if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &req.group_id) {
        return encode_top_level(error_code, version);
    }

    let result = match coordinator.find(&req.group_id) {
        None => unknown_group_result(&req, version),
        Some(handle) => {
            let (tx, rx) = oneshot::channel();
            let message = GroupActorMessage::ClassicLeave {
                req: req.clone(),
                version,
                reply: tx,
            };
            if handle.tx.send(message).await.is_err() {
                LeaveResult {
                    error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                    members: Vec::new(),
                }
            } else {
                match rx.await.unwrap_or_default() {
                    // The actor answers a group of a kind the classic leave
                    // cannot reach with `UNKNOWN_MEMBER_ID`, which Kafka
                    // shapes exactly as a missing group.
                    result
                        if result.error_code == codes::UNKNOWN_MEMBER_ID
                            && result.members.is_empty() =>
                    {
                        unknown_group_result(&req, version)
                    }
                    result => result,
                }
            }
        }
    };

    crate::handlers::encode_response(
        &LeaveGroupResponse {
            error_code: result.error_code,
            throttle_time_ms: 0,
            members: result.members,
            ..Default::default()
        },
        version,
    )
}

fn encode_top_level(error_code: i16, version: i16) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(
        &LeaveGroupResponse {
            error_code,
            ..Default::default()
        },
        version,
    )
}

/// Kafka's answer for a group the coordinator does not hold: one
/// `UNKNOWN_MEMBER_ID` row per requested identity under a top-level `NONE`.
/// A v0-v2 request names one member in the top-level `member_id`, and
/// `LeaveGroupResponse` carries that row's code at the top level instead.
fn unknown_group_result(req: &LeaveGroupRequest, version: i16) -> LeaveResult {
    if version < 3 {
        return LeaveResult {
            error_code: codes::UNKNOWN_MEMBER_ID,
            members: Vec::new(),
        };
    }
    LeaveResult {
        error_code: codes::NONE,
        members: req
            .members
            .iter()
            .map(|member| MemberResponse {
                member_id: member.member_id.clone(),
                group_instance_id: member.group_instance_id.clone(),
                error_code: codes::UNKNOWN_MEMBER_ID,
                ..Default::default()
            })
            .collect(),
    }
}
