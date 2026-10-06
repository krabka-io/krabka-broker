//! `Heartbeat` (`api_key=12`).
//!
//! This handler validates `(generation, member)`. It then refreshes the
//! member's `last_heartbeat` clock inside the group's actor.

use krabka_protocol::owned::{
    heartbeat_request::HeartbeatRequest, heartbeat_response::HeartbeatResponse,
};
use tokio::sync::oneshot;

use crate::{
    broker::Broker, codes, coordinator::unified::actor::GroupActorMessage, error::BrokerError,
    handlers::group_read_denied,
};

// cargo-mutants: the generated protocol default for throttle_time_ms is zero.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn handle(
    broker: &Broker,
    req: HeartbeatRequest,
    _version: i16,
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<HeartbeatResponse, BrokerError> {
    let coordinator = broker.group_coordinator.clone();

    // ── ACL preamble ────────────────────────────────────────────
    // `Read` on `Group(group_id)`. On Deny → whole-response
    // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
    {
        let image = broker.controller.current_image();
        if group_read_denied(
            broker.config.authorizer.as_ref(),
            &image,
            ctx,
            &req.group_id,
        ) {
            return Ok(denied());
        }
    }

    // Kafka's `GroupCoordinatorService.heartbeat` answers an empty group
    // id before any group lookup.
    // `GroupCoordinatorService.heartbeat` answers `NONE` in place of
    // `COORDINATOR_LOAD_IN_PROGRESS`, so a member keeps its session while
    // the new coordinator loads.
    let invalid_group = req.group_id.is_empty().then_some(codes::INVALID_GROUP_ID);
    if let Some(error_code) = invalid_group
        .or_else(|| crate::handlers::group_coordinator_error(broker, &req.group_id))
        .map(|code| {
            if code == codes::COORDINATOR_LOAD_IN_PROGRESS {
                codes::NONE
            } else {
                code
            }
        })
    {
        return Ok(HeartbeatResponse {
            error_code,
            throttle_time_ms: 0,
            ..Default::default()
        });
    }

    let error_code = match coordinator.find(&req.group_id) {
        None => codes::UNKNOWN_MEMBER_ID,
        Some(handle) => {
            let (tx, rx) = oneshot::channel();
            if handle
                .tx
                .send(GroupActorMessage::ClassicHeartbeat { req, reply: tx })
                .await
                .is_err()
            {
                codes::UNKNOWN_MEMBER_ID
            } else {
                rx.await.unwrap_or(codes::UNKNOWN_MEMBER_ID)
            }
        }
    };

    Ok(HeartbeatResponse {
        error_code,
        throttle_time_ms: 0,
        ..Default::default()
    })
}

/// Whole-response `GROUP_AUTHORIZATION_FAILED (30)` that the handler builds
/// on Deny.
fn denied() -> HeartbeatResponse {
    HeartbeatResponse {
        error_code: codes::GROUP_AUTHORIZATION_FAILED,
        throttle_time_ms: 0,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let principal = crate::test_support::principal("ANONYMOUS");
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = crate::test_support::request_context(&principal, &peer, "heartbeat-client");

        assert!(group_read_denied(&authorizer, &image, &ctx, "g"));

        assert!(
            denied()
                == HeartbeatResponse {
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    throttle_time_ms: 0,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                }
        );
    }
}
