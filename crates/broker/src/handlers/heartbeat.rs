//! `Heartbeat` (`api_key=12`).
//!
//! This handler validates `(generation, member)`. It then refreshes the
//! member's `last_heartbeat` clock inside the group's actor.

use krabka_protocol::owned::{
    heartbeat_request::HeartbeatRequest, heartbeat_response::HeartbeatResponse,
};

use crate::{
    broker::Broker,
    codes,
    coordinator::unified::actor::GroupActorMessage,
    error::BrokerError,
    handlers::{ErrorCodeResponse as _, group_read_denied},
    task_util::ask,
};

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
            return Ok(HeartbeatResponse::error(codes::GROUP_AUTHORIZATION_FAILED));
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
        return Ok(HeartbeatResponse::error(error_code));
    }

    let error_code = match coordinator.find(&req.group_id) {
        None => codes::UNKNOWN_MEMBER_ID,
        // A closed mailbox and a dropped reply both read as a member the
        // group no longer knows.
        Some(handle) => ask(&handle.tx, |reply| GroupActorMessage::ClassicHeartbeat {
            req,
            reply,
        })
        .await
        .unwrap_or(codes::UNKNOWN_MEMBER_ID),
    };

    Ok(HeartbeatResponse::error(error_code))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::test_support::peer;

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let principal = crate::test_support::principal("ANONYMOUS");
        let peer = peer();
        let ctx = crate::test_support::request_context(&principal, &peer, "heartbeat-client");

        assert!(group_read_denied(&authorizer, &image, &ctx, "g"));

        assert!(
            HeartbeatResponse::error(codes::GROUP_AUTHORIZATION_FAILED)
                == HeartbeatResponse {
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    throttle_time_ms: 0,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                }
        );
    }
}
