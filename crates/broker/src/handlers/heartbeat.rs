//! `Heartbeat` (`api_key=12`).
//!
//! This handler validates `(generation, member)`. It then refreshes the
//! member's `last_heartbeat` clock inside the group's actor.

use {
    crate::{
        broker::Broker,
        codes,
        error::BrokerError,
        handlers::{ApiVersion, ErrorCodeResponse as _, RequestContext, actor_stopped_code},
        task_util::{AskError, ask},
    },
    krabka_protocol::owned::{
        heartbeat_request::HeartbeatRequest, heartbeat_response::HeartbeatResponse,
    },
    std::sync::Arc,
};

pub async fn handle(
    broker: &Broker,
    req: HeartbeatRequest,
    _version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<HeartbeatResponse, BrokerError> {
    use crate::{
        coordinator::unified::actor::GroupActorMessage, handlers::acl_gates::classic_group_refusal,
    };

    let coordinator = Arc::clone(&broker.group_coordinator);

    // Kafka's `GroupCoordinatorService.heartbeat` answers an empty group
    // id before any group lookup.
    // `GroupCoordinatorService.heartbeat` answers `NONE` in place of
    // `COORDINATOR_LOAD_IN_PROGRESS`, so a member keeps its session while
    // the new coordinator loads.
    if let Some(error_code) = classic_group_refusal(broker, ctx, &req.group_id).map(|code| {
        if code == codes::COORDINATOR_LOAD_IN_PROGRESS {
            codes::NONE
        } else {
            code
        }
    }) {
        return Ok(HeartbeatResponse::error(error_code));
    }

    let error_code = match coordinator.find(&req.group_id) {
        None => codes::UNKNOWN_MEMBER_ID,
        // A closed mailbox reads as a member the group no longer knows. An
        // actor that stopped with the heartbeat in its mailbox still holds
        // the member in the log, so the member keeps its id and retries.
        Some(handle) => {
            let group_id = req.group_id.clone();
            match ask(&handle.tx, |reply| GroupActorMessage::ClassicHeartbeat {
                req,
                reply,
            })
            .await
            {
                Ok(code) => code,
                Err(AskError::Closed) => codes::UNKNOWN_MEMBER_ID,
                Err(AskError::Dropped) => actor_stopped_code(broker, &group_id),
            }
        }
    };

    Ok(HeartbeatResponse::error(error_code))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            handlers::group_read_denied,
            test_support::{EmptyAclImage, empty_acl_image},
        },
        assert2::assert,
    };

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        let EmptyAclImage { authorizer, image } = empty_acl_image();
        anonymous_request_identity!(principal, peer, ctx, "heartbeat-client",);

        assert!(group_read_denied(&authorizer, &image, &ctx, "g"));

        assert!(
            HeartbeatResponse::error(codes::GROUP_AUTHORIZATION_FAILED)
                == krabka_broker_macros::unthrottled_wire!(HeartbeatResponse {
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                })
        );
    }
}
