//! The `BrokerHeartbeatResponse` bodies the handler returns, and the encode
//! step that turns one into wire bytes.
//!
//! Every response the handler sends comes from one of these builders, so the
//! default-valued fields stay in one place.

use bytes::Bytes;
use krabka_protocol::owned::broker_heartbeat_response::BrokerHeartbeatResponse;

use crate::{codes, error::BrokerError};

pub(super) fn not_controller_response() -> BrokerHeartbeatResponse {
    error_response(codes::NOT_CONTROLLER)
}

pub(super) fn error_response(error_code: i16) -> BrokerHeartbeatResponse {
    BrokerHeartbeatResponse {
        error_code,
        ..Default::default()
    }
}

pub(super) fn success_response(
    is_caught_up: bool,
    is_fenced: bool,
    should_shut_down: bool,
) -> BrokerHeartbeatResponse {
    BrokerHeartbeatResponse {
        is_caught_up,
        is_fenced,
        should_shut_down,
        ..Default::default()
    }
}

pub(super) fn denied_response_body() -> BrokerHeartbeatResponse {
    error_response(codes::CLUSTER_AUTHORIZATION_FAILED)
}

pub(super) fn encode_response(
    version: i16,
    resp: &BrokerHeartbeatResponse,
) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(resp, version)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn heartbeat_response_builders_preserve_non_default_fields() {
        let expected_not_controller = BrokerHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: codes::NOT_CONTROLLER,
            is_caught_up: false,
            is_fenced: true,
            should_shut_down: false,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(not_controller_response() == expected_not_controller);

        let expected_success = BrokerHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            is_caught_up: true,
            is_fenced: false,
            should_shut_down: true,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(success_response(true, false, true) == expected_success);

        let expected_denied = BrokerHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            is_caught_up: false,
            is_fenced: true,
            should_shut_down: false,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(denied_response_body() == expected_denied);
    }
}
