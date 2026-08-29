//! The shared failure response for `SaslAuthenticate`.
//!
//! Every mechanism handler rejects a bad credential the same way, so the
//! builder lives in one place rather than once per mechanism.

use krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse;

use crate::codes::SASL_AUTHENTICATION_FAILED;

/// Builds a [`SASL_AUTHENTICATION_FAILED`] response.
///
/// The function logs `reason` at `debug` and never returns it over the wire.
/// Auth failures are deliberately opaque, so that an attacker cannot tell
/// "no such user" from "bad password".
pub fn fail_authenticate(reason: &str) -> SaslAuthenticateResponse {
    tracing::debug!(reason, "SASL authenticate failed");
    SaslAuthenticateResponse {
        error_code: SASL_AUTHENTICATION_FAILED,
        error_message: Some("authentication failed".to_string()),
        auth_bytes: bytes::Bytes::new(),
        session_lifetime_ms: 0,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::auth::test_support::assert_failed_authenticate_response;

    #[test]
    fn fail_authenticate_has_kafka_sasl_failure_shape() {
        let resp = fail_authenticate("unit-test");
        assert_failed_authenticate_response(&resp);
    }
}
