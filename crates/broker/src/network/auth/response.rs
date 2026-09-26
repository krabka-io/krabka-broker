//! The shared failure responses for `SaslAuthenticate`.
//!
//! Kafka's `SaslServerAuthenticator` sends two kinds of failure text. A
//! mechanism that throws `SaslAuthenticationException` chose a message for the
//! client (PLAIN's "Invalid username or password", OAUTHBEARER's error JSON),
//! and it goes out as-is. Any other failure gets one generic sentence naming
//! only the mechanism, so a client cannot tell "no such user" from "bad
//! password". [`fail_authenticate_with`] builds the first kind;
//! [`fail_authenticate`] builds the second, and leaves `error_message` empty
//! for [`generic_failure_message`] to fill in at dispatch, where the mechanism
//! and whether the exchange is a re-authentication are known.

use krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse;
use krabka_security::SaslMechanism;

use crate::codes::SASL_AUTHENTICATION_FAILED;

/// Builds a [`SASL_AUTHENTICATION_FAILED`] response whose client-facing text
/// is Kafka's generic invalid-credentials sentence.
///
/// The function logs `reason` at `debug` and never returns it over the wire.
/// The response leaves `error_message` as `None`; the dispatcher replaces that
/// with [`generic_failure_message`] before it encodes the response.
pub fn fail_authenticate(reason: &str) -> SaslAuthenticateResponse {
    tracing::debug!(reason, "SASL authenticate failed");
    SaslAuthenticateResponse {
        error_code: SASL_AUTHENTICATION_FAILED,
        error_message: None,
        auth_bytes: bytes::Bytes::new(),
        session_lifetime_ms: 0,
        ..Default::default()
    }
}

/// Builds a [`SASL_AUTHENTICATION_FAILED`] response carrying `message`, the
/// text of a Kafka `SaslAuthenticationException`.
pub fn fail_authenticate_with(message: String) -> SaslAuthenticateResponse {
    tracing::debug!(message, "SASL authenticate failed");
    SaslAuthenticateResponse {
        error_code: SASL_AUTHENTICATION_FAILED,
        error_message: Some(message),
        auth_bytes: bytes::Bytes::new(),
        session_lifetime_ms: 0,
        ..Default::default()
    }
}

/// Kafka's message for a failed exchange whose mechanism raised a plain
/// `SaslException`: `SaslServerAuthenticator.handleSaslToken` deliberately
/// names nothing but the phase and the mechanism.
#[must_use]
pub fn generic_failure_message(mechanism: SaslMechanism, reauthentication: bool) -> String {
    let phase = if reauthentication {
        "re-authentication"
    } else {
        "authentication"
    };
    format!(
        "Authentication failed during {phase} due to invalid credentials with SASL mechanism {}",
        mechanism.wire_name()
    )
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn generic_failure_message_names_only_phase_and_mechanism() {
        let cases = [
            (
                SaslMechanism::ScramSha512,
                false,
                "Authentication failed during authentication due to invalid credentials with \
                 SASL mechanism SCRAM-SHA-512",
            ),
            (
                SaslMechanism::Gssapi,
                true,
                "Authentication failed during re-authentication due to invalid credentials with \
                 SASL mechanism GSSAPI",
            ),
        ];
        for (mechanism, reauthentication, expected) in cases {
            assert!(generic_failure_message(mechanism, reauthentication) == expected);
        }
    }

    #[test]
    fn fail_authenticate_keeps_the_reason_off_the_wire() {
        let resp = fail_authenticate("unknown user alice");
        assert!(
            resp == SaslAuthenticateResponse {
                error_code: SASL_AUTHENTICATION_FAILED,
                ..Default::default()
            }
        );
    }
}
