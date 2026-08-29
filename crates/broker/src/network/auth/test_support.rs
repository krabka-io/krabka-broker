//! Whole-struct assertions on the two `SaslAuthenticate` response shapes.
//!
//! The mechanism modules each check the same success and failure envelopes,
//! so the comparisons live here rather than once per test module.

use assert2::assert;
use krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse;

use crate::codes::SASL_AUTHENTICATION_FAILED;

pub(super) fn assert_success_authenticate_response(
    resp: &SaslAuthenticateResponse,
    expected_auth_bytes: &[u8],
    expected_session_lifetime_ms: i64,
) {
    let expected = SaslAuthenticateResponse {
        error_code: 0,
        error_message: None,
        auth_bytes: bytes::Bytes::copy_from_slice(expected_auth_bytes),
        session_lifetime_ms: expected_session_lifetime_ms,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(*resp == expected);
}

pub(super) fn assert_failed_authenticate_response(resp: &SaslAuthenticateResponse) {
    let expected = SaslAuthenticateResponse {
        error_code: SASL_AUTHENTICATION_FAILED,
        error_message: Some("authentication failed".to_string()),
        auth_bytes: bytes::Bytes::new(),
        session_lifetime_ms: 0,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(*resp == expected);
}
