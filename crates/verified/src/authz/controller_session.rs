use {
    super::{RequestAuthState, request_auth_admission},
    creusot_std::prelude::*,
};

/// Controller connections currently require reconnecting to reauthenticate.
/// Expiry therefore fails the authentication phase before any API dispatch.
/// A session expires only strictly after its deadline, as Kafka's
/// `KafkaChannel.serverAuthenticationSessionExpired` decides, so a request at
/// exactly `expiry_ms` is still admitted.
#[ensures(result == match expiry_ms {
    None => true,
    Some(at) => now_ms@ <= at@,
})]
#[must_use]
pub fn controller_request_admission(expiry_ms: Option<i64>, api_key: i16, now_ms: i64) -> bool {
    let state = match expiry_ms {
        Some(at) if now_ms > at => RequestAuthState::Failed,
        _ => RequestAuthState::Authenticated,
    };
    request_auth_admission(state, api_key)
}
