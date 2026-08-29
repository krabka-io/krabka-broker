//! The two `CreateDelegationTokenResponse` shapes the handler emits: the
//! error-only refusal and the minted-token reply.
//!
//! Both are field-for-field contracts with the JVM admin client, so the
//! construction sits apart from the policy that decides which one to send.

use krabka_protocol::owned::create_delegation_token_response::CreateDelegationTokenResponse;
use krabka_security::KafkaPrincipal;

use super::lifetime::TokenDeadlines;

/// Builds the refusal response carrying `code` and nothing else.
pub(super) fn err_response(code: i16) -> CreateDelegationTokenResponse {
    CreateDelegationTokenResponse {
        error_code: code,
        ..Default::default()
    }
}

/// Builds the success response for a token that the quorum has accepted.
///
/// `caller` is always populated into `token_requester_*`. On self-mint this
/// equals the owner; on act-as it identifies the super-user who minted on
/// behalf of `owner`. Matches Kafka's
/// `DelegationTokenManager.createDelegationToken` (the JVM admin CLI
/// displays both columns unconditionally).
pub(super) fn minted_response(
    owner: &KafkaPrincipal,
    caller: KafkaPrincipal,
    issue_timestamp_ms: i64,
    deadlines: &TokenDeadlines,
    token_id: String,
    hmac: Vec<u8>,
) -> CreateDelegationTokenResponse {
    let (requester_type, requester_name) = (caller.principal_type, caller.name);

    CreateDelegationTokenResponse {
        principal_type: owner.principal_type.clone(),
        principal_name: owner.name.clone(),
        token_requester_principal_type: requester_type,
        token_requester_principal_name: requester_name,
        issue_timestamp_ms,
        expiry_timestamp_ms: deadlines.initial_expiry_ms,
        max_timestamp_ms: deadlines.max_timestamp_ms,
        token_id,
        hmac: bytes::Bytes::from(hmac),
        ..Default::default()
    }
}
