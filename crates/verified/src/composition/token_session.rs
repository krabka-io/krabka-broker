use {
    crate::{
        authz::{
            RequestAuthState, request_auth_admission, sasl_session_expiry,
            session_expired_for_request,
        },
        delegation_token::{
            ScramCredentialSource, TokenApi, TokenApiAdmission, TokenCreateDecision,
            create_token_deadlines, scram_credential_source, token_api_admission,
        },
    },
    creusot_std::prelude::*,
};

// Stored expiry, maximum lifetime, session deadline, advertised lifetime,
// request admission, and token-API isolation.
type TokenSessionReceipt = (i64, i64, i64, i64, bool, TokenApiAdmission);

/// Create a token, classify SCRAM round one, then combine the session expiry
/// at round two with the request and token-API gates.
///
/// A stored token authenticates whether or not it has expired, as Kafka's
/// `ScramServerCallbackHandler` reads the token cache without an expiry
/// check. The session is bound to the token's expiry, so a token that expired
/// before round two completed gets a lifetime of 0 and is past its deadline
/// for every later request. The receipt derives deadline coherence from
/// creation rather than assuming it, and proves admission up to and at the
/// earliest ceiling as well as rejection after it, the strict deadline
/// Kafka's KIP-368 check uses. No re-authentication has started on the
/// connection, so past the deadline only a `SaslHandshake` (17), which begins
/// one, is still admitted. Cryptographic authentication success, faithful
/// pending expiry/identity transfer, coherent metadata and nondecreasing clock
/// observations remain host obligations. Renewal/revocation after round one is
/// outside this snapshot.
#[requires(0 <= times.0@ && times.0@ <= times.1@ && times.1@ <= times.2@)]
#[ensures((result == None) == (periods.1@ <= 0 || periods.2@ <= 0))]
#[ensures(match result {
    None => true,
    Some(receipt) => times.0@ <= receipt.0@ && receipt.0@ <= receipt.1@
        && receipt.1@ <= times.0@ + periods.1@
        && (periods.0@ > 0 ==> receipt.1@ <= times.0@ + periods.0@)
        && receipt.0@ <= times.0@ + periods.2@
        && receipt.2@ <= receipt.0@
        && (periods.3@ > 0 ==> receipt.2@ <= times.1@ + periods.3@)
        && receipt.3@ == (if receipt.2@ <= times.1@ { 0 } else { receipt.2@ - times.1@ })
        && (times.1@ >= receipt.0@ ==> receipt.3@ == 0)
        && receipt.4 == (api_key@ == 17 || times.2@ <= receipt.2@)
        && receipt.5 == TokenApiAdmission::Reject,
})]
#[ensures(match result {
    None => true,
    Some(receipt) =>
        ((times.2@ > receipt.0@ || times.2@ > receipt.1@
            || (periods.3@ > 0 && times.2@ > times.1@ + periods.3@))
            ==> receipt.4 == (api_key@ == 17))
        && ((times.2@ <= receipt.0@
            && (periods.3@ <= 0 || times.2@ <= times.1@ + periods.3@)) ==> receipt.4),
})]
pub(super) fn created_token_session_bounds_requests(
    times: (i64, i64, i64),        // creation, SCRAM round two, request
    periods: (i64, i64, i64, i64), // requested lifetime, ceiling, renewal, reauth cap
    api_key: i16,
    token_api: TokenApi,
) -> Option<TokenSessionReceipt> {
    let TokenCreateDecision::Create(deadlines) =
        create_token_deadlines(times.0, periods.0, periods.1, periods.2)
    else {
        return None;
    };
    if !matches!(
        scram_credential_source(false, true, true),
        ScramCredentialSource::DelegationToken
    ) {
        return None;
    }
    let pending_expiry = deadlines.initial_expiry_ms;
    let (Some(at), lifetime) = sasl_session_expiry(times.1, Some(pending_expiry), Some(periods.3))
    else {
        return None;
    };
    let admitted = request_auth_admission(RequestAuthState::Authenticated, api_key)
        && !session_expired_for_request(Some(at), api_key, times.2, None);
    let token_admission = token_api_admission(true, true, token_api);
    Some((
        pending_expiry,
        deadlines.max_timestamp_ms,
        at,
        lifetime,
        admitted,
        token_admission,
    ))
}

mod renewal;
