use creusot_std::prelude::*;

/// Bind a SASL session to the earlier credential or positive listener cap.
/// A sum beyond the clock range stays finite at `i64::MAX`.
#[ensures((result.0 == None) == (credential_expiry_ms == None
    && match reauth_ms { None => true, Some(cap) => cap@ <= 0 }))]
#[ensures(match (credential_expiry_ms, result.0) {
    (Some(credential), Some(at)) => at@ <= credential@,
    _ => true,
})]
#[ensures(match (reauth_ms, result.0) {
    (Some(cap), Some(at)) => cap@ > 0 ==> at@ <= now_ms@ + cap@,
    _ => true,
})]
#[ensures(match result.0 {
    None => true,
    Some(at) => credential_expiry_ms == Some(at) || match reauth_ms {
        None => false,
        Some(cap) => cap@ > 0 && (at@ == now_ms@ + cap@ || at@ == i64::MAX@),
    },
})]
#[ensures(match result.0 {
    None => result.1@ == 0,
    Some(at) => result.1@ == if at@ <= now_ms@ { 0 }
        else if at@ - now_ms@ > i64::MAX@ { i64::MAX@ }
        else { at@ - now_ms@ },
})]
#[must_use]
pub fn sasl_session_expiry(
    now_ms: i64,
    credential_expiry_ms: Option<i64>,
    reauth_ms: Option<i64>,
) -> (Option<i64>, i64) {
    let cap_expiry = match reauth_ms {
        Some(cap) if cap > 0 => Some(now_ms.saturating_add(cap)),
        _ => None,
    };
    let expiry = match (credential_expiry_ms, cap_expiry) {
        (Some(credential), Some(cap)) => Some(credential.min(cap)),
        (credential, cap) => credential.or(cap),
    };
    let lifetime = match expiry {
        None => 0,
        Some(at) => at.saturating_sub(now_ms).max(0),
    };
    (expiry, lifetime)
}

/// Expired sessions may start/continue reauthentication (17/36), but cannot
/// serve ordinary requests at or after their deadline. Authentication phase
/// admission is a separate gate, including during the reauthentication rounds.
#[ensures(result == (api_key@ != 17 && api_key@ != 36 && match expiry_ms {
    None => false,
    Some(at) => at@ <= now_ms@,
}))]
#[must_use]
pub fn session_expired_for_request(expiry_ms: Option<i64>, api_key: i16, now_ms: i64) -> bool {
    if matches!(api_key, 17 | 36) {
        return false;
    }
    match expiry_ms {
        Some(at) => at <= now_ms,
        None => false,
    }
}
