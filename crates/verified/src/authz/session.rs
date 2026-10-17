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

/// Kafka's `KafkaChannel.MIN_REAUTH_INTERVAL_ONE_SECOND_NANOS`, in ms: a
/// connection may not start a re-authentication more than once a second.
pub const MIN_REAUTH_INTERVAL_MS: i64 = 1_000;

/// Whether a `SaslHandshake` on a session that expires at `expiry_ms` begins
/// a KIP-368 re-authentication, as Kafka's
/// `KafkaChannel.maybeBeginServerReauthentication` decides: the session must
/// have an expiry, and the last re-authentication on the connection, if any,
/// must have started [`MIN_REAUTH_INTERVAL_MS`] or more before `now_ms`.
#[ensures(result == (expiry_ms != None && match last_start_ms {
    None => true,
    Some(last) => now_ms@ - last@ >= 1000,
}))]
#[must_use]
pub fn reauthentication_begins(
    expiry_ms: Option<i64>,
    now_ms: i64,
    last_start_ms: Option<i64>,
) -> bool {
    match (expiry_ms, last_start_ms) {
        (None, _) => false,
        (Some(_), None) => true,
        (Some(_), Some(last)) => now_ms.saturating_sub(last) >= MIN_REAUTH_INTERVAL_MS,
    }
}

/// Whether a request must close the connection because its KIP-368 session
/// has expired, as Kafka's `Processor.processCompletedReceives` decides.
///
/// A session expires only strictly after its deadline, as in
/// `KafkaChannel.serverAuthenticationSessionExpired`: a request at exactly
/// `expiry_ms` is still served. Past the deadline the one exempt frame is a
/// `SaslHandshake` (17) that [`reauthentication_begins`] accepts, because
/// Kafka asks `maybeBeginServerReauthentication` before it checks the
/// deadline. Every other frame closes, a `SaslAuthenticate` (36) with no
/// handshake before it and a handshake inside the one-second interval
/// included. Authentication phase admission is a separate gate, including
/// during the re-authentication rounds.
#[ensures(result == (match expiry_ms {
    None => false,
    Some(at) => now_ms@ > at@,
} && !(api_key@ == 17 && match last_reauth_start_ms {
    None => true,
    Some(last) => now_ms@ - last@ >= 1000,
})))]
#[must_use]
pub fn session_expired_for_request(
    expiry_ms: Option<i64>,
    api_key: i16,
    now_ms: i64,
    last_reauth_start_ms: Option<i64>,
) -> bool {
    match expiry_ms {
        Some(at) if now_ms > at => {
            !(api_key == 17 && reauthentication_begins(expiry_ms, now_ms, last_reauth_start_ms))
        }
        _ => false,
    }
}
