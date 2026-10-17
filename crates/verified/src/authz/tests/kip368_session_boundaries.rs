use super::*;

/// Kafka's `KafkaChannel.serverAuthenticationSessionExpired` expires a session
/// only strictly after its deadline, and `Processor.processCompletedReceives`
/// spares only a `SaslHandshake` that `maybeBeginServerReauthentication`
/// accepts. Every other frame on an expired session closes the connection,
/// `SaslAuthenticate` (36) included.
#[yare::parameterized(
    no_deadline_never_expires = { None, 0, i64::MAX, None, false },
    before_the_deadline = { Some(1_000), 0, 999, None, false },
    at_the_deadline = { Some(1_000), 0, 1_000, None, false },
    one_ms_after_the_deadline = { Some(1_000), 0, 1_001, None, true },
    api_versions_after_the_deadline = { Some(1_000), 18, 1_001, None, true },
    sasl_authenticate_at_the_deadline = { Some(1_000), 36, 1_000, None, false },
    sasl_authenticate_after_the_deadline = { Some(1_000), 36, 1_001, None, true },
    handshake_that_begins_reauth = { Some(1_000), 17, 1_001, None, false },
    handshake_a_second_after_the_last_reauth = { Some(1_000), 17, 2_001, Some(1_001), false },
    handshake_inside_the_reauth_interval = { Some(1_000), 17, 2_000, Some(1_001), true },
    handshake_inside_the_interval_before_the_deadline = { Some(1_000), 17, 1_000, Some(999), false },
    i64_max_deadline_never_passes = { Some(i64::MAX), 0, i64::MAX, None, false },
    i64_min_deadline = { Some(i64::MIN), 0, i64::MIN + 1, None, true },
    interval_does_not_overflow = { Some(i64::MIN), 17, i64::MAX, Some(i64::MIN), false },
)]
fn session_expired_for_request_matches_kafka(
    expiry_ms: Option<i64>,
    api_key: i16,
    now_ms: i64,
    last_reauth_start_ms: Option<i64>,
    expired: bool,
) {
    check!(
        session_expired_for_request(expiry_ms, api_key, now_ms, last_reauth_start_ms) == expired
    );
}

/// Kafka's `KafkaChannel.maybeBeginServerReauthentication`: a session with
/// an expiry may begin a re-authentication when none has started on the
/// connection, or the last one started a second or more ago.
#[yare::parameterized(
    no_expiry_never_reauthenticates = { None, 5_000, None, false },
    first_reauthentication = { Some(1_000), 5_000, None, true },
    exactly_a_second_later = { Some(1_000), 5_000, Some(4_000), true },
    one_ms_inside_the_interval = { Some(1_000), 5_000, Some(4_001), false },
    clock_rollback = { Some(1_000), 5_000, Some(6_000), false },
    wide_gap_saturates = { Some(1_000), i64::MAX, Some(i64::MIN), true },
    wide_rollback_saturates = { Some(1_000), i64::MIN, Some(i64::MAX), false },
)]
fn reauthentication_begins_matches_kafka(
    expiry_ms: Option<i64>,
    now_ms: i64,
    last_start_ms: Option<i64>,
    begins: bool,
) {
    check!(reauthentication_begins(expiry_ms, now_ms, last_start_ms) == begins);
}
