use super::*;
#[cfg(test)]
use std::vec;

#[cfg(test)]
#[rstest_bdd_macros::then(
    "authz tests kip368 session boundaries: session expired for request matches kafka for {example}"
)]
fn bdd_verify_session_expired_for_request_matches_kafka(example: String) {
    type Example = (Option<i64>, i16, i64, Option<i64>, bool);
    let (expiry_ms, api_key, now_ms, last_reauth_start_ms, expired): Example = match example
        .as_str()
    {
        "no deadline never expires" => (None, 0, i64::MAX, None, false),
        "before the deadline" => (Some(1_000), 0, 999, None, false),
        "at the deadline" => (Some(1_000), 0, 1_000, None, false),
        "one ms after the deadline" => (Some(1_000), 0, 1_001, None, true),
        "api versions after the deadline" => (Some(1_000), 18, 1_001, None, true),
        "sasl authenticate at the deadline" => (Some(1_000), 36, 1_000, None, false),
        "sasl authenticate after the deadline" => (Some(1_000), 36, 1_001, None, true),
        "handshake that begins reauth" => (Some(1_000), 17, 1_001, None, false),
        "handshake a second after the last reauth" => (Some(1_000), 17, 2_001, Some(1_001), false),
        "handshake inside the reauth interval" => (Some(1_000), 17, 2_000, Some(1_001), true),
        "handshake inside the interval before the deadline" => {
            (Some(1_000), 17, 1_000, Some(999), false)
        }
        "i64 max deadline never passes" => (Some(i64::MAX), 0, i64::MAX, None, false),
        "i64 min deadline" => (Some(i64::MIN), 0, i64::MIN + 1, None, true),
        "interval does not overflow" => (Some(i64::MIN), 17, i64::MAX, Some(i64::MIN), false),
        _ => panic!("unknown BDD example: {example}"),
    };

    check!(
        session_expired_for_request(expiry_ms, api_key, now_ms, last_reauth_start_ms) == expired
    );
}

/// Kafka's `KafkaChannel.serverAuthenticationSessionExpired` expires a session
/// only strictly after its deadline, and `Processor.processCompletedReceives`
/// spares only a `SaslHandshake` that `maybeBeginServerReauthentication`
/// accepts. Every other frame on an expired session closes the connection,
/// `SaslAuthenticate` (36) included.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/authz/tests/kip368_session_boundaries.feature",
    name = "session expired for request matches kafka"
)]
fn session_expired_for_request_matches_kafka(example: String) { let _ = example; }

#[cfg(test)]
#[rstest_bdd_macros::then(
    "authz tests kip368 session boundaries: reauthentication begins matches kafka for {example}"
)]
fn bdd_verify_reauthentication_begins_matches_kafka(example: String) {
    type Example = (Option<i64>, i64, Option<i64>, bool);
    let (expiry_ms, now_ms, last_start_ms, begins): Example = match example.as_str() {
        "no expiry never reauthenticates" => (None, 5_000, None, false),
        "first reauthentication" => (Some(1_000), 5_000, None, true),
        "exactly a second later" => (Some(1_000), 5_000, Some(4_000), true),
        "one ms inside the interval" => (Some(1_000), 5_000, Some(4_001), false),
        "clock rollback" => (Some(1_000), 5_000, Some(6_000), false),
        "wide gap saturates" => (Some(1_000), i64::MAX, Some(i64::MIN), true),
        "wide rollback saturates" => (Some(1_000), i64::MIN, Some(i64::MAX), false),
        _ => panic!("unknown BDD example: {example}"),
    };

    check!(reauthentication_begins(expiry_ms, now_ms, last_start_ms) == begins);
}

/// Kafka's `KafkaChannel.maybeBeginServerReauthentication`: a session with
/// an expiry may begin a re-authentication when none has started on the
/// connection, or the last one started a second or more ago.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/authz/tests/kip368_session_boundaries.feature",
    name = "reauthentication begins matches kafka"
)]
fn reauthentication_begins_matches_kafka(example: String) { let _ = example; }
