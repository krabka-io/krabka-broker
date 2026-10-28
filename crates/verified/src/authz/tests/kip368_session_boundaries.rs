use {
    super::*,
    rstest::fixture,
    rstest_bdd_macros::{given, scenarios, then, when},
    std::vec,
};

#[derive(Default)]
struct SessionBoundary {
    expiry_ms: Option<i64>,
    last_reauth_start_ms: Option<i64>,
    expired: Option<bool>,
    reauthentication: Option<bool>,
}

#[fixture]
fn session() -> SessionBoundary {
    // Start this scenario with fresh inputs and observations.

    SessionBoundary {
        expiry_ms: None,
        last_reauth_start_ms: None,
        expired: None,
        reauthentication: None,
    }
}

fn millis(value: &str) -> i64 {
    match value {
        "minimum" => i64::MIN,
        "maximum" => i64::MAX,
        "minimum-plus-one" => i64::MIN + 1,
        _ => value.parse().expect("a signed millisecond timestamp"),
    }
}

fn optional_millis(value: &str) -> Option<i64> {
    if value == "none" {
        None
    } else {
        Some(millis(value))
    }
}

#[given(
    "a session expiring at {expiry_ms} ms with its last reauthentication at {last_start_ms} ms"
)]
fn session_deadline(session: &mut SessionBoundary, expiry_ms: String, last_start_ms: String) {
    session.expiry_ms = optional_millis(&expiry_ms);
    session.last_reauth_start_ms = optional_millis(&last_start_ms);
}

#[when("a {request} request arrives at {now_ms} ms")]
fn request_arrives(session: &mut SessionBoundary, request: String, now_ms: String) {
    let api_key = match request.as_str() {
        "Produce" => 0,
        "ApiVersions" => 18,
        "SaslHandshake" => 17,
        "SaslAuthenticate" => 36,
        _ => panic!("unknown session-boundary request: {request}"),
    };
    session.expired = Some(session_expired_for_request(
        session.expiry_ms,
        api_key,
        millis(&now_ms),
        session.last_reauth_start_ms,
    ));
}

#[then("the request expires the authentication session: {expired}")]
fn request_expiry(session: &SessionBoundary, expired: bool) {
    check!(session.expired == Some(expired));
}

#[when("reauthentication is attempted at {now_ms} ms")]
fn attempt_reauthentication(session: &mut SessionBoundary, now_ms: String) {
    session.reauthentication = Some(reauthentication_begins(
        session.expiry_ms,
        millis(&now_ms),
        session.last_reauth_start_ms,
    ));
}

#[then("reauthentication can begin: {begins}")]
fn reauthentication_admitted(session: &SessionBoundary, begins: bool) {
    check!(session.reauthentication == Some(begins));
}

scenarios!("tests/features/migrated/src/authz/tests/kip368_session_boundaries.feature", fixtures = [session: SessionBoundary]);
