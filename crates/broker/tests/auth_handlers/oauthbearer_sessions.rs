//! SASL/OAUTHBEARER re-authentication (KIP-368) end-to-end.
//!
//! Seven scenarios exercise the full session-lifetime and in-band re-auth
//! surface: the response carries `session_lifetime_ms ~ exp - now`; a broker
//! cap clamps it; a request past the token's `exp` closes the connection; an
//! in-band re-auth with a fresh token re-arms the session even when it starts
//! after the old token expired; an in-band re-auth with a different principal
//! name returns 58 and closes the connection; an in-band attempt to switch
//! mechanism answers the handshake then closes on the next frame; and, as a regression, a PLAIN listener reports
//! `session_lifetime_ms = 0` and expires nothing.
//!
//! The expiry deadline is a wall-clock instant that the dispatch loop compares
//! each arriving request against, the way Kafka's
//! `Processor.processCompletedReceives` does, so these tests use short real
//! windows and real sleeps rather than a paused tokio clock. There is no timer
//! to jump past, and a session that has expired stays open until a request
//! that is not part of a re-authentication arrives on it.

use {
    crate::{
        harness::{alice_password, round_trip, sasl_authenticate_round_trip, start_broker_stream},
        kafka_wire::exchange,
        oauthbearer::{
            drive_inband_reauth, drive_sasl_oauthbearer_session_open, now_unix_secs,
            oauthbearer_fixture, oauthbearer_zero_skew_validator, unsecured_jws,
        },
        support::{
            sasl::{plain_authentication_body, sasl_temp_config},
            sasl_plaintext_config,
            wire::{
                CorrelationId as WireCorrelationId, HeaderEncoding as RequestHeaderEncoding,
                SocketExchangeSetup,
            },
        },
    },
    assert2::assert,
    bytes::BytesMut,
    krabka_broker::{Broker, BrokerHandle},
    krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion},
    krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request, api_versions_response, metadata_request, metadata_response,
            sasl_authenticate_response, sasl_handshake_request, sasl_handshake_response,
        },
    },
    krabka_security::SaslMechanism,
    rstest::fixture,
    rstest_bdd_macros::{given, then, when},
    std::{io, time::Duration},
    tempfile::TempDir,
    tokio::{io::AsyncReadExt, net::TcpStream, time},
};

/// Sends one Metadata request on a session whose window has elapsed.
///
/// That request is what ends the connection: KIP-368 expiry is enforced where
/// a request is admitted, not by a timer, so a test that only waits out the
/// window would wait forever. The broker closes without answering, so the
/// round trip's own result is nothing to assert on — the EOF the caller then
/// reads is.
async fn send_metadata_on_expired_session(stream: &mut TcpStream) {
    let mut md_body = BytesMut::new();
    metadata_request::MetadataRequest::default()
        .encode(&mut md_body, 12)
        .expect("Metadata encode must succeed");
    let _ = round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(3),
            version: WireApiVersion(12),
            correlation: WireCorrelationId(98),
            header: RequestHeaderEncoding::Flexible,
        },
        &md_body,
    )
    .await;
}

/// Test #1: a successful OAUTHBEARER authentication carries
/// `session_lifetime_ms ≈ exp - now`.
///
/// `session_lifetime_ms` is the KIP-368 wire field on
/// `sasl_authenticate_response::SaslAuthenticateResponse v1+`.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/oauthbearer_sessions.feature",
    name = "oauthbearer session lifetime ms set from token exp"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_session_lifetime_ms_set_from_token_exp(lifetime: TokenSession) {
    lifetime
        .broker
        .expect("a session-lifetime broker")
        .shutdown()
        .await;
}

/// The broker clamps `session_lifetime_ms` when the listener sets
/// `connections.max.reauth.ms`.
///
/// The response value becomes `min(token_exp_ms - now_ms, cap * 1000)`. The
/// dispatch loop anchors its deadline to the CLAMPED value, so the broker
/// enforces what it told the client.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/oauthbearer_sessions.feature",
    name = "oauthbearer session capped by connections max reauth"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_session_capped_by_connections_max_reauth(lifetime: TokenSession) {
    lifetime
        .broker
        .expect("a session-lifetime broker")
        .shutdown()
        .await;
}

/// Test #2: a request that arrives past the token's `exp` closes the TCP
/// stream, and the client observes EOF on the next read.
///
/// The token is given a few seconds of life and the test waits it out, rather
/// than jumping a paused tokio clock: the deadline is a wall-clock instant the
/// dispatch loop compares each arriving request against, not a timer.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/oauthbearer_sessions.feature",
    name = "oauthbearer session expires closes connection"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_session_expires_closes_connection(lifetime: TokenSession) {
    lifetime
        .broker
        .expect("a session-lifetime broker")
        .shutdown()
        .await;
}

/// Test #3: an in-band SaslHandshake/SaslAuthenticate pair with a fresh token
/// on an already-authenticated stream resets the per-connection deadline.
///
/// The re-auth here runs *after* token A has already expired, which is what a
/// JVM client does: it picks a re-auth point inside the window but acts on it
/// only when it next has a request to write. The broker must serve the
/// exchange and then re-arm the session from token B's `exp`.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/oauthbearer_sessions.feature",
    name = "oauthbearer in band reauth with fresh token resets timer"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_in_band_reauth_with_fresh_token_resets_timer(lifetime: TokenSession) {
    lifetime
        .broker
        .expect("a session-lifetime broker")
        .shutdown()
        .await;
}

/// Test #4: the broker rejects an in-band re-auth with a token whose `sub`
/// differs from the original principal name.
///
/// `sasl_authenticate_response::SaslAuthenticateResponse` carries
/// `error_code = SASL_AUTHENTICATION_FAILED (58)`, and the connection closes.
/// The client then reads EOF. KIP-368 forbids a change of principal across an
/// in-band re-auth.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/oauthbearer_sessions.feature",
    name = "oauthbearer in band reauth with different principal closes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_in_band_reauth_with_different_principal_closes(lifetime: TokenSession) {
    lifetime
        .broker
        .expect("a session-lifetime broker")
        .shutdown()
        .await;
}

/// Test #5: the broker rejects an in-band `SaslHandshake` whose `mechanism`
/// differs from the mechanism it first negotiated.
///
/// The response carries `NONE`, and the next frame closes the connection
/// (Kafka's `REAUTH_BAD_MECHANISM`). KIP-368 needs
/// the same mechanism across an in-band re-auth, even when the broker would
/// otherwise accept SCRAM on a fresh connection.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/oauthbearer_sessions.feature",
    name = "oauthbearer in band reauth with different mechanism closes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_in_band_reauth_with_different_mechanism_closes(lifetime: TokenSession) {
    lifetime
        .broker
        .expect("a session-lifetime broker")
        .shutdown()
        .await;
}

/// Test #6: regression for PLAIN listeners.
///
/// A PLAIN `SaslAuthenticate` response must carry `session_lifetime_ms = 0`.
/// The KIP-368 wire field has a meaning for OAUTHBEARER only. The dispatch
/// loop must NOT arm a per-connection deadline. An advance of the tokio clock
/// by one hour is harmless, and a Metadata RPC still succeeds.
///
/// The test uses the `current_thread` flavor, because `tokio::time::pause()`
/// needs a single-threaded runtime. See the comment on test #2 for why we
/// pause after the handshake and do not set `start_paused = true`.
///
/// The fixture switches `connections.max.idle.ms` off. That deadline is the
/// other reason this connection could be closed an hour on, and it is not the
/// one under test here -- `crates/broker/tests/connections_max_idle.rs` covers
/// it. Leaving it at Kafka's ten minutes would close the connection for being
/// idle and say nothing about the KIP-368 timer.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/oauthbearer_sessions.feature",
    name = "plain listener session lifetime ms is zero and no timer"
)]
#[tokio::test(flavor = "current_thread")]
async fn plain_listener_session_lifetime_ms_is_zero_and_no_timer(lifetime: TokenSession) {
    lifetime
        .broker
        .expect("a session-lifetime broker")
        .shutdown()
        .await;
}

async fn read_session_end(stream: &mut TcpStream) -> usize {
    let mut buf = [0_u8; 16];
    time::timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("read should not hang")
        .expect("read should not error")
}

async fn wait_past_expiration(expiration: i64) {
    let remaining = u64::try_from(expiration.saturating_sub(now_unix_secs()).max(0))
        .expect("a non-negative second count fits in u64");
    time::sleep(Duration::from_secs(remaining) + Duration::from_millis(500)).await;
}

struct TokenSession {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    stream: Option<TcpStream>,
    token: Option<String>,
    expiration: Option<i64>,
    session_lifetime: Option<i64>,
    reauthentication: Option<io::Result<()>>,
    metadata: Option<metadata_response::MetadataResponse>,
    bytes_read: Option<usize>,
    handshake: Option<sasl_handshake_response::SaslHandshakeResponse>,
    plain_authentication: Option<sasl_authenticate_response::SaslAuthenticateResponse>,
    handshake_body: Vec<u8>,
    next_frame: Option<io::Result<Vec<u8>>>,
    metadata_correlation: i32,
}

#[fixture]
fn lifetime() -> TokenSession {
    // Start this scenario with fresh inputs and observations.

    TokenSession {
        broker: None,
        directory: None,
        stream: None,
        token: None,
        expiration: None,
        session_lifetime: None,
        reauthentication: None,
        metadata: None,
        bytes_read: None,
        handshake: None,
        plain_authentication: None,
        handshake_body: Vec::new(),
        next_frame: None,
        metadata_correlation: 99,
    }
}

#[given("an uncapped OAUTHBEARER listener with zero clock skew")]
async fn uncapped_bearer(lifetime: &mut TokenSession) {
    let (directory, broker, _addr) =
        oauthbearer_fixture(oauthbearer_zero_skew_validator(), None).await;
    lifetime.broker = Some(broker);
    lifetime.directory = Some(directory);
}

#[given("an OAUTHBEARER listener capped at 300 milliseconds with zero clock skew")]
async fn capped_bearer(lifetime: &mut TokenSession) {
    let (directory, broker, _addr) = oauthbearer_fixture(
        oauthbearer_zero_skew_validator(),
        Some(krabka_units::millis(300)),
    )
    .await;
    lifetime.broker = Some(broker);
    lifetime.directory = Some(directory);
}

#[given("the listener enables OAUTHBEARER and SCRAM-SHA-512 with zero clock skew")]
async fn dual_mechanism_bearer(lifetime: &mut TokenSession) {
    let (directory, mut config) =
        sasl_temp_config(vec![SaslMechanism::OAuthBearer, SaslMechanism::ScramSha512]);
    config.oauthbearer_validator = oauthbearer_zero_skew_validator();
    lifetime.broker = Some(Broker::start(config).await.expect("broker must start"));
    lifetime.directory = Some(directory);
}

#[given("a token for {principal} expires in {seconds} seconds")]
fn session_token(lifetime: &mut TokenSession, principal: String, seconds: i64) {
    let expiration = now_unix_secs() + seconds;
    lifetime.token = Some(unsecured_jws(&principal, expiration));
    lifetime.expiration = Some(expiration);
}

#[when("the client authenticates using that bearer token")]
async fn authenticate_bearer(lifetime: &mut TokenSession) {
    let addr = lifetime
        .broker
        .as_ref()
        .expect("an OAUTHBEARER listener")
        .listen_addr();
    let token = lifetime.token.as_ref().expect("a bearer token");
    let (stream, session_lifetime) = drive_sasl_oauthbearer_session_open(addr, token)
        .await
        .expect("OAUTHBEARER session must succeed");
    lifetime.stream = Some(stream);
    lifetime.session_lifetime = Some(session_lifetime);
}

#[given("alice is bearer-authenticated with a token expiring in {seconds} seconds")]
async fn established_bearer(lifetime: &mut TokenSession, seconds: i64) {
    session_token(lifetime, "alice".into(), seconds);
    authenticate_bearer(lifetime).await;
}

#[then("the bearer lifetime is at least {minimum} and below {maximum} milliseconds")]
fn token_expiration_lifetime(lifetime: &TokenSession, minimum: i64, maximum: i64) {
    let actual = lifetime
        .session_lifetime
        .expect("bearer authentication returned its lifetime");
    assert!(
        (minimum..maximum).contains(&actual),
        "session_lifetime_ms = {actual}, expected {minimum}..{maximum}"
    );
}

#[then("the capped bearer lifetime is between 200 and 300 milliseconds")]
fn capped_token_lifetime(lifetime: &TokenSession) {
    let actual = lifetime
        .session_lifetime
        .expect("bearer authentication returned its lifetime");
    assert!(
        (200..=300).contains(&actual),
        "session_lifetime_ms = {actual}, expected ~300 (capped)"
    );
}

#[when("500 milliseconds elapse before the bearer client requests Metadata")]
async fn expired_capped_request(lifetime: &mut TokenSession) {
    time::sleep(Duration::from_millis(500)).await;
    expired_bearer_metadata(lifetime).await;
}

#[when("the current bearer token's expiration passes")]
async fn expired_bearer_token(lifetime: &TokenSession) {
    wait_past_expiration(
        lifetime
            .expiration
            .expect("a token expiration was recorded"),
    )
    .await;
}

#[when("the expired bearer client requests Metadata")]
async fn expired_bearer_metadata(lifetime: &mut TokenSession) {
    send_metadata_on_expired_session(
        lifetime
            .stream
            .as_mut()
            .expect("an authenticated bearer connection"),
    )
    .await;
}

#[when("the bearer client reads the connection")]
async fn read_bearer_end(lifetime: &mut TokenSession) {
    lifetime.bytes_read = Some(
        read_session_end(
            lifetime
                .stream
                .as_mut()
                .expect("an authenticated bearer connection"),
        )
        .await,
    );
}

#[then("the expired or refused bearer connection reaches EOF")]
fn ended_bearer_session(lifetime: &TokenSession) {
    assert!(
        lifetime.bytes_read == Some(0),
        "expected EOF after session expiry or refused reauthentication, got {:?} bytes",
        lifetime.bytes_read
    );
}

#[when("the same connection reauthenticates with the new bearer token")]
async fn refresh_bearer(lifetime: &mut TokenSession) {
    let token = lifetime.token.as_ref().expect("a fresh bearer token");
    lifetime.reauthentication = Some(
        drive_inband_reauth(
            lifetime
                .stream
                .as_mut()
                .expect("an authenticated bearer connection"),
            token,
        )
        .await,
    );
}

#[then("in-band bearer reauthentication succeeds past the old token's expiration")]
fn refreshed_bearer(lifetime: &TokenSession) {
    lifetime
        .reauthentication
        .as_ref()
        .expect("in-band reauthentication was attempted")
        .as_ref()
        .expect("in-band re-auth with fresh token must succeed past token A's exp");
}

#[when("the authenticated connection requests Metadata")]
async fn live_bearer_metadata(lifetime: &mut TokenSession) {
    let mut body = BytesMut::new();
    metadata_request::MetadataRequest::default()
        .encode(&mut body, 12)
        .expect("Metadata encode must succeed");
    let response = round_trip(
        lifetime
            .stream
            .as_mut()
            .expect("an authenticated connection"),
        SocketExchangeSetup {
            api_key: WireApiKey(3),
            version: WireApiVersion(12),
            correlation: WireCorrelationId(lifetime.metadata_correlation),
            header: RequestHeaderEncoding::Flexible,
        },
        &body,
    )
    .await
    .expect("Metadata RPC must succeed after authentication");
    lifetime.metadata = Some(
        metadata_response::MetadataResponse::decode(&mut &response[..], 12)
            .expect("Metadata decode must succeed"),
    );
}

#[then("the live session's Metadata response names at least one broker")]
fn live_bearer_response(lifetime: &TokenSession) {
    assert!(
        !lifetime
            .metadata
            .as_ref()
            .expect("Metadata was requested on the live connection")
            .brokers
            .is_empty(),
        "Metadata response must carry at least one broker"
    );
}

#[then("changing the bearer principal is refused with SASL_AUTHENTICATION_FAILED")]
fn changed_bearer_principal(lifetime: &TokenSession) {
    let result = lifetime
        .reauthentication
        .as_ref()
        .expect("a different-principal reauthentication was attempted");
    let error = result
        .as_ref()
        .expect_err("re-auth with different principal must fail");
    assert!(
        error.to_string().contains("error_code=58"),
        "expected SASL_AUTHENTICATION_FAILED (58); got {error}"
    );
}

#[when("the authenticated bearer client handshakes as SCRAM-SHA-512")]
async fn switch_bearer_mechanism(lifetime: &mut TokenSession) {
    let mut body = BytesMut::new();
    sasl_handshake_request::SaslHandshakeRequest {
        mechanism: "SCRAM-SHA-512".to_owned(),
        ..Default::default()
    }
    .encode(&mut body, 1)
    .expect("SaslHandshake encode must succeed");
    let response = round_trip(
        lifetime
            .stream
            .as_mut()
            .expect("an authenticated bearer connection"),
        SocketExchangeSetup {
            api_key: WireApiKey(17),
            version: WireApiVersion(1),
            correlation: WireCorrelationId(200),
            ..Default::default()
        },
        &body,
    )
    .await
    .expect("handshake round-trip");
    lifetime.handshake = Some(
        sasl_handshake_response::SaslHandshakeResponse::decode(&mut &response[..], 1)
            .expect("SaslHandshake decode must succeed"),
    );
    lifetime.handshake_body = body.to_vec();
}

#[then("the mechanism-switch handshake succeeds and lists both enabled mechanisms")]
fn recognized_but_changed_mechanism(lifetime: &TokenSession) {
    assert!(
        lifetime.handshake.as_ref()
            == Some(&sasl_handshake_response::SaslHandshakeResponse {
                error_code: 0,
                mechanisms: vec!["OAUTHBEARER".to_owned(), "SCRAM-SHA-512".to_owned()],
                ..Default::default()
            })
    );
}

#[when("another frame follows the bearer mechanism switch")]
async fn frame_after_mechanism_switch(lifetime: &mut TokenSession) {
    lifetime.next_frame = Some(
        round_trip(
            lifetime
                .stream
                .as_mut()
                .expect("a mechanism-switch connection"),
            SocketExchangeSetup {
                api_key: WireApiKey(17),
                version: WireApiVersion(1),
                correlation: WireCorrelationId(201),
                ..Default::default()
            },
            &lifetime.handshake_body,
        )
        .await,
    );
}

#[then("the frame after the mechanism switch receives no response")]
fn changed_mechanism_closes(lifetime: &TokenSession) {
    assert!(
        lifetime
            .next_frame
            .as_ref()
            .expect("a frame followed the mechanism switch")
            .is_err(),
        "the connection must close on the frame after a mechanism switch"
    );
}

#[given("a PLAIN listener has neither idle expiry nor a reauthentication cap")]
async fn untimed_plain_listener(lifetime: &mut TokenSession) {
    let directory = tempfile::tempdir().unwrap();
    let mut config = sasl_plaintext_config(directory.path().to_path_buf());
    config.connections_max_idle = Some(krabka_units::millis(0));
    config.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    config
        .plain_credentials
        .insert("alice".to_owned(), alice_password());
    let broker_stream = start_broker_stream(config).await;
    lifetime.broker = Some(broker_stream.broker);
    lifetime.directory = Some(directory);
    lifetime.stream = Some(broker_stream.stream);
    lifetime.metadata_correlation = 5;
}

#[when("alice authenticates with PLAIN and reads the session lifetime")]
async fn untimed_plain_authentication(lifetime: &mut TokenSession) {
    let stream = lifetime.stream.as_mut().expect("a PLAIN session socket");
    let _: api_versions_response::ApiVersionsResponse = exchange()
        .stream(stream)
        .request(&api_versions_request::ApiVersionsRequest::default())
        .client_id("krabka-sasl-test")
        .call()
        .await
        .expect("ApiVersions round-trip");
    let mut handshake = BytesMut::new();
    sasl_handshake_request::SaslHandshakeRequest {
        mechanism: "PLAIN".to_owned(),
        ..Default::default()
    }
    .encode(&mut handshake, 1)
    .unwrap();
    let response = round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(17),
            version: WireApiVersion(1),
            correlation: WireCorrelationId(2),
            ..Default::default()
        },
        &handshake,
    )
    .await
    .expect("SaslHandshake round-trip");
    lifetime.handshake = Some(
        sasl_handshake_response::SaslHandshakeResponse::decode(&mut &response[..], 1).unwrap(),
    );
    let token = plain_authentication_body("alice", alice_password().as_bytes());
    let response = sasl_authenticate_round_trip(stream, &token)
        .await
        .expect("SaslAuthenticate round-trip");
    lifetime.plain_authentication = Some(
        sasl_authenticate_response::SaslAuthenticateResponse::decode(&mut &response[..], 2)
            .unwrap(),
    );
}

#[then("PLAIN handshake and authentication succeed with session lifetime zero")]
fn no_plain_deadline(lifetime: &TokenSession) {
    assert!(
        lifetime
            .handshake
            .as_ref()
            .expect("a PLAIN handshake response")
            .error_code
            == 0,
        "PLAIN handshake must succeed"
    );
    let response = lifetime
        .plain_authentication
        .as_ref()
        .expect("a PLAIN authentication response");
    assert!(response.error_code == 0, "PLAIN authenticate must succeed");
    assert!(
        response.session_lifetime_ms == 0,
        "PLAIN listener must report session_lifetime_ms = 0 (no KIP-368 deadline)"
    );
}

#[when("the Tokio clock advances by one hour")]
async fn advance_plain_clock() {
    time::pause();
    time::advance(Duration::from_hours(1)).await;
    time::resume();
}
