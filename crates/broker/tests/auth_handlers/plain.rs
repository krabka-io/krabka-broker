//! SASL/PLAIN end-to-end: the happy path, a wrong password that closes the
//! connection, and the per-mechanism authentication counters those two
//! sessions tick on the `/metrics` scrape.

use {
    crate::{
        harness::{
            alice_password, alice_plain_config, common_imports::*, metadata_probe, read_after_auth,
            round_trip, start_alice_plain, start_reauth_broker, wrong_scram_password,
        },
        kafka_wire::{
            PlainAuthenticationSetup, sasl_plain_authenticate_on, sasl_plain_reauthenticate_on,
        },
        reauth_cases::{Reauthentication, reauthentication},
        support::{
            client::scrape_metrics as scrape_metrics_client,
            sasl::plain_authentication_body,
            wire::{CorrelationId, HeaderEncoding as RequestHeaderEncoding, SocketExchangeSetup},
        },
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, then, when},
    std::{io, net::SocketAddr},
    tempfile::TempDir,
};

/// Happy-path drive of a SASL/PLAIN session: `ApiVersions` → `SaslHandshake`
/// → `SaslAuthenticate` → Metadata.
///
/// The test asserts that the connection survives every step and that the
/// final Metadata response carries this broker. The dial side sends raw
/// bytes over `TcpStream` and not `Client`, because `Client` does not speak
/// SASL yet.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/plain.feature",
    name = "sasl plain happy path"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_happy_path(authentication: PlainAuthentication) {
    authentication
        .broker
        .expect("a PLAIN listener")
        .shutdown()
        .await;
}

/// SASL PLAIN metrics: one happy-path session and one wrong-password session
/// tick both the `successful_authentication_total` and the
/// `failed_authentication_total` per-mechanism counters on the `/metrics`
/// scrape.
///
/// The test checks the end-to-end wire path from the `SaslAuthenticate`
/// dispatch site to the rendered Prometheus text.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/plain.feature",
    name = "sasl plain authentication metrics tick for success and failure"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_authentication_metrics_tick_for_success_and_failure(
    authentication: PlainAuthentication,
) {
    authentication
        .broker
        .expect("a PLAIN listener")
        .shutdown()
        .await;
}

/// Send an HTTP GET `/metrics` to `addr` and return the response body.
///
/// The returned body holds no HTTP head.
async fn scrape_metrics(addr: SocketAddr) -> String { scrape_metrics_client(addr).await }

/// Negative path: with a wrong password, `SaslAuthenticate` responds with
/// `error_code = SASL_AUTHENTICATION_FAILED` (58) and the broker closes the
/// connection.
///
/// `drive_sasl_plain_session_on` reports the failure as an `Err` in two cases.
/// The first case is a non-zero `error_code` on the auth response. The second
/// case is an EOF on the Metadata read that follows, when the peer closed the
/// connection.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/plain.feature",
    name = "sasl plain wrong password closes connection"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_wrong_password_closes_connection(authentication: PlainAuthentication) {
    authentication
        .broker
        .expect("a PLAIN listener")
        .shutdown()
        .await;
}

/// Drive a complete SASL/PLAIN session against a `SASL_PLAINTEXT` listener.
///
/// On success, this helper returns `Ok(())` after a successful post-auth
/// Metadata round-trip. It returns `Err` when any step fails: frame I/O,
/// response decode, a non-zero error code on a SASL response, or EOF before
/// Metadata.
///
/// This helper handles these wire-protocol mechanics inline, without the
/// `Client` API:
/// - Request headers: v1, non-flexible, for `ApiVersions v0` and
///   `SaslHandshake v1`. v2, flexible with a trailing `0x00` tagged-fields
///   byte, for `SaslAuthenticate v2` and `Metadata v12`.
/// - Response headers: always v0, which holds only `correlation_id`, for
///   `ApiVersions`, whatever the body flexibility. v1, which holds `corr_id`
///   plus a `0x00` tagged byte, for every other flexible response. v0 for
///   non-flexible.
/// - Length framing: a 4-byte big-endian length prefix on every frame in both
///   directions, the same as `krabka_broker::network::codec`.
async fn drive_sasl_plain_session_on(
    stream: &mut TcpStream,
    user: &str,
    password: &[u8],
) -> Result<(), io::Error> {
    sasl_plain_authenticate_on(stream, &PlainAuthenticationSetup {
        client_id: "krabka-sasl-test",
        user,
        password: (password).to_vec(),
    })
    .await?;
    metadata_probe(stream, 4).await
}

/// Opens a PLAIN session on `stream` and returns the `SaslAuthenticate`
/// response, whose `session_lifetime_ms` is the KIP-368 window. `corr` keeps
/// correlation ids unique across the initial authentication and any later
/// in-band re-auth on the same connection.
pub async fn plain_authenticate(
    stream: &mut TcpStream,
    corr: &mut CorrelationId,
    setup: &PlainAuthenticationSetup<'_>,
) -> Result<SaslAuthenticateResponse, io::Error> {
    sasl_plain_reauthenticate_on(stream, corr, setup).await
}

/// KIP-368: with `connections.max.reauth.ms` set, a PLAIN session reports the
/// window as `session_lifetime_ms`, and a data-plane request that arrives after
/// it elapses without an in-band re-authentication closes the connection.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/plain.feature",
    name = "plain session capped by connections max reauth then closes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_session_capped_by_connections_max_reauth_then_closes(
    reauthentication: Reauthentication,
) {
    reauthentication.shutdown().await;
}

/// KIP-368: an in-band re-authentication with the same principal succeeds and
/// re-opens the data plane on the same connection.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/plain.feature",
    name = "plain in band reauth same principal reopens data plane"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_in_band_reauth_same_principal_reopens_data_plane(
    reauthentication: Reauthentication,
) {
    reauthentication.shutdown().await;
}

/// KIP-368 forbids a principal switch mid-connection: a PLAIN re-auth that
/// authenticates a different user answers `SASL_AUTHENTICATION_FAILED` (58)
/// and the broker closes the connection.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/plain.feature",
    name = "plain in band reauth with different principal closes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_in_band_reauth_with_different_principal_closes(reauthentication: Reauthentication) {
    reauthentication.shutdown().await;
}

/// A `SaslAuthenticate` before any handshake closes the connection with no
/// response, as Kafka's `SaslServerAuthenticator.handleKafkaRequest` does
/// (`InvalidRequestException`), rather than authenticating anyone.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/plain.feature",
    name = "plain authenticate without handshake closes the connection"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_authenticate_without_handshake_closes_the_connection(
    authentication: PlainAuthentication,
) {
    authentication
        .broker
        .expect("a PLAIN listener")
        .shutdown()
        .await;
}

/// KIP-368: a PLAIN connection re-authenticates round after round, and each
/// round may begin after its window has already elapsed.
///
/// A JVM client re-authenticates only when it next has a request to write, so
/// on its own send cadence it opens the exchange past the deadline routinely.
/// The window here is short and the test sleeps past it before every round, so
/// each of the three rounds starts expired.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/plain.feature",
    name = "plain reauth after the window elapsed keeps serving round after round"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_reauth_after_the_window_elapsed_keeps_serving_round_after_round(
    reauthentication: Reauthentication,
) {
    reauthentication.shutdown().await;
}

struct PlainAuthentication {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    password: Vec<u8>,
    stream: Option<TcpStream>,
    result: Option<io::Result<()>>,
    raw_result: Option<io::Result<Vec<u8>>>,
    metrics: Option<String>,
    bytes_read: Option<usize>,
}

#[fixture]
fn authentication() -> PlainAuthentication {
    // Start this scenario with fresh inputs and observations.

    PlainAuthentication {
        broker: None,
        directory: None,
        password: Vec::new(),
        stream: None,
        result: None,
        raw_result: None,
        metrics: None,
        bytes_read: None,
    }
}

#[given("a PLAIN listener provisioned with alice's configured password")]
async fn plain_listener(authentication: &mut PlainAuthentication) {
    let (directory, broker, _addr) = start_alice_plain(alice_password()).await;
    authentication.directory = Some(directory);
    authentication.broker = Some(broker);
}

#[given("the PLAIN listener exports authentication metrics")]
async fn plain_metrics_listener(authentication: &mut PlainAuthentication) {
    let directory = tempfile::tempdir().unwrap();
    let mut config = alice_plain_config(directory.path().to_path_buf(), alice_password());
    config.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());
    authentication.broker = Some(Broker::start(config).await.expect("broker must start"));
    authentication.directory = Some(directory);
}

#[given("a PLAIN listener with a 30 second reauthentication cap")]
async fn plain_reauth_listener(authentication: &mut PlainAuthentication) {
    let directory = tempfile::tempdir().unwrap();
    authentication.broker = Some(
        start_reauth_broker(
            directory.path(),
            krabka_units::secs(30),
            SaslMechanism::Plain,
        )
        .await,
    );
    authentication.directory = Some(directory);
}

#[given("alice supplies the configured PLAIN password")]
fn correct_plain_password(authentication: &mut PlainAuthentication) {
    authentication.password = alice_password().into_bytes();
}

#[given("alice supplies a wrong PLAIN password")]
fn wrong_plain_password(authentication: &mut PlainAuthentication) {
    authentication.password = wrong_scram_password().into_bytes();
}

#[when("alice opens a fresh connection and attempts PLAIN authentication followed by Metadata")]
async fn plain_session(authentication: &mut PlainAuthentication) {
    let addr = authentication
        .broker
        .as_ref()
        .expect("a PLAIN listener")
        .listen_addr();
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("connect to the PLAIN listener");
    authentication.result =
        Some(drive_sasl_plain_session_on(&mut stream, "alice", &authentication.password).await);
    authentication.stream = Some(stream);
}

#[then("PLAIN authentication and Metadata complete successfully")]
fn plain_authenticated(authentication: &PlainAuthentication) {
    authentication
        .result
        .as_ref()
        .expect("a PLAIN session was attempted")
        .as_ref()
        .expect("SASL/PLAIN session must succeed end-to-end");
}

#[then("PLAIN credentials are rejected with SASL_AUTHENTICATION_FAILED")]
fn plain_authentication_failed(authentication: &PlainAuthentication) {
    let result = authentication
        .result
        .as_ref()
        .expect("a PLAIN session was attempted");
    assert!(
        result.is_err(),
        "wrong password must fail the SASL session: {result:?}"
    );
    let error = result
        .as_ref()
        .expect_err("wrong credentials must be rejected");
    assert!(
        error.to_string().contains("error_code=58"),
        "the broker must reject credentials, not fail unrelated I/O: {error}"
    );
}

#[when("the refused PLAIN client reads the connection")]
async fn read_refused_plain_session(authentication: &mut PlainAuthentication) {
    authentication.bytes_read = Some(
        read_after_auth(
            authentication
                .stream
                .as_mut()
                .expect("an attempted PLAIN session"),
        )
        .await,
    );
}

#[then("the refused PLAIN connection is closed at EOF")]
fn plain_connection_closed(authentication: &PlainAuthentication) {
    assert!(
        authentication.bytes_read == Some(0),
        "the broker must close after failed PLAIN authentication"
    );
}

#[when("the PLAIN authentication metrics are scraped")]
async fn plain_metric_samples(authentication: &mut PlainAuthentication) {
    let addr = authentication
        .broker
        .as_ref()
        .expect("a PLAIN listener")
        .metrics_addr()
        .expect("metrics server should be bound");
    authentication.metrics = Some(scrape_metrics(addr).await);
}

#[then("PLAIN successful and failed authentication counters both equal 1")]
fn plain_metric_counts(authentication: &PlainAuthentication) {
    let body = authentication
        .metrics
        .as_ref()
        .expect("authentication metrics were scraped");
    let success = "krabka_broker_successful_authentication_total{mechanism=\"PLAIN\"} 1";
    let failed = "krabka_broker_failed_authentication_total{mechanism=\"PLAIN\"} 1";
    assert!(
        body.contains(success),
        "missing or wrong-value {success} in:\n{body}"
    );
    assert!(
        body.contains(failed),
        "missing or wrong-value {failed} in:\n{body}"
    );
}

#[when("alice sends SaslAuthenticate without any SaslHandshake")]
async fn authenticate_without_handshake(authentication: &mut PlainAuthentication) {
    let addr = authentication
        .broker
        .as_ref()
        .expect("a PLAIN listener")
        .listen_addr();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let auth_body = plain_authentication_body("alice", alice_password().as_bytes());
    authentication.raw_result = Some(
        round_trip(
            &mut stream,
            SocketExchangeSetup {
                api_key: WireApiKey(36),
                version: WireApiVersion(2),
                header: RequestHeaderEncoding::Flexible,
                ..Default::default()
            },
            &auth_body,
        )
        .await,
    );
    authentication.stream = Some(stream);
}

#[then("the pre-handshake SaslAuthenticate receives no response")]
fn authenticate_requires_handshake(authentication: &PlainAuthentication) {
    let result = authentication
        .raw_result
        .as_ref()
        .expect("a bare authentication was attempted");
    check!(
        result.is_err(),
        "a SaslAuthenticate before a handshake must close with no response"
    );
}
