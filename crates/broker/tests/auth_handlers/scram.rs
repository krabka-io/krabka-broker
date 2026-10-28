//! SASL/SCRAM end-to-end for both SHA-256 and SHA-512: the RFC 5802
//! two-round exchange against a credential provisioned through the
//! controller, and the wrong-password path that closes the connection.

use {
    crate::{
        harness::{
            RetainedBroker, alice_password, metadata_probe, read_after_auth, start_scram_alice,
            wrong_scram_password,
        },
        kafka_wire::{
            ScramAuthenticationSetup, sasl_authenticate_on, sasl_handshake_on,
            sasl_scram_authenticate_on,
        },
        reauth_cases::{Reauthentication, reauthentication},
        support::{
            sasl::{HashIterations, provision_scram, sasl_plaintext_mechanisms},
            wire::CorrelationId,
        },
    },
    assert2::assert,
    bytes::Bytes,
    krabka_broker::{Broker, BrokerHandle},
    krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse,
    krabka_security::SaslMechanism,
    rstest::fixture,
    rstest_bdd_macros::{given, then, when},
    std::{io, net::SocketAddr},
    tempfile::TempDir,
    tokio::net::TcpStream,
};

/// Happy-path drive of a SASL/SCRAM-SHA-512 session.
///
/// The test provisions a credential for "alice" with the shared test
/// password. It goes through the controller directly and not through the
/// public `AlterUserScramCredentials` handler. It then runs the two-round
/// RFC 5802 exchange end-to-end and asserts that the post-auth Metadata
/// request succeeds.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/scram.feature",
    name = "sasl scram sha512 happy path"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha512_happy_path(authentication: ScramAuthentication) {
    authentication
        .broker
        .expect("a SCRAM listener")
        .shutdown()
        .await;
}

/// Negative path: with a wrong password, `SaslAuthenticate` round 2 responds
/// with `error_code = 58`, which is `SASL_AUTHENTICATION_FAILED`, and the
/// broker closes the connection.
///
/// `drive_sasl_scram_session` reports the failure as a non-zero error code on
/// the auth response, or as an EOF when the Metadata read that follows
/// returns no bytes.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/scram.feature",
    name = "sasl scram sha512 wrong password closes connection"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha512_wrong_password_closes_connection(authentication: ScramAuthentication) {
    authentication
        .broker
        .expect("a SCRAM listener")
        .shutdown()
        .await;
}

/// SASL/SCRAM-SHA-256 happy path.
///
/// The test is a copy of the SHA-512 test, but it provisions a SHA-256
/// credential and configures the listener to enable only SHA-256. This proves
/// that the new mechanism is wired end-to-end, and that it does not use the
/// SHA-512 code by accident.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/scram.feature",
    name = "sasl scram sha256 happy path"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha256_happy_path(authentication: ScramAuthentication) {
    authentication
        .broker
        .expect("a SCRAM listener")
        .shutdown()
        .await;
}

/// Negative path for SHA-256: a wrong password must close the connection,
/// the same as in the SHA-512 variant.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/scram.feature",
    name = "sasl scram sha256 wrong password closes connection"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha256_wrong_password_closes_connection(authentication: ScramAuthentication) {
    authentication
        .broker
        .expect("a SCRAM listener")
        .shutdown()
        .await;
}

/// Kafka's `ScramSaslServer` refuses a stored credential stretched fewer than
/// 4096 times in round 1, with the generic invalid-credentials text, and
/// accepts one at 4096. Such a credential reaches the image only through a
/// bootstrap record, never through `AlterUserScramCredentials`.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/scram.feature",
    name = "scram credential below 4096 iterations is refused in round one"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_credential_below_4096_iterations_is_refused_in_round_one(
    authentication: ScramAuthentication,
    mechanism: String,
    iterations: u32,
    expectation: String,
) {
    let _ = (mechanism, iterations, expectation);
    authentication
        .broker
        .expect("a SCRAM listener")
        .shutdown()
        .await;
}

/// Drive a complete SASL/SCRAM session against a `SASL_PLAINTEXT` listener.
///
/// This helper works for both SHA-256 and SHA-512. It passes the mechanism
/// through to the handshake and to the client state machine.
///
/// On success it returns `Ok(())` after a successful post-auth Metadata
/// round-trip. It returns `Err` when any step fails: a non-zero error code on
/// either of the two `SaslAuthenticate` rounds, a server-final signature
/// mismatch in the client-side proof, or EOF before Metadata returns.
pub async fn drive_sasl_scram_session(
    addr: SocketAddr,
    user: &str,
    password: &str,
    mechanism: krabka_security::SaslMechanism,
) -> Result<(), io::Error> {
    let mut stream = TcpStream::connect(addr).await?;
    drive_sasl_scram_session_on(&mut stream, user, password, mechanism).await
}

async fn drive_sasl_scram_session_on(
    stream: &mut TcpStream,
    user: &str,
    password: &str,
    mechanism: SaslMechanism,
) -> io::Result<()> {
    sasl_scram_authenticate_on(stream, &ScramAuthenticationSetup {
        client_id: "krabka-sasl-test",
        user,
        password: password.into(),
        mechanism,
    })
    .await?;
    metadata_probe(stream, 5).await
}

/// Kafka's `ScramFormatter.xor` throws `IllegalArgumentException` on a
/// client-final proof that does not decode to the hash length, and
/// `Selector` then closes the connection with no `SaslAuthenticate`
/// response. A proof of the right length and the wrong value still gets the
/// usual error-58 answer.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/scram.feature",
    name = "a scram proof of the wrong length closes with no response"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scram_proof_of_the_wrong_length_closes_with_no_response(
    authentication: ScramAuthentication,
    proof: String,
    outcome: String,
) {
    let _ = (proof, outcome);
    authentication
        .broker
        .expect("a SCRAM listener")
        .shutdown()
        .await;
}

/// Runs the two RFC 5802 rounds on `stream` and returns the round-2
/// response, whose `session_lifetime_ms` is the KIP-368 window.
pub async fn scram_authenticate(
    stream: &mut TcpStream,
    corr: &mut CorrelationId,
    setup: &ScramAuthenticationSetup<'_>,
) -> Result<SaslAuthenticateResponse, io::Error> {
    corr.0 += 1;
    sasl_handshake_on()
        .stream(stream)
        .client_id(setup.client_id)
        .correlation(*corr)
        .mechanism(setup.mechanism.wire_name())
        .call()
        .await?;

    let (client_first, client) =
        krabka_macros::scram_client_first!(setup.user, setup.password.as_str(), setup.mechanism)?;
    corr.0 += 1;
    let first = sasl_authenticate_on()
        .stream(stream)
        .auth_bytes(bytes::Bytes::from(client_first))
        .client_id(setup.client_id)
        .correlation(*corr)
        .call()
        .await?;
    if first.error_code != 0 {
        return Ok(first);
    }

    let (client_final, _client) = client
        .step(&first.auth_bytes)
        .map_err(|e| io::Error::other(format!("scram client step: {e:?}")))?;
    corr.0 += 1;
    sasl_authenticate_on()
        .stream(stream)
        .auth_bytes(bytes::Bytes::from(client_final))
        .client_id(setup.client_id)
        .correlation(*corr)
        .call()
        .await
}

/// KIP-368: a SCRAM session under `connections.max.reauth.ms` reports the
/// window as `session_lifetime_ms`, and a data-plane request that arrives after
/// it elapses without an in-band re-authentication closes the connection.
///
/// The close happens on that request and not on a timer, which is where Kafka
/// puts it (`Processor.processCompletedReceives`): a connection that has fallen
/// silent is reclaimed by `connections.max.idle.ms`, not by this window.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/scram.feature",
    name = "scram session capped by connections max reauth then closes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_session_capped_by_connections_max_reauth_then_closes(
    reauthentication: Reauthentication,
) {
    reauthentication.shutdown().await;
}

/// KIP-368: a SCRAM re-auth for the same principal runs both rounds in the
/// `Reauthenticating` state and re-arms the window.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/scram.feature",
    name = "scram in band reauth same principal reopens data plane"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_in_band_reauth_same_principal_reopens_data_plane(
    reauthentication: Reauthentication,
) {
    reauthentication.shutdown().await;
}

/// KIP-368 forbids a principal switch mid-connection: a SCRAM re-auth as a
/// different user answers `SASL_AUTHENTICATION_FAILED` (58) and closes.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/scram.feature",
    name = "scram in band reauth with different principal closes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_in_band_reauth_with_different_principal_closes(reauthentication: Reauthentication) {
    reauthentication.shutdown().await;
}

/// KIP-368: a connection re-authenticates round after round for the whole life
/// of the connection, and it may begin each round *after* its window has
/// already elapsed.
///
/// That lateness is not an edge case. A JVM client picks its re-auth point at
/// 85–95% of the advertised `session_lifetime_ms`, but acts on it only when it
/// next has a request to write, so a producer on its own send cadence opens the
/// exchange past the deadline as a matter of course. Kafka serves it: the
/// broker runs no timer, and an expired session is closed only by a request
/// that is not part of a re-authentication.
///
/// The window here is short and the test sleeps past it before every round, so
/// each of the three rounds starts expired. A broker that closed on the deadline
/// itself fails on the first one.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/scram.feature",
    name = "scram reauth after the window elapsed keeps serving round after round"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_reauth_after_the_window_elapsed_keeps_serving_round_after_round(
    reauthentication: Reauthentication,
) {
    reauthentication.shutdown().await;
}

async fn scram_fixture(mechanism: SaslMechanism) -> RetainedBroker {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_scram_alice(log_dir.path().to_path_buf(), mechanism).await;
    RetainedBroker { log_dir, handle }
}

struct ScramAuthentication {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    mechanism: Option<SaslMechanism>,
    password: String,
    stream: Option<TcpStream>,
    result: Option<io::Result<()>>,
    response: Option<io::Result<SaslAuthenticateResponse>>,
    final_without_proof: Option<String>,
    bytes_read: Option<usize>,
}

#[fixture]
fn authentication() -> ScramAuthentication {
    // Start this scenario with fresh inputs and observations.

    ScramAuthentication {
        broker: None,
        directory: None,
        mechanism: None,
        password: String::new(),
        stream: None,
        result: None,
        response: None,
        final_without_proof: None,
        bytes_read: None,
    }
}

fn scram_mechanism(name: &str) -> SaslMechanism {
    match name {
        "SCRAM-SHA-256" => SaslMechanism::ScramSha256,
        "SCRAM-SHA-512" => SaslMechanism::ScramSha512,
        other => panic!("unsupported SCRAM fixture mechanism {other}"),
    }
}

#[given("a {mechanism} listener with alice's SCRAM credential")]
async fn scram_listener(authentication: &mut ScramAuthentication, mechanism: String) {
    let mechanism = scram_mechanism(&mechanism);
    let RetainedBroker { log_dir, handle } = scram_fixture(mechanism).await;
    authentication.directory = Some(log_dir);
    authentication.broker = Some(handle);
    authentication.mechanism = Some(mechanism);
}

#[given("alice supplies the configured SCRAM password")]
fn correct_scram_password(authentication: &mut ScramAuthentication) {
    authentication.password = alice_password();
}

#[given("alice supplies a wrong SCRAM password")]
fn wrong_scram_password_given(authentication: &mut ScramAuthentication) {
    authentication.password = wrong_scram_password();
}

#[when(
    "alice opens a fresh SCRAM connection and requests Metadata after verifying the server signature"
)]
async fn complete_scram_session(authentication: &mut ScramAuthentication) {
    let addr = authentication
        .broker
        .as_ref()
        .expect("a SCRAM listener")
        .listen_addr();
    let mechanism = authentication
        .mechanism
        .expect("the enabled SCRAM mechanism");
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("connect to the SCRAM listener");
    authentication.result = Some(
        drive_sasl_scram_session_on(&mut stream, "alice", &authentication.password, mechanism)
            .await,
    );
    authentication.stream = Some(stream);
}

#[then("the SCRAM server signature verifies and Metadata succeeds")]
fn successful_scram_session(authentication: &ScramAuthentication) {
    authentication
        .result
        .as_ref()
        .expect("a SCRAM session was attempted")
        .as_ref()
        .expect("SASL/SCRAM session and server signature must succeed end-to-end");
}

#[then("SCRAM credentials are rejected with SASL_AUTHENTICATION_FAILED")]
fn refused_scram_session(authentication: &ScramAuthentication) {
    let result = authentication
        .result
        .as_ref()
        .expect("a SCRAM session was attempted");
    assert!(
        result.is_err(),
        "wrong password must fail SCRAM session: {result:?}"
    );
    let error = result
        .as_ref()
        .expect_err("wrong credentials must be rejected");
    assert!(
        error.to_string().contains("error_code=58"),
        "the broker must reject credentials, not fail unrelated I/O: {error}"
    );
}

#[when("the refused SCRAM client reads the connection")]
async fn read_refused_scram(authentication: &mut ScramAuthentication) {
    authentication.bytes_read = Some(
        read_after_auth(
            authentication
                .stream
                .as_mut()
                .expect("an attempted SCRAM connection"),
        )
        .await,
    );
}

#[then("the refused SCRAM connection is closed at EOF")]
fn refused_scram_closed(authentication: &ScramAuthentication) {
    assert!(
        authentication.bytes_read == Some(0),
        "the broker must close after failed SCRAM authentication"
    );
}

#[given("alice has a {mechanism} credential stretched with {iterations} iterations")]
async fn stored_iterations(
    authentication: &mut ScramAuthentication,
    mechanism: String,
    iterations: u32,
) {
    let mechanism = scram_mechanism(&mechanism);
    let directory = tempfile::tempdir().unwrap();
    let config = sasl_plaintext_mechanisms(directory.path().to_path_buf(), vec![mechanism]);
    let broker = Broker::start(config).await.expect("broker must start");
    provision_scram()
        .broker(&broker)
        .mechanism(mechanism)
        .iterations(HashIterations(iterations))
        .call()
        .await;
    authentication.broker = Some(broker);
    authentication.directory = Some(directory);
    authentication.mechanism = Some(mechanism);
    authentication.password = alice_password();
}

#[when("alice performs the SCRAM authentication rounds")]
async fn authenticate_stored_credential(authentication: &mut ScramAuthentication) {
    let addr = authentication
        .broker
        .as_ref()
        .expect("a stored SCRAM credential")
        .listen_addr();
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    authentication.response = Some(
        scram_authenticate(
            &mut stream,
            &mut CorrelationId(0),
            &ScramAuthenticationSetup {
                client_id: "krabka-sasl-test",
                user: "alice",
                password: authentication.password.clone(),
                mechanism: authentication
                    .mechanism
                    .expect("the stored credential mechanism"),
            },
        )
        .await,
    );
    authentication.stream = Some(stream);
}

#[then("SCRAM credential validation reports {expectation}")]
fn credential_iteration_result(authentication: &ScramAuthentication, expectation: String) {
    let response = authentication
        .response
        .as_ref()
        .expect("SCRAM authentication was attempted")
        .as_ref()
        .expect("SCRAM rounds complete");
    match expectation.as_str() {
        "accepted" => assert!(response.error_code == 0),
        "invalid-credentials" => {
            let mechanism = authentication
                .mechanism
                .expect("the stored credential mechanism")
                .wire_name();
            assert!(
                response
                    == &SaslAuthenticateResponse {
                        error_code: 58,
                        error_message: Some(format!(
                            "Authentication failed during authentication due to invalid credentials with SASL mechanism {mechanism}"
                        )),
                        ..Default::default()
                    }
            );
        }
        other => panic!("unknown SCRAM credential expectation {other}"),
    }
}

#[given("alice has completed a valid SHA-256 SCRAM first round")]
async fn prepared_scram_final(authentication: &mut ScramAuthentication) {
    scram_listener(authentication, "SCRAM-SHA-256".into()).await;
    let addr = authentication
        .broker
        .as_ref()
        .expect("a SHA-256 SCRAM listener")
        .listen_addr();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    sasl_handshake_on()
        .stream(&mut stream)
        .correlation(1)
        .mechanism(SaslMechanism::ScramSha256.wire_name())
        .call()
        .await
        .unwrap();
    let (client_first, client) = krabka_macros::scram_client_first!(
        "alice",
        alice_password().as_str(),
        SaslMechanism::ScramSha256
    )
    .unwrap();
    let first = sasl_authenticate_on()
        .stream(&mut stream)
        .auth_bytes(Bytes::from(client_first))
        .correlation(2)
        .call()
        .await
        .unwrap();
    let (client_final, _client) = client.step(&first.auth_bytes).unwrap();
    let client_final = String::from_utf8(client_final).unwrap();
    let (without_proof, _) = client_final.rsplit_once(",p=").unwrap();
    authentication.final_without_proof = Some(without_proof.to_owned());
    authentication.stream = Some(stream);
}

#[when("a SCRAM client-final message with proof {proof} is submitted")]
async fn malformed_scram_final(authentication: &mut ScramAuthentication, proof: String) {
    let proof = match proof.as_str() {
        "empty" => String::new(),
        "30-bytes" => "A".repeat(40),
        "32-bytes-wrong" => format!("{}=", "A".repeat(43)),
        other => panic!("unknown SCRAM proof fixture {other}"),
    };
    let prefix = authentication
        .final_without_proof
        .as_ref()
        .expect("a prepared SCRAM client-final message");
    authentication.response = Some(
        sasl_authenticate_on()
            .stream(
                authentication
                    .stream
                    .as_mut()
                    .expect("an active SCRAM exchange"),
            )
            .auth_bytes(Bytes::from(format!("{prefix},p={proof}")))
            .correlation(3)
            .call()
            .await,
    );
}

#[then("the SCRAM proof response is {outcome}")]
fn scram_proof_result(authentication: &ScramAuthentication, outcome: String) {
    let result = authentication
        .response
        .as_ref()
        .expect("a SCRAM final proof was submitted");
    let error = result.as_ref().map(|response| response.error_code);
    match outcome.as_str() {
        "absent" => assert!(
            error.is_err(),
            "a wrong-length proof must close without a response: {result:?}"
        ),
        "SASL_AUTHENTICATION_FAILED" => assert!(
            error.ok() == Some(58),
            "a wrong-value proof must be refused: {result:?}"
        ),
        other => panic!("unknown SCRAM proof outcome {other}"),
    }
}
