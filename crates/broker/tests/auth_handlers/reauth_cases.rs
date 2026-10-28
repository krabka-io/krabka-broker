//! KIP-368 scenarios shared by the PLAIN and SCRAM wire drivers.

use {
    crate::{
        harness,
        harness::common_imports::*,
        kafka_wire::{
            PlainAuthenticationSetup, ScramAuthenticationSetup, exchange, plain_payload,
            sasl_authenticate_on,
        },
        plain, scram,
        support::wire::{
            CorrelationId as WireCorrelationId, HeaderEncoding as RequestHeaderEncoding,
        },
    },
    krabka_protocol::owned::{
        metadata_request::MetadataRequest, metadata_response::MetadataResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::{io, time::Duration},
    tokio::time,
};

pub struct Session {
    broker: krabka_broker::BrokerHandle,
    stream: TcpStream,
    correlation: WireCorrelationId,
    mechanism: SaslMechanism,
    _dir: tempfile::TempDir,
}

impl Session {
    async fn start(mechanism: SaslMechanism, max_reauth: krabka_units::Time) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let broker = match mechanism {
            SaslMechanism::Plain | SaslMechanism::ScramSha512 => {
                harness::start_reauth_broker(dir.path(), max_reauth, mechanism).await
            }
            _ => panic!("unsupported fixture mechanism"),
        };
        let stream = TcpStream::connect(broker.listen_addr()).await.unwrap();
        Self {
            broker,
            stream,
            correlation: WireCorrelationId(0),
            mechanism,
            _dir: dir,
        }
    }

    async fn authenticate(&mut self, user: &str) -> io::Result<SaslAuthenticateResponse> {
        match self.mechanism {
            SaslMechanism::Plain => {
                plain::plain_authenticate(
                    &mut self.stream,
                    &mut self.correlation,
                    &PlainAuthenticationSetup {
                        client_id: "krabka-sasl-test",
                        user,
                        ..Default::default()
                    },
                )
                .await
            }
            SaslMechanism::ScramSha512 => {
                scram::scram_authenticate(
                    &mut self.stream,
                    &mut self.correlation,
                    &ScramAuthenticationSetup {
                        client_id: "krabka-sasl-test",
                        user,
                        mechanism: self.mechanism,
                        ..Default::default()
                    },
                )
                .await
            }
            _ => panic!("unsupported fixture mechanism"),
        }
    }

    /// One `SaslHandshake` v1 for this session's mechanism.
    async fn handshake(&mut self) -> io::Result<SaslHandshakeResponse> {
        self.correlation.0 += 1;
        exchange()
            .stream(&mut self.stream)
            .request(&SaslHandshakeRequest {
                mechanism: self.mechanism.wire_name().to_owned(),
                ..Default::default()
            })
            .api_key(17)
            .version(1)
            .correlation(self.correlation.0)
            .client_id("krabka-sasl-test")
            .call()
            .await
    }

    /// One PLAIN `SaslAuthenticate` v2 with no handshake before it.
    async fn bare_authenticate(&mut self, user: &str) -> io::Result<SaslAuthenticateResponse> {
        self.correlation.0 += 1;
        sasl_authenticate_on()
            .stream(&mut self.stream)
            .auth_bytes(plain_payload(user, harness::alice_password().as_bytes()))
            .client_id("krabka-sasl-test")
            .correlation(self.correlation.0)
            .call()
            .await
    }

    async fn metadata(&mut self) -> io::Result<MetadataResponse> {
        self.correlation.0 += 1;
        exchange()
            .stream(&mut self.stream)
            .request(&MetadataRequest::default())
            .api_key(3)
            .version(12)
            .correlation(self.correlation.0)
            .client_id("krabka-sasl-test")
            .header(RequestHeaderEncoding::Flexible)
            .call()
            .await
    }
}

struct AuthenticationRound {
    response: io::Result<SaslAuthenticateResponse>,
    metadata: io::Result<MetadataResponse>,
}

pub struct Reauthentication {
    session: Option<Session>,
    response: Option<io::Result<SaslAuthenticateResponse>>,
    handshake: Option<io::Result<SaslHandshakeResponse>>,
    metadata: Option<io::Result<MetadataResponse>>,
    bytes_read: Option<usize>,
    rounds: Vec<AuthenticationRound>,
}

impl Reauthentication {
    fn session(&mut self) -> &mut Session {
        self.session.as_mut().expect("a SASL session was opened")
    }

    pub async fn shutdown(self) {
        self.session
            .expect("a SASL session was opened")
            .broker
            .shutdown()
            .await;
    }
}

#[fixture]
pub fn reauthentication() -> Reauthentication {
    // Start this scenario with fresh inputs and observations.

    Reauthentication {
        session: None,
        response: None,
        handshake: None,
        metadata: None,
        bytes_read: None,
        rounds: Vec::new(),
    }
}

fn mechanism(name: &str) -> SaslMechanism {
    match name {
        "PLAIN" => SaslMechanism::Plain,
        "SCRAM-SHA-512" => SaslMechanism::ScramSha512,
        other => panic!("unsupported reauthentication fixture mechanism {other}"),
    }
}

#[given("a {name} listener with a {window} millisecond reauthentication window")]
async fn bounded_listener(reauthentication: &mut Reauthentication, name: String, window: u32) {
    reauthentication.session =
        Some(Session::start(mechanism(&name), krabka_units::millis(window)).await);
}

#[given("alice is already authenticated using {name} with a {window} millisecond window")]
async fn established_session(reauthentication: &mut Reauthentication, name: String, window: u32) {
    bounded_listener(reauthentication, name, window).await;
    let response = reauthentication
        .session()
        .authenticate("alice")
        .await
        .expect("initial authenticate");
    check!(
        response.error_code == 0,
        "the scenario requires an authenticated alice session"
    );
}

#[when("alice authenticates on the connection")]
async fn authenticate_alice(reauthentication: &mut Reauthentication) {
    reauthentication.response = Some(reauthentication.session().authenticate("alice").await);
}

#[when("the connection reauthenticates as {user}")]
async fn authenticate_principal(reauthentication: &mut Reauthentication, user: String) {
    reauthentication.response = Some(reauthentication.session().authenticate(&user).await);
}

#[when("{delay} milliseconds pass before alice reauthenticates")]
async fn delayed_authentication(reauthentication: &mut Reauthentication, delay: u64) {
    time::sleep(Duration::from_millis(delay)).await;
    authenticate_alice(reauthentication).await;
}

#[when("{delay} milliseconds pass before the client requests Metadata")]
async fn expired_metadata(reauthentication: &mut Reauthentication, delay: u64) {
    time::sleep(Duration::from_millis(delay)).await;
    request_metadata(reauthentication).await;
}

#[when("the client requests Metadata on the same connection")]
async fn request_metadata(reauthentication: &mut Reauthentication) {
    reauthentication.metadata = Some(reauthentication.session().metadata().await);
}

#[when("{delay} milliseconds pass before a bare SaslAuthenticate for alice")]
async fn delayed_bare_authentication(reauthentication: &mut Reauthentication, delay: u64) {
    time::sleep(Duration::from_millis(delay)).await;
    reauthentication.response = Some(reauthentication.session().bare_authenticate("alice").await);
}

#[when("{delay} milliseconds pass before another SaslHandshake")]
async fn delayed_handshake(reauthentication: &mut Reauthentication, delay: u64) {
    time::sleep(Duration::from_millis(delay)).await;
    reauthentication.handshake = Some(reauthentication.session().handshake().await);
}

#[when("the client reads the remainder of the connection")]
async fn read_connection(reauthentication: &mut Reauthentication) {
    reauthentication.bytes_read =
        Some(harness::read_after_auth(&mut reauthentication.session().stream).await);
}

#[when("alice completes 3 expired reauthentication rounds 1050 milliseconds apart")]
async fn repeated_reauthentication(reauthentication: &mut Reauthentication) {
    for _ in 0..3 {
        time::sleep(Duration::from_millis(1_050)).await;
        let response = reauthentication.session().authenticate("alice").await;
        let metadata = reauthentication.session().metadata().await;
        reauthentication
            .rounds
            .push(AuthenticationRound { response, metadata });
    }
}

fn authentication_response(reauthentication: &Reauthentication) -> &SaslAuthenticateResponse {
    reauthentication
        .response
        .as_ref()
        .expect("authentication was attempted")
        .as_ref()
        .expect("authentication must receive a response")
}

#[then("authentication succeeds")]
fn successful_authentication(reauthentication: &Reauthentication) {
    check!(authentication_response(reauthentication).error_code == 0);
}

#[then("the session lifetime is between {minimum} and {maximum} milliseconds")]
fn bounded_lifetime(reauthentication: &Reauthentication, minimum: i64, maximum: i64) {
    let response = authentication_response(reauthentication);
    check!(
        (minimum..=maximum).contains(&response.session_lifetime_ms),
        "session lifetime must be within {minimum}..={maximum}, got {}",
        response.session_lifetime_ms
    );
}

#[then("Metadata succeeds and names at least one broker")]
fn reachable_metadata(reauthentication: &Reauthentication) {
    let response = reauthentication
        .metadata
        .as_ref()
        .expect("Metadata was requested")
        .as_ref()
        .expect("Metadata RPC after in-band re-auth");
    check!(!response.brokers.is_empty());
}

#[then("changing from alice to bob is rejected with SASL_AUTHENTICATION_FAILED")]
fn principal_switch_rejected(reauthentication: &Reauthentication) {
    check!(
        authentication_response(reauthentication)
            == &SaslAuthenticateResponse {
                error_code: 58,
                error_message: Some(
                    "Cannot change principals during re-authentication from User.alice: User.bob"
                        .to_owned()
                ),
                ..Default::default()
            }
    );
}

#[then("the expired bare SaslAuthenticate receives no response")]
fn bare_authentication_closed(reauthentication: &Reauthentication) {
    let response = reauthentication
        .response
        .as_ref()
        .expect("a bare authenticate was attempted");
    check!(
        response.is_err(),
        "an expired session must close with no response, got {response:?}"
    );
}

#[then("the expired SaslHandshake receives no response")]
fn handshake_closed(reauthentication: &Reauthentication) {
    let response = reauthentication
        .handshake
        .as_ref()
        .expect("a handshake was attempted");
    check!(
        response.is_err(),
        "an expired session must close with no response, got {response:?}"
    );
}

#[then("the connection reaches EOF without any remaining bytes")]
fn closed_connection(reauthentication: &Reauthentication) {
    check!(
        reauthentication.bytes_read == Some(0),
        "expected EOF, got {:?} bytes",
        reauthentication.bytes_read
    );
}

#[then(
    "all 3 rounds succeed with lifetimes between {minimum} and {maximum} milliseconds and reachable Metadata"
)]
fn successful_rounds(reauthentication: &Reauthentication, minimum: i64, maximum: i64) {
    check!(reauthentication.rounds.len() == 3);
    for (index, round) in reauthentication.rounds.iter().enumerate() {
        let index = index + 1;
        let response = round
            .response
            .as_ref()
            .unwrap_or_else(|error| panic!("re-auth round {index} must round-trip: {error}"));
        check!(
            response.error_code == 0,
            "re-auth round {index} must succeed, got {} {:?}",
            response.error_code,
            response.error_message
        );
        check!(
            (minimum..=maximum).contains(&response.session_lifetime_ms),
            "re-auth round {index} must re-arm the window, got {}",
            response.session_lifetime_ms
        );
        let metadata = round
            .metadata
            .as_ref()
            .unwrap_or_else(|error| panic!("Metadata after re-auth round {index}: {error}"));
        check!(!metadata.brokers.is_empty(), "round {index}");
    }
}

#[scenario(
    path = "tests/features/migrated/tests/auth_handlers/reauth_cases.feature",
    name = "plain expired session closes on a bare sasl authenticate"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_expired_session_closes_on_a_bare_sasl_authenticate(
    reauthentication: Reauthentication,
) {
    reauthentication.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/auth_handlers/reauth_cases.feature",
    name = "plain expired session closes on a handshake inside the reauth interval"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_expired_session_closes_on_a_handshake_inside_the_reauth_interval(
    reauthentication: Reauthentication,
) {
    reauthentication.shutdown().await;
}
