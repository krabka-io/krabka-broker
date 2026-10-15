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
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
    std::ops::RangeInclusive,
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

    async fn authenticate(&mut self, user: &str) -> std::io::Result<SaslAuthenticateResponse> {
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
    async fn handshake(&mut self) -> std::io::Result<SaslHandshakeResponse> {
        self.correlation.0 += 1;
        exchange()
            .stream(&mut self.stream)
            .request(&SaslHandshakeRequest {
                mechanism: self.mechanism.wire_name().to_string(),
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
    async fn bare_authenticate(&mut self, user: &str) -> std::io::Result<SaslAuthenticateResponse> {
        self.correlation.0 += 1;
        sasl_authenticate_on()
            .stream(&mut self.stream)
            .auth_bytes(plain_payload(user, harness::alice_password().as_bytes()))
            .client_id("krabka-sasl-test")
            .correlation(self.correlation.0)
            .call()
            .await
    }

    async fn metadata(
        &mut self,
    ) -> std::io::Result<krabka_protocol::owned::metadata_response::MetadataResponse> {
        self.correlation.0 += 1;
        exchange()
            .stream(&mut self.stream)
            .request(&krabka_protocol::owned::metadata_request::MetadataRequest::default())
            .api_key(3)
            .version(12)
            .correlation(self.correlation.0)
            .client_id("krabka-sasl-test")
            .header(RequestHeaderEncoding::Flexible)
            .call()
            .await
    }
}

pub async fn capped_session_closes(mechanism: SaslMechanism) {
    let mut session = Session::start(mechanism, krabka_units::millis(300)).await;
    let response = session
        .authenticate("alice")
        .await
        .expect("authenticate round-trip");
    check!(response.error_code == 0);
    check!(
        (200..=300).contains(&response.session_lifetime_ms),
        "session_lifetime_ms = {}, expected the 300 ms cap",
        response.session_lifetime_ms
    );
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    // A data-plane request, rather than a timer, closes the expired session.
    let _ = session.metadata().await;
    let n = harness::read_after_auth(&mut session.stream).await;
    check!(
        n == 0,
        "expected EOF after the re-auth window, got {n} bytes"
    );
    session.broker.shutdown().await;
}

/// KIP-368: a `SaslAuthenticate` that arrives on an expired session with no
/// handshake before it closes the connection with no response, as Kafka's
/// `Processor.processCompletedReceives` closes on any frame past the deadline
/// that does not begin a re-authentication.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_expired_session_closes_on_a_bare_sasl_authenticate() {
    let mut session = Session::start(SaslMechanism::Plain, krabka_units::millis(300)).await;
    let initial = session.authenticate("alice").await.expect("authenticate");
    check!(initial.error_code == 0);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let response = session.bare_authenticate("alice").await;
    check!(
        response.is_err(),
        "an expired session must close with no response, got {response:?}"
    );
    check!(harness::read_after_auth(&mut session.stream).await == 0);
    session.broker.shutdown().await;
}

/// KIP-368: a `SaslHandshake` on an expired session that Kafka's one-second
/// re-authentication interval refuses does not begin a re-authentication, so
/// it closes the connection with no response rather than answering
/// `ILLEGAL_SASL_STATE`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_expired_session_closes_on_a_handshake_inside_the_reauth_interval() {
    let mut session = Session::start(SaslMechanism::Plain, krabka_units::millis(300)).await;
    let initial = session.authenticate("alice").await.expect("authenticate");
    check!(initial.error_code == 0);
    // Past the first window, so this re-authentication starts expired and
    // records its start.
    tokio::time::sleep(std::time::Duration::from_millis(1_050)).await;
    let reauth = session
        .authenticate("alice")
        .await
        .expect("re-authenticate");
    check!(reauth.error_code == 0);
    // Past the renewed 300 ms window, still inside a second of its start.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let response = session.handshake().await;
    check!(
        response.is_err(),
        "an expired session must close with no response, got {response:?}"
    );
    check!(harness::read_after_auth(&mut session.stream).await == 0);
    session.broker.shutdown().await;
}

pub async fn same_principal_reopens(mechanism: SaslMechanism) {
    let mut session = Session::authenticated(mechanism).await;
    let reauth = session
        .authenticate("alice")
        .await
        .expect("in-band re-auth round-trip");
    check!(reauth.error_code == 0, "in-band re-auth must succeed");
    check!(
        (29_000..=30_000).contains(&reauth.session_lifetime_ms),
        "re-auth must re-arm the window, got {}",
        reauth.session_lifetime_ms
    );
    let response = session
        .metadata()
        .await
        .expect("Metadata RPC after in-band re-auth");
    check!(!response.brokers.is_empty());
    session.broker.shutdown().await;
}

pub async fn different_principal_closes(mechanism: SaslMechanism) {
    let mut session = Session::authenticated(mechanism).await;
    let reauth = session
        .authenticate("bob")
        .await
        .expect("re-auth round-trip completes");
    check!(
        reauth
            == SaslAuthenticateResponse {
                error_code: 58,
                error_message: Some(
                    "Cannot change principals during re-authentication from User.alice: \
                     User.bob"
                        .to_string()
                ),
                ..Default::default()
            }
    );
    let n = harness::read_after_auth(&mut session.stream).await;
    check!(n == 0, "expected EOF after a refused re-auth");
    session.broker.shutdown().await;
}

pub async fn repeated_expired_reauth(
    mechanism: SaslMechanism,
    max_reauth: krabka_units::Time,
    window: RangeInclusive<i64>,
) {
    let mut session = Session::start(mechanism, max_reauth).await;
    let initial = session
        .authenticate("alice")
        .await
        .expect("initial authenticate");
    check!(initial.error_code == 0);
    for round in 1..=3 {
        // Also cross Kafka's minimum one-second reauthentication interval.
        tokio::time::sleep(std::time::Duration::from_millis(1_050)).await;
        let reauth = session
            .authenticate("alice")
            .await
            .unwrap_or_else(|e| panic!("re-auth round {round} must round-trip: {e}"));
        check!(
            reauth.error_code == 0,
            "re-auth round {round} must succeed, got {} {:?}",
            reauth.error_code,
            reauth.error_message
        );
        check!(
            window.contains(&reauth.session_lifetime_ms),
            "re-auth round {round} must re-arm the window, got {}",
            reauth.session_lifetime_ms
        );
        let response = session
            .metadata()
            .await
            .unwrap_or_else(|e| panic!("Metadata after re-auth round {round}: {e}"));
        check!(!response.brokers.is_empty(), "round {round}");
    }
    session.broker.shutdown().await;
}

impl Session {
    async fn authenticated(mechanism: SaslMechanism) -> Self {
        let mut session = Self::start(mechanism, krabka_units::secs(30)).await;
        session
            .authenticate("alice")
            .await
            .expect("initial authenticate");
        session
    }
}
