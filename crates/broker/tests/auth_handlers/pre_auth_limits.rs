//! What a `SASL_PLAINTEXT` listener holds a connection to before it finishes
//! authenticating, and what a failed authentication costs it.
//!
//! - `sasl.server.max.receive.size` replaces `socket.request.max.bytes` as the
//!   frame limit for the whole exchange (`SaslServerAuthenticator`'s own
//!   `NetworkReceive`), and the larger limit returns once the peer is
//!   authenticated. An oversize frame fails the authentication.
//! - `connection.failed.authentication.delay.ms` holds a failed
//!   authentication's answer, and the close after it (`Selector`'s
//!   `delayedClosingChannels`).
//! - After one valid `ApiVersions`, `SaslServerAuthenticator` takes only a
//!   `SaslHandshake`: a second `ApiVersions` closes the connection, and an
//!   `UNSUPPORTED_VERSION` or `INVALID_REQUEST` answer does not count as the
//!   first.
//! - `SaslServerAuthenticator` answers that first `ApiVersions` without the
//!   KIP-1242 routing check, which only `KafkaApis` runs.

use {
    crate::{
        harness::{admin_plain_password, round_trip, wrong_scram_password},
        support::{
            client::http_get,
            discovery::{api_versions_request_for, topic_metadata_request},
            sasl::sasl_temp_config,
            topics::metadata_topic,
            wire::{
                CorrelationId as WireCorrelationId, HeaderEncoding as RequestHeaderEncoding,
                SocketExchangeSetup, request_frame as support_request_frame,
            },
        },
    },
    assert2::{assert, check},
    bytes::{BufMut, BytesMut},
    krabka_broker::{Broker, BrokerConfig, BrokerHandle, api_catalog::UnstableApiVersions},
    krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion},
    krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request, api_versions_response, metadata_request, metadata_response,
            sasl_authenticate_request, sasl_authenticate_response, sasl_handshake_request,
            sasl_handshake_response,
        },
        primitives::uuid::Uuid,
    },
    krabka_security::SaslMechanism,
    rstest::fixture,
    rstest_bdd_macros::{given, then, when},
    std::{
        io,
        net::SocketAddr,
        time::{Duration, Instant},
    },
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        time,
    },
};

/// `REBOOTSTRAP_REQUIRED`, KIP-1242.
const REBOOTSTRAP_REQUIRED: i16 = 129;

/// A `SASL_PLAINTEXT` broker serving PLAIN for `admin`.
async fn start_broker(customize: impl FnOnce(&mut BrokerConfig)) -> BrokerHandle {
    let (log_dir, mut cfg) = sasl_temp_config(vec![SaslMechanism::Plain]);
    cfg.plain_credentials
        .insert("admin".to_owned(), admin_plain_password());
    customize(&mut cfg);
    let handle = Broker::start(cfg).await.expect("broker must start");
    // The broker keeps its log directory for its whole life.
    let _ = log_dir.keep();
    handle
}

fn encode<M: Encode>(message: &M, version: i16) -> Vec<u8> {
    let mut body = BytesMut::new();
    message.encode(&mut body, version).expect("encode");
    body.to_vec()
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct RequestFrameSetup<'a> {
    #[default(krabka_ids::ApiKey(18))]
    api_key: krabka_ids::ApiKey,
    #[default(krabka_ids::ApiVersion(3))]
    version: krabka_ids::ApiVersion,
    #[default(WireCorrelationId(2))]
    correlation: WireCorrelationId,
    #[default(RequestHeaderEncoding::Flexible)]
    header: RequestHeaderEncoding,
    body: &'a [u8],
}

/// A sized request frame with the pre-authentication client's header.
#[bon::builder]
fn request_frame(
    #[builder(into, default = krabka_ids :: ApiKey (18))] api_key: krabka_ids::ApiKey,
    #[builder(into, default = krabka_ids :: ApiVersion (3))] version: krabka_ids::ApiVersion,
    #[builder(default = WireCorrelationId(2))] correlation: WireCorrelationId,
    #[builder(default = RequestHeaderEncoding::Flexible)] header: RequestHeaderEncoding,
    #[builder(default)] body: &'_ [u8],
) -> Vec<u8> {
    let setup = RequestFrameSetup {
        api_key,
        version,
        correlation,
        header,
        body,
    };
    let frame = support_request_frame()
        .api_key(setup.api_key)
        .version(setup.version)
        .correlation(setup.correlation)
        .header(setup.header)
        .client_id("c")
        .body(setup.body)
        .call();
    let mut out = BytesMut::new();
    out.put_i32(i32::try_from(frame.len()).expect("frame length fits i32"));
    out.put_slice(&frame);
    out.to_vec()
}

async fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let length = stream.read_u32().await?;
    let mut frame = vec![0u8; length as usize];
    stream.read_exact(&mut frame).await?;
    Ok(frame)
}

/// Waits for the broker to close `stream` without writing another frame.
async fn assert_closed_without_a_frame(stream: &mut TcpStream, what: &str) {
    let outcome = time::timeout(Duration::from_secs(10), read_frame(stream))
        .await
        .unwrap_or_else(|_| panic!("{what}: the broker held the connection open"));
    assert!(
        outcome.is_err(),
        "{what}: the broker answered with {outcome:?}"
    );
}

async fn api_versions(
    stream: &mut TcpStream,
    version: i16,
    request: &api_versions_request::ApiVersionsRequest,
) -> api_versions_response::ApiVersionsResponse {
    let body = round_trip(
        stream,
        SocketExchangeSetup {
            version: WireApiVersion(version),
            header: RequestHeaderEncoding::from_wire(version >= 3),
            ..Default::default()
        },
        &encode(request, version),
    )
    .await
    .expect("ApiVersions answer");
    api_versions_response::ApiVersionsResponse::decode(&mut &body[..], version)
        .expect("decode ApiVersions")
}

fn named_client(name: &str) -> api_versions_request::ApiVersionsRequest {
    api_versions_request_for(name, "1.0")
}

/// PLAIN login as `admin` on `stream`, after whichever requests already ran.
async fn plain_login(stream: &mut TcpStream, password: &str) -> Result<(), io::Error> {
    let handshake = encode(
        &sasl_handshake_request::SaslHandshakeRequest {
            mechanism: "PLAIN".to_owned(),
            ..Default::default()
        },
        1,
    );
    let answer = round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(17),
            version: WireApiVersion(1),
            correlation: WireCorrelationId(2),
            ..Default::default()
        },
        &handshake,
    )
    .await?;
    let answer = sasl_handshake_response::SaslHandshakeResponse::decode(&mut &answer[..], 1)
        .expect("decode handshake");
    if answer.error_code != 0 {
        return Err(io::Error::other(format!("handshake {}", answer.error_code)));
    }
    let token = sasl_authenticate_request::SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(format!("\0admin\0{password}").into_bytes()),
        ..Default::default()
    };
    let answer = round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(36),
            version: WireApiVersion(2),
            correlation: WireCorrelationId(3),
            header: RequestHeaderEncoding::Flexible,
        },
        &encode(&token, 2),
    )
    .await?;
    let answer = sasl_authenticate_response::SaslAuthenticateResponse::decode(&mut &answer[..], 2)
        .expect("decode authenticate");
    if answer.error_code != 0 {
        return Err(io::Error::other(format!(
            "authenticate {}",
            answer.error_code
        )));
    }
    Ok(())
}

async fn scrape_metrics(addr: SocketAddr) -> String { http_get(addr, "/metrics", false).await }

/// A size prefix over `sasl.server.max.receive.size` fails the authentication
/// before the frame is read. The peer gets no answer, since Kafka builds none
/// for it, and the broker counts a failed authentication.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/pre_auth_limits.feature",
    name = "a frame over the sasl receive limit fails the authentication"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_frame_over_the_sasl_receive_limit_fails_the_authentication(limits: PreAuthentication) {
    limits
        .broker
        .expect("a pre-authentication broker")
        .shutdown()
        .await;
}

/// The frame limit is `sasl.server.max.receive.size` for the whole exchange
/// and `socket.request.max.bytes` afterwards: the same 2 KiB request that fails
/// an authentication in flight is served once the peer has authenticated.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/pre_auth_limits.feature",
    name = "the larger request limit returns once the peer authenticated"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_larger_request_limit_returns_once_the_peer_authenticated(limits: PreAuthentication) {
    limits
        .broker
        .expect("a pre-authentication broker")
        .shutdown()
        .await;
}

/// A failed authentication answers no sooner than
/// `connection.failed.authentication.delay.ms`, and a zero delay answers at
/// once. The answer still carries the failure.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/pre_auth_limits.feature",
    name = "a failed authentication holds its answer for the configured delay"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_authentication_holds_its_answer_for_the_configured_delay(
    limits: PreAuthentication,
    delay: u32,
) {
    let _ = delay;
    limits
        .broker
        .expect("a pre-authentication broker")
        .shutdown()
        .await;
}

/// After one valid `ApiVersions` only a `SaslHandshake` is taken. A second
/// `ApiVersions` closes the connection with no answer. An `ApiVersions` that
/// was answered `UNSUPPORTED_VERSION` or `INVALID_REQUEST` left the state
/// where it was, so the client may ask again.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/pre_auth_limits.feature",
    name = "a second api versions before the handshake closes the connection"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_api_versions_before_the_handshake_closes_the_connection(
    limits: PreAuthentication,
) {
    limits
        .broker
        .expect("a pre-authentication broker")
        .shutdown()
        .await;
}

/// `SaslServerAuthenticator` answers the first `ApiVersions` from its own
/// supplier, whatever cluster or node the client meant to reach. Once the
/// connection is authenticated `KafkaApis` answers, and a stale identity asks
/// the client to rebootstrap.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/pre_auth_limits.feature",
    name = "a pre auth api versions carries no rebootstrap check"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pre_auth_api_versions_carries_no_rebootstrap_check(limits: PreAuthentication) {
    limits
        .broker
        .expect("a pre-authentication broker")
        .shutdown()
        .await;
}

struct PreAuthentication {
    broker: Option<BrokerHandle>,
    stream: Option<TcpStream>,
    versions: Option<api_versions_response::ApiVersionsResponse>,
    metadata_request: Vec<u8>,
    metadata: Option<metadata_response::MetadataResponse>,
    login: Option<io::Result<()>>,
    minimum_delay: Duration,
    elapsed: Option<Duration>,
    metrics: Option<String>,
    stale_identity: Option<api_versions_request::ApiVersionsRequest>,
}

#[fixture]
fn limits() -> PreAuthentication {
    // Start this scenario with fresh inputs and observations.

    PreAuthentication {
        broker: None,
        stream: None,
        versions: None,
        metadata_request: Vec::new(),
        metadata: None,
        login: None,
        minimum_delay: Duration::ZERO,
        elapsed: None,
        metrics: None,
        stale_identity: None,
    }
}

#[given("a PLAIN broker with a 1 KiB SASL receive limit and authentication metrics")]
async fn limited_metrics_broker(limits: &mut PreAuthentication) {
    limits.broker = Some(
        start_broker(|config| {
            config.sasl_server_max_receive = krabka_units::kibibytes(1);
            config.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());
        })
        .await,
    );
}

#[given("a PLAIN broker with a 1 KiB SASL receive limit")]
async fn limited_broker(limits: &mut PreAuthentication) {
    limits.broker = Some(
        start_broker(|config| config.sasl_server_max_receive = krabka_units::kibibytes(1)).await,
    );
}

#[given("a PLAIN broker with failed-authentication delay {delay} milliseconds")]
async fn delayed_broker(limits: &mut PreAuthentication, delay: u32) {
    limits.broker = Some(
        start_broker(|config| {
            config.connection_failed_authentication_delay = krabka_units::millis(delay);
        })
        .await,
    );
    limits.minimum_delay = Duration::from_millis(u64::from(delay));
}

#[given("a PLAIN broker before any ApiVersions negotiation")]
async fn default_preauth_broker(limits: &mut PreAuthentication) {
    limits.broker = Some(start_broker(|_| {}).await);
}

#[given("a PLAIN broker with unstable API versions enabled")]
async fn unstable_preauth_broker(limits: &mut PreAuthentication) {
    limits.broker = Some(
        start_broker(|config| config.features.unstable_api_versions = UnstableApiVersions::Enabled)
            .await,
    );
}

#[given("a fresh unauthenticated socket to that PLAIN broker")]
async fn fresh_preauth_socket(limits: &mut PreAuthentication) {
    let addr = limits
        .broker
        .as_ref()
        .expect("a PLAIN broker")
        .listen_addr();
    limits.stream = Some(TcpStream::connect(addr).await.unwrap());
}

#[given("a Metadata request for 64 topics exceeds 2048 encoded bytes")]
fn large_metadata_request(limits: &mut PreAuthentication) {
    let topics = (0..64)
        .map(|index| {
            metadata_topic(
                Some(format!("a-topic-name-of-thirty-two-bytes-{index:02}")),
                Uuid::default(),
            )
        })
        .collect::<Vec<_>>();
    limits.metadata_request = encode(
        &metadata_request::MetadataRequest {
            allow_auto_topic_creation: false,
            ..topic_metadata_request(Some(topics))
        },
        12,
    );
    assert!(
        limits.metadata_request.len() > 2048,
        "the request is {} bytes",
        limits.metadata_request.len()
    );
}

#[given("PLAIN has been selected but authentication is incomplete")]
async fn incomplete_plain_exchange(limits: &mut PreAuthentication) {
    fresh_preauth_socket(limits).await;
    let handshake = encode(
        &sasl_handshake_request::SaslHandshakeRequest {
            mechanism: "PLAIN".to_owned(),
            ..Default::default()
        },
        1,
    );
    round_trip(
        limits.stream.as_mut().expect("a PLAIN socket"),
        SocketExchangeSetup {
            api_key: WireApiKey(17),
            version: WireApiVersion(1),
            ..Default::default()
        },
        &handshake,
    )
    .await
    .expect("handshake");
}

#[given("the requested cluster id is stale and node id is 41")]
fn stale_cluster_identity(limits: &mut PreAuthentication) {
    limits.stale_identity = Some(api_versions_request::ApiVersionsRequest {
        cluster_id: Some("some-other-cluster".into()),
        node_id: 41,
        ..api_versions_request_for("krabka-test", "1.0")
    });
}

#[when("the client declares a 2048 byte frame but sends only 8 body bytes")]
async fn oversize_prefix(limits: &mut PreAuthentication) {
    fresh_preauth_socket(limits).await;
    let mut frame = 2048_u32.to_be_bytes().to_vec();
    frame.extend_from_slice(&[0; 8]);
    limits
        .stream
        .as_mut()
        .expect("a pre-authentication socket")
        .write_all(&frame)
        .await
        .unwrap();
}

#[when("a 2048 byte SaslAuthenticate body is sent mid-exchange")]
async fn oversize_mid_exchange(limits: &mut PreAuthentication) {
    limits
        .stream
        .as_mut()
        .expect("an incomplete PLAIN exchange")
        .write_all(&request_frame().api_key(36).body(&vec![0u8; 2048]).call())
        .await
        .unwrap();
}

#[then("the unauthenticated socket closes without a response frame")]
async fn limited_connection_closed(limits: &mut PreAuthentication) {
    assert_closed_without_a_frame(
        limits.stream.as_mut().expect("a pre-authentication socket"),
        "the pre-authentication request",
    )
    .await;
}

#[when("the pre-authentication metrics are scraped")]
async fn preauth_metric_samples(limits: &mut PreAuthentication) {
    let addr = limits
        .broker
        .as_ref()
        .expect("a metrics-enabled PLAIN broker")
        .metrics_addr()
        .expect("metrics server bound");
    limits.metrics = Some(scrape_metrics(addr).await);
}

#[then("the Unknown-mechanism failed-authentication counter equals 1")]
fn oversized_authentication_counted(limits: &PreAuthentication) {
    let body = limits
        .metrics
        .as_ref()
        .expect("pre-authentication metrics were scraped");
    let needle = "krabka_broker_failed_authentication_total{mechanism=\"Unknown\"} 1";
    assert!(body.contains(needle), "missing {needle} in:\n{body}");
}

#[given("admin authenticates on a fresh PLAIN socket")]
async fn authenticated_admin_socket(limits: &mut PreAuthentication) {
    fresh_preauth_socket(limits).await;
    plain_login(
        limits.stream.as_mut().expect("a fresh PLAIN socket"),
        &admin_plain_password(),
    )
    .await
    .expect("login");
}

#[when("the authenticated socket sends that large Metadata request")]
async fn authenticated_large_request(limits: &mut PreAuthentication) {
    let body = round_trip(
        limits
            .stream
            .as_mut()
            .expect("an authenticated PLAIN socket"),
        SocketExchangeSetup {
            api_key: WireApiKey(3),
            version: WireApiVersion(12),
            correlation: WireCorrelationId(4),
            header: RequestHeaderEncoding::Flexible,
        },
        &limits.metadata_request,
    )
    .await
    .expect("Metadata after authentication");
    limits.metadata = Some(
        metadata_response::MetadataResponse::decode(&mut &body[..], 12).expect("decode Metadata"),
    );
}

#[then("the authenticated Metadata response contains all 64 requested topics")]
fn large_authenticated_response(limits: &PreAuthentication) {
    check!(
        limits
            .metadata
            .as_ref()
            .expect("a large Metadata request was answered")
            .topics
            .len()
            == 64
    );
}

#[when("admin attempts PLAIN authentication with a wrong password")]
async fn delayed_wrong_password(limits: &mut PreAuthentication) {
    fresh_preauth_socket(limits).await;
    let started = Instant::now();
    limits.login = Some(
        plain_login(
            limits.stream.as_mut().expect("a fresh PLAIN socket"),
            &wrong_scram_password(),
        )
        .await,
    );
    limits.elapsed = Some(started.elapsed());
}

#[then("the failed authentication honors the configured minimum delay")]
fn delayed_failed_authentication(limits: &PreAuthentication) {
    let outcome = limits
        .login
        .as_ref()
        .expect("wrong-password authentication was attempted");
    check!(outcome.is_err(), "{outcome:?}");
    let elapsed = limits
        .elapsed
        .expect("the authentication response time was measured");
    let delay = limits.minimum_delay;
    check!(
        elapsed >= delay,
        "the failure came after {elapsed:?}, before the {delay:?} delay"
    );
}

#[when("the client sends an unsupported ApiVersions request")]
async fn unsupported_api_version(limits: &mut PreAuthentication) {
    let body = round_trip(
        limits.stream.as_mut().expect("a pre-authentication socket"),
        SocketExchangeSetup {
            version: WireApiVersion(i16::MAX),
            header: RequestHeaderEncoding::Flexible,
            ..Default::default()
        },
        &[0xff],
    )
    .await
    .expect("UNSUPPORTED_VERSION answer");
    limits.versions = Some(
        api_versions_response::ApiVersionsResponse::decode(&mut &body[..], 0).expect("decode"),
    );
}

#[when("ApiVersions v3 client name {name} is negotiated")]
async fn named_api_versions(limits: &mut PreAuthentication, name: String) {
    limits.versions = Some(
        api_versions(
            limits.stream.as_mut().expect("a pre-authentication socket"),
            3,
            &named_client(&name),
        )
        .await,
    );
}

#[then("the ApiVersions request returns error {code}")]
fn negotiated_error(limits: &PreAuthentication, code: i16) {
    let response = limits.versions.as_ref().expect("ApiVersions was requested");
    check!(response.error_code == code, "{response:?}");
}

#[when("admin authenticates on the current PLAIN socket")]
async fn login_after_negotiation(limits: &mut PreAuthentication) {
    limits.login = Some(
        plain_login(
            limits.stream.as_mut().expect("the negotiated PLAIN socket"),
            &admin_plain_password(),
        )
        .await,
    );
}

#[then("PLAIN authentication succeeds after the negotiation")]
fn login_after_api_versions(limits: &PreAuthentication) {
    limits
        .login
        .as_ref()
        .expect("admin attempted PLAIN authentication")
        .as_ref()
        .expect("the handshake follows one valid ApiVersions");
}

#[when("a second valid ApiVersions frame is sent before a handshake")]
async fn duplicate_api_versions(limits: &mut PreAuthentication) {
    limits
        .stream
        .as_mut()
        .expect("a socket that already negotiated ApiVersions")
        .write_all(
            &request_frame()
                .body(&encode(&named_client("krabka-test"), 3))
                .call(),
        )
        .await
        .unwrap();
}

#[when("ApiVersions v5 is requested with that stale cluster identity")]
async fn stale_api_versions(limits: &mut PreAuthentication) {
    let request = limits
        .stale_identity
        .as_ref()
        .expect("a stale cluster identity");
    limits.versions =
        Some(api_versions(limits.stream.as_mut().expect("a PLAIN socket"), 5, request).await);
}

#[when("a fresh client sends that cluster id without a node id")]
async fn missing_node_identity(limits: &mut PreAuthentication) {
    let addr = limits
        .broker
        .as_ref()
        .expect("an unstable-version PLAIN broker")
        .listen_addr();
    let mut request = limits
        .stale_identity
        .as_ref()
        .expect("a stale cluster identity")
        .clone();
    request.node_id = -1;
    let mut fresh = TcpStream::connect(addr).await.unwrap();
    limits.versions = Some(api_versions(&mut fresh, 5, &request).await);
}

#[then("the pre-authentication ApiVersions response advertises supported APIs")]
fn preauth_apis_nonempty(limits: &PreAuthentication) {
    check!(
        !limits
            .versions
            .as_ref()
            .expect("ApiVersions was requested before authentication")
            .api_keys
            .is_empty()
    );
}

#[then("the authenticated stale identity requires rebootstrap")]
fn stale_routing_rejected(limits: &PreAuthentication) {
    let response = limits
        .versions
        .as_ref()
        .expect("the authenticated stale identity was requested");
    check!(
        response.error_code == REBOOTSTRAP_REQUIRED,
        "after authentication: {response:?}"
    );
}
