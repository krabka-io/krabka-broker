//! `ApiVersions` on a `SASL_PLAINTEXT` controller listener, before and after
//! authentication.
//!
//! Kafka's `SocketServer` gives `SaslServerAuthenticator` the same
//! `apiVersionSupplier` that `ControllerApis` answers from. So the answer before
//! authentication is the full controller-listener table with the features, and
//! an unsupported version gets `UNSUPPORTED_VERSION` without closing the
//! connection.
#![cfg(test)]

mod support;

use {
    crate::support::{discovery::api_versions_request_for, listeners::configure_sasl_plaintext},
    assert2::{assert, check},
    krabka_broker::{Broker, BrokerConfig, config::InterBrokerCredentials},
    krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            sasl_authenticate_request::SaslAuthenticateRequest,
            sasl_handshake_request::SaslHandshakeRequest,
        },
    },
    krabka_security::{ListenerProtocol, SaslMechanism},
    std::{
        net::SocketAddr,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        time,
    },
};

// A request frame; flexible selects v2, and the prefix keeps the original u32 limit.
krabka_macros::vector_request_fixture!(frame, u32);

/// Sends one request and returns the response frame after the correlation id.
async fn round_trip(stream: &mut TcpStream, request: Vec<u8>, correlation_id: i32) -> Vec<u8> {
    stream.write_all(&request).await.expect("write request");
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await.expect("response length");
    let mut response = vec![0u8; u32::from_be_bytes(len) as usize];
    stream
        .read_exact(&mut response)
        .await
        .expect("response frame");
    assert!(response[..4] == correlation_id.to_be_bytes());
    response.split_off(4)
}

fn encode<M: Encode>(message: &M, version: i16) -> Vec<u8> {
    let mut body = bytes::BytesMut::new();
    message.encode(&mut body, version).expect("encode");
    body.to_vec()
}

async fn api_versions(
    stream: &mut TcpStream,
    version: i16,
    correlation_id: i32,
) -> ApiVersionsResponse {
    let body = encode(&api_versions_request_for("krabka-test", "1.0"), version);
    let response = round_trip(
        stream,
        frame(18, version, correlation_id, version >= 3, &body),
        correlation_id,
    )
    .await;
    ApiVersionsResponse::decode(&mut &response[..], version).expect("decode ApiVersions")
}

/// A data directory and the controller listener bound for it.
struct ControllerListenerFixture {
    dir: TempDir,
    listener: TcpListener,
    addr: SocketAddr,
}

async fn controller_listener_fixture() -> ControllerListenerFixture {
    let dir = TempDir::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    ControllerListenerFixture {
        dir,
        listener,
        addr,
    }
}

/// A running broker, its controller listener address and the directory that
/// backs it.
struct ControllerNode {
    broker: krabka_broker::BrokerHandle,
    controller_addr: SocketAddr,
    dir: TempDir,
}

async fn start_sasl_controller() -> ControllerNode {
    let ControllerListenerFixture {
        dir,
        listener: controller,
        addr: controller_addr,
    } = controller_listener_fixture().await;
    let data_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    cfg.controller_listen_addr = controller_addr;
    cfg.controller_quorum_voters = vec![(cfg.node_id, controller_addr.to_string())];
    configure_sasl_plaintext(&mut cfg, data_addr, ListenerProtocol::SaslPlaintext);
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("broker".to_owned(), "secret".to_owned());
    cfg.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
        username: "broker".to_owned(),
        password: "secret".to_owned(),
    });
    let broker = Broker::start_with_controller_listener(cfg, Some(controller))
        .await
        .expect("start broker");
    ControllerNode {
        broker,
        controller_addr,
        dir,
    }
}

/// A broker whose controller listener is PLAINTEXT, on `customize`d settings.
async fn start_plaintext_controller(customize: impl FnOnce(&mut BrokerConfig)) -> ControllerNode {
    let ControllerListenerFixture {
        dir,
        listener: controller,
        addr: controller_addr,
    } = controller_listener_fixture().await;
    let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    cfg.controller_listen_addr = controller_addr;
    cfg.controller_quorum_voters = vec![(cfg.node_id, controller_addr.to_string())];
    customize(&mut cfg);
    let broker = Broker::start_with_controller_listener(cfg, Some(controller))
        .await
        .expect("start broker");
    ControllerNode {
        broker,
        controller_addr,
        dir,
    }
}

/// Waits for the listener to end `stream`: the next read is end of input or
/// an error, never a byte.
async fn assert_closed(stream: &mut TcpStream, what: &str) {
    let mut byte = [0u8; 1];
    let outcome = time::timeout(Duration::from_secs(15), stream.read(&mut byte))
        .await
        .unwrap_or_else(|_| panic!("{what}: the listener held the connection open"));
    check!(
        outcome.as_ref().map_or(true, |read| *read == 0),
        "{what}: {outcome:?}"
    );
}

struct ControllerDiscovery {
    node: Option<ControllerNode>,
    stream: Option<TcpStream>,
    idle: Option<TcpStream>,
    before: Option<ApiVersionsResponse>,
    after: Option<ApiVersionsResponse>,
    refusal: Option<ApiVersionsResponse>,
    handshake: Option<Vec<u8>>,
    authenticate: Option<Vec<u8>>,
    busy_errors: Vec<i16>,
}
#[rstest::fixture]
fn discovery() -> ControllerDiscovery {
    // Start this scenario with fresh inputs and observations.

    ControllerDiscovery {
        node: None,
        stream: None,
        idle: None,
        before: None,
        after: None,
        refusal: None,
        handshake: None,
        authenticate: None,
        busy_errors: Vec::new(),
    }
}
#[rstest_bdd_macros::given("an unauthenticated PLAIN connection to a SASL controller listener")]
async fn sasl_controller_source(discovery: &mut ControllerDiscovery) {
    let node = start_sasl_controller().await;
    discovery.stream = Some(
        TcpStream::connect(node.controller_addr)
            .await
            .expect("connect"),
    );
    discovery.node = Some(node);
}
#[rstest_bdd_macros::when("the client sends unsupported API versions v5 with an invalid body")]
async fn unsupported_controller_versions(discovery: &mut ControllerDiscovery) {
    let response = round_trip(
        discovery.stream.as_mut().expect("controller connection"),
        frame(18, 5, 1, true, &[0xff]),
        1,
    )
    .await;
    discovery.refusal =
        Some(ApiVersionsResponse::decode(&mut &response[..], 0).expect("decode refusal"));
}
#[rstest_bdd_macros::then(
    "the controller returns the exact v0 refusal advertising API versions 0 through 4"
)]
fn controller_version_refusal(discovery: &ControllerDiscovery) {
    check!(
        discovery.refusal.as_ref().expect("v5 requested")
            == &ApiVersionsResponse {
                error_code: 35,
                api_keys: vec![ApiVersion {
                    api_key: 18,
                    min_version: 0,
                    max_version: 4,
                    ..Default::default()
                }],
                ..Default::default()
            }
    );
}
#[rstest_bdd_macros::when("the unauthenticated client requests API versions v{version}")]
async fn preauth_controller_versions(discovery: &mut ControllerDiscovery, version: i16) {
    discovery.before = Some(
        api_versions(
            discovery.stream.as_mut().expect("controller connection"),
            version,
            2,
        )
        .await,
    );
}
#[rstest_bdd_macros::when("the client authenticates as broker with PLAIN")]
async fn authenticate_controller_client(discovery: &mut ControllerDiscovery) {
    let stream = discovery.stream.as_mut().expect("controller connection");
    let handshake = round_trip(
        stream,
        frame(
            17,
            1,
            3,
            false,
            &encode(
                &SaslHandshakeRequest {
                    mechanism: "PLAIN".into(),
                    ..Default::default()
                },
                1,
            ),
        ),
        3,
    )
    .await;

    let authenticate = round_trip(
        stream,
        frame(
            36,
            2,
            4,
            true,
            &encode(
                &SaslAuthenticateRequest {
                    auth_bytes: bytes::Bytes::from_static(b"\0broker\0secret"),
                    ..Default::default()
                },
                2,
            ),
        ),
        4,
    )
    .await;
    // The flexible response header's tagged-fields byte, then error 0.

    discovery.handshake = Some(handshake);
    discovery.authenticate = Some(authenticate);
}
#[rstest_bdd_macros::then("both PLAIN handshake and authentication return success")]
fn controller_auth_succeeded(discovery: &ControllerDiscovery) {
    assert!(discovery.handshake.as_ref().expect("handshake requested")[..2] == 0i16.to_be_bytes());
    assert!(
        discovery
            .authenticate
            .as_ref()
            .expect("authentication requested")[..3]
            == [0, 0, 0]
    );
}
#[rstest_bdd_macros::when("the authenticated client requests API versions v{version}")]
async fn postauth_controller_versions(discovery: &mut ControllerDiscovery, version: i16) {
    discovery.after = Some(
        api_versions(
            discovery.stream.as_mut().expect("controller connection"),
            version,
            5,
        )
        .await,
    );
}
#[rstest_bdd_macros::then(
    "the v{version} controller API and feature table is unchanged and its epoch does not regress"
)]
fn controller_catalog_preserved(discovery: &ControllerDiscovery, version: i16) {
    let before = discovery.before.as_ref().expect("pre-auth table requested");
    let after = discovery.after.as_ref().expect("post-auth table requested");
    // `finalized_features_epoch` is the controller's metadata offset, as
    // Kafka's `KRaftMetadataCache.features` reports it, so a record the
    // controller commits between the two requests may move it on. The
    // rest of the answer is unchanged by authentication.
    check!(
        after.finalized_features_epoch >= before.finalized_features_epoch,
        "v{version}"
    );
    check!(
        ApiVersionsResponse {
            finalized_features_epoch: before.finalized_features_epoch,
            ..after.clone()
        } == *before,
        "v{version}"
    );
    check!(before.error_code == 0, "v{version}");
    check!(
        before.api_keys.iter().any(|key| key.api_key == 52),
        "v{version}: Vote is advertised"
    );
    if version >= 3 {
        check!(!before.supported_features.is_empty(), "v{version}");
    }
}
#[rstest_bdd_macros::given("a PLAINTEXT controller with a 4 KiB request size limit")]
async fn bounded_controller_source(discovery: &mut ControllerDiscovery) {
    let node =
        start_plaintext_controller(|cfg| cfg.socket_request_max = krabka_units::kibibytes(4)).await;
    discovery.stream = Some(
        TcpStream::connect(node.controller_addr)
            .await
            .expect("connect"),
    );
    discovery.node = Some(node);
}
#[rstest_bdd_macros::when("the client requests API versions v0 on the bounded controller")]
async fn bounded_controller_request(discovery: &mut ControllerDiscovery) {
    discovery.before = Some(
        api_versions(
            discovery.stream.as_mut().expect("controller connection"),
            0,
            1,
        )
        .await,
    );
}
#[rstest_bdd_macros::then("the bounded controller serves the valid request")]
fn bounded_controller_served(discovery: &ControllerDiscovery) {
    check!(
        discovery
            .before
            .as_ref()
            .expect("API versions requested")
            .error_code
            == 0
    );
}
#[rstest_bdd_macros::when("the client declares a 1 MiB frame without sending its body")]
async fn oversized_controller_frame(discovery: &mut ControllerDiscovery) {
    discovery
        .stream
        .as_mut()
        .expect("controller connection")
        .write_all(&(1024 * 1024_u32).to_be_bytes())
        .await
        .expect("write the size prefix");
}
#[rstest_bdd_macros::then("the controller closes the oversize connection without returning bytes")]
async fn controller_oversize_closed(discovery: &mut ControllerDiscovery) {
    assert_closed(
        discovery.stream.as_mut().expect("controller connection"),
        "an oversize request",
    )
    .await;
}
#[rstest_bdd_macros::given(
    "a PLAINTEXT controller with an 800 millisecond idle timeout and two clients"
)]
async fn idle_controller_source(discovery: &mut ControllerDiscovery) {
    let node = start_plaintext_controller(|cfg| {
        cfg.connections_max_idle_overrides
            .insert("CONTROLLER".to_owned(), krabka_units::millis(800));
    })
    .await;
    discovery.idle = Some(
        TcpStream::connect(node.controller_addr)
            .await
            .expect("connect"),
    );
    discovery.stream = Some(
        TcpStream::connect(node.controller_addr)
            .await
            .expect("connect"),
    );
    discovery.node = Some(node);
}
#[rstest_bdd_macros::when(
    "one client remains idle while the other requests API versions every 200 milliseconds for 1600 milliseconds"
)]
async fn keep_controller_client_busy(discovery: &mut ControllerDiscovery) {
    let window = Duration::from_millis(800);
    let started = Instant::now();
    let mut correlation_id = 1;
    while started.elapsed() < window * 2 {
        let response = api_versions(
            discovery.stream.as_mut().expect("busy connection"),
            0,
            correlation_id,
        )
        .await;
        discovery.busy_errors.push(response.error_code);
        correlation_id += 1;
        time::sleep(window / 4).await;
    }
}
#[rstest_bdd_macros::then("every request on the active client succeeds")]
fn busy_controller_survived(discovery: &ControllerDiscovery) {
    check!(!discovery.busy_errors.is_empty());
    for error in &discovery.busy_errors {
        check!(*error == 0);
    }
}
#[rstest_bdd_macros::then("the idle client is closed without returning bytes")]
async fn idle_controller_closed(discovery: &mut ControllerDiscovery) {
    assert_closed(
        discovery.idle.as_mut().expect("idle connection"),
        "an idle connection",
    )
    .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/controller_listener_api_versions.feature",
    name = "sasl controller listener answers api versions before authentication"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sasl_controller_listener_answers_api_versions_before_authentication(
    discovery: ControllerDiscovery,
    version: i16,
) {
    let _ = version;
    let ControllerNode {
        broker,
        dir: _directory,
        ..
    } = discovery.node.expect("controller started");
    broker.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/controller_listener_api_versions.feature",
    name = "the controller listener refuses a request over socket request max"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_controller_listener_refuses_a_request_over_socket_request_max(
    discovery: ControllerDiscovery,
) {
    let ControllerNode {
        broker,
        dir: _directory,
        ..
    } = discovery.node.expect("controller started");
    broker.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/controller_listener_api_versions.feature",
    name = "the controller listener closes an idle connection"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_controller_listener_closes_an_idle_connection(discovery: ControllerDiscovery) {
    let ControllerNode {
        broker,
        dir: _directory,
        ..
    } = discovery.node.expect("controller started");
    broker.shutdown().await;
}
