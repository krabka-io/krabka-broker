//! `connections.max.idle.ms` on the accept path.
//!
//! Kafka reclaims a connection that goes `connections.max.idle.ms` without a
//! complete request frame, on every listener and whatever the connection's
//! auth state. The four scenarios here cover what that buys:
//!
//! 1. A PLAINTEXT connection that completes the TCP handshake and then sends
//!    nothing at all is closed once the window passes, and the close is
//!    counted under `connection_closes{reason="idle"}`. This is the
//!    fd-exhaustion shape: the peer never authenticates, so no ACL or SASL
//!    gate stands between it and an indefinitely held connection.
//! 2. A connection that sends a request every half-window is never closed, so
//!    the deadline really does reset on each frame read rather than capping a
//!    connection's total lifetime.
//! 3. A per-listener override wins over the broker-wide value for the
//!    listener it names.
//! 4. On a TLS listener the window covers the handshake too, so a peer that
//!    opens the socket and never sends a `ClientHello` -- the cheapest version
//!    of scenario 1, because it costs the peer no crypto at all -- is
//!    reclaimed rather than parked forever in `TlsAcceptor::accept`.
//!
//! Scenarios 1 to 3 drive the deadline with `tokio::time::pause()` and
//! `advance()`. Each pauses *after* the broker has started, not with
//! `start_paused = true`, because the broker's own start-up timers -- raft
//! heartbeats, disk scans -- need real wall-clock progress or `Broker::start`
//! hangs. The dispatch loop arms its idle deadline against the real tokio
//! clock as it enters the frame read, so a later `advance` jumps past that
//! `Instant` -- but only once the loop has got that far, which is why each
//! waits on the `active_connections` gauge before it advances. The gauge is
//! incremented in the same poll that arms the deadline, so seeing it move is
//! proof the timer is running.
//!
//! Scenario 4 cannot use that gauge, because the connection it describes never
//! reaches the serve loop that increments it. It runs on a real, short window
//! instead, and the bounded read it blocks on gives that window twenty times
//! its length to fire.
#![cfg(test)]

mod support;

use {
    assert2::assert,
    bytes::{Buf, BufMut, BytesMut},
    krabka_broker::{
        Broker, BrokerConfig, BrokerHandle,
        metrics::{ConnectionCloseReason, ConnectionCloseReasonLabel},
    },
    krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        },
    },
    krabka_security::{ClientAuthMode, ListenerProtocol, TlsConfig},
    krabka_units::Time,
    rustls::crypto::aws_lc_rs,
    std::{
        fs, io, iter,
        net::SocketAddr,
        path::{Path, PathBuf},
        time::Duration,
    },
    support::listeners::loopback_listener,
    tempfile::TempDir,
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        time,
    },
};

/// The dev server certificate scenario 4's TLS listener presents. Nothing
/// verifies it -- the peer under test never sends a `ClientHello` -- but the
/// listener will not bind without one.
const DEV_CERT: &str = include_str!("fixtures/security/dev_cert.pem");
const DEV_KEY: &str = include_str!("fixtures/security/dev_key.pem");

/// Writes one PEM fixture into `dir` and returns its path.
fn write_pem(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, body).expect("write PEM fixture");
    path
}

/// A single-broker cluster serving one PLAINTEXT listener, plus the log
/// directory that has to outlive it.
struct Fixture {
    handle: BrokerHandle,
    addr: SocketAddr,
    _log_dir: TempDir,
}

impl Fixture {
    /// Boots a broker over `cfg`, whose listener list this function fills in.
    async fn start(mut cfg: BrokerConfig, log_dir: TempDir, listener: &str) -> Self {
        cfg.listeners = vec![loopback_listener(listener, ListenerProtocol::Plaintext)];
        cfg.inter_broker_listener_name = listener.to_owned();
        let handle = Broker::start(cfg).await.expect("broker must start");
        let addr = handle.listen_addr();
        Self {
            handle,
            addr,
            _log_dir: log_dir,
        }
    }

    /// Opens a connection and returns once the serve loop has armed its idle
    /// deadline for it, which the `active_connections` gauge reports.
    async fn connect(&self) -> TcpStream {
        let before = self.handle.metrics().active_connections.get();
        let stream = TcpStream::connect(self.addr).await.expect("connect");
        self.handle
            .wait_for_metrics("the new connection to reach the serve loop", |metrics| {
                metrics.active_connections.get() > before
            })
            .await;
        stream
    }

    /// How many connections this broker has closed for being idle.
    fn idle_closes(&self) -> u64 {
        self.handle
            .metrics()
            .connection_closes
            .get_or_create(&ConnectionCloseReasonLabel {
                reason: ConnectionCloseReason::Idle,
            })
            .get()
    }
}

/// A broker config and the directory that backs its log.
struct IdleWindowConfig {
    cfg: BrokerConfig,
    log_dir: TempDir,
}

/// A test broker whose PLAINTEXT listener is held to `idle`.
fn config_with_idle_window(idle: Time) -> IdleWindowConfig {
    let log_dir = tempfile::tempdir().expect("tempdir");
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.connections_max_idle = Some(idle);
    IdleWindowConfig { cfg, log_dir }
}

/// Sends one `ApiVersions` v0 request and reads its response. Returning the
/// decoded response is what proves the connection is still serving requests.
async fn api_versions(stream: &mut TcpStream, corr_id: i32) -> io::Result<ApiVersionsResponse> {
    let mut body = BytesMut::new();
    ApiVersionsRequest::default()
        .encode(&mut body, 0)
        .map_err(|error| io::Error::other(format!("ApiVersions encode: {error}")))?;

    let mut frame = BytesMut::with_capacity(16 + body.len());
    frame.put_i16(18);
    frame.put_i16(0);
    frame.put_i32(corr_id);
    frame.put_i16(-1); // null client_id
    frame.put_slice(&body);
    stream
        .write_u32(u32::try_from(frame.len()).expect("frame length fits u32"))
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let len = stream.read_u32().await?;
    let mut response = vec![0_u8; len as usize];
    stream.read_exact(&mut response).await?;
    let mut cur: &[u8] = &response;
    let _correlation_id = cur.get_i32();
    ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|error| io::Error::other(format!("ApiVersions decode: {error}")))
}

/// Jumps the tokio clock forward without letting the wall clock move.
async fn advance(by: Duration) {
    time::pause();
    time::advance(by).await;
    time::resume();
}

/// Reads one byte with a bounded wait and reports whether the peer closed.
async fn is_closed(stream: &mut TcpStream) -> bool {
    let mut buf = [0_u8; 1];
    let read = time::timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("read must not hang")
        .expect("read must not error");
    read == 0
}

async fn assert_connection_closed(stream: &mut TcpStream) {
    assert!(is_closed(stream).await);
}

struct IdleConnections {
    fixture: Option<Fixture>,
    pem_directory: Option<TempDir>,
    stream: Option<TcpStream>,
    wire: Vec<u8>,
    responses: Vec<ApiVersionsResponse>,
    correlation: Option<i32>,
}
#[rstest::fixture]
fn idle_connections() -> IdleConnections {
    // Start this scenario with fresh inputs and observations.

    IdleConnections {
        fixture: None,
        pem_directory: None,
        stream: None,
        wire: Vec::new(),
        responses: Vec::new(),
        correlation: None,
    }
}
#[rstest_bdd_macros::given(
    "a {scope} idle timeout of {window_secs} seconds on a plaintext listener"
)]
async fn plaintext_idle_window(
    idle_connections: &mut IdleConnections,
    scope: String,
    window_secs: u32,
) {
    let per_listener = match scope.as_str() {
        "broker-wide" => false,
        "listener-override" => true,
        other => panic!("unknown idle window source {other}"),
    };
    let IdleWindowConfig { mut cfg, log_dir } = if per_listener {
        let log_dir = tempfile::tempdir().expect("tempdir");
        IdleWindowConfig {
            cfg: BrokerConfig::for_tests(log_dir.path().to_path_buf()),
            log_dir,
        }
    } else {
        config_with_idle_window(krabka_units::secs(window_secs))
    };
    if per_listener {
        cfg.connections_max_idle_overrides =
            iter::once(("EXTERNAL".to_owned(), krabka_units::secs(window_secs))).collect();
    }
    let listener = if per_listener {
        "EXTERNAL"
    } else {
        "PLAINTEXT"
    };
    idle_connections.fixture = Some(Box::pin(Fixture::start(cfg, log_dir, listener)).await);
}
#[rstest_bdd_macros::when("a peer connects and the plaintext listener arms its idle timer")]
async fn connect_idle_plaintext(idle_connections: &mut IdleConnections) {
    idle_connections.stream = Some(
        idle_connections
            .fixture
            .as_ref()
            .expect("listener configured")
            .connect()
            .await,
    );
}
#[rstest_bdd_macros::when("the clock advances {seconds} seconds without client traffic")]
async fn advance_idle_window(seconds: u64) { advance(Duration::from_secs(seconds)).await; }
#[rstest_bdd_macros::then("the silent connection closes without a response")]
async fn silent_idle_connection_closed(idle_connections: &mut IdleConnections) {
    assert_connection_closed(idle_connections.stream.as_mut().expect("peer connected")).await;
}
#[rstest_bdd_macros::then("exactly {count} connections have been counted as idle closes")]
fn idle_connection_close_count(idle_connections: &IdleConnections, count: u64) {
    assert!(
        idle_connections
            .fixture
            .as_ref()
            .expect("listener configured")
            .idle_closes()
            == count
    );
}
#[rstest_bdd_macros::when(
    "the client sends API versions requests every 30 seconds for four half-windows"
)]
async fn request_half_windows(idle_connections: &mut IdleConnections) {
    let stream = idle_connections.stream.as_mut().expect("peer connected");
    for corr_id in 0..4 {
        advance(Duration::from_secs(30)).await;
        let response = api_versions(stream, corr_id)
            .await
            .expect("connection must still serve requests");
        idle_connections.responses.push(response);
    }
}
#[rstest_bdd_macros::then("all four API versions requests are served successfully")]
fn active_idle_connection_served(idle_connections: &IdleConnections) {
    assert!(idle_connections.responses.len() == 4);
    for response in &idle_connections.responses {
        assert!(response.error_code == 0);
    }
}
#[rstest_bdd_macros::given("an API versions v0 frame with correlation id 7")]
fn trickled_frame_input(idle_connections: &mut IdleConnections) {
    let mut body = BytesMut::new();
    ApiVersionsRequest::default()
        .encode(&mut body, 0)
        .expect("ApiVersions encode");
    let mut frame = BytesMut::new();
    frame.put_i16(18);
    frame.put_i16(0);
    frame.put_i32(7);
    frame.put_i16(-1); // null client_id
    frame.put_slice(&body);
    let mut wire = u32::try_from(frame.len())
        .expect("frame length fits u32")
        .to_be_bytes()
        .to_vec();
    wire.extend_from_slice(&frame);

    idle_connections.wire = wire;
}
#[rstest_bdd_macros::when("the client trickles the frame in four chunks separated by 30 seconds")]
async fn send_trickled_frame(idle_connections: &mut IdleConnections) {
    let stream = idle_connections.stream.as_mut().expect("peer connected");
    // The size prefix, then the 10-byte header in three pieces: four
    // half-windows between the first byte and the last.
    for chunk in [
        &idle_connections.wire[..4],
        &idle_connections.wire[4..7],
        &idle_connections.wire[7..10],
        &idle_connections.wire[10..],
    ] {
        stream.write_all(chunk).await.expect("write a chunk");
        stream.flush().await.expect("flush a chunk");
        // Lets the serve loop take the chunk before the clock moves on.
        time::sleep(Duration::from_millis(20)).await;
        advance(Duration::from_secs(30)).await;
    }
}
#[rstest_bdd_macros::when("the client reads the trickled request response")]
async fn read_trickled_frame(idle_connections: &mut IdleConnections) {
    let stream = idle_connections.stream.as_mut().expect("peer connected");
    let len = stream
        .read_u32()
        .await
        .expect("the trickled request is answered");
    let mut response = vec![0_u8; len as usize];
    stream
        .read_exact(&mut response)
        .await
        .expect("response body");
    let mut cur: &[u8] = &response;
    idle_connections.correlation = Some(cur.get_i32());
    let response = ApiVersionsResponse::decode(&mut cur, 0).expect("ApiVersions decode");
    idle_connections.responses.push(response);
}
#[rstest_bdd_macros::then("the trickled request succeeds with correlation id 7")]
fn trickled_frame_served(idle_connections: &IdleConnections) {
    assert!(idle_connections.correlation == Some(7));
    let [response] = &idle_connections.responses[..] else {
        panic!("one trickled response expected")
    };
    assert!(response.error_code == 0);
}
#[rstest_bdd_macros::given("a TLS listener with an idle timeout of 250 milliseconds")]
async fn tls_idle_window(idle_connections: &mut IdleConnections) {
    let _ = aws_lc_rs::default_provider().install_default();

    let log_dir = tempfile::tempdir().expect("tempdir");
    let pem_dir = tempfile::tempdir().expect("tempdir");
    let cert_path = write_pem(pem_dir.path(), "cert.pem", DEV_CERT);
    let key_path = write_pem(pem_dir.path(), "key.pem", DEV_KEY);

    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.connections_max_idle = Some(krabka_units::millis(250));
    cfg.listeners = vec![loopback_listener("SSL", ListenerProtocol::Ssl)];
    cfg.inter_broker_listener_name = "SSL".to_owned();
    cfg.tls_config = Some(TlsConfig {
        cert_chain_path: cert_path,
        private_key_path: key_path,
        trust_roots_path: None,
        client_ca_path: None,
        client_auth: ClientAuthMode::Disabled,
    });

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    idle_connections.fixture = Some(Fixture {
        handle,
        addr,
        _log_dir: log_dir,
    });
    idle_connections.pem_directory = Some(pem_dir);
}
#[rstest_bdd_macros::when("a peer opens a TCP connection without starting TLS")]
async fn connect_silent_tls(idle_connections: &mut IdleConnections) {
    let addr = idle_connections
        .fixture
        .as_ref()
        .expect("TLS listener configured")
        .addr;
    idle_connections.stream = Some(TcpStream::connect(addr).await.expect("connect"));
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/connections_max_idle.feature",
    name = "silent plaintext connection closes past its idle window"
)]
#[tokio::test(flavor = "current_thread")]
async fn silent_plaintext_connection_closes_past_its_idle_window(
    idle_connections: IdleConnections,
    scope: String,
    window_secs: u32,
    advance_secs: u64,
) {
    let _ = (scope, window_secs, advance_secs);
    idle_connections
        .fixture
        .expect("listener configured")
        .handle
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/connections_max_idle.feature",
    name = "a connection speaking each half window is never closed"
)]
#[tokio::test(flavor = "current_thread")]
async fn a_connection_speaking_each_half_window_is_never_closed(idle_connections: IdleConnections) {
    idle_connections
        .fixture
        .expect("listener configured")
        .handle
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/connections_max_idle.feature",
    name = "a request trickled across two windows is served"
)]
#[tokio::test(flavor = "current_thread")]
async fn a_request_trickled_across_two_windows_is_served(idle_connections: IdleConnections) {
    idle_connections
        .fixture
        .expect("listener configured")
        .handle
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/connections_max_idle.feature",
    name = "a tls listener closes a socket that never starts its handshake"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tls_listener_closes_a_socket_that_never_starts_its_handshake(
    idle_connections: IdleConnections,
) {
    idle_connections
        .fixture
        .expect("listener configured")
        .handle
        .shutdown()
        .await;
}
