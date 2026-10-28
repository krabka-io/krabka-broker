#![cfg(test)]

//! mTLS client authentication.
//!
//! Drives the full stack: rustls client cert handshake → broker
//! verifies the cert chain against `client_ca_path` → dispatch layer
//! extracts the cert's Subject DN as the connection's `Principal` →
//! authorizer reads that name when checking ACLs.
//!
//! The test exercises the principal-derivation path. It sets the
//! cert DN as a super-user and then sends a request that the
//! authorizer would refuse for any other principal. A successful round
//! trip proves that the broker resolved the connection to the cert DN
//! and not to `ANONYMOUS`.
//!
//! The test is gated to non-Windows. There is no multi-broker dependency, but
//! the dev cert fixture path resolution and tempfile semantics are easier to
//! keep consistent with the existing TLS integration tests.

mod kafka_wire;

mod support;

use {
    crate::support::{
        tls::{PinnedCertVerifier, fixture_signature_schemes, ssl_config},
        topics::{creatable_topic, create_topic_request},
        wire::HeaderEncoding as RequestHeaderEncoding,
    },
    assert2::assert,
    bytes::BytesMut,
    krabka_broker::{Broker, BrokerConfig, BrokerHandle},
    krabka_protocol::{Decode, Encode, owned::create_topics_response::CreateTopicsResponse},
    krabka_security::{ClientAuthMode, TlsConfig},
    rustls::crypto::aws_lc_rs,
    std::{
        fs, io,
        net::SocketAddr,
        path::{Path, PathBuf},
        sync::Arc,
        time::Duration,
    },
    tokio::{
        io::{AsyncRead, AsyncWrite},
        net::TcpStream,
        time,
    },
    tokio_rustls::{
        TlsConnector,
        client::TlsStream,
        rustls::{
            ClientConfig,
            pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject as _},
        },
    },
};

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-mtls-test";

use crate::support::tls::{
    CLIENT_PRINCIPAL, DEV_CERT, DEV_CLIENT_CA, DEV_CLIENT_CERT, DEV_CLIENT_KEY, DEV_KEY,
};

fn write_fixture(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let p = dir.join(name);
    fs::write(&p, contents).unwrap();
    p
}

/// Build a rustls `ClientConfig` that:
/// - skips server-cert verification (the broker presents the self-issued
///   `dev_cert` fixture, which rustls's default verifier rejects as
///   `CaUsedAsEndEntity`),
/// - presents the fixture client cert and private key on the
///   `CertificateRequest` callback.
fn client_config_with_pinned_server_and_client_cert(
    broker_cert: CertificateDer<'static>,
) -> Arc<ClientConfig> {
    let client_certs: Vec<CertificateDer<'static>> =
        CertificateDer::pem_slice_iter(DEV_CLIENT_CERT.as_bytes())
            .collect::<Result<_, _>>()
            .expect("parse client cert PEM");
    let client_key = PrivateKeyDer::from_pem_slice(DEV_CLIENT_KEY.as_bytes())
        .expect("parse client private key PEM");
    let cfg = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedCertVerifier {
            pinned: broker_cert,
            schemes: fixture_signature_schemes(),
            mismatch: "presented server cert does not match pinned dev cert",
        }))
        .with_client_auth_cert(client_certs, client_key)
        .expect("rustls accepts client cert + key");
    Arc::new(cfg)
}

async fn timed_create_topics<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    body: &[u8],
    timeout: Duration,
    timeout_context: &str,
) -> io::Result<Vec<u8>> {
    time::timeout(
        timeout,
        kafka_wire::round_trip()
            .stream(stream)
            .api_key(19)
            .version(7)
            .client_id(CLIENT_ID)
            .header(RequestHeaderEncoding::Flexible)
            .body(body)
            .call(),
    )
    .await
    .expect(timeout_context)
}

fn check_create_topics_success(bytes: &[u8], context: &str) {
    let mut cursor = bytes;
    let response =
        CreateTopicsResponse::decode(&mut cursor, 7).expect("decode CreateTopicsResponse");
    assert!(response.topics.len() == 1);
    assert!(
        response.topics[0].error_code == 0,
        "{context} — got {:?}",
        response.topics[0]
    );
}

/// A broker configuration with the temporary directories it reads and writes.
struct MtlsFixture {
    log_dir: tempfile::TempDir,
    pem_dir: tempfile::TempDir,
    config: BrokerConfig,
}

fn mtls_fixture(client_auth: ClientAuthMode) -> MtlsFixture {
    let log_dir = tempfile::tempdir().unwrap();
    let pem_dir = tempfile::tempdir().unwrap();
    let server_cert_path = write_fixture(pem_dir.path(), "server.pem", DEV_CERT);
    let server_key_path = write_fixture(pem_dir.path(), "server.key", DEV_KEY);
    let client_ca_path = write_fixture(pem_dir.path(), "client_ca.pem", DEV_CLIENT_CA);

    let cfg = ssl_config(log_dir.path().to_path_buf(), TlsConfig {
        cert_chain_path: server_cert_path,
        private_key_path: server_key_path,
        trust_roots_path: None,
        client_ca_path: Some(client_ca_path),
        client_auth,
    });
    MtlsFixture {
        log_dir,
        pem_dir,
        config: cfg,
    }
}

/// A running super-user broker with the temporary directories that back it.
struct MtlsBroker {
    log_dir: tempfile::TempDir,
    pem_dir: tempfile::TempDir,
    handle: BrokerHandle,
    addr: SocketAddr,
}

fn server_certificate() -> CertificateDer<'static> {
    CertificateDer::pem_slice_iter(DEV_CERT.as_bytes())
        .next()
        .expect("dev server cert present")
        .expect("dev server cert parses")
}

fn create_topics_body(topic: &str) -> BytesMut {
    let request = create_topic_request(creatable_topic().name((topic).into()).call());
    let mut body = BytesMut::new();
    request.encode(&mut body, 7).expect("encode CreateTopics");
    body
}

async fn mtls_connect(
    addr: SocketAddr,
    connector: TlsConnector,
    context: &str,
) -> TlsStream<TcpStream> {
    let tcp = TcpStream::connect(addr).await.expect("tcp connect");
    let server_name = ServerName::try_from("krabka-dev").unwrap();
    connector.connect(server_name, tcp).await.expect(context)
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};
struct CertificateSession {
    configured: Option<MtlsFixture>,
    broker: Option<MtlsBroker>,
    connector: Option<TlsConnector>,
    tls: Option<TlsStream<TcpStream>>,
    response: Option<io::Result<Vec<u8>>>,
    request_timeout: Option<Duration>,
}
#[fixture]
fn session() -> CertificateSession {
    // Start this scenario with fresh inputs and observations.

    CertificateSession {
        configured: None,
        broker: None,
        connector: None,
        tls: None,
        response: None,
        request_timeout: None,
    }
}
#[given("a TLS listener uses client authentication mode {mode} and trusts the fixture client CA")]
fn listener(session: &mut CertificateSession, mode: String) {
    let _ = aws_lc_rs::default_provider().install_default();
    let mode = match mode.as_str() {
        "required" => ClientAuthMode::Required,
        "optional" => ClientAuthMode::Optional,
        _ => panic!("unknown client authentication mode {mode}"),
    };
    session.configured = Some(mtls_fixture(mode));
}
#[given("the fixture certificate Subject DN is the only configured superuser")]
fn privileged_dn(session: &mut CertificateSession) {
    session
        .configured
        .as_mut()
        .expect("TLS listener configured")
        .config
        .super_users = maplit::hashset! {CLIENT_PRINCIPAL.to_owned()};
}
#[given("the listener accepts only these certificate principal mapping rules")]
fn mapping(session: &mut CertificateSession, datatable: Vec<Vec<String>>) {
    let rules: Vec<String> = datatable
        .into_iter()
        .skip(1)
        .map(|row| row.into_iter().next().expect("principal mapping rule"))
        .collect();
    let rules: Vec<&str> = rules.iter().map(String::as_str).collect();
    session
        .configured
        .as_mut()
        .expect("TLS listener configured")
        .config
        .listeners[0]
        .principal_mapper =
        krabka_broker::SslPrincipalMapper::parse(&rules).expect("mapping rules parse");
}
#[given("the configured TLS broker is running")]
async fn running(session: &mut CertificateSession) {
    let MtlsFixture {
        log_dir,
        pem_dir,
        config,
    } = session.configured.take().expect("TLS listener configured");
    let handle = Broker::start(config).await.expect("TLS broker starts");
    let addr = handle.listen_addr();
    session.broker = Some(MtlsBroker {
        log_dir,
        pem_dir,
        handle,
        addr,
    });
}
#[given("the TLS client pins the server and presents the trusted fixture certificate")]
fn with_certificate(session: &mut CertificateSession) {
    session.connector = Some(TlsConnector::from(
        client_config_with_pinned_server_and_client_cert(server_certificate()),
    ));
}
#[given("the TLS client pins the server and presents no client certificate")]
fn without_certificate(session: &mut CertificateSession) {
    let config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedCertVerifier {
            pinned: server_certificate(),
            schemes: fixture_signature_schemes(),
            mismatch: "presented server cert does not match pinned dev cert",
        }))
        .with_no_client_auth();
    session.connector = Some(TlsConnector::from(Arc::new(config)));
}
#[given("the client bounds its request wait to ten seconds")]
fn deadline(session: &mut CertificateSession) {
    session.request_timeout = Some(Duration::from_secs(10));
}
#[when("the client completes TLS and requests creation of topic {topic}")]
async fn request(session: &mut CertificateSession, topic: String) {
    let broker = session.broker.as_ref().expect("TLS broker running");
    let mut tls = mtls_connect(
        broker.addr,
        session.connector.take().expect("TLS client configured"),
        "configured TLS handshake must succeed",
    )
    .await;
    let body = create_topics_body(&topic);
    session.response = Some(if let Some(timeout) = session.request_timeout {
        timed_create_topics(
            &mut tls,
            &body,
            timeout,
            "the broker must close or respond rather than hang",
        )
        .await
    } else {
        kafka_wire::round_trip()
            .stream(&mut tls)
            .api_key(19)
            .version(7)
            .client_id(CLIENT_ID)
            .header(RequestHeaderEncoding::Flexible)
            .body(&body)
            .call()
            .await
    });
    session.tls = Some(tls);
}
#[then("the broker responds successfully through the certificate DN superuser session")]
fn authorized(session: &CertificateSession) {
    let bytes = session
        .response
        .as_ref()
        .expect("TLS request completed")
        .as_ref()
        .expect("broker responds to authenticated session");
    check_create_topics_success(bytes, "certificate DN superuser CreateTopics");
}
#[then("the unmappable certificate connection closes without any Kafka response")]
fn closed(session: &CertificateSession) {
    let response = session.response.as_ref().expect("TLS request completed");
    assert!(
        response.is_err(),
        "unmappable cert DN must not be served: {response:?}"
    );
}
#[then("the broker serves the certificate less ANONYMOUS session successfully")]
fn anonymous(session: &CertificateSession) {
    let bytes = session
        .response
        .as_ref()
        .expect("TLS request completed")
        .as_ref()
        .expect("broker answers certificate less connection");
    check_create_topics_success(bytes, "ANONYMOUS session must be served");
}
async fn shutdown(session: CertificateSession) {
    let MtlsBroker {
        handle,
        log_dir,
        pem_dir,
        ..
    } = session.broker.expect("TLS broker started");
    handle.shutdown().await;
    drop((log_dir, pem_dir));
}

#[scenario(
    path = "tests/features/migrated/tests/mtls.feature",
    name = "A trusted client certificate authorizes its Subject DN superuser"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_principal_is_cert_dn_and_super_user_bypass_works(session: CertificateSession) {
    shutdown(session).await;
}

#[scenario(
    path = "tests/features/migrated/tests/mtls.feature",
    name = "A trusted certificate matching no mapping rule is disconnected"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unmappable_certificate_dn_closes_the_connection(session: CertificateSession) {
    shutdown(session).await;
}

#[scenario(
    path = "tests/features/migrated/tests/mtls.feature",
    name = "An optional certificate listener serves clients that present no certificate"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_with_no_certificate_is_served_rather_than_closed(
    session: CertificateSession,
) {
    shutdown(session).await;
}
