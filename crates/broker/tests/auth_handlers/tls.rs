//! TLS listener coverage: a stock `tokio_rustls` client completes a
//! handshake against an SSL-only listener, and every configured listener
//! reaches the broker's self-registration record and survives a Metadata
//! round-trip.

use {
    crate::{
        DEV_CERT, DEV_KEY,
        support::{
            client::connect_owned,
            listeners,
            tls::{PinnedCertVerifier, fixture_signature_schemes, ssl_config},
        },
    },
    assert2::assert,
    krabka_broker::{Broker, BrokerConfig},
    krabka_protocol::owned::{
        metadata_request::MetadataRequest, metadata_response::MetadataResponse,
    },
    krabka_security::{ListenerProtocol, TlsConfig},
    rustls::crypto::aws_lc_rs,
    std::{
        fs, io,
        path::{Path, PathBuf},
        sync::Arc,
    },
    tokio::net::TcpStream,
    tokio_rustls::{
        TlsConnector,
        client::TlsStream,
        rustls::{
            ClientConfig,
            pki_types::{CertificateDer, ServerName, pem::PemObject as _},
        },
    },
};

/// The certificate and private key files a fixture writes.
struct PemPaths {
    cert: PathBuf,
    key: PathBuf,
}

fn write_dev_pem(dir: &Path) -> PemPaths {
    let cp = dir.join("cert.pem");
    let kp = dir.join("key.pem");
    fs::write(&cp, DEV_CERT).unwrap();
    fs::write(&kp, DEV_KEY).unwrap();
    PemPaths { cert: cp, key: kp }
}

struct TlsListener {
    files: TlsFixture,
    config: Option<BrokerConfig>,
    broker: Option<krabka_broker::BrokerHandle>,
    connector: Option<TlsConnector>,
    handshake: Option<io::Result<TlsStream<TcpStream>>>,
    metadata: Option<MetadataResponse>,
}

#[rstest::fixture]
fn tls_listener() -> TlsListener {
    // Start this scenario with fresh inputs and observations.

    TlsListener {
        files: tls_fixture(),
        config: None,
        broker: None,
        connector: None,
        handshake: None,
        metadata: None,
    }
}

#[rstest_bdd_macros::given("an SSL listener and a client pinning its development certificate")]
fn pinned_ssl_listener(tls_listener: &mut TlsListener) {
    let files = &tls_listener.files;
    tls_listener.config = Some(ssl_config(
        files.log_dir.path().to_path_buf(),
        dev_tls_config(files.cert_path.clone(), files.key_path.clone()),
    ));
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&files.cert_path)
        .expect("open dev cert")
        .collect::<Result<_, _>>()
        .expect("parse dev cert");
    let client_cfg = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedCertVerifier {
            pinned: certs.into_iter().next().expect("at least one cert"),
            schemes: fixture_signature_schemes(),
            mismatch: "presented cert does not match pinned dev cert",
        }))
        .with_no_client_auth();
    tls_listener.connector = Some(TlsConnector::from(Arc::new(client_cfg)));
}

#[rstest_bdd_macros::given("a broker configured with PLAINTEXT and SSL listeners")]
fn dual_tls_listeners(tls_listener: &mut TlsListener) {
    let files = &tls_listener.files;
    let mut cfg = BrokerConfig::for_tests(files.log_dir.path().to_path_buf());
    cfg.listeners = vec![
        listeners::listener(
            "PLAINTEXT",
            "127.0.0.1:0".parse().unwrap(),
            ListenerProtocol::Plaintext,
        ),
        listeners::listener("SSL", "0.0.0.0:0".parse().unwrap(), ListenerProtocol::Ssl),
    ];
    cfg.inter_broker_listener_name = "PLAINTEXT".to_owned();
    cfg.tls_config = Some(dev_tls_config(
        files.cert_path.clone(),
        files.key_path.clone(),
    ));
    tls_listener.config = Some(cfg);
}

#[rstest_bdd_macros::when("the configured TLS broker starts")]
async fn start_tls_listener(tls_listener: &mut TlsListener) {
    tls_listener.broker = Some(
        Broker::start(tls_listener.config.take().expect("listener configuration"))
            .await
            .expect("broker must start"),
    );
}

#[rstest_bdd_macros::when("the pinned client negotiates TLS with the SSL listener")]
async fn negotiate_pinned_tls(tls_listener: &mut TlsListener) {
    let addr = tls_listener
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    let tcp = TcpStream::connect(addr).await.unwrap();
    tls_listener.handshake = Some(
        tls_listener
            .connector
            .as_ref()
            .expect("pinned connector")
            .connect(ServerName::try_from("krabka-dev").unwrap(), tcp)
            .await,
    );
}

#[rstest_bdd_macros::then("the pinned client has an established TLS stream")]
fn tls_stream_established(tls_listener: &TlsListener) {
    assert!(
        tls_listener
            .handshake
            .as_ref()
            .expect("TLS negotiation attempted")
            .is_ok()
    );
}

#[rstest_bdd_macros::then(
    "self registration contains exactly PLAINTEXT and SSL with their protocols"
)]
async fn registered_tls_endpoints(tls_listener: &TlsListener) {
    let handle = tls_listener.broker.as_ref().expect("broker started");
    let node_id = handle.node_id();
    handle
        .wait_for_image(|img| {
            img.broker(krabka_broker::NodeId(node_id))
                .is_some_and(|broker| broker.endpoints.len() >= 2)
        })
        .await;
    let endpoints = handle.self_registration_endpoints();
    assert!(endpoints.len() == 2);
    let mut names: Vec<&str> = endpoints
        .iter()
        .map(|endpoint| endpoint.name.as_str())
        .collect();
    names.sort_unstable();
    assert!(names == vec!["PLAINTEXT", "SSL"]);
    assert!(
        endpoints
            .iter()
            .find(|endpoint| endpoint.name == "PLAINTEXT")
            .unwrap()
            .protocol
            == ListenerProtocol::Plaintext
    );
    assert!(
        endpoints
            .iter()
            .find(|endpoint| endpoint.name == "SSL")
            .unwrap()
            .protocol
            == ListenerProtocol::Ssl
    );
}

#[rstest_bdd_macros::when("a client requests Metadata through the PLAINTEXT listener")]
async fn plaintext_listener_metadata(tls_listener: &mut TlsListener) {
    let addr = tls_listener
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    let client = connect_owned(&addr.to_string(), "krabka-auth-test", "client build").await;
    tls_listener.metadata = Some(
        client
            .send(MetadataRequest::default())
            .await
            .expect("Metadata round-trip"),
    );
}

#[rstest_bdd_macros::then("Metadata includes this broker with node id 1")]
fn tls_metadata_node(tls_listener: &TlsListener) {
    let response = tls_listener.metadata.as_ref().expect("Metadata requested");
    assert!(!response.brokers.is_empty());
    assert!(response.brokers.iter().any(|broker| broker.node_id == 1));
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/tls.feature",
    name = "tls listener accepts tls handshake only"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_listener_accepts_tls_handshake_only(tls_listener: TlsListener) {
    tls_listener
        .broker
        .expect("broker started")
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/tls.feature",
    name = "metadata response carries listener endpoints"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_response_carries_listener_endpoints(tls_listener: TlsListener) {
    tls_listener
        .broker
        .expect("broker started")
        .shutdown()
        .await;
}

/// A log directory, the directory holding the dev PEM files, and their paths.
struct TlsFixture {
    log_dir: tempfile::TempDir,
    _pem_dir: tempfile::TempDir,
    cert_path: PathBuf,
    key_path: PathBuf,
}

fn dev_tls_config(cert_chain_path: PathBuf, private_key_path: PathBuf) -> TlsConfig {
    TlsConfig {
        cert_chain_path,
        private_key_path,
        trust_roots_path: None,
        client_ca_path: None,
        client_auth: krabka_security::ClientAuthMode::Disabled,
    }
}

fn tls_fixture() -> TlsFixture {
    // Installing before broker startup also permits the first client-side TLS build.
    let _ = aws_lc_rs::default_provider().install_default();
    let log_dir = tempfile::tempdir().unwrap();
    let pem_dir = tempfile::tempdir().unwrap();
    let PemPaths {
        cert: cert_path,
        key: key_path,
    } = write_dev_pem(pem_dir.path());
    TlsFixture {
        log_dir,
        _pem_dir: pem_dir,
        cert_path,
        key_path,
    }
}
