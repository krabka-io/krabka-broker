#![cfg(test)]

//! TLS hot-reload.
//!
//! This test starts a broker with cert A and completes a handshake against
//! it. It then overwrites the cert files with cert B, calls
//! [`krabka_broker::BrokerHandle::reload_tls`], and verifies that the next
//! handshake serves cert B.
//!
//! The client pins exact DER blobs, so the assertion is "the server presented
//! cert X". The A cert cannot satisfy a B-pinned verifier, and the B cert
//! cannot satisfy an A-pinned verifier.

mod support;

use {
    crate::support::tls::{
        CertificateDer, ClientAuthMode, ClientConfig, PemObject as _, PinnedCertVerifier,
        ServerName, TcpStream, TlsConfig, TlsConnector, fixture_signature_schemes, ssl_config,
    },
    assert2::assert,
    krabka_broker::Broker,
    rustls::crypto::aws_lc_rs,
    std::{
        fs, io,
        net::SocketAddr,
        path::{Path, PathBuf},
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::time,
};

const DEV_CERT_A: &str = include_str!("fixtures/security/dev_cert.pem");
const DEV_KEY_A: &str = include_str!("fixtures/security/dev_key.pem");
const DEV_CERT_B: &str = include_str!("fixtures/security/dev_cert_alt.pem");
const DEV_KEY_B: &str = include_str!("fixtures/security/dev_key_alt.pem");

fn write(dir: &Path, name: &str, body: &str) -> PathBuf {
    let p = dir.join(name);
    fs::write(&p, body).unwrap();
    p
}

fn pinned_client_config(pinned: CertificateDer<'static>) -> Arc<ClientConfig> {
    let cfg = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedCertVerifier {
            pinned,
            schemes: fixture_signature_schemes().into_iter().take(6).collect(),
            mismatch: "presented server cert does not match pinned cert",
        }))
        .with_no_client_auth();
    Arc::new(cfg)
}

fn cert_der_from_pem(pem: &str) -> CertificateDer<'static> {
    CertificateDer::pem_slice_iter(pem.as_bytes())
        .next()
        .expect("at least one cert in fixture")
        .expect("cert parses")
}

/// Tries a TLS handshake against `addr` and verifies the server cert against
/// `pinned`. It returns `Ok(())` on a successful handshake, and `Err` in every
/// other case. Only the completed negotiation matters, so it drops the
/// connection at once afterwards.
async fn handshake_against(
    addr: SocketAddr,
    pinned: CertificateDer<'static>,
) -> Result<(), io::Error> {
    let client_cfg = pinned_client_config(pinned);
    let connector = TlsConnector::from(client_cfg);
    let tcp = TcpStream::connect(addr).await?;
    let server_name = ServerName::try_from("krabka-dev").unwrap();
    connector
        .connect(server_name, tcp)
        .await
        .map(|_| ())
        .map_err(io::Error::other)
}

/// The directories a case keeps alive, the certificate and key paths it
/// rotates, and the broker config that serves them.
struct CertFixture {
    log_dir: tempfile::TempDir,
    pem_dir: tempfile::TempDir,
    cert_path: PathBuf,
    key_path: PathBuf,
    cfg: krabka_broker::BrokerConfig,
}

fn cert_fixture() -> CertFixture {
    let log_dir = tempfile::tempdir().unwrap();
    let pem_dir = tempfile::tempdir().unwrap();
    let cert_path = write(pem_dir.path(), "cert.pem", DEV_CERT_A);
    let key_path = write(pem_dir.path(), "key.pem", DEV_KEY_A);

    let cfg = ssl_config(log_dir.path().to_path_buf(), TlsConfig {
        cert_chain_path: cert_path.clone(),
        private_key_path: key_path.clone(),
        trust_roots_path: None,
        client_ca_path: None,
        client_auth: ClientAuthMode::Disabled,
    });
    CertFixture {
        log_dir,
        pem_dir,
        cert_path,
        key_path,
        cfg,
    }
}

use {
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

#[derive(Default)]
struct CertificateRotation {
    broker: Option<BrokerHandle>,
    directories: Option<(TempDir, TempDir)>,
    cert_path: PathBuf,
    key_path: PathBuf,
    handshake: Option<Result<(), io::Error>>,
    reload: Option<Result<(), String>>,
}

#[fixture]
fn rotation() -> CertificateRotation {
    // Start this scenario with fresh inputs and observations.

    CertificateRotation {
        broker: None,
        directories: None,
        cert_path: PathBuf::new(),
        key_path: PathBuf::new(),
        handshake: None,
        reload: None,
    }
}

fn certificate(name: &str) -> CertificateDer<'static> {
    cert_der_from_pem(match name {
        "A" => DEV_CERT_A,
        "B" => DEV_CERT_B,
        _ => panic!("unknown fixture certificate {name}"),
    })
}

#[given("a TLS broker serving certificate A with watcher interval {interval} ms")]
async fn serve_a(rotation: &mut CertificateRotation, interval: u32) {
    let _ = aws_lc_rs::default_provider().install_default();
    let CertFixture {
        log_dir,
        pem_dir,
        cert_path,
        key_path,
        mut cfg,
    } = cert_fixture();
    cfg.tls_reload_interval = krabka_units::millis(interval);
    rotation.broker = Some(Broker::start(cfg).await.expect("broker start"));
    rotation.directories = Some((log_dir, pem_dir));
    rotation.cert_path = cert_path;
    rotation.key_path = key_path;
}

#[when("a fresh TLS client pins certificate {name}")]
async fn pinned_handshake(rotation: &mut CertificateRotation, name: String) {
    rotation.handshake = Some(
        handshake_against(
            rotation.broker.as_ref().unwrap().listen_addr(),
            certificate(&name),
        )
        .await,
    );
}

#[then("the TLS handshake is accepted")]
fn accepted(rotation: &CertificateRotation) {
    assert!(
        rotation.handshake.as_ref().unwrap().is_ok(),
        "TLS handshake failed: {:?}",
        rotation.handshake
    );
}

#[then("the TLS handshake is rejected")]
fn rejected(rotation: &CertificateRotation) {
    assert!(
        rotation.handshake.as_ref().unwrap().is_err(),
        "unexpected TLS acceptance"
    );
}

#[when("the certificate and key files are replaced with B after {delay} ms")]
async fn replace_files(rotation: &CertificateRotation, delay: u64) {
    if delay > 0 {
        time::sleep(Duration::from_millis(delay)).await;
    }
    fs::write(&rotation.cert_path, DEV_CERT_B).unwrap();
    fs::write(&rotation.key_path, DEV_KEY_B).unwrap();
}

#[when("the broker explicitly reloads TLS")]
fn reload(rotation: &mut CertificateRotation) {
    rotation.reload = Some(
        rotation
            .broker
            .as_ref()
            .unwrap()
            .reload_tls()
            .map_err(|error| error.to_string()),
    );
}

#[then("the explicit reload succeeds")]
fn reloaded(rotation: &CertificateRotation) {
    assert!(
        rotation.reload.as_ref().unwrap().is_ok(),
        "reload failed: {:?}",
        rotation.reload
    );
}

#[when("fresh B-pinned handshakes retry every {retry} ms for at most {budget} ms")]
async fn await_watcher(rotation: &mut CertificateRotation, retry: u64, budget: u64) {
    let addr = rotation.broker.as_ref().unwrap().listen_addr();
    let deadline = Instant::now() + Duration::from_millis(budget);
    loop {
        let result = handshake_against(addr, certificate("B")).await;
        if result.is_ok() || Instant::now() >= deadline {
            rotation.handshake = Some(result);
            break;
        }
        time::sleep(Duration::from_millis(retry)).await;
    }
}

#[scenario(
    path = "tests/features/migrated/tests/cert_hot_reload.feature",
    name = "reload tls swaps served cert"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reload_tls_swaps_served_cert(rotation: CertificateRotation) {
    rotation.broker.unwrap().shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/cert_hot_reload.feature",
    name = "periodic watcher reloads on mtime change"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_watcher_reloads_on_mtime_change(rotation: CertificateRotation) {
    rotation.broker.unwrap().shutdown().await;
}
