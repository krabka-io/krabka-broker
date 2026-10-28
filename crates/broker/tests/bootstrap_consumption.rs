//! `krabka format --add-scram` -> broker bootstrap consumption.
//!
//! Each test either runs the `krabka` CLI to produce a `log_dir` that holds a
//! `bootstrap.records.bin` file, or writes that file directly for the
//! corruption case. It then starts a single-broker `Broker` that points at the
//! dir, and verifies that the broker's bootstrap path behaves correctly.
//!
//! The happy-path test also drives a full SASL/SCRAM-SHA-512 handshake against
//! the broker's `SASL_PLAINTEXT` listener. That proves the metadata image can
//! answer a query for the seeded credential as soon as `Broker::start`
//! returns.
//!
//! ## Helper duplication
//!
//! `drive_sasl_scram_session` and `round_trip` are copied verbatim from
//! `tests/auth_handlers.rs`. Cargo's integration-test model gives each
//! `tests/*.rs` file its own crate root, so sharing code between two such
//! files needs either a `tests/common/mod.rs` submodule or a copy. This test
//! file is self-contained and uses only those two helpers, so a verbatim copy
//! keeps the blast radius small and leaves the auth test file, over 1500
//! lines, untouched.
#![cfg(test)]

mod kafka_wire;

mod support;

use {
    crate::{
        kafka_wire::ScramAuthenticationSetup,
        support::{listeners, wire::HeaderEncoding as RequestHeaderEncoding},
    },
    assert2::assert,
    krabka_broker::{Broker, BrokerConfig},
    krabka_protocol::owned::{
        metadata_request::MetadataRequest, metadata_response::MetadataResponse,
    },
    krabka_security::{ListenerProtocol, SaslMechanism},
    rustls::crypto::aws_lc_rs,
    std::{fs, io, net::SocketAddr, path::Path},
    tokio::net::TcpStream,
};

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-bootstrap-test";

/// Formats `log_dir`, seeding one SCRAM credential.
///
/// Calls the formatter in process rather than spawning it. The formatting is
/// setup for the test below, not the thing under test -- `krabka-format`'s own
/// `format_smoke` suite runs the real binary -- and a subprocess would need a
/// Cargo working tree to build from, which a Bazel test sandbox does not have.
async fn run_krabka_format(log_dir: &Path, add_scram: &str) {
    let code = krabka_format::run_from_args([
        "krabka-format",
        "--log-dir",
        log_dir.to_str().unwrap(),
        "--node-id",
        "1",
        "--add-scram",
        add_scram,
    ])
    .await;
    assert!(code == 0, "krabka-format exited {code}");
}

use krabka_broker::{BrokerError, BrokerHandle};
struct BootstrapConsumption {
    directory: Option<tempfile::TempDir>,
    config: Option<BrokerConfig>,
    started: Option<Result<BrokerHandle, BrokerError>>,
    authenticated: Option<io::Result<()>>,
}
#[rstest::fixture]
fn bootstrap() -> BootstrapConsumption {
    // Start this scenario with fresh inputs and observations.

    BootstrapConsumption {
        directory: None,
        config: None,
        started: None,
        authenticated: None,
    }
}
#[rstest_bdd_macros::given(
    "a formatted bootstrap log seeds alice wonderland with SCRAM SHA 512 at 4096 iterations"
)]
async fn scram_bootstrap_input(bootstrap: &mut BootstrapConsumption) {
    // The broker installs the rustls crypto provider in `Broker::start`,
    // but the SCRAM client side of this test also performs PBKDF2 / SHA
    // through the same provider. Install it defensively so the test is
    // order-independent. `.ok()` swallows `AlreadySet`.
    let _ = aws_lc_rs::default_provider().install_default();

    let dir = tempfile::tempdir().unwrap();
    // Format the log_dir into the *child* `boot` subdir — the CLI refuses
    // to overwrite a non-empty directory, and `tempfile::tempdir()` returns
    // a path whose parent already exists.
    let boot_dir = dir.path().join("boot");
    run_krabka_format(
        &boot_dir,
        "SCRAM-SHA-512=[name=alice,password=wonderland,iterations=4096]",
    )
    .await;

    let mut cfg = BrokerConfig::for_tests(boot_dir.clone());
    cfg.listeners = vec![listeners::loopback_listener(
        "SASL_PLAINTEXT",
        ListenerProtocol::SaslPlaintext,
    )];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".into();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::ScramSha512];
    cfg.bootstrap_mode = krabka_broker::BootstrapMode::Bootstrap;

    bootstrap.directory = Some(dir);
    bootstrap.config = Some(cfg);
}
#[rstest_bdd_macros::given(
    "bootstrap records contain bytes that are not a length prefixed metadata record"
)]
fn corrupt_bootstrap_input(bootstrap: &mut BootstrapConsumption) {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("bootstrap.records.bin"),
        b"this is not a length-prefixed metadata record",
    )
    .unwrap();
    let cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    bootstrap.directory = Some(dir);
    bootstrap.config = Some(cfg);
}
#[rstest_bdd_macros::given("a fresh log directory has no bootstrap records file")]
fn absent_bootstrap_input(bootstrap: &mut BootstrapConsumption) {
    let dir = tempfile::tempdir().unwrap();
    bootstrap.config = Some(BrokerConfig::for_tests(dir.path().to_path_buf()));
    bootstrap.directory = Some(dir);
}
#[rstest_bdd_macros::when("the broker consumes its configured bootstrap log at startup")]
async fn consume_bootstrap_log(bootstrap: &mut BootstrapConsumption) {
    bootstrap.started =
        Some(Broker::start(bootstrap.config.take().expect("bootstrap configuration")).await);
}
#[rstest_bdd_macros::then("the broker starts successfully")]
fn bootstrap_broker_started(bootstrap: &BootstrapConsumption) {
    assert!(
        bootstrap
            .started
            .as_ref()
            .expect("startup attempted")
            .is_ok()
    );
}
#[rstest_bdd_macros::when("alice authenticates through SCRAM and requests Metadata")]
async fn bootstrap_scram_login(bootstrap: &mut BootstrapConsumption) {
    let addr = bootstrap
        .started
        .as_ref()
        .expect("startup attempted")
        .as_ref()
        .expect("broker started")
        .listen_addr();
    bootstrap.authenticated = Some(drive_sasl_scram_session(addr, "alice", "wonderland").await);
}
#[rstest_bdd_macros::then(
    "the seeded credential authenticates and serves Metadata immediately after startup"
)]
fn bootstrap_scram_available(bootstrap: &BootstrapConsumption) {
    let result = bootstrap
        .authenticated
        .as_ref()
        .expect("SCRAM login attempted");
    assert!(
        result.is_ok(),
        "alice/wonderland should authenticate via SCRAM: {result:?}"
    );
}
#[rstest_bdd_macros::then("startup is refused specifically with a BootstrapFile error")]
async fn bootstrap_corruption_refused(bootstrap: &mut BootstrapConsumption) {
    let result = bootstrap.started.take().expect("startup attempted");
    match result {
        Err(krabka_broker::BrokerError::BootstrapFile { .. }) => {}
        Err(other) => panic!("expected BootstrapFile error, got {other:?}"),
        Ok(handle) => {
            handle.shutdown().await;
            panic!("expected BootstrapFile error, broker started successfully");
        }
    }
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/bootstrap_consumption.feature",
    name = "bootstrap records provisions scram user"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_records_provisions_scram_user(bootstrap: BootstrapConsumption) {
    if let Some(Ok(handle)) = bootstrap.started {
        handle.shutdown().await;
    }
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/bootstrap_consumption.feature",
    name = "corrupt bootstrap refuses start"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_bootstrap_refuses_start(bootstrap: BootstrapConsumption) {
    if let Some(Ok(handle)) = bootstrap.started {
        handle.shutdown().await;
    }
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/bootstrap_consumption.feature",
    name = "bootstrap absent legacy path"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_absent_legacy_path(bootstrap: BootstrapConsumption) {
    if let Some(Ok(handle)) = bootstrap.started {
        handle.shutdown().await;
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Helpers copied verbatim from `tests/auth_handlers.rs`.
// Cargo's integration-test model gives each `tests/*.rs` its own crate
// root; sharing helpers requires a `tests/common/mod.rs` submodule. The
// verbatim copy is intentional — see file-level docs above.
// ─────────────────────────────────────────────────────────────────────────

/// Drives a complete SASL/SCRAM-SHA-512 session against a `SASL_PLAINTEXT`
/// listener.
async fn drive_sasl_scram_session(
    addr: SocketAddr,
    user: &str,
    password: &str,
) -> Result<(), io::Error> {
    let mut stream = TcpStream::connect(addr).await?;
    kafka_wire::sasl_scram_authenticate_on(&mut stream, &ScramAuthenticationSetup {
        client_id: CLIENT_ID,
        user,
        password: (password).into(),
        ..Default::default()
    })
    .await?;
    let md_resp: MetadataResponse = kafka_wire::exchange()
        .stream(&mut stream)
        .request(&MetadataRequest::default())
        .api_key(3)
        .version(12)
        .correlation(5)
        .client_id(CLIENT_ID)
        .header(RequestHeaderEncoding::Flexible)
        .call()
        .await?;
    if md_resp.brokers.is_empty() {
        return Err(io::Error::other("Metadata response carried no brokers"));
    }
    Ok(())
}
