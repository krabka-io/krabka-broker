//! Plaintext single-broker bring-up for the JVM acceptance suites.
//!
//! These helpers start one in-process broker on the allocated client listener,
//! which is what a suite needs when it only drives the JVM tools against a
//! single node.

use std::path::Path;

use krabka_broker::{BootstrapMode, Broker, BrokerConfig};
use krabka_log::LogConfig;

use super::ports::{broker0_advertised, broker0_listen, controller_addr_0};

/// Spawn the broker on `broker0_listen()`. The advertised listener is
/// an allocated port. Inside the cp-kafka containers, the test
/// adds a hosts entry that points that name at the bridge gateway.
pub(crate) async fn start_host_broker() -> (krabka_broker::BrokerHandle, tempfile::TempDir) {
    start_host_broker_with(|_| {}).await
}

/// [`start_host_broker`], letting the caller adjust the config first.
///
/// A suite that drives one of the coordinators needs its internal topic to be
/// hostable here: the defaults ask for 50 partitions at replication factor 3,
/// which one node cannot satisfy, so the partition a key hashes to may never
/// open.
pub(crate) async fn start_host_broker_with(
    adjust: impl FnOnce(&mut BrokerConfig),
) -> (krabka_broker::BrokerHandle, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let handle = start_host_broker_in_with(dir.path(), adjust).await;
    (handle, dir)
}

/// [`start_host_broker`] on a log directory the caller owns.
///
/// A restart case shuts the handle down and calls this again with the same
/// path, which the other two helpers cannot express: they own the temporary
/// directory and hand it back alongside the handle, so the second boot would
/// land on an empty one.
pub(crate) async fn start_host_broker_in(dir: &Path) -> krabka_broker::BrokerHandle {
    start_host_broker_in_with(dir, |_| {}).await
}

/// The one copy of the single-node config every helper above boots.
async fn start_host_broker_in_with(
    dir: &Path,
    adjust: impl FnOnce(&mut BrokerConfig),
) -> krabka_broker::BrokerHandle {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("krabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();
    let listen_addr: std::net::SocketAddr = broker0_listen().parse().expect("static addr");
    let controller_addr: std::net::SocketAddr =
        controller_addr_0().parse().expect("allocated addr");
    let mut config = BrokerConfig {
        broker_id: 1,
        listen_addr,
        advertised_listener: broker0_advertised().into(),
        log_dir: dir.to_path_buf(),
        log_config: LogConfig::default(),
        node_id: krabka_broker::NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(krabka_broker::NodeId(1), controller_addr.to_string())],
        heartbeat_interval: krabka_units::millis(3_000),
        heartbeat_timeout: krabka_units::millis(9_000),
        replica_lag_time_max: krabka_units::millis(30_000),
        controller_election_timeout: krabka_units::secs(5),
        controller_heartbeat_interval: krabka_units::millis(500),
        bootstrap_mode: bootstrap_mode(dir),
        ..BrokerConfig::default()
    };
    adjust(&mut config);
    let handle = Broker::start(config).await.expect("start broker");
    eprintln!(
        "KRABKA[test] broker started listen={listen} advertised={bootstrap}",
        bootstrap = broker0_advertised(),
        listen = broker0_listen()
    );
    tracing::info!(listen = %broker0_listen(), advertised = %broker0_advertised(), "broker started for jvm acceptance");
    handle
}

/// `Bootstrap` on a fresh directory, `Rejoin` once a committed raft log is
/// there, the same choice `detect_bootstrap_mode` makes in the broker binary.
///
/// A first boot is always the former. The second boot of a restart case is the
/// latter, and asking for `Bootstrap` on top of a non-empty metadata log is
/// rejected at controller start.
fn bootstrap_mode(dir: &Path) -> BootstrapMode {
    if krabka_raft::metadata_log_nonempty(&dir.join("__cluster_metadata")) {
        BootstrapMode::Rejoin
    } else {
        BootstrapMode::Bootstrap
    }
}

/// Like [`start_host_broker`] but configures a second JBOD data directory
/// (KIP-113). Returns the two host-side log dirs with the handle, so
/// the test can assert which absolute paths `DescribeLogDirs` reports.
pub(crate) async fn start_host_broker_jbod() -> (
    krabka_broker::BrokerHandle,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("krabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();
    let primary = tempfile::tempdir().expect("tempdir");
    let extra = tempfile::tempdir().expect("tempdir");
    let listen_addr: std::net::SocketAddr = broker0_listen().parse().expect("static addr");
    let controller_addr: std::net::SocketAddr =
        controller_addr_0().parse().expect("allocated addr");
    let config = BrokerConfig {
        broker_id: 1,
        listen_addr,
        advertised_listener: broker0_advertised().into(),
        log_dir: primary.path().to_path_buf(),
        extra_log_dirs: vec![extra.path().to_path_buf()],
        log_config: LogConfig::default(),
        node_id: krabka_broker::NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(krabka_broker::NodeId(1), controller_addr.to_string())],
        heartbeat_interval: krabka_units::millis(3_000),
        heartbeat_timeout: krabka_units::millis(9_000),
        replica_lag_time_max: krabka_units::millis(30_000),
        controller_election_timeout: krabka_units::secs(5),
        controller_heartbeat_interval: krabka_units::millis(500),
        bootstrap_mode: krabka_broker::BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config).await.expect("start broker");
    (handle, primary, extra)
}
