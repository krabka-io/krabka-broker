//! Broker bring-up with the S3 tiered-storage backend pointed at `MinIO`.
//!
//! The helper shortens the `RemoteLogManager` tick so a copy and a local
//! eviction happen inside the test's wall clock rather than at the production
//! default.

use krabka_broker::{Broker, BrokerConfig};
use krabka_log::LogConfig;

use super::ports::{broker0_advertised, broker0_listen, controller_addr_0};

/// Same shape as [`start_host_broker`] but with the S3 tiered-storage
/// backend wired in and a lower `RemoteLogManager` tick, so the acceptance
/// loop completes in seconds rather than at the 30s production default.
///
/// `rlmm` selects the [`krabka_broker::RlmmKind`]. Pass
/// `RlmmKind::InMemory` for tests that only need a single-run round-trip.
/// Pass `RlmmKind::TopicBacked(…)` when the test needs durable metadata that
/// survives a broker restart.
///
/// Returns the broker handle, the temp dir, and the `BrokerConfig` so the
/// caller can reuse it for a restart. The caller must keep the temp dir
/// alive.
pub(crate) fn start_host_broker_with_minio_tier(
    s3: krabka_remote_storage::S3Config,
    rlmm: krabka_broker::RlmmKind,
) -> impl std::future::Future<
    Output = (
        krabka_broker::BrokerHandle,
        tempfile::TempDir,
        krabka_broker::BrokerConfig,
    ),
> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("krabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().expect("tempdir");
    let listen_addr: std::net::SocketAddr = broker0_listen().parse().expect("static addr");
    let controller_addr: std::net::SocketAddr =
        controller_addr_0().parse().expect("allocated addr");
    let config = BrokerConfig {
        broker_id: 1,
        listen_addr,
        advertised_listener: broker0_advertised().into(),
        log_dir: dir.path().to_path_buf(),
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
        remote_storage_backend: Some(krabka_broker::RemoteStorageBackend::S3(s3)),
        // 1s tick so the producer's sealed segments reach S3 (and the
        // local-retention pass evicts them) within the test's wall clock.
        remote_log_manager_interval: krabka_units::secs(1),
        remote_log_metadata: rlmm,
        ..BrokerConfig::default()
    };
    Box::pin(async move {
        let handle = Broker::start(config.clone()).await.expect("start broker");
        eprintln!(
            "KRABKA[test] broker started listen={listen} advertised={bootstrap} (tiered S3 backend)",
            bootstrap = broker0_advertised(),
            listen = broker0_listen()
        );
        (handle, dir, config)
    })
}
