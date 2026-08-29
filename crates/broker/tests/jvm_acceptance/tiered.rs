//! Broker bring-up with the S3 tiered-storage backend pointed at `MinIO`.
//!
//! Both helpers shorten the `RemoteLogManager` tick so a copy and a local
//! eviction happen inside the test's wall clock rather than at the production
//! default.

use krabka_broker::{Broker, BrokerConfig};
use krabka_log::LogConfig;

use super::ports::{
    broker0_advertised, broker0_listen, broker1_advertised, broker1_listen, controller_addr_0,
    controller_addr_1, rlmm_broker0_advertised,
};

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

// ---------------------------------------------------------------------------
// Multi-broker tiered-storage RLMM metadata sharing test.
//
// Proves that the topic-backed RLMM propagates segment metadata from the
// partition leader to a non-leader broker via `__remote_log_metadata` so that
// after a leader crash the surviving broker can serve the remote read using
// metadata it consumed from the topic — without having run the copy task itself.
//
// Network routing note for Mac + Docker Desktop
// ─────────────────────────────────────────────
// On Mac with Docker Desktop, `host.docker.internal` only resolves from
// *inside* containers (it maps to the Docker gateway IP, typically
// 192.168.65.254). From the host process itself, the name is unresolvable.
//
// The RLMM Kafka client runs in-process on the host and needs to connect to
// the broker(s) hosting `__remote_log_metadata` partitions. If those brokers
// advertise `host.docker.internal:PORT` in Metadata responses, the RLMM
// client cannot reach them.
//
// Additionally, the Krabka producer does not yet implement leader-redirect
// retry on NOT_LEADER_OR_FOLLOWER (error_code 19): when the target
// `__remote_log_metadata` partition is led by a different broker, the produce
// fails instead of transparently re-routing to the actual leader.
//
// Work-around used here: the `__remote_log_metadata` topic is created with
// `num_partitions=1, replication=1`, hosted entirely on broker 1. Both
// brokers' RLMM clients are bootstrapped explicitly to an allocated port
// (broker 1's loopback). This ensures:
//   • Broker 1's RLMM producer always reaches partition 0's leader directly.
//   • Broker 2's RLMM consumer reads partition 0 from broker 1 over loopback,
//     consuming all metadata events produced there.
// The discriminating property is preserved: broker 2 learns segment locations
// exclusively from the topic (not from in-memory state or having run the copy
// task itself), so the test still proves cross-broker durable metadata sharing.
// ---------------------------------------------------------------------------

/// Loopback address of broker 1's data listener. The RLMM clients of both
/// brokers use it as their bootstrap, so they reach the single
/// `__remote_log_metadata` partition on broker 1 without
/// Boot a two-broker plaintext cluster with an S3 tiered-storage backend and a
/// topic-backed RLMM.
///
/// Port assignment mirrors [`start_two_sasl_brokers`]:
///   broker 1: `broker0_listen()` / `broker0_advertised()`, controller `controller_addr_0()`
///   broker 2: `broker1_listen()` / `broker1_advertised()`, controller `controller_addr_1()`
///
/// The RLMM clients of both brokers bootstrap explicitly to
/// `broker0_loopback()`, broker 1's loopback. See the module-level routing note
/// above.
///
/// The heartbeat and replica-lag timers are shortened to 200 ms / 2 s / 2 s,
/// so the test detects leader failover quickly.
///
/// This function spawns both brokers concurrently and then joins them. An
/// await on broker 1 alone would deadlock, because a majority-quorum leader
/// election needs both voters up. See [`start_two_sasl_brokers`] for the
/// full explanation.
pub(crate) async fn start_two_brokers_with_minio_tier(
    s3: krabka_remote_storage::S3Config,
) -> (
    krabka_broker::BrokerHandle,
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

    let dir0 = tempfile::tempdir().expect("tempdir b0");
    let dir1 = tempfile::tempdir().expect("tempdir b1");

    let listen0: std::net::SocketAddr = broker0_listen().parse().expect("static addr");
    let listen1: std::net::SocketAddr = broker1_listen().parse().expect("static addr");
    let ctrl0: std::net::SocketAddr = controller_addr_0().parse().expect("allocated addr");
    let ctrl1: std::net::SocketAddr = controller_addr_1().parse().expect("allocated addr");
    let voters = [(1_u64, ctrl0), (2_u64, ctrl1)];

    // Both brokers point their RLMM client at broker 1's loopback so that
    // (a) broker 1's producer reaches the __remote_log_metadata partition 0
    //     leader directly without requiring host.docker.internal resolution,
    // (b) broker 2's consumer can fetch partition 0 from broker 1 over loopback.
    // `num_partitions=1` collapses all user-topic-partition metadata to a single
    // metadata partition (partition 0 = hash(...) % 1), guaranteeing the RLMM
    // producer always writes to the same partition that broker 2's consumer reads.
    // `replication=1` keeps that partition exclusively on broker 1, so both
    // RLMM clients reach it by going directly to 127.0.0.1:9092.
    let rlmm_cfg = krabka_broker::KafkaRlmmConfig {
        bootstrap: rlmm_broker0_advertised().to_string(),
        num_partitions: 1,
        replication: 1,
        snapshot_interval: krabka_units::secs(2),
        snapshot_dir: std::path::PathBuf::new(), // derived from log.dir
        security: None,
        ..krabka_broker::KafkaRlmmConfig::default()
    };

    let s3_b0 = s3.clone();
    let s3_b1 = s3.clone();
    let rlmm_b0 = rlmm_cfg.clone();
    let rlmm_b1 = rlmm_cfg.clone();

    let cfg0 = BrokerConfig {
        broker_id: 1,
        listen_addr: listen0,
        advertised_listener: broker0_advertised().to_string(),
        log_dir: dir0.path().to_path_buf(),
        log_config: LogConfig::default(),
        node_id: krabka_broker::NodeId(1),
        controller_listen_addr: ctrl0,
        controller_quorum_voters: voters
            .iter()
            .map(|(id, a)| (krabka_broker::NodeId(*id), a.to_string()))
            .collect(),
        // Accelerated timers for fast failover — matches acks_all_survives_leader_crash.
        heartbeat_interval: krabka_units::millis(200),
        heartbeat_timeout: krabka_units::millis(2_000),
        replica_lag_time_max: krabka_units::millis(2_000),
        controller_election_timeout: krabka_units::millis(500),
        controller_heartbeat_interval: krabka_units::millis(100),
        bootstrap_mode: krabka_broker::BootstrapMode::Bootstrap,
        remote_storage_backend: Some(krabka_broker::RemoteStorageBackend::S3(s3_b0)),
        remote_log_manager_interval: krabka_units::secs(1),
        remote_log_metadata: krabka_broker::RlmmKind::TopicBacked(rlmm_b0),
        ..BrokerConfig::default()
    };

    let cfg1 = BrokerConfig {
        broker_id: 2,
        listen_addr: listen1,
        advertised_listener: broker1_advertised().to_string(),
        log_dir: dir1.path().to_path_buf(),
        log_config: LogConfig::default(),
        node_id: krabka_broker::NodeId(2),
        controller_listen_addr: ctrl1,
        controller_quorum_voters: voters
            .iter()
            .map(|(id, a)| (krabka_broker::NodeId(*id), a.to_string()))
            .collect(),
        heartbeat_interval: krabka_units::millis(200),
        heartbeat_timeout: krabka_units::millis(2_000),
        replica_lag_time_max: krabka_units::millis(2_000),
        controller_election_timeout: krabka_units::millis(500),
        controller_heartbeat_interval: krabka_units::millis(100),
        bootstrap_mode: krabka_broker::BootstrapMode::Bootstrap,
        remote_storage_backend: Some(krabka_broker::RemoteStorageBackend::S3(s3_b1)),
        remote_log_manager_interval: krabka_units::secs(1),
        remote_log_metadata: krabka_broker::RlmmKind::TopicBacked(rlmm_b1),
        ..BrokerConfig::default()
    };

    // Static cold-boot (KIP-595): every voter is seeded with the full static
    // `controller_quorum_voters` set in Bootstrap mode, so the quorum forms by
    // electing among the concurrently-booting voters. `Broker::start` blocks
    // until its controller sees a committed leader, and a leader needs a
    // majority of the static set up and dialable — so awaiting broker 0 alone
    // would deadlock. Spawn all starts concurrently and join them.
    let h0 = tokio::spawn(async move { Broker::start(cfg0).await });
    let h1 = tokio::spawn(async move { Broker::start(cfg1).await });
    let broker0 = h0
        .await
        .expect("broker 0 spawn join")
        .expect("start broker 0");
    let broker1 = h1
        .await
        .expect("broker 1 spawn join")
        .expect("start broker 1");

    eprintln!(
        "KRABKA[test] two-broker tiered: b0={listen} adv={bootstrap} b1={listen_b1} adv={bootstrap_b1} \
         (MinIO S3 + topic-backed RLMM num_partitions=1 replication=1 bootstrap={rlmm_bootstrap})",
        bootstrap = broker0_advertised(),
        bootstrap_b1 = broker1_advertised(),
        listen = broker0_listen(),
        listen_b1 = broker1_listen(),
        rlmm_bootstrap = rlmm_broker0_advertised()
    );
    (broker0, broker1, dir0, dir1)
}
