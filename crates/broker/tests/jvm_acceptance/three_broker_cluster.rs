//! Three-broker `SASL_PLAINTEXT` clusters.
//!
//! A third voter is what lets the JVM `kafka-leader-election` and
//! `kafka-reassign-partitions` tools move a leader off a live node, so the
//! suites that drive those tools boot the cluster here.

use krabka_broker::{Broker, BrokerConfig};
use krabka_log::LogConfig;

use super::ports::{
    broker0_advertised, broker0_listen, broker1_advertised, broker1_listen, broker2_advertised,
    broker2_listen, controller_addr_0, controller_addr_1, controller_addr_2,
};

/// Third broker for the 3-broker `SASL_PLAINTEXT` JVM cluster.
/// Broker 2 (`node_id`=2) lives on `broker1_listen()` / `broker1_advertised()`.
/// Spawn three in-process brokers that share one inter-broker SASL credential.
///
/// * Broker 1: 0.0.0.0:9092 (data) / 0.0.0.0:9093 (controller)
/// * Broker 2: 0.0.0.0:9094 (data) / 0.0.0.0:9095 (controller)
/// * Broker 3: 0.0.0.0:9096 (data) / 0.0.0.0:9097 (controller)
///
/// Returns `(h1, h2, h3, cfg1, cfg2, cfg3, dir1, dir2, dir3)`.
/// A caller needs the `cfg*` values to revive a broker after shutdown.
/// Pass them with `BootstrapMode::Rejoin`.
pub(crate) async fn start_three_broker_sasl_plaintext_jvm_cluster(
    admin: &str,
    admin_pass: &str,
) -> (
    krabka_broker::BrokerHandle,
    krabka_broker::BrokerHandle,
    krabka_broker::BrokerHandle,
    BrokerConfig,
    BrokerConfig,
    BrokerConfig,
    tempfile::TempDir,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    use krabka_broker::config::{InterBrokerCredentials, ListenerSpec};
    use krabka_security::{ListenerProtocol, SaslMechanism};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("krabka_broker=info")),
        )
        .with_test_writer()
        .try_init();
    let _ = rustls::crypto::ring::default_provider().install_default();

    let dir0 = tempfile::tempdir().expect("tempdir b0");
    let dir1 = tempfile::tempdir().expect("tempdir b1");
    let dir2 = tempfile::tempdir().expect("tempdir b2");

    let listen0: std::net::SocketAddr = broker0_listen().parse().expect("static addr");
    let listen1: std::net::SocketAddr = broker1_listen().parse().expect("static addr");
    let listen2: std::net::SocketAddr = broker2_listen().parse().expect("static addr");

    let ctrl0: std::net::SocketAddr = controller_addr_0().parse().expect("allocated addr");
    let ctrl1: std::net::SocketAddr = controller_addr_1().parse().expect("allocated addr");
    let ctrl2: std::net::SocketAddr = controller_addr_2().parse().expect("allocated addr");

    let voters = [(1_u64, ctrl0), (2_u64, ctrl1), (3_u64, ctrl2)];

    let mk_cfg = |idx: u64,
                  listen: std::net::SocketAddr,
                  ctrl: std::net::SocketAddr,
                  advertised: &str,
                  log_dir: std::path::PathBuf,
                  mode: krabka_broker::BootstrapMode|
     -> BrokerConfig {
        let mut cfg = BrokerConfig {
            broker_id: i32::try_from(idx).unwrap(),
            listen_addr: listen,
            advertised_listener: advertised.to_string(),
            log_dir,
            log_config: LogConfig::default(),
            node_id: krabka_broker::NodeId(idx),
            controller_listen_addr: ctrl,
            controller_quorum_voters: voters
                .iter()
                .map(|(id, a)| (krabka_broker::NodeId(*id), a.to_string()))
                .collect(),
            heartbeat_interval: krabka_units::millis(3_000),
            heartbeat_timeout: krabka_units::millis(9_000),
            replica_lag_time_max: krabka_units::millis(30_000),
            controller_election_timeout: krabka_units::secs(5),
            controller_heartbeat_interval: krabka_units::millis(500),
            bootstrap_mode: mode,
            listeners: vec![ListenerSpec {
                name: "SASL_PLAINTEXT".to_string(),
                bind_addr: listen,
                advertised: advertised.to_string(),
                protocol: ListenerProtocol::SaslPlaintext,
                tls_config: None,
                sasl_mechanisms: None,
                principal_mapper: krabka_broker::SslPrincipalMapper::default(),
            }],
            inter_broker_listener_name: "SASL_PLAINTEXT".to_string(),
            enabled_sasl_mechanisms: vec![SaslMechanism::Plain],
            super_users: maplit::hashset! {admin.to_string()},
            inter_broker_credentials: Some(InterBrokerCredentials::Plain {
                username: admin.to_string(),
                password: admin_pass.to_string(),
            }),
            ..BrokerConfig::default()
        };
        cfg.authorizer = std::sync::Arc::new(krabka_broker::authorizer::SimpleAclAuthorizer::new(
            cfg.super_users.clone(),
        ));
        cfg.plain_credentials
            .insert(admin.to_string(), admin_pass.to_string());
        cfg
    };

    let cfg0 = mk_cfg(
        1,
        listen0,
        ctrl0,
        broker0_advertised(),
        dir0.path().to_path_buf(),
        krabka_broker::BootstrapMode::Bootstrap,
    );
    // Static cold-boot (KIP-595): every voter is seeded with the full static
    // `controller_quorum_voters` set in Bootstrap mode, so the quorum forms by
    // electing among the concurrently-booting voters. `Broker::start` blocks
    // until its controller sees a committed leader, and a leader needs a
    // majority of the static set up and dialable — so awaiting broker 0 alone
    // would deadlock. Spawn all starts concurrently and join them. (The old
    // openraft bootstrap-then-join via add_learner/change_membership is gone
    // with the static voter set.)
    let cfg1 = mk_cfg(
        2,
        listen1,
        ctrl1,
        broker1_advertised(),
        dir1.path().to_path_buf(),
        krabka_broker::BootstrapMode::Bootstrap,
    );
    let cfg2 = mk_cfg(
        3,
        listen2,
        ctrl2,
        broker2_advertised(),
        dir2.path().to_path_buf(),
        krabka_broker::BootstrapMode::Bootstrap,
    );
    let h0 = tokio::spawn({
        let c = cfg0.clone();
        async move { Broker::start(c).await }
    });
    let h1 = tokio::spawn({
        let c = cfg1.clone();
        async move { Broker::start(c).await }
    });
    let h2 = tokio::spawn({
        let c = cfg2.clone();
        async move { Broker::start(c).await }
    });
    let broker0 = h0
        .await
        .expect("broker 0 spawn join")
        .expect("start broker 0");
    let broker1 = h1
        .await
        .expect("broker 1 spawn join")
        .expect("broker 1 start");
    let broker2 = h2
        .await
        .expect("broker 2 spawn join")
        .expect("broker 2 start");

    eprintln!(
        "KRABKA[test] three-broker sasl: b0={listen} adv={bootstrap} b1={listen_b1} adv={bootstrap_b1} b2={listen_b2} adv={bootstrap_b2}",
        bootstrap = broker0_advertised(),
        bootstrap_b1 = broker1_advertised(),
        bootstrap_b2 = broker2_advertised(),
        listen = broker0_listen(),
        listen_b1 = broker1_listen(),
        listen_b2 = broker2_listen()
    );
    (
        broker0, broker1, broker2, cfg0, cfg1, cfg2, dir0, dir1, dir2,
    )
}

/// Like [`start_three_broker_sasl_plaintext_jvm_cluster`] but also provisions
/// `extra_users` as PLAIN credentials on all three brokers.
///
/// Returns `(h1, h2, h3, cfg1, cfg2, cfg3, dir1, dir2, dir3)`.
pub(crate) async fn start_three_broker_sasl_plaintext_jvm_cluster_with_users(
    admin: &str,
    admin_pass: &str,
    extra_users: &[(&str, &str)],
) -> (
    krabka_broker::BrokerHandle,
    krabka_broker::BrokerHandle,
    krabka_broker::BrokerHandle,
    BrokerConfig,
    BrokerConfig,
    BrokerConfig,
    tempfile::TempDir,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    use krabka_broker::config::{InterBrokerCredentials, ListenerSpec};
    use krabka_security::{ListenerProtocol, SaslMechanism};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("krabka_broker=info")),
        )
        .with_test_writer()
        .try_init();
    let _ = rustls::crypto::ring::default_provider().install_default();

    let dir0 = tempfile::tempdir().expect("tempdir b0");
    let dir1 = tempfile::tempdir().expect("tempdir b1");
    let dir2 = tempfile::tempdir().expect("tempdir b2");

    let listen0: std::net::SocketAddr = broker0_listen().parse().expect("static addr");
    let listen1: std::net::SocketAddr = broker1_listen().parse().expect("static addr");
    let listen2: std::net::SocketAddr = broker2_listen().parse().expect("static addr");

    let ctrl0: std::net::SocketAddr = controller_addr_0().parse().expect("allocated addr");
    let ctrl1: std::net::SocketAddr = controller_addr_1().parse().expect("allocated addr");
    let ctrl2: std::net::SocketAddr = controller_addr_2().parse().expect("allocated addr");

    let voters = [(1_u64, ctrl0), (2_u64, ctrl1), (3_u64, ctrl2)];

    let mk_cfg = |idx: u64,
                  listen: std::net::SocketAddr,
                  ctrl: std::net::SocketAddr,
                  advertised: &str,
                  log_dir: std::path::PathBuf,
                  mode: krabka_broker::BootstrapMode|
     -> BrokerConfig {
        let mut cfg = BrokerConfig {
            broker_id: i32::try_from(idx).unwrap(),
            listen_addr: listen,
            advertised_listener: advertised.to_string(),
            log_dir,
            log_config: LogConfig::default(),
            node_id: krabka_broker::NodeId(idx),
            controller_listen_addr: ctrl,
            controller_quorum_voters: voters
                .iter()
                .map(|(id, a)| (krabka_broker::NodeId(*id), a.to_string()))
                .collect(),
            heartbeat_interval: krabka_units::millis(3_000),
            heartbeat_timeout: krabka_units::millis(9_000),
            replica_lag_time_max: krabka_units::millis(30_000),
            controller_election_timeout: krabka_units::secs(5),
            controller_heartbeat_interval: krabka_units::millis(500),
            bootstrap_mode: mode,
            listeners: vec![ListenerSpec {
                name: "SASL_PLAINTEXT".to_string(),
                bind_addr: listen,
                advertised: advertised.to_string(),
                protocol: ListenerProtocol::SaslPlaintext,
                tls_config: None,
                sasl_mechanisms: None,
                principal_mapper: krabka_broker::SslPrincipalMapper::default(),
            }],
            inter_broker_listener_name: "SASL_PLAINTEXT".to_string(),
            enabled_sasl_mechanisms: vec![SaslMechanism::Plain],
            super_users: maplit::hashset! {admin.to_string()},
            inter_broker_credentials: Some(InterBrokerCredentials::Plain {
                username: admin.to_string(),
                password: admin_pass.to_string(),
            }),
            ..BrokerConfig::default()
        };
        cfg.authorizer = std::sync::Arc::new(krabka_broker::authorizer::SimpleAclAuthorizer::new(
            cfg.super_users.clone(),
        ));
        cfg.plain_credentials
            .insert(admin.to_string(), admin_pass.to_string());
        for (u, p) in extra_users {
            cfg.plain_credentials
                .insert((*u).to_string(), (*p).to_string());
        }
        cfg
    };

    let cfg0 = mk_cfg(
        1,
        listen0,
        ctrl0,
        broker0_advertised(),
        dir0.path().to_path_buf(),
        krabka_broker::BootstrapMode::Bootstrap,
    );
    // Static cold-boot (KIP-595): every voter is seeded with the full static
    // `controller_quorum_voters` set in Bootstrap mode, so the quorum forms by
    // electing among the concurrently-booting voters. `Broker::start` blocks
    // until its controller sees a committed leader, and a leader needs a
    // majority of the static set up and dialable — so awaiting broker 0 alone
    // would deadlock. Spawn all starts concurrently and join them. (The old
    // openraft bootstrap-then-join via add_learner/change_membership is gone
    // with the static voter set.)
    let cfg1 = mk_cfg(
        2,
        listen1,
        ctrl1,
        broker1_advertised(),
        dir1.path().to_path_buf(),
        krabka_broker::BootstrapMode::Bootstrap,
    );
    let cfg2 = mk_cfg(
        3,
        listen2,
        ctrl2,
        broker2_advertised(),
        dir2.path().to_path_buf(),
        krabka_broker::BootstrapMode::Bootstrap,
    );
    let h0 = tokio::spawn({
        let c = cfg0.clone();
        async move { Broker::start(c).await }
    });
    let h1 = tokio::spawn({
        let c = cfg1.clone();
        async move { Broker::start(c).await }
    });
    let h2 = tokio::spawn({
        let c = cfg2.clone();
        async move { Broker::start(c).await }
    });
    let broker0 = h0
        .await
        .expect("broker 0 spawn join")
        .expect("start broker 0");
    let broker1 = h1
        .await
        .expect("broker 1 spawn join")
        .expect("broker 1 start");
    let broker2 = h2
        .await
        .expect("broker 2 spawn join")
        .expect("broker 2 start");

    eprintln!(
        "KRABKA[test] three-broker sasl (with_users): b0={listen} adv={bootstrap} b1={listen_b1} adv={bootstrap_b1} b2={listen_b2} adv={bootstrap_b2}",
        bootstrap = broker0_advertised(),
        bootstrap_b1 = broker1_advertised(),
        bootstrap_b2 = broker2_advertised(),
        listen = broker0_listen(),
        listen_b1 = broker1_listen(),
        listen_b2 = broker2_listen()
    );
    (
        broker0, broker1, broker2, cfg0, cfg1, cfg2, dir0, dir1, dir2,
    )
}
