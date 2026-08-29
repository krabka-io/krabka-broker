//! Two-broker clusters that authenticate their own inter-broker traffic.
//!
//! The pair shares one SASL credential and dials its peer through the
//! advertised `host.docker.internal` name, which is the same address the JVM
//! containers use, so one metadata response serves both.

use krabka_broker::{Broker, BrokerConfig};
use krabka_log::LogConfig;

use super::ports::{
    broker0_advertised, broker0_listen, broker1_advertised, broker1_listen, controller_addr_0,
    controller_addr_1,
};

/// Host port assignments for the two-broker JVM inter-broker test. The
/// `SASL_PLAINTEXT` listener of broker 0 binds an allocated port (advertised as
/// an allocated port) and broker 1 binds an allocated port
/// (advertised as an allocated port). Inter-broker traffic flows
/// over the same listeners. Each broker uses the host's resolver to resolve
/// Spawn two in-process brokers that share a single inter-broker SASL
/// credential. Each broker has one `SASL_PLAINTEXT` listener. Both set
/// `plain_credentials[admin] = admin_pass`, so each broker can authenticate
/// to the other with the same admin identity. The inter-broker listener
/// name on both is `"SASL_PLAINTEXT"`, so the broker peers dial each
/// other's advertised host. This function sets that host to
/// `host.docker.internal:<port>`, so the JVM containers can use the same
/// metadata response.
pub(crate) async fn start_two_sasl_brokers(
    admin: &str,
    admin_pass: &str,
) -> (
    krabka_broker::BrokerHandle,
    krabka_broker::BrokerHandle,
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
    let listen0: std::net::SocketAddr = broker0_listen().parse().expect("static addr");
    let listen1: std::net::SocketAddr = broker1_listen().parse().expect("static addr");
    let ctrl0: std::net::SocketAddr = controller_addr_0().parse().expect("allocated addr");
    let ctrl1: std::net::SocketAddr = controller_addr_1().parse().expect("allocated addr");
    let voters = [(1_u64, ctrl0), (2_u64, ctrl1)];

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
    let h0 = tokio::spawn(async move { Broker::start(cfg0).await });
    let h1 = tokio::spawn(async move { Broker::start(cfg1).await });
    let broker0 = h0
        .await
        .expect("broker 0 spawn join")
        .expect("start broker 0");
    let broker1 = h1
        .await
        .expect("broker 1 spawn join")
        .expect("broker 1 start");

    eprintln!(
        "KRABKA[test] two-broker sasl: b0={listen} adv={bootstrap} b1={listen_b1} adv={bootstrap_b1}",
        bootstrap = broker0_advertised(),
        bootstrap_b1 = broker1_advertised(),
        listen = broker0_listen(),
        listen_b1 = broker1_listen()
    );
    (broker0, broker1, dir0, dir1)
}

/// Spawn two in-process brokers that share an inter-broker SASL
/// credential AND both terminate TLS on the data plane and the controller
/// quorum listener. Mirrors [`start_two_sasl_brokers`] but with the
/// `SASL_SSL` listener protocol and `controller_listener_protocol = ctrl`,
/// which is usually `ListenerProtocol::SaslSsl`. Each broker advertises
/// `host.docker.internal:<port>` so the JVM containers can reach them with
/// `--add-host=host.docker.internal:host-gateway` AND so each broker can
/// dial its peer with the same host name.
pub(crate) async fn start_two_sasl_ssl_brokers_with_controller_protocol(
    ctrl_protocol: krabka_security::ListenerProtocol,
    admin: &str,
    admin_pass: &str,
) -> (
    krabka_broker::BrokerHandle,
    krabka_broker::BrokerHandle,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    use krabka_broker::config::{InterBrokerCredentials, ListenerSpec};
    use krabka_security::{ListenerProtocol, SaslMechanism, TlsConfig};

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
    let listen0: std::net::SocketAddr = broker0_listen().parse().expect("static addr");
    let listen1: std::net::SocketAddr = broker1_listen().parse().expect("static addr");
    let ctrl0: std::net::SocketAddr = controller_addr_0().parse().expect("allocated addr");
    let ctrl1: std::net::SocketAddr = controller_addr_1().parse().expect("allocated addr");
    let voters = [(1_u64, ctrl0), (2_u64, ctrl1)];

    let manifest_dir = crate::support::manifest_dir();
    let cert_path = manifest_dir
        .join("tests")
        .join("fixtures")
        .join("security")
        .join("dev_cert.pem");
    let key_path = manifest_dir
        .join("tests")
        .join("fixtures")
        .join("security")
        .join("dev_key.pem");

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
            // Slightly more generous than the SASL_PLAINTEXT helper because
            // both data-plane and controller-plane handshakes now include
            // a TLS handshake on top of SASL; on a busy WSL/CI runner the
            // extra round trips can push past 5s.
            controller_election_timeout: krabka_units::secs(8),
            controller_heartbeat_interval: krabka_units::millis(500),
            bootstrap_mode: mode,
            listeners: vec![ListenerSpec {
                name: "SASL_SSL".to_string(),
                bind_addr: listen,
                advertised: advertised.to_string(),
                protocol: ListenerProtocol::SaslSsl,
                tls_config: None,
                sasl_mechanisms: None,
            }],
            inter_broker_listener_name: "SASL_SSL".to_string(),
            controller_listener_protocol: ctrl_protocol,
            tls_config: Some(TlsConfig {
                cert_chain_path: cert_path.clone(),
                private_key_path: key_path.clone(),
                // Each broker must trust the dev cert that its peer
                // presents on inter-broker raft + replication dials.
                // Without this, the InterBrokerClient TlsConnector has
                // an empty trust-root store and rejects the peer's
                // self-signed cert as `UnknownIssuer`.
                trust_roots_path: Some(cert_path.clone()),
                client_ca_path: None,
                client_auth: krabka_security::ClientAuthMode::Disabled,
            }),
            enabled_sasl_mechanisms: vec![SaslMechanism::Plain, SaslMechanism::ScramSha512],
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
    let h0 = tokio::spawn(async move { Broker::start(cfg0).await });
    let h1 = tokio::spawn(async move { Broker::start(cfg1).await });
    let broker0 = h0
        .await
        .expect("broker 0 spawn join")
        .expect("start broker 0");
    let broker1 = h1
        .await
        .expect("broker 1 spawn join")
        .expect("broker 1 start");

    eprintln!(
        "KRABKA[test] two-broker sasl_ssl: b0={listen} adv={bootstrap} b1={listen_b1} adv={bootstrap_b1} ctrl_protocol={ctrl_protocol:?}",
        bootstrap = broker0_advertised(),
        bootstrap_b1 = broker1_advertised(),
        listen = broker0_listen(),
        listen_b1 = broker1_listen()
    );
    (broker0, broker1, dir0, dir1)
}
