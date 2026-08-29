//! KIP-48 delegation-token cluster and the parser for what the token CLI
//! prints.
//!
//! The token tool emits its token id and HMAC in several layouts across Kafka
//! versions, so the cluster that mints a token and the reader that recovers it
//! stay together.

use krabka_broker::{Broker, BrokerConfig};
use krabka_log::LogConfig;

use super::ports::{
    broker0_advertised, broker0_listen, broker1_advertised, broker1_listen, broker2_advertised,
    broker2_listen, controller_addr_0, controller_addr_1, controller_addr_2,
};

/// Like [`start_three_broker_sasl_plaintext_jvm_cluster_with_users`] but
/// also enables `SCRAM-SHA-256` on the listener and installs the given
/// `secret_key` as the HMAC master for KIP-48 delegation tokens on every
/// broker. The admin user is provisioned as PLAIN, so the JVM CLI's
/// `kafka-delegation-tokens --create/--describe/--expire` calls can
/// authenticate over PLAIN. The *token consumer* needs the SCRAM-SHA-256
/// mechanism: `kafka-console-producer` authenticates as the new token with
/// SCRAM-SHA-256, and the broker satisfies that on the token-fallback path,
/// where `TokenID` becomes the username and the HMAC becomes the password.
///
/// Returns `(h1, h2, h3, cfg1, cfg2, cfg3, dir1, dir2, dir3)`.
pub(crate) async fn start_three_broker_sasl_plaintext_jvm_cluster_with_delegation_tokens(
    admin: &str,
    admin_pass: &str,
    secret_key: &[u8],
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
    use krabka_security::{ListenerProtocol, SaslMechanism, SecretBytes};

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
            }],
            inter_broker_listener_name: "SASL_PLAINTEXT".to_string(),
            // PLAIN for the admin/inter-broker channel; SCRAM-SHA-256 so the
            // freshly minted delegation token (TokenID/HMAC) can authenticate
            // via the token-fallback path on the SCRAM handler.
            enabled_sasl_mechanisms: vec![SaslMechanism::Plain, SaslMechanism::ScramSha256],
            super_users: std::collections::HashSet::from([admin.to_string()]),
            inter_broker_credentials: Some(InterBrokerCredentials::Plain {
                username: admin.to_string(),
                password: admin_pass.to_string(),
            }),
            delegation_token_secret_key: Some(SecretBytes::new(secret_key.to_vec())),
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
        "KRABKA[test] three-broker sasl (delegation tokens): b0={listen} adv={bootstrap} b1={listen_b1} adv={bootstrap_b1} b2={listen_b2} adv={bootstrap_b2}",
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

/// Parse the JVM `kafka-delegation-tokens --create` stdout for a line
/// matching `<key>\t<value>` or `<key>=<value>` and return `<value>`.
/// The tool prints both a header row and a data row separated by tabs. This
/// function scans every line and returns the first match on the key.
pub(crate) fn extract_jvm_kv(stdout: &str, key: &str) -> String {
    // The kafka-delegation-tokens tool prints output in three forms
    // across versions and code paths:
    //   1. `key = value` lines, or
    //   2. `key : value` lines (used by the "Created delegation token
    //      with tokenId : <id>" preamble), or
    //   3. a space-aligned column table:
    //         TOKENID                              HMAC      OWNER ...
    //                                                                 <- blank
    //         <id>                                 <hmac>    User:admin ...
    // Try each in order.
    for line in stdout.lines() {
        if let Some(rest) = line.strip_prefix(&format!("{key} = ")) {
            return rest.trim().to_string();
        }
        if let Some(rest) = line.strip_prefix(&format!("{key}=")) {
            return rest.trim().to_string();
        }
    }
    // `Created delegation token with tokenId : <id>` is the canonical
    // single-line output for TOKENID after a successful --create.
    if key.eq_ignore_ascii_case("tokenid") {
        for line in stdout.lines() {
            if let Some(rest) = line.split_once("tokenId :") {
                return rest.1.trim().to_string();
            }
        }
    }
    // Column table — split on runs of whitespace.
    let mut header_cols: Option<Vec<String>> = None;
    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let cols: Vec<String> = trimmed.split_whitespace().map(str::to_string).collect();
        if header_cols.is_none() {
            if cols.iter().any(|c| c.eq_ignore_ascii_case(key)) {
                header_cols = Some(cols);
            }
            continue;
        }
        let idx = header_cols
            .as_ref()
            .unwrap()
            .iter()
            .position(|c| c.eq_ignore_ascii_case(key));
        if let Some(i) = idx
            && i < cols.len()
        {
            return cols[i].clone();
        }
    }
    panic!("could not extract key={key} from stdout: {stdout}");
}
