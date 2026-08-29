//! Single-broker `SASL_PLAINTEXT` bring-up and the JAAS strings that reach it.
//!
//! The JVM tools authenticate with a JAAS login-module entry, so the builders
//! for those entries live beside the brokers that accept them.

use krabka_broker::{Broker, BrokerConfig};
use krabka_log::LogConfig;

use super::ports::{broker0_advertised, broker0_listen, controller_addr_0};

/// Build a JAAS config string for the `PlainLoginModule`. The trailing `;`
/// is mandatory. Kafka's JAAS parser rejects the entry without it.
pub(crate) fn plain_jaas(user: &str, pass: &str) -> String {
    format!(
        "org.apache.kafka.common.security.plain.PlainLoginModule required \
         username=\"{user}\" password=\"{pass}\";",
    )
}

/// Build a JAAS config string for the `ScramLoginModule`. The
/// SCRAM-SHA-512 acceptance test uses it.
pub(crate) fn scram_jaas(user: &str, pass: &str) -> String {
    format!(
        "org.apache.kafka.common.security.scram.ScramLoginModule required \
         username=\"{user}\" password=\"{pass}\";",
    )
}

/// Spawn the broker with a single `SASL_PLAINTEXT` listener on
/// an allocated port, advertised as an allocated port. The listener
/// starts with the given PLAIN `users` already installed. Mirrors
/// [`start_host_broker`] otherwise.
pub(crate) fn start_sasl_plaintext_broker(
    users: &[(&str, &str)],
) -> impl std::future::Future<Output = (krabka_broker::BrokerHandle, tempfile::TempDir)> {
    use krabka_broker::config::ListenerSpec;
    use krabka_security::{ListenerProtocol, SaslMechanism};

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
    let mut config = BrokerConfig {
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
        listeners: vec![ListenerSpec {
            name: "SASL_PLAINTEXT".to_string(),
            bind_addr: listen_addr,
            advertised: broker0_advertised().to_string(),
            protocol: ListenerProtocol::SaslPlaintext,
            tls_config: None,
            sasl_mechanisms: None,
        }],
        inter_broker_listener_name: "SASL_PLAINTEXT".to_string(),
        enabled_sasl_mechanisms: vec![SaslMechanism::Plain],
        ..BrokerConfig::default()
    };
    for (u, p) in users {
        config
            .plain_credentials
            .insert((*u).to_string(), (*p).to_string());
    }
    Box::pin(async move {
        let handle = Broker::start(config).await.expect("start sasl broker");
        eprintln!(
            "KRABKA[test] sasl broker started listen={listen} advertised={bootstrap}",
            bootstrap = broker0_advertised(),
            listen = broker0_listen()
        );
        tracing::info!(
            listen = %broker0_listen(),
            advertised = %broker0_advertised(),
            "sasl broker started for jvm acceptance"
        );
        (handle, dir)
    })
}

/// Spawn the broker with a single `SASL_PLAINTEXT` listener that enables
/// PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512 mechanisms, plus a single PLAIN
/// super-user (`admin` / `admin_pass`). The super-user designation grants
/// the admin principal `CLUSTER_AUTHORIZATION` on
/// `AlterUserScramCredentials` (51). The admin runs the JVM `kafka-configs
/// --alter --entity-type users` tool over PLAIN, so that tool can provision
/// SCRAM credentials for other users.
///
/// `jvm_sasl_scram_sha512_produce_consume` and
/// `jvm_sasl_scram_sha256_produce_consume` use this broker.
pub(crate) fn start_dual_mech_broker(
    admin: &str,
    admin_pass: &str,
) -> impl std::future::Future<Output = (krabka_broker::BrokerHandle, tempfile::TempDir)> {
    use krabka_broker::config::ListenerSpec;
    use krabka_security::{ListenerProtocol, SaslMechanism};

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
    let mut config = BrokerConfig {
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
        listeners: vec![ListenerSpec {
            name: "SASL_PLAINTEXT".to_string(),
            bind_addr: listen_addr,
            advertised: broker0_advertised().to_string(),
            protocol: ListenerProtocol::SaslPlaintext,
            tls_config: None,
            sasl_mechanisms: None,
        }],
        inter_broker_listener_name: "SASL_PLAINTEXT".to_string(),
        enabled_sasl_mechanisms: vec![
            SaslMechanism::Plain,
            SaslMechanism::ScramSha256,
            SaslMechanism::ScramSha512,
        ],
        super_users: std::collections::HashSet::from([admin.to_string()]),
        ..BrokerConfig::default()
    };
    config.authorizer = std::sync::Arc::new(krabka_broker::authorizer::SimpleAclAuthorizer::new(
        config.super_users.clone(),
    ));
    config
        .plain_credentials
        .insert(admin.to_string(), admin_pass.to_string());
    Box::pin(async move {
        let handle = Broker::start(config).await.expect("start dual-mech broker");
        eprintln!(
            "KRABKA[test] dual-mech broker started listen={listen} advertised={bootstrap}",
            bootstrap = broker0_advertised(),
            listen = broker0_listen()
        );
        tracing::info!(
            listen = %broker0_listen(),
            advertised = %broker0_advertised(),
            "dual-mech broker started for jvm acceptance"
        );
        (handle, dir)
    })
}

/// JAAS config for the JVM `OAuthBearerLoginModule` built-in *unsecured*
/// token issuer. `unsecuredLoginStringClaim_sub` mints an
/// `alg:none` JWS with `sub=<user>`, `iat=now`, `exp=now+3600s`. That is
/// exactly the token shape Krabka's
/// [`krabka_security::UnsecuredJwsValidator`] accepts. It pairs with
/// `OAuthBearerUnsecuredLoginCallbackHandler` on the client.
pub(crate) fn oauthbearer_jaas(sub: &str) -> String {
    format!(
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
         unsecuredLoginStringClaim_sub=\"{sub}\";",
    )
}

/// Spawn a single `SASL_PLAINTEXT` broker that enables **only** OAUTHBEARER.
/// The broker validates the JVM client's unsecured JWS with the default
/// validator (principal claim `sub`). Mirrors [`start_sasl_plaintext_broker`].
pub(crate) async fn start_oauthbearer_broker() -> (krabka_broker::BrokerHandle, tempfile::TempDir) {
    use krabka_broker::config::ListenerSpec;
    use krabka_security::{ListenerProtocol, SaslMechanism};

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
        listeners: vec![ListenerSpec {
            name: "SASL_PLAINTEXT".to_string(),
            bind_addr: listen_addr,
            advertised: broker0_advertised().to_string(),
            protocol: ListenerProtocol::SaslPlaintext,
            tls_config: None,
            sasl_mechanisms: None,
        }],
        inter_broker_listener_name: "SASL_PLAINTEXT".to_string(),
        enabled_sasl_mechanisms: vec![SaslMechanism::OAuthBearer],
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config)
        .await
        .expect("start oauthbearer broker");
    eprintln!(
        "KRABKA[test] oauthbearer broker started listen={listen} advertised={bootstrap}",
        bootstrap = broker0_advertised(),
        listen = broker0_listen()
    );
    (handle, dir)
}

/// Spawn the broker with a single `SASL_PLAINTEXT` listener that enables
/// PLAIN, plus a configured PLAIN super-user. Mirrors
/// [`start_sasl_plaintext_broker`] otherwise. The ACL JVM acceptance tests
/// use it: the super-user authenticates with PLAIN and runs
/// `kafka-acls --add/--remove/--list`. Those flags hit `CreateAcls (30)`,
/// `DeleteAcls (31)`, and `DescribeAcls (29)`, which all need the
/// `Cluster Alter` or `Cluster Describe` operation. The super-user bypass
/// in `authorize()` short-circuits that check.
pub(crate) fn start_sasl_plaintext_broker_with_super_user(
    super_user: &str,
    users: &[(&str, &str)],
) -> impl std::future::Future<Output = (krabka_broker::BrokerHandle, tempfile::TempDir)> {
    use krabka_broker::config::ListenerSpec;
    use krabka_security::{ListenerProtocol, SaslMechanism};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("krabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().expect("tempdir");
    let super_user = super_user.to_string();
    let listen_addr: std::net::SocketAddr = broker0_listen().parse().expect("static addr");
    let controller_addr: std::net::SocketAddr =
        controller_addr_0().parse().expect("allocated addr");
    let mut config = BrokerConfig {
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
        listeners: vec![ListenerSpec {
            name: "SASL_PLAINTEXT".to_string(),
            bind_addr: listen_addr,
            advertised: broker0_advertised().to_string(),
            protocol: ListenerProtocol::SaslPlaintext,
            tls_config: None,
            sasl_mechanisms: None,
        }],
        inter_broker_listener_name: "SASL_PLAINTEXT".to_string(),
        enabled_sasl_mechanisms: vec![SaslMechanism::Plain],
        super_users: std::collections::HashSet::from([super_user.clone()]),
        ..BrokerConfig::default()
    };
    config.authorizer = std::sync::Arc::new(krabka_broker::authorizer::SimpleAclAuthorizer::new(
        config.super_users.clone(),
    ));
    for (u, p) in users {
        config
            .plain_credentials
            .insert((*u).to_string(), (*p).to_string());
    }
    Box::pin(async move {
        let handle = Broker::start(config)
            .await
            .expect("start sasl broker with super-user");
        eprintln!(
            "KRABKA[test] sasl super-user broker started listen={listen} advertised={bootstrap} super_user={super_user}",
            bootstrap = broker0_advertised(),
            listen = broker0_listen()
        );
        (handle, dir)
    })
}
