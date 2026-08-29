//! Single-broker bring-up for the TLS-terminating listeners, `SSL` and
//! `SASL_SSL`.
//!
//! The JVM client needs the dev certificate in a JKS truststore before it can
//! complete either handshake, so the keytool round-trip that builds one lives
//! here too.

use std::process::Command;

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig};
use krabka_log::LogConfig;

use super::{
    docker::KAFKA_IMAGE,
    ports::{broker0_advertised, broker0_listen, controller_addr_0},
};

/// Spawn the broker with a single `SSL` listener on an allocated port
/// (advertised as an allocated port) with the dev cert/key from
/// `crates/broker/tests/fixtures/security/`. No SASL. Mirrors
/// [`start_host_broker`] otherwise, but flips the protocol to `Ssl` and
/// supplies a [`TlsConfig`].
pub(crate) async fn start_ssl_broker() -> (krabka_broker::BrokerHandle, tempfile::TempDir) {
    use krabka_broker::config::ListenerSpec;
    use krabka_security::{ListenerProtocol, TlsConfig};

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

    // Resolve the on-disk paths of the dev fixture certs, which live under this
    // crate's own tests/fixtures/security since krabka-security moved to the
    // krabka-protocol repository.
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
    assert!(
        cert_path.exists(),
        "dev_cert.pem missing at {}",
        cert_path.display(),
    );
    assert!(
        key_path.exists(),
        "dev_key.pem missing at {}",
        key_path.display(),
    );

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
            name: "SSL".to_string(),
            bind_addr: listen_addr,
            advertised: broker0_advertised().to_string(),
            protocol: ListenerProtocol::Ssl,
            tls_config: None,
            sasl_mechanisms: None,
        }],
        inter_broker_listener_name: "SSL".to_string(),
        tls_config: Some(TlsConfig {
            cert_chain_path: cert_path,
            private_key_path: key_path,
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: krabka_security::ClientAuthMode::Disabled,
        }),
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config).await.expect("start ssl broker");
    eprintln!(
        "KRABKA[test] ssl broker started listen={listen} advertised={bootstrap}",
        bootstrap = broker0_advertised(),
        listen = broker0_listen()
    );
    tracing::info!(
        listen = %broker0_listen(),
        advertised = %broker0_advertised(),
        "ssl broker started for jvm acceptance"
    );
    (handle, dir)
}

/// Build a JKS truststore from the dev cert PEM. This function runs
/// `keytool` inside a one-shot Docker container. It returns the host-side
/// path to a `ts.jks` file, chmod `0644` so the non-root user of the
/// cp-kafka container can read it once it is bind-mounted.
///
/// The result is cached under `<tmp>/krabka-jvm-truststore/ts.jks`, so later
/// calls from this test and from the `SASL_SSL` test skip the keytool
/// round-trip.
///
/// The cp-kafka:6.1.1 image ships its own JRE and `keytool` binary, so this
/// function reuses them with `--entrypoint keytool` instead of pulling
/// `openjdk:17`. The image is always on disk, because the SSL test itself
/// runs `kafka-broker-api-versions` from the same image.
pub(crate) fn prepare_jks_truststore() -> std::path::PathBuf {
    let cache_dir = std::env::temp_dir().join("krabka-jvm-truststore");
    std::fs::create_dir_all(&cache_dir).expect("mkdir truststore cache");
    let ts_path = cache_dir.join("ts.jks");

    // Stage the cert in the cache dir so the bind mount is a directory we
    // control. This sidesteps mount-path quoting on /mnt/c under WSL.
    let manifest_dir = crate::support::manifest_dir();
    let cert_src = manifest_dir
        .join("tests")
        .join("fixtures")
        .join("security")
        .join("dev_cert.pem");
    let cert_staged = cache_dir.join("dev_cert.pem");
    std::fs::copy(&cert_src, &cert_staged).expect("copy dev_cert.pem to cache");

    if !ts_path.exists() {
        let mount = format!("{}:/work", cache_dir.display());
        // Run keytool + chmod as root inside the container so the host
        // file ends up world-readable. `--user 0:0` lets keytool create
        // `/work/ts.jks` regardless of host-dir owner (CI runner-owned
        // tmpdir blocks cp-kafka's non-root default user). The `chmod
        // 0644` is inside the container too because the file is owned
        // by root on the host once keytool runs as root, so the host-side
        // runner user can't chmod it later.
        let inner = "set -e; \
             keytool -import -alias krabka -file /work/dev_cert.pem \
                 -keystore /work/ts.jks -storepass changeit -noprompt && \
             chmod 0644 /work/ts.jks";
        let out = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--user",
                "0:0",
                "-v",
                &mount,
                "--entrypoint",
                "bash",
                KAFKA_IMAGE,
                "-c",
                inner,
            ])
            .output()
            .expect("spawn keytool");
        assert!(
            out.status.success(),
            "keytool import failed: stdout={} stderr={}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
        assert!(
            ts_path.exists(),
            "keytool reported success but ts.jks missing at {}",
            ts_path.display(),
        );
    }

    ts_path
}

/// Spawn the broker with a single `SASL_SSL` listener. The listener enables
/// the PLAIN and SCRAM-SHA-512 mechanisms, uses the dev cert/key for TLS,
/// and gets `admin` as the super-user PLAIN identity, so `admin` can call
/// `AlterUserScramCredentials` to provision SCRAM users.
///
/// This is the dual-mech broker from [`start_dual_mech_broker`] flipped
/// from `SaslPlaintext` to `SaslSsl` with a `TlsConfig` attached. That is
/// the production-shape listener configuration.
pub(crate) fn start_sasl_ssl_broker(
    admin: &str,
    admin_pass: &str,
) -> impl std::future::Future<Output = (krabka_broker::BrokerHandle, tempfile::TempDir)> {
    use krabka_broker::config::ListenerSpec;
    use krabka_security::{ListenerProtocol, SaslMechanism, TlsConfig};

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
            name: "SASL_SSL".to_string(),
            bind_addr: listen_addr,
            advertised: broker0_advertised().to_string(),
            protocol: ListenerProtocol::SaslSsl,
            tls_config: None,
            sasl_mechanisms: None,
        }],
        inter_broker_listener_name: "SASL_SSL".to_string(),
        tls_config: Some(TlsConfig {
            cert_chain_path: cert_path,
            private_key_path: key_path,
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: krabka_security::ClientAuthMode::Disabled,
        }),
        enabled_sasl_mechanisms: vec![SaslMechanism::Plain, SaslMechanism::ScramSha512],
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
        let handle = Broker::start(config).await.expect("start sasl_ssl broker");
        eprintln!(
            "KRABKA[test] sasl_ssl broker started listen={listen} advertised={bootstrap}",
            bootstrap = broker0_advertised(),
            listen = broker0_listen()
        );
        tracing::info!(
            listen = %broker0_listen(),
            advertised = %broker0_advertised(),
            "sasl_ssl broker started for jvm acceptance"
        );
        (handle, dir)
    })
}
