//! KIP-371 `ssl.principal.mapping.rules` under a JVM client.
//!
//! The other TLS cases authenticate with SASL over the encrypted channel; this
//! one authenticates with the certificate itself. The fixture client cert's
//! Subject DN is `CN=test-client,OU=integration,O=crabka`, and the listener
//! carries `RULE:^CN=(.*?),.*$/$1/` ahead of `DEFAULT`, so the broker has to
//! resolve the connection to `test-client`.
//!
//! The proof is authorization: `test-client` is the only super-user, so a JVM
//! `kafka-topics --create` succeeds only if the rule ran. Without it the
//! principal would be the whole DN, which is in nobody's super-user set, and
//! the create would come back `CLUSTER_AUTHORIZATION_FAILED`. That is the same
//! shape `tests/mtls.rs` uses in process, driven here through the JVM stack:
//! a real Java `SSLEngine` presenting a client certificate.
//!
//! A second certificate identity would let the case grant the ACL with
//! `kafka-acls --allow-principal User:test-client` from an admin DN instead of
//! naming a super-user. The fixtures carry one client identity, and the
//! authorization decision the mapping feeds is the same either way, so the
//! case stays on the single fixture cert.

use std::{
    io::Write,
    process::{Command, Stdio},
};

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig, SslPrincipalMapper, config::ListenerSpec};
use krabka_log::LogConfig;
use krabka_security::{ClientAuthMode, ListenerProtocol, TlsConfig};

use crate::jvm_acceptance::{
    KAFKA_IMAGE_TXN, broker0_advertised, broker0_listen, controller_addr_0,
    docker_run_kafka_tool_with_image_and_mounts, nc_check_connectivity, prepare_jks_truststore,
    write_client_props,
};

/// The Subject DN of `tests/fixtures/security/dev_client_cert.pem`, as
/// `x509-parser` renders it, and the principal the listener's rule maps it to.
const CLIENT_DN: &str = "CN=test-client,OU=integration,O=crabka";
const MAPPED_PRINCIPAL: &str = "test-client";

/// Path of one file under `tests/fixtures/security/`.
fn fixture(name: &str) -> std::path::PathBuf {
    crate::support::manifest_dir()
        .join("tests")
        .join("fixtures")
        .join("security")
        .join(name)
}

/// Builds a PKCS#12 keystore holding the fixture client cert and key, which
/// is what the JVM client presents on the `CertificateRequest`.
///
/// `openssl` and the bind mount both run inside the cp-kafka container, the
/// way [`prepare_jks_truststore`] runs `keytool` there: the host needs no
/// toolchain, and the result is chmod `0644` so the image's non-root user can
/// read it. Java reads a PKCS#12 keystore directly, so no `keytool`
/// conversion follows.
fn prepare_client_keystore() -> std::path::PathBuf {
    let cache_dir = std::env::temp_dir().join("krabka-jvm-mtls-keystore");
    std::fs::create_dir_all(&cache_dir).expect("mkdir keystore cache");
    let keystore_path = cache_dir.join("client.p12");
    if keystore_path.exists() {
        return keystore_path;
    }
    for name in ["dev_client_cert.pem", "dev_client_key.pem"] {
        std::fs::copy(fixture(name), cache_dir.join(name)).expect("stage client fixture");
    }
    let inner = "set -e; \
         openssl pkcs12 -export -name client \
             -in /work/dev_client_cert.pem -inkey /work/dev_client_key.pem \
             -out /work/client.p12 -passout pass:changeit && \
         chmod 0644 /work/client.p12";
    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--user",
            "0:0",
            "-v",
            &format!("{}:/work", cache_dir.display()),
            "--entrypoint",
            "bash",
            KAFKA_IMAGE_TXN,
            "-c",
            inner,
        ])
        .output()
        .expect("spawn openssl pkcs12");
    assert!(
        out.status.success(),
        "pkcs12 export failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    keystore_path
}

/// Spawns a broker with a single `SSL` listener that requires a client
/// certificate, trusts the fixture client CA, and maps the peer DN through
/// KIP-371 rules. The mapped short name is the only super-user.
async fn start_mtls_broker() -> (krabka_broker::BrokerHandle, tempfile::TempDir) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("krabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().expect("tempdir");
    let listen_addr: std::net::SocketAddr = broker0_listen().parse().expect("allocated addr");
    let controller_addr: std::net::SocketAddr =
        controller_addr_0().parse().expect("allocated addr");
    let tls = TlsConfig {
        cert_chain_path: fixture("dev_cert.pem"),
        private_key_path: fixture("dev_key.pem"),
        trust_roots_path: None,
        client_ca_path: Some(fixture("dev_client_ca.pem")),
        client_auth: ClientAuthMode::Required,
    };
    let principal_mapper = SslPrincipalMapper::parse(&["RULE:^CN=(.*?),.*$/$1/", "DEFAULT"])
        .expect("KIP-371 rules parse");
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
            name: "SSL".to_string(),
            bind_addr: listen_addr,
            advertised: broker0_advertised().to_string(),
            protocol: ListenerProtocol::Ssl,
            tls_config: Some(tls.clone()),
            sasl_mechanisms: None,
            principal_mapper,
        }],
        inter_broker_listener_name: "SSL".to_string(),
        tls_config: Some(tls),
        super_users: maplit::hashset! {MAPPED_PRINCIPAL.to_string()},
        ..BrokerConfig::default()
    };
    config.authorizer = std::sync::Arc::new(krabka_broker::authorizer::SimpleAclAuthorizer::new(
        config.super_users.clone(),
    ));
    let handle = Broker::start(config).await.expect("start mtls broker");
    (handle, dir)
}

/// A JVM client that presents `CN=test-client,OU=integration,O=crabka` is
/// authorized as `test-client`, the name the listener's rule maps that DN to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_mtls_principal_mapping_rules_shorten_the_subject_dn() {
    const TOPIC: &str = "krabka-mtls-mapping-itest";

    let (broker, _dir) = start_mtls_broker().await;
    nc_check_connectivity();
    let truststore_path = prepare_jks_truststore();
    let keystore_path = prepare_client_keystore();
    let ts_mount = format!("{}:/truststore.jks:ro", truststore_path.display());
    let ks_mount = format!("{}:/client.p12:ro", keystore_path.display());

    let props = write_client_props(
        "security.protocol=SSL\n\
         ssl.truststore.location=/truststore.jks\n\
         ssl.truststore.password=changeit\n\
         ssl.keystore.type=PKCS12\n\
         ssl.keystore.location=/client.p12\n\
         ssl.keystore.password=changeit\n\
         ssl.key.password=changeit\n\
         ssl.endpoint.identification.algorithm=\n\
         enable.idempotence=false\n\
         acks=1\n",
    );
    let props_mount = props.mount_str();

    // `CreateTopics` needs `Cluster Create`, which only the super-user has.
    // It passes only if the broker resolved this connection to the mapped
    // short name rather than to `CLIENT_DN`.
    docker_run_kafka_tool_with_image_and_mounts(
        KAFKA_IMAGE_TXN,
        &[&props_mount, &ts_mount, &ks_mount],
        &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );

    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "-v",
            &props_mount,
            "-v",
            &ts_mount,
            "-v",
            &ks_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-console-producer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--producer.config",
            "/client.properties",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn producer");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"msg-0\n")
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "producer failed for {CLIENT_DN}: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    let consumer_out = docker_run_kafka_tool_with_image_and_mounts(
        KAFKA_IMAGE_TXN,
        &[&props_mount, &ts_mount, &ks_mount],
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            "1",
            "--timeout-ms",
            "20000",
            "--consumer.config",
            "/client.properties",
        ],
    );
    let consumed = String::from_utf8_lossy(&consumer_out.stdout);
    assert!(
        consumed.contains("msg-0"),
        "consumer missing msg-0: {consumed:?}"
    );

    broker.shutdown().await;
}
