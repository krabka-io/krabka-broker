//! Shared harness for the `jvm_acceptance_*` suites.
//!
//! These tests drive the official Apache Kafka command-line tools against a
//! `crabka-broker` running on the host, with the tools inside cp-kafka
//! containers. They are split across several `tests/jvm_acceptance_*.rs` files
//! so Bazel runs them as separate targets concurrently; as one binary the set
//! took roughly nine minutes, serialised by a single shared port allocation.
//! Each binary is its own process, so [`ports`] hands each one a private set of
//! listeners and the groups cannot collide.
//!
//! Networking: the broker listens on an allocated host port. The CLI containers
//! use Docker's default bridge plus `--add-host=host.docker.internal:host-gateway`,
//! which maps that name onto the bridge gateway the host bound. The broker
//! advertises the same name, because `AdminClient` reconnects after `Metadata`
//! and that connect has to resolve from inside the container.
//!
//! These tests deliberately do NOT use `--network host`. On hosted GitHub
//! Actions ubuntu-24.04 runners that mode silently fails to share the host's
//! loopback: the container can run `nc -zv 127.0.0.1 9092`, but a Java NIO
//! `SocketChannel.connect()` to the same address times out.

// Each suite links the whole harness and uses only the helpers it needs, the
// same arrangement as `tests/support/mod.rs`.
#![allow(dead_code)]

use std::{
    io::Write,
    process::{Command, Stdio},
};

use assert2::assert;
use crabka_broker::{Broker, BrokerConfig};
use crabka_log::LogConfig;

/// Ports for this test process, allocated once rather than fixed at 9092-9097.
///
/// This suite runs up to three brokers, each with a client and a controller
/// listener, plus `MinIO` for the tiered-storage cases. Fixed ports meant no two
/// container suites could run at once -- the second to start lost the bind and
/// reported `Address already in use` as a test failure.
///
/// Accessors return `&'static str` so ordinary use sites read as the constants
/// they replaced, and are named apart from the locals a format string binds.
pub(crate) struct Ports {
    client: [String; 3],
    advertised: [String; 3],
    controller: [String; 3],
    loopback: String,
    minio: u16,
}

pub(crate) fn ports() -> &'static Ports {
    static PORTS: std::sync::OnceLock<Ports> = std::sync::OnceLock::new();
    PORTS.get_or_init(|| {
        let client: [u16; 3] = std::array::from_fn(|_| crate::support::free_port());
        let controller: [u16; 3] = std::array::from_fn(|_| crate::support::free_port());
        Ports {
            client: client.map(|p| format!("0.0.0.0:{p}")),
            advertised: client.map(|p| format!("host.docker.internal:{p}")),
            controller: controller.map(|p| format!("0.0.0.0:{p}")),
            loopback: format!("127.0.0.1:{}", client[0]),
            minio: crate::support::free_port(),
        }
    })
}

pub(crate) fn broker0_advertised() -> &'static str {
    &ports().advertised[0]
}

pub(crate) fn broker0_listen() -> &'static str {
    &ports().client[0]
}

pub(crate) fn controller_addr_0() -> &'static str {
    &ports().controller[0]
}

pub(crate) fn broker1_advertised() -> &'static str {
    &ports().advertised[1]
}

pub(crate) fn broker1_listen() -> &'static str {
    &ports().client[1]
}

pub(crate) fn controller_addr_1() -> &'static str {
    &ports().controller[1]
}

pub(crate) fn broker2_advertised() -> &'static str {
    &ports().advertised[2]
}

pub(crate) fn broker2_listen() -> &'static str {
    &ports().client[2]
}

pub(crate) fn controller_addr_2() -> &'static str {
    &ports().controller[2]
}

/// Broker 0 over loopback. The tests' own clients use this; only the containers
/// use the advertised `host.docker.internal` name.
pub(crate) fn rlmm_broker0_advertised() -> &'static str {
    &ports().loopback
}

pub(crate) fn host_port() -> u16 {
    ports().client[0]
        .rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .expect("client addr has a numeric port")
}

pub(crate) fn minio_port() -> u16 {
    ports().minio
}

/// Address the Kafka CLI containers use for bootstrap AND that the broker
/// advertises in `Metadata`. [`docker_run_kafka_tool`] resolves it with
/// `--add-host=host.docker.internal:host-gateway`.
/// Bind to all interfaces so the Docker bridge can reach the broker at the
/// host gateway IP.
pub(crate) const KAFKA_IMAGE: &str = "mirror.gcr.io/confluentinc/cp-kafka:6.1.1";

/// Newer Kafka image for tests that need tools or client APIs not bundled in
/// [`KAFKA_IMAGE`]. These tests use it:
///
/// - `kafka_cluster_describe`: `cp-kafka:6.1.1` has no `kafka-cluster`
///   binary, but `cp-kafka:7.5.0` has one.
///
/// - `transactional_console_producer_eos`: the image includes `javac` and the
///   Kafka 3.5 client jars used by the transactional Java helper.
pub(crate) const KAFKA_IMAGE_TXN: &str = "mirror.gcr.io/confluentinc/cp-kafka:7.5.0";

/// Kafka 0.10.1 console tools from Confluent Platform 3.1.2. The
/// legacy-client acceptance tests (`jvm_legacy_010_*`) use them. The
/// 0.10.x-era producer emits v1 `MessageSet` records by default, with
/// KIP-32 per-message timestamps. The consumer negotiates Fetch v0–3. This
/// image exercises the broker's `kafka_3_6_2`-namespace handlers and the
/// up/down-conversion paths from slices 2b+2c (#226).
pub(crate) const KAFKA_IMAGE_LEGACY: &str = "mirror.gcr.io/confluentinc/cp-kafka:3.1.2";

/// Spawn the broker on `broker0_listen()`. The advertised listener is
/// an allocated port. Inside the cp-kafka containers, the test
/// adds a hosts entry that points that name at the bridge gateway.
pub(crate) async fn start_host_broker() -> (crabka_broker::BrokerHandle, tempfile::TempDir) {
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
) -> (crabka_broker::BrokerHandle, tempfile::TempDir) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
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
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(crabka_broker::NodeId(1), controller_addr.to_string())],
        heartbeat_interval: crabka_units::millis(3_000),
        heartbeat_timeout: crabka_units::millis(9_000),
        replica_lag_time_max: crabka_units::millis(30_000),
        controller_election_timeout: crabka_units::secs(5),
        controller_heartbeat_interval: crabka_units::millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    };
    let mut config = config;
    adjust(&mut config);
    let handle = Broker::start(config).await.expect("start broker");
    eprintln!(
        "CRABKA[test] broker started listen={listen} advertised={bootstrap}",
        bootstrap = broker0_advertised(),
        listen = broker0_listen()
    );
    tracing::info!(listen = %broker0_listen(), advertised = %broker0_advertised(), "broker started for jvm acceptance");
    (handle, dir)
}

/// Verify TCP connectivity from inside a bridge-network container with
/// `--add-host=host.docker.internal:host-gateway`.
pub(crate) fn nc_check_connectivity() {
    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            "alpine",
            "sh",
            "-c",
            &format!(
                "apk add --no-cache netcat-openbsd >/dev/null 2>&1 && nc -zv {} {}",
                "host.docker.internal",
                host_port()
            ),
        ])
        .output()
        .expect("spawn nc check");
    eprintln!(
        "NC CHECK status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Run `docker run --rm --add-host=host.docker.internal:host-gateway
/// <image> <args...>` and assert that it succeeds.
pub(crate) fn docker_run_kafka_tool(args: &[&str]) -> std::process::Output {
    docker_run_kafka_tool_with_image(KAFKA_IMAGE, args)
}

/// Like [`docker_run_kafka_tool`] but lets the caller choose the image.
/// Use it when a test needs a newer image. For example, `cp-kafka:7.5.0`
/// bundles `kafka-cluster` and `6.1.1` does not.
pub(crate) fn docker_run_kafka_tool_with_image(image: &str, args: &[&str]) -> std::process::Output {
    let out = Command::new("docker")
        .arg("run")
        .arg("--rm")
        .arg("--add-host=host.docker.internal:host-gateway")
        .arg(image)
        .args(args)
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("spawn docker run");
    eprintln!(
        "CRABKA[test] docker_run image={image} {args:?} status={} stderr_len={}",
        out.status,
        out.stderr.len(),
    );
    assert!(
        out.status.success(),
        "docker run image={image} {args:?} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

pub(crate) const TRANSACTIONAL_PRODUCER_JAVA: &str = r#"
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class TransactionalProducer {
  public static void main(String[] args) throws Exception {
    Properties config = new Properties();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "eos-tid");

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
      producer.initTransactions();
      producer.beginTransaction();
      for (int i = 0; i < 6; i++) {
        producer.send(new ProducerRecord<>(args[1], "committed-" + i)).get();
      }
      producer.commitTransaction();
    }

    // Mirror two independent CLI invocations. The second init obtains the
    // post-EndTxn epoch before it writes the transaction that is aborted.
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
      producer.initTransactions();
      producer.beginTransaction();
      for (int i = 0; i < 2; i++) {
        producer.send(new ProducerRecord<>(args[1], "aborted-" + i)).get();
      }
      producer.abortTransaction();
    }
    System.out.println("TXNPROBE OK");
  }
}
"#;

// ────────────────────────────────────────────────────────────────────────
// SASL / TLS JVM acceptance tests.
// ────────────────────────────────────────────────────────────────────────

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
) -> impl std::future::Future<Output = (crabka_broker::BrokerHandle, tempfile::TempDir)> {
    use crabka_broker::config::ListenerSpec;
    use crabka_security::{ListenerProtocol, SaslMechanism};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
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
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(crabka_broker::NodeId(1), controller_addr.to_string())],
        heartbeat_interval: crabka_units::millis(3_000),
        heartbeat_timeout: crabka_units::millis(9_000),
        replica_lag_time_max: crabka_units::millis(30_000),
        controller_election_timeout: crabka_units::secs(5),
        controller_heartbeat_interval: crabka_units::millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
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
            "CRABKA[test] sasl broker started listen={listen} advertised={bootstrap}",
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
) -> impl std::future::Future<Output = (crabka_broker::BrokerHandle, tempfile::TempDir)> {
    use crabka_broker::config::ListenerSpec;
    use crabka_security::{ListenerProtocol, SaslMechanism};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
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
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(crabka_broker::NodeId(1), controller_addr.to_string())],
        heartbeat_interval: crabka_units::millis(3_000),
        heartbeat_timeout: crabka_units::millis(9_000),
        replica_lag_time_max: crabka_units::millis(30_000),
        controller_election_timeout: crabka_units::secs(5),
        controller_heartbeat_interval: crabka_units::millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
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
    config.authorizer = std::sync::Arc::new(crabka_broker::authorizer::SimpleAclAuthorizer::new(
        config.super_users.clone(),
    ));
    config
        .plain_credentials
        .insert(admin.to_string(), admin_pass.to_string());
    Box::pin(async move {
        let handle = Broker::start(config).await.expect("start dual-mech broker");
        eprintln!(
            "CRABKA[test] dual-mech broker started listen={listen} advertised={bootstrap}",
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

/// Write `props` to a `tempfile::NamedTempFile` and chmod it to `0644` on
/// unix, so the non-root user of the cp-kafka container can read it once it
/// is bind-mounted. `tempfile` creates files `0600` by default, which causes
/// a silent `IOException: client.properties (Permission denied)` inside the
/// JVM tool. The returned object holds the tempfile open. Drop it after the
/// last docker invocation that needs the mount.
pub(crate) fn write_client_props(props: &str) -> ClientPropsFile {
    let tmp = tempfile::NamedTempFile::new().expect("tmp");
    std::fs::write(tmp.path(), props).expect("write props");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o644))
            .expect("chmod props");
    }
    ClientPropsFile { tmp }
}

/// Owns a `client.properties` tempfile + builds the `-v` mount spec for it.
pub(crate) struct ClientPropsFile {
    tmp: tempfile::NamedTempFile,
}

impl ClientPropsFile {
    /// `<host_path>:/client.properties:ro`, the second positional argument
    /// to `docker run -v`. Inside the container the file is always at
    /// `/client.properties`, so JVM tool flags can use a fixed path.
    pub(crate) fn mount_str(&self) -> String {
        format!("{}:/client.properties:ro", self.tmp.path().display())
    }
}

/// Run a cp-kafka tool with an extra `-v <mount>` bind. Otherwise identical
/// to [`docker_run_kafka_tool`]: it asserts success and captures
/// stdout+stderr.
pub(crate) fn docker_run_kafka_tool_with_mount(mount: &str, args: &[&str]) -> std::process::Output {
    docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE, mount, args)
}

/// Like [`docker_run_kafka_tool_with_mount`] but lets the caller choose the
/// image. The SCRAM-SHA-512 acceptance test uses it and needs
/// `cp-kafka:7.5.0`, because `kafka-configs --alter --entity-type users` on
/// `cp-kafka:6.1.1` (Kafka 2.7) sends `IncrementalAlterConfigs (api_key 44)`
/// rather than `AlterUserScramCredentials (51)`. Kafka 3.5+ uses the typed
/// KIP-554 request, which is what the broker implements.
pub(crate) fn docker_run_kafka_tool_with_image_and_mount(
    image: &str,
    mount: &str,
    args: &[&str],
) -> std::process::Output {
    let out = Command::new("docker")
        .arg("run")
        .arg("--rm")
        .arg("-v")
        .arg(mount)
        .arg("--add-host=host.docker.internal:host-gateway")
        .arg(image)
        .args(args)
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("spawn docker run");
    eprintln!(
        "CRABKA[test] docker_run image={image} mount={mount} {args:?} status={} stderr_len={}",
        out.status,
        out.stderr.len(),
    );
    assert!(
        out.status.success(),
        "docker run image={image} mount={mount} {args:?} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

/// JAAS config for the JVM `OAuthBearerLoginModule` built-in *unsecured*
/// token issuer. `unsecuredLoginStringClaim_sub` mints an
/// `alg:none` JWS with `sub=<user>`, `iat=now`, `exp=now+3600s`. That is
/// exactly the token shape Crabka's
/// [`crabka_security::UnsecuredJwsValidator`] accepts. It pairs with
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
pub(crate) async fn start_oauthbearer_broker() -> (crabka_broker::BrokerHandle, tempfile::TempDir) {
    use crabka_broker::config::ListenerSpec;
    use crabka_security::{ListenerProtocol, SaslMechanism};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
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
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(crabka_broker::NodeId(1), controller_addr.to_string())],
        heartbeat_interval: crabka_units::millis(3_000),
        heartbeat_timeout: crabka_units::millis(9_000),
        replica_lag_time_max: crabka_units::millis(30_000),
        controller_election_timeout: crabka_units::secs(5),
        controller_heartbeat_interval: crabka_units::millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
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
        "CRABKA[test] oauthbearer broker started listen={listen} advertised={bootstrap}",
        bootstrap = broker0_advertised(),
        listen = broker0_listen()
    );
    (handle, dir)
}

/// Spawn the broker with a single `SSL` listener on an allocated port
/// (advertised as an allocated port) with the dev cert/key from
/// `crates/broker/tests/fixtures/security/`. No SASL. Mirrors
/// [`start_host_broker`] otherwise, but flips the protocol to `Ssl` and
/// supplies a [`TlsConfig`].
pub(crate) async fn start_ssl_broker() -> (crabka_broker::BrokerHandle, tempfile::TempDir) {
    use crabka_broker::config::ListenerSpec;
    use crabka_security::{ListenerProtocol, TlsConfig};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().expect("tempdir");
    let listen_addr: std::net::SocketAddr = broker0_listen().parse().expect("static addr");
    let controller_addr: std::net::SocketAddr =
        controller_addr_0().parse().expect("allocated addr");

    // Resolve the on-disk paths of the dev fixture certs, which live under this
    // crate's own tests/fixtures/security since crabka-security moved to the
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
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(crabka_broker::NodeId(1), controller_addr.to_string())],
        heartbeat_interval: crabka_units::millis(3_000),
        heartbeat_timeout: crabka_units::millis(9_000),
        replica_lag_time_max: crabka_units::millis(30_000),
        controller_election_timeout: crabka_units::secs(5),
        controller_heartbeat_interval: crabka_units::millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
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
            client_auth: crabka_security::ClientAuthMode::Disabled,
        }),
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config).await.expect("start ssl broker");
    eprintln!(
        "CRABKA[test] ssl broker started listen={listen} advertised={bootstrap}",
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
/// The result is cached under `<tmp>/crabka-jvm-truststore/ts.jks`, so later
/// calls from this test and from the `SASL_SSL` test skip the keytool
/// round-trip.
///
/// The cp-kafka:6.1.1 image ships its own JRE and `keytool` binary, so this
/// function reuses them with `--entrypoint keytool` instead of pulling
/// `openjdk:17`. The image is always on disk, because the SSL test itself
/// runs `kafka-broker-api-versions` from the same image.
pub(crate) fn prepare_jks_truststore() -> std::path::PathBuf {
    let cache_dir = std::env::temp_dir().join("crabka-jvm-truststore");
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
             keytool -import -alias crabka -file /work/dev_cert.pem \
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

// ────────────────────────────────────────────────────────────────────────
// SASL_SSL full stack + JVM-driven inter-broker SASL replication.
// ────────────────────────────────────────────────────────────────────────

/// Like [`docker_run_kafka_tool_with_image_and_mount`] but supports multiple
/// bind mounts. The `SASL_SSL` test needs this, because it mounts both a
/// `client.properties` file and a JKS truststore into the same container.
pub(crate) fn docker_run_kafka_tool_with_image_and_mounts(
    image: &str,
    mounts: &[&str],
    args: &[&str],
) -> std::process::Output {
    let mut cmd = Command::new("docker");
    cmd.arg("run").arg("--rm");
    for m in mounts {
        cmd.arg("-v").arg(m);
    }
    cmd.arg("--add-host=host.docker.internal:host-gateway")
        .arg(image)
        .args(args)
        .stderr(Stdio::piped())
        .stdout(Stdio::piped());
    let out = cmd.output().expect("spawn docker run");
    eprintln!(
        "CRABKA[test] docker_run image={image} mounts={mounts:?} {args:?} status={} stderr_len={}",
        out.status,
        out.stderr.len(),
    );
    assert!(
        out.status.success(),
        "docker run image={image} mounts={mounts:?} {args:?} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
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
) -> impl std::future::Future<Output = (crabka_broker::BrokerHandle, tempfile::TempDir)> {
    use crabka_broker::config::ListenerSpec;
    use crabka_security::{ListenerProtocol, SaslMechanism, TlsConfig};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
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
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(crabka_broker::NodeId(1), controller_addr.to_string())],
        heartbeat_interval: crabka_units::millis(3_000),
        heartbeat_timeout: crabka_units::millis(9_000),
        replica_lag_time_max: crabka_units::millis(30_000),
        controller_election_timeout: crabka_units::secs(5),
        controller_heartbeat_interval: crabka_units::millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
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
            client_auth: crabka_security::ClientAuthMode::Disabled,
        }),
        enabled_sasl_mechanisms: vec![SaslMechanism::Plain, SaslMechanism::ScramSha512],
        super_users: std::collections::HashSet::from([admin.to_string()]),
        ..BrokerConfig::default()
    };
    config.authorizer = std::sync::Arc::new(crabka_broker::authorizer::SimpleAclAuthorizer::new(
        config.super_users.clone(),
    ));
    config
        .plain_credentials
        .insert(admin.to_string(), admin_pass.to_string());
    Box::pin(async move {
        let handle = Broker::start(config).await.expect("start sasl_ssl broker");
        eprintln!(
            "CRABKA[test] sasl_ssl broker started listen={listen} advertised={bootstrap}",
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
    crabka_broker::BrokerHandle,
    crabka_broker::BrokerHandle,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    use crabka_broker::config::{InterBrokerCredentials, ListenerSpec};
    use crabka_security::{ListenerProtocol, SaslMechanism};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=info")),
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
                  mode: crabka_broker::BootstrapMode|
     -> BrokerConfig {
        let mut cfg = BrokerConfig {
            broker_id: i32::try_from(idx).unwrap(),
            listen_addr: listen,
            advertised_listener: advertised.to_string(),
            log_dir,
            log_config: LogConfig::default(),
            node_id: crabka_broker::NodeId(idx),
            controller_listen_addr: ctrl,
            controller_quorum_voters: voters
                .iter()
                .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
                .collect(),
            heartbeat_interval: crabka_units::millis(3_000),
            heartbeat_timeout: crabka_units::millis(9_000),
            replica_lag_time_max: crabka_units::millis(30_000),
            controller_election_timeout: crabka_units::secs(5),
            controller_heartbeat_interval: crabka_units::millis(500),
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
            super_users: std::collections::HashSet::from([admin.to_string()]),
            inter_broker_credentials: Some(InterBrokerCredentials::Plain {
                username: admin.to_string(),
                password: admin_pass.to_string(),
            }),
            ..BrokerConfig::default()
        };
        cfg.authorizer = std::sync::Arc::new(crabka_broker::authorizer::SimpleAclAuthorizer::new(
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
        crabka_broker::BootstrapMode::Bootstrap,
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
        crabka_broker::BootstrapMode::Bootstrap,
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
        "CRABKA[test] two-broker sasl: b0={listen} adv={bootstrap} b1={listen_b1} adv={bootstrap_b1}",
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
    ctrl_protocol: crabka_security::ListenerProtocol,
    admin: &str,
    admin_pass: &str,
) -> (
    crabka_broker::BrokerHandle,
    crabka_broker::BrokerHandle,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    use crabka_broker::config::{InterBrokerCredentials, ListenerSpec};
    use crabka_security::{ListenerProtocol, SaslMechanism, TlsConfig};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=info")),
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
                  mode: crabka_broker::BootstrapMode|
     -> BrokerConfig {
        let mut cfg = BrokerConfig {
            broker_id: i32::try_from(idx).unwrap(),
            listen_addr: listen,
            advertised_listener: advertised.to_string(),
            log_dir,
            log_config: LogConfig::default(),
            node_id: crabka_broker::NodeId(idx),
            controller_listen_addr: ctrl,
            controller_quorum_voters: voters
                .iter()
                .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
                .collect(),
            heartbeat_interval: crabka_units::millis(3_000),
            heartbeat_timeout: crabka_units::millis(9_000),
            replica_lag_time_max: crabka_units::millis(30_000),
            // Slightly more generous than the SASL_PLAINTEXT helper because
            // both data-plane and controller-plane handshakes now include
            // a TLS handshake on top of SASL; on a busy WSL/CI runner the
            // extra round trips can push past 5s.
            controller_election_timeout: crabka_units::secs(8),
            controller_heartbeat_interval: crabka_units::millis(500),
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
                client_auth: crabka_security::ClientAuthMode::Disabled,
            }),
            enabled_sasl_mechanisms: vec![SaslMechanism::Plain, SaslMechanism::ScramSha512],
            super_users: std::collections::HashSet::from([admin.to_string()]),
            inter_broker_credentials: Some(InterBrokerCredentials::Plain {
                username: admin.to_string(),
                password: admin_pass.to_string(),
            }),
            ..BrokerConfig::default()
        };
        cfg.authorizer = std::sync::Arc::new(crabka_broker::authorizer::SimpleAclAuthorizer::new(
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
        crabka_broker::BootstrapMode::Bootstrap,
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
        crabka_broker::BootstrapMode::Bootstrap,
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
        "CRABKA[test] two-broker sasl_ssl: b0={listen} adv={bootstrap} b1={listen_b1} adv={bootstrap_b1} ctrl_protocol={ctrl_protocol:?}",
        bootstrap = broker0_advertised(),
        bootstrap_b1 = broker1_advertised(),
        listen = broker0_listen(),
        listen_b1 = broker1_listen()
    );
    (broker0, broker1, dir0, dir1)
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
) -> impl std::future::Future<Output = (crabka_broker::BrokerHandle, tempfile::TempDir)> {
    use crabka_broker::config::ListenerSpec;
    use crabka_security::{ListenerProtocol, SaslMechanism};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
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
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(crabka_broker::NodeId(1), controller_addr.to_string())],
        heartbeat_interval: crabka_units::millis(3_000),
        heartbeat_timeout: crabka_units::millis(9_000),
        replica_lag_time_max: crabka_units::millis(30_000),
        controller_election_timeout: crabka_units::secs(5),
        controller_heartbeat_interval: crabka_units::millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
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
    config.authorizer = std::sync::Arc::new(crabka_broker::authorizer::SimpleAclAuthorizer::new(
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
            "CRABKA[test] sasl super-user broker started listen={listen} advertised={bootstrap} super_user={super_user}",
            bootstrap = broker0_advertised(),
            listen = broker0_listen()
        );
        (handle, dir)
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// JVM kafka-leader-election --election-type preferred
// ─────────────────────────────────────────────────────────────────────────────

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
    crabka_broker::BrokerHandle,
    crabka_broker::BrokerHandle,
    crabka_broker::BrokerHandle,
    BrokerConfig,
    BrokerConfig,
    BrokerConfig,
    tempfile::TempDir,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    use crabka_broker::config::{InterBrokerCredentials, ListenerSpec};
    use crabka_security::{ListenerProtocol, SaslMechanism};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=info")),
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
                  mode: crabka_broker::BootstrapMode|
     -> BrokerConfig {
        let mut cfg = BrokerConfig {
            broker_id: i32::try_from(idx).unwrap(),
            listen_addr: listen,
            advertised_listener: advertised.to_string(),
            log_dir,
            log_config: LogConfig::default(),
            node_id: crabka_broker::NodeId(idx),
            controller_listen_addr: ctrl,
            controller_quorum_voters: voters
                .iter()
                .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
                .collect(),
            heartbeat_interval: crabka_units::millis(3_000),
            heartbeat_timeout: crabka_units::millis(9_000),
            replica_lag_time_max: crabka_units::millis(30_000),
            controller_election_timeout: crabka_units::secs(5),
            controller_heartbeat_interval: crabka_units::millis(500),
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
            super_users: std::collections::HashSet::from([admin.to_string()]),
            inter_broker_credentials: Some(InterBrokerCredentials::Plain {
                username: admin.to_string(),
                password: admin_pass.to_string(),
            }),
            ..BrokerConfig::default()
        };
        cfg.authorizer = std::sync::Arc::new(crabka_broker::authorizer::SimpleAclAuthorizer::new(
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
        crabka_broker::BootstrapMode::Bootstrap,
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
        crabka_broker::BootstrapMode::Bootstrap,
    );
    let cfg2 = mk_cfg(
        3,
        listen2,
        ctrl2,
        broker2_advertised(),
        dir2.path().to_path_buf(),
        crabka_broker::BootstrapMode::Bootstrap,
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
        "CRABKA[test] three-broker sasl: b0={listen} adv={bootstrap} b1={listen_b1} adv={bootstrap_b1} b2={listen_b2} adv={bootstrap_b2}",
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

/// Poll until `handle` reports `leader` as the leader for `(topic, partition)`.
pub(crate) async fn wait_jvm_partition_leader(
    handle: &crabka_broker::BrokerHandle,
    topic: &str,
    partition: i32,
    leader: u64,
) {
    handle
        .wait_for_image(|img| {
            img.partition(topic, partition)
                .is_some_and(|p| p.leader == leader)
        })
        .await;
}

/// Poll until the ISR for `(topic, partition)` contains `node`.
pub(crate) async fn wait_jvm_isr_contains(
    handle: &crabka_broker::BrokerHandle,
    topic: &str,
    partition: i32,
    node: u64,
) {
    handle
        .wait_for_image(|img| {
            img.partition(topic, partition)
                .is_some_and(|p| p.isr.contains(&crabka_metadata::NodeId(node)))
        })
        .await;
}

/// Poll until `handle` reports any non-zero leader for `(topic, partition)`.
/// Returns the leader node id.
pub(crate) async fn wait_jvm_partition_any_leader(
    handle: &crabka_broker::BrokerHandle,
    topic: &str,
    partition: i32,
) -> u64 {
    handle
        .wait_for_image(|img| {
            img.partition(topic, partition)
                .is_some_and(|p| p.leader != 0)
        })
        .await;
    handle
        .partition_leader_for_test(topic, partition)
        .expect("non-zero leader present after wait")
}

/// Poll until all three brokers have seen `n_brokers` registered brokers.
pub(crate) async fn wait_three_brokers_registered(
    h1: &crabka_broker::BrokerHandle,
    h2: &crabka_broker::BrokerHandle,
    h3: &crabka_broker::BrokerHandle,
    n_brokers: usize,
) {
    h1.wait_until_brokers_registered(n_brokers).await;
    h2.wait_until_brokers_registered(n_brokers).await;
    h3.wait_until_brokers_registered(n_brokers).await;
}

// ---------------------------------------------------------------------------
// Helper: write an arbitrary tempfile and return a TempFileMount that owns
// the NamedTempFile (so it stays alive as long as the returned value is alive)
// and exposes the host path for Docker `-v` mount specs.
// ---------------------------------------------------------------------------

pub(crate) struct TempFileMount {
    tmp: tempfile::NamedTempFile,
}

impl TempFileMount {
    /// `<host_path>:<container_path>`. The caller appends `:ro` if it wants
    /// a read-only mount.
    pub(crate) fn host_path(&self) -> String {
        self.tmp.path().display().to_string()
    }
}

pub(crate) fn write_temp_file(filename: &str, contents: &str) -> TempFileMount {
    let tmp = tempfile::Builder::new()
        .prefix(filename)
        .tempfile()
        .expect("tempfile");
    std::fs::write(tmp.path(), contents).expect("write tempfile");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o644))
            .expect("chmod tempfile");
    }
    TempFileMount { tmp }
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
    crabka_broker::BrokerHandle,
    crabka_broker::BrokerHandle,
    crabka_broker::BrokerHandle,
    BrokerConfig,
    BrokerConfig,
    BrokerConfig,
    tempfile::TempDir,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    use crabka_broker::config::{InterBrokerCredentials, ListenerSpec};
    use crabka_security::{ListenerProtocol, SaslMechanism};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=info")),
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
                  mode: crabka_broker::BootstrapMode|
     -> BrokerConfig {
        let mut cfg = BrokerConfig {
            broker_id: i32::try_from(idx).unwrap(),
            listen_addr: listen,
            advertised_listener: advertised.to_string(),
            log_dir,
            log_config: LogConfig::default(),
            node_id: crabka_broker::NodeId(idx),
            controller_listen_addr: ctrl,
            controller_quorum_voters: voters
                .iter()
                .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
                .collect(),
            heartbeat_interval: crabka_units::millis(3_000),
            heartbeat_timeout: crabka_units::millis(9_000),
            replica_lag_time_max: crabka_units::millis(30_000),
            controller_election_timeout: crabka_units::secs(5),
            controller_heartbeat_interval: crabka_units::millis(500),
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
            super_users: std::collections::HashSet::from([admin.to_string()]),
            inter_broker_credentials: Some(InterBrokerCredentials::Plain {
                username: admin.to_string(),
                password: admin_pass.to_string(),
            }),
            ..BrokerConfig::default()
        };
        cfg.authorizer = std::sync::Arc::new(crabka_broker::authorizer::SimpleAclAuthorizer::new(
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
        crabka_broker::BootstrapMode::Bootstrap,
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
        crabka_broker::BootstrapMode::Bootstrap,
    );
    let cfg2 = mk_cfg(
        3,
        listen2,
        ctrl2,
        broker2_advertised(),
        dir2.path().to_path_buf(),
        crabka_broker::BootstrapMode::Bootstrap,
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
        "CRABKA[test] three-broker sasl (with_users): b0={listen} adv={bootstrap} b1={listen_b1} adv={bootstrap_b1} b2={listen_b2} adv={bootstrap_b2}",
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

/// Like [`start_host_broker`] but configures a second JBOD data directory
/// (KIP-113). Returns the two host-side log dirs with the handle, so
/// the test can assert which absolute paths `DescribeLogDirs` reports.
pub(crate) async fn start_host_broker_jbod() -> (
    crabka_broker::BrokerHandle,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
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
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(crabka_broker::NodeId(1), controller_addr.to_string())],
        heartbeat_interval: crabka_units::millis(3_000),
        heartbeat_timeout: crabka_units::millis(9_000),
        replica_lag_time_max: crabka_units::millis(30_000),
        controller_election_timeout: crabka_units::secs(5),
        controller_heartbeat_interval: crabka_units::millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config).await.expect("start broker");
    (handle, primary, extra)
}

// ────────────────────────────────────────────────────────────────────────
// KIP-48: delegation-token JVM acceptance.
// ────────────────────────────────────────────────────────────────────────

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
    crabka_broker::BrokerHandle,
    crabka_broker::BrokerHandle,
    crabka_broker::BrokerHandle,
    BrokerConfig,
    BrokerConfig,
    BrokerConfig,
    tempfile::TempDir,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    use crabka_broker::config::{InterBrokerCredentials, ListenerSpec};
    use crabka_security::{ListenerProtocol, SaslMechanism, SecretBytes};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=info")),
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
                  mode: crabka_broker::BootstrapMode|
     -> BrokerConfig {
        let mut cfg = BrokerConfig {
            broker_id: i32::try_from(idx).unwrap(),
            listen_addr: listen,
            advertised_listener: advertised.to_string(),
            log_dir,
            log_config: LogConfig::default(),
            node_id: crabka_broker::NodeId(idx),
            controller_listen_addr: ctrl,
            controller_quorum_voters: voters
                .iter()
                .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
                .collect(),
            heartbeat_interval: crabka_units::millis(3_000),
            heartbeat_timeout: crabka_units::millis(9_000),
            replica_lag_time_max: crabka_units::millis(30_000),
            controller_election_timeout: crabka_units::secs(5),
            controller_heartbeat_interval: crabka_units::millis(500),
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
        cfg.authorizer = std::sync::Arc::new(crabka_broker::authorizer::SimpleAclAuthorizer::new(
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
        crabka_broker::BootstrapMode::Bootstrap,
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
        crabka_broker::BootstrapMode::Bootstrap,
    );
    let cfg2 = mk_cfg(
        3,
        listen2,
        ctrl2,
        broker2_advertised(),
        dir2.path().to_path_buf(),
        crabka_broker::BootstrapMode::Bootstrap,
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
        "CRABKA[test] three-broker sasl (delegation tokens): b0={listen} adv={bootstrap} b1={listen_b1} adv={bootstrap_b1} b2={listen_b2} adv={bootstrap_b2}",
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

// ---------------------------------------------------------------------------
// MinIO-backed tiered-storage acceptance test (KIP-405 S3 backend).
//
// Spins up a real `mirror.gcr.io/minio/minio` container, points the broker at it via the
// S3-compatible `S3RemoteStorage` backend, then drives a JVM producer +
// consumer against a topic with `remote.storage.enable=true` and aggressive
// `segment.bytes` / `local.retention.bytes` overrides. We assert both that
// segment objects materialise in the MinIO bucket and that the JVM consumer
// reads back every record — including offsets whose local segments have
// already been evicted by `local_retention_pass`, forcing the read to come
// from the remote tier through `RemoteReader`.
// ---------------------------------------------------------------------------

pub(crate) const MINIO_IMAGE: &str = "mirror.gcr.io/minio/minio:RELEASE.2025-09-07T16-13-09Z";

pub(crate) const MINIO_CLIENT_IMAGE: &str = "mirror.gcr.io/minio/mc:RELEASE.2025-08-13T08-35-41Z";

pub(crate) const MINIO_ACCESS_KEY: &str = "minioadmin";

pub(crate) const MINIO_SECRET_KEY: &str = "minioadmin";

pub(crate) const MINIO_BUCKET: &str = "crabka-tiered";

/// `KIP-405` topic configs (`remote.storage.enable`, `local.retention.bytes`)
/// landed in Apache Kafka 3.6 / Confluent Platform 7.6. The default
/// [`KAFKA_IMAGE`] (`mirror.gcr.io/confluentinc/cp-kafka:6.1.1` / Kafka 2.7)
/// and [`KAFKA_IMAGE_TXN`] (`mirror.gcr.io/confluentinc/cp-kafka:7.5.0` /
/// Kafka 3.5) both predate KIP-405. Their `TopicCommand` client validates
/// `--config` keys against the local `LogConfig.configNames` set and rejects
/// unknown ones before it sends the `CreateTopics` request, so the
/// tiered-storage test cannot reuse them.
/// `mirror.gcr.io/confluentinc/cp-kafka:7.8.8` ships Kafka 3.8, where
/// KIP-405 is GA.
pub(crate) const KAFKA_IMAGE_TIERED: &str = "mirror.gcr.io/confluentinc/cp-kafka:7.8.8";

/// Owns a `docker run -d` `MinIO` container and tears it down on drop.
pub(crate) struct MinioContainer {
    name: String,
}

impl MinioContainer {
    pub(crate) fn start() -> Self {
        // Unique name per test invocation so back-to-back runs don't see a
        // stale container squatting on port 9000.
        let minio_port = minio_port();
        let name = format!("crabka-minio-test-{}", uuid::Uuid::new_v4().simple());
        // Best-effort orphan reap from a prior aborted run.
        let _ = Command::new("docker")
            .args(["rm", "-f", &name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let status = Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "--name",
                &name,
                "-p",
                &format!("{minio_port}:9000"),
                "-e",
                &format!("MINIO_ROOT_USER={MINIO_ACCESS_KEY}"),
                "-e",
                &format!("MINIO_ROOT_PASSWORD={MINIO_SECRET_KEY}"),
                MINIO_IMAGE,
                "server",
                "/data",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .status()
            .expect("spawn docker run minio");
        assert!(status.success(), "docker run minio failed");
        wait_for_minio_ready();
        Self { name }
    }
}

/// Poll the published host port until `MinIO`'s HTTP listener answers. This
/// avoids a race with the first health check of the fast-starting image.
pub(crate) fn wait_for_minio_ready() {
    let minio_port = minio_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{minio_port}")
        .parse()
        .expect("static addr");
    for _ in 0..60 {
        if std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(500))
            .is_ok()
        {
            // TCP accept != fully-initialised S3 server; give the
            // listenbuckets path a moment to come up.
            std::thread::sleep(std::time::Duration::from_millis(500));
            return;
        }
        // intentional: bounded readiness poll of the external MinIO process;
        // no crabka metric reflects its TCP/S3 listener coming up.
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    panic!("MinIO never accepted TCP on 127.0.0.1:{minio_port}");
}

pub(crate) fn minio_make_bucket(bucket: &str) {
    // `mc mb -p` is idempotent and creates parent prefixes; the inner
    // loop retries the `alias set` so a slow MinIO startup doesn't fail
    // the test on the first probe.
    let minio_port = minio_port();
    let script = format!(
        "for i in 1 2 3 4 5 6 7 8 9 10; do \
           mc alias set local http://host.docker.internal:{minio_port} {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null 2>&1 && break; \
           sleep 1; \
         done && mc mb -p local/{bucket}"
    );
    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            "--entrypoint",
            "/bin/sh",
            MINIO_CLIENT_IMAGE,
            "-c",
            &script,
        ])
        .output()
        .expect("spawn mc mb");
    assert!(
        out.status.success(),
        "mc mb failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// `mc ls --recursive local/<bucket>` for assertion-side bucket inspection.
pub(crate) fn minio_list_objects(bucket: &str) -> String {
    let minio_port = minio_port();
    let script = format!(
        "mc alias set local http://host.docker.internal:{minio_port} {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null && \
         mc ls --recursive local/{bucket}"
    );
    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            "--entrypoint",
            "/bin/sh",
            MINIO_CLIENT_IMAGE,
            "-c",
            &script,
        ])
        .output()
        .expect("spawn mc ls");
    assert!(
        out.status.success(),
        "mc ls failed: stderr={}",
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

impl Drop for MinioContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// Same shape as [`start_host_broker`] but with the S3 tiered-storage
/// backend wired in and a lower `RemoteLogManager` tick, so the acceptance
/// loop completes in seconds rather than at the 30s production default.
///
/// `rlmm` selects the [`crabka_broker::RlmmKind`]. Pass
/// `RlmmKind::InMemory` for tests that only need a single-run round-trip.
/// Pass `RlmmKind::TopicBacked(…)` when the test needs durable metadata that
/// survives a broker restart.
///
/// Returns the broker handle, the temp dir, and the `BrokerConfig` so the
/// caller can reuse it for a restart. The caller must keep the temp dir
/// alive.
pub(crate) fn start_host_broker_with_minio_tier(
    s3: crabka_remote_storage::S3Config,
    rlmm: crabka_broker::RlmmKind,
) -> impl std::future::Future<
    Output = (
        crabka_broker::BrokerHandle,
        tempfile::TempDir,
        crabka_broker::BrokerConfig,
    ),
> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
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
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(crabka_broker::NodeId(1), controller_addr.to_string())],
        heartbeat_interval: crabka_units::millis(3_000),
        heartbeat_timeout: crabka_units::millis(9_000),
        replica_lag_time_max: crabka_units::millis(30_000),
        controller_election_timeout: crabka_units::secs(5),
        controller_heartbeat_interval: crabka_units::millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        remote_storage_backend: Some(crabka_broker::RemoteStorageBackend::S3(s3)),
        // 1s tick so the producer's sealed segments reach S3 (and the
        // local-retention pass evicts them) within the test's wall clock.
        remote_log_manager_interval: crabka_units::secs(1),
        remote_log_metadata: rlmm,
        ..BrokerConfig::default()
    };
    Box::pin(async move {
        let handle = Broker::start(config.clone()).await.expect("start broker");
        eprintln!(
            "CRABKA[test] broker started listen={listen} advertised={bootstrap} (tiered S3 backend)",
            bootstrap = broker0_advertised(),
            listen = broker0_listen()
        );
        (handle, dir, config)
    })
}

// ---------------------------------------------------------------------------
// Shared helpers for tiered-storage tests.
// ---------------------------------------------------------------------------

/// Create a KIP-405 tiered topic and wait for the config overrides to propagate
/// into the partition's `LogConfig`.
///
/// This function uses `segment.bytes=2048` and `local.retention.bytes=1`, so
/// a small produce batch seals several segments and the broker evicts every
/// copied segment from local disk at once. Later reads must then go through
/// the remote tier.
///
/// The function waits up to 10 s for `ReplicatorSupervisor::reconcile` to
/// apply the config to the live partition. Without this gate, the producer's
/// first batches land in a default-config `Log` with 1 GiB segments and
/// `remote_storage_enable=false`, and nothing triggers the tier-copy path.
/// See `compact_log_cleaner_round_trip` for the same pattern.
pub(crate) async fn create_tiered_topic(broker: &crabka_broker::BrokerHandle, topic: &str) {
    // Uses the KIP-405-aware `cp-kafka:7.8.8` image — older clients' `TopicCommand`
    // validates `--config` keys client-side and rejects `remote.storage.enable` /
    // `local.retention.bytes` before the request leaves the container.
    docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TIERED,
        &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--config",
            "remote.storage.enable=true",
            "--config",
            "segment.bytes=2048",
            "--config",
            "local.retention.bytes=1",
            "--config",
            "retention.bytes=-1",
            "--config",
            "retention.ms=-1",
            "--bootstrap-server",
            broker0_advertised(),
        ],
    );

    let cfg_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if let Some(cfg) = broker.partition_log_config_for_test(topic, 0)
            && cfg.remote_storage_enable
            && cfg.segment_size == crabka_units::bytes(2048)
            && cfg.local_retention_size == Some(crabka_units::bytes(1))
        {
            break;
        }
        assert!(
            std::time::Instant::now() <= cfg_deadline,
            "tiered-storage topic config never propagated within 10s; saw {:?}",
            broker.partition_log_config_for_test(topic, 0)
        );
        // intentional: bounded poll of the local reconciled LogConfig override;
        // `partition_log_config_for_test` is not surfaced by any awaiter/metric.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Stream `n` records with the format `record-NNNN` into `topic` through the
/// JVM console producer.
///
/// This function forces per-record batches with `batch.size=1` and
/// `linger.ms=0`, so the broker rolls segments at `segment.bytes=2048`.
/// Without that, the JVM producer collects everything into one large batch
/// and writes it into a single segment. Nothing then triggers a segment
/// roll, and the tier-copy path gets no work.
pub(crate) fn produce_records(topic: &str, n: usize) {
    let mut payload = String::with_capacity(n * 12);
    for i in 0..n {
        use std::fmt::Write as _;
        let _ = writeln!(payload, "record-{i:04}");
    }
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            topic,
            "--producer-property",
            "batch.size=1",
            "--producer-property",
            "linger.ms=0",
            "--producer-property",
            "max.in.flight.requests.per.connection=1",
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
        .write_all(payload.as_bytes())
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "producer failed: {}",
        String::from_utf8_lossy(&producer_out.stderr)
    );
}

/// Poll `mc ls --recursive local/<bucket>` until it lists at least
/// `min_log_objects` entries whose path ends with `.log`, then return the
/// full listing.
///
/// The poll runs at 500 ms intervals for up to 20 s (40 iterations). It
/// panics if the listing never reaches the threshold.
pub(crate) async fn wait_for_minio_segments(bucket: &str, min_log_objects: usize) -> String {
    let mut bucket_listing = String::new();
    let mut copied_log_objects = 0usize;
    for _ in 0..40 {
        // intentional: bounded poll of an external process (MinIO via `mc ls`);
        // no crabka metric reflects object arrival in the bucket.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        bucket_listing = minio_list_objects(bucket);
        copied_log_objects = bucket_listing
            .lines()
            .filter(|line| {
                std::path::Path::new(line)
                    .extension()
                    .is_some_and(|extension| extension == "log")
            })
            .count();
        if copied_log_objects >= min_log_objects {
            return bucket_listing;
        }
    }
    panic!(
        "expected ≥{min_log_objects} segment `.log` objects in MinIO; \
         saw {copied_log_objects}. Bucket listing:\n{bucket_listing}"
    );
}

/// Consume up to `max` records from `topic` (partition 0, from-beginning)
/// with the JVM console consumer. Returns the number of non-empty output
/// lines.
///
/// `bootstrap_host_port` is the Kafka bootstrap address that is visible from
/// inside the Docker container, for example an allocated port.
/// Single-broker callers should pass `broker0_advertised()`.
pub(crate) fn consume_records(
    topic: &str,
    max: usize,
    timeout_ms: u64,
    bootstrap_host_port: &str,
) -> usize {
    let consumer_out = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TIERED,
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            bootstrap_host_port,
            "--topic",
            topic,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            &max.to_string(),
            "--timeout-ms",
            &timeout_ms.to_string(),
        ],
    );
    let stdout = String::from_utf8_lossy(&consumer_out.stdout);
    stdout.lines().filter(|l| !l.trim().is_empty()).count()
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
// Additionally, the Crabka producer does not yet implement leader-redirect
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
    s3: crabka_remote_storage::S3Config,
) -> (
    crabka_broker::BrokerHandle,
    crabka_broker::BrokerHandle,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
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
    let rlmm_cfg = crabka_broker::KafkaRlmmConfig {
        bootstrap: rlmm_broker0_advertised().to_string(),
        num_partitions: 1,
        replication: 1,
        snapshot_interval: crabka_units::secs(2),
        snapshot_dir: std::path::PathBuf::new(), // derived from log.dir
        security: None,
        ..crabka_broker::KafkaRlmmConfig::default()
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
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: ctrl0,
        controller_quorum_voters: voters
            .iter()
            .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
            .collect(),
        // Accelerated timers for fast failover — matches acks_all_survives_leader_crash.
        heartbeat_interval: crabka_units::millis(200),
        heartbeat_timeout: crabka_units::millis(2_000),
        replica_lag_time_max: crabka_units::millis(2_000),
        controller_election_timeout: crabka_units::millis(500),
        controller_heartbeat_interval: crabka_units::millis(100),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        remote_storage_backend: Some(crabka_broker::RemoteStorageBackend::S3(s3_b0)),
        remote_log_manager_interval: crabka_units::secs(1),
        remote_log_metadata: crabka_broker::RlmmKind::TopicBacked(rlmm_b0),
        ..BrokerConfig::default()
    };

    let cfg1 = BrokerConfig {
        broker_id: 2,
        listen_addr: listen1,
        advertised_listener: broker1_advertised().to_string(),
        log_dir: dir1.path().to_path_buf(),
        log_config: LogConfig::default(),
        node_id: crabka_broker::NodeId(2),
        controller_listen_addr: ctrl1,
        controller_quorum_voters: voters
            .iter()
            .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
            .collect(),
        heartbeat_interval: crabka_units::millis(200),
        heartbeat_timeout: crabka_units::millis(2_000),
        replica_lag_time_max: crabka_units::millis(2_000),
        controller_election_timeout: crabka_units::millis(500),
        controller_heartbeat_interval: crabka_units::millis(100),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        remote_storage_backend: Some(crabka_broker::RemoteStorageBackend::S3(s3_b1)),
        remote_log_manager_interval: crabka_units::secs(1),
        remote_log_metadata: crabka_broker::RlmmKind::TopicBacked(rlmm_b1),
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
        "CRABKA[test] two-broker tiered: b0={listen} adv={bootstrap} b1={listen_b1} adv={bootstrap_b1} \
         (MinIO S3 + topic-backed RLMM num_partitions=1 replication=1 bootstrap={rlmm_bootstrap})",
        bootstrap = broker0_advertised(),
        bootstrap_b1 = broker1_advertised(),
        listen = broker0_listen(),
        listen_b1 = broker1_listen(),
        rlmm_bootstrap = rlmm_broker0_advertised()
    );
    (broker0, broker1, dir0, dir1)
}
