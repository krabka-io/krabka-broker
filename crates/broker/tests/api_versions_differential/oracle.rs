//! The two brokers this suite reads an `ApiVersions` table from, and the one
//! JVM tool it reads them with.
//!
//! krabka runs in this process and is addressed from a throwaway tool container
//! over the bridge, the way the rest of the JVM harness does it. The Kafka
//! oracle is the same image running as a broker, and the tool runs inside that
//! container against its own loopback listener -- so the oracle needs no
//! published port and cannot collide with a concurrent suite.

use std::process::Command;

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_log::LogConfig;

use crate::support::{JvmListeners, unique_container_name};

/// The Kafka release this suite compares krabka against.
///
/// 4.3.1 is the newest image //bazel/images pins, and the newest release is the
/// right oracle for a table that is meant to be current: an older one would
/// report an absent key for every API Kafka has added since.
pub(crate) const ORACLE_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.3.1";

/// The tool, at its path inside the `apache/kafka` image.
const TOOL: &str = "/opt/kafka/bin/kafka-broker-api-versions.sh";

/// One-second attempts the oracle gets to answer before the suite gives up.
const ORACLE_BOOT_ATTEMPTS: u32 = 60;

/// Boot an in-process krabka broker on `listeners`, advertised under the name
/// the tool containers resolve through `--add-host`.
pub(crate) async fn start_krabka(listeners: &JvmListeners) -> (BrokerHandle, tempfile::TempDir) {
    crate::support::init_tracing();
    let dir = tempfile::tempdir().expect("tempdir");
    let controller_addr: std::net::SocketAddr =
        listeners.controller.parse().expect("allocated addr");
    let config = BrokerConfig {
        broker_id: 1,
        listen_addr: listeners.listen.parse().expect("allocated addr"),
        advertised_listener: listeners.advertised.clone(),
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
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config).await.expect("start broker");
    eprintln!(
        "KRABKA[test] broker started listen={} advertised={}",
        listeners.listen, listeners.advertised
    );
    (handle, dir)
}

/// Run the tool from a throwaway container against the krabka broker that
/// `listeners` advertises, and return its stdout.
pub(crate) fn krabka_api_versions(listeners: &JvmListeners) -> String {
    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            ORACLE_IMAGE,
            TOOL,
            "--bootstrap-server",
            &listeners.advertised,
        ])
        .output()
        .expect("spawn docker run kafka-broker-api-versions");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success(),
        "kafka-broker-api-versions against krabka failed: status={} stdout={stdout} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );
    stdout
}

/// A running single-node Kafka oracle. Dropping it removes the container.
pub(crate) struct OracleBroker {
    name: String,
}

impl OracleBroker {
    /// Boot [`ORACLE_IMAGE`] as a single-node `KRaft` broker and wait until its
    /// own tool can read its `ApiVersions` table.
    ///
    /// # Panics
    ///
    /// Panics when the container will not start, or when the broker does not
    /// answer within [`ORACLE_BOOT_ATTEMPTS`] one-second attempts.
    pub(crate) fn start() -> Self {
        let name = unique_container_name("krabka-apiversions-oracle");
        let out = Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                &name,
                "-e",
                "KAFKA_NODE_ID=1",
                "-e",
                "KAFKA_PROCESS_ROLES=broker,controller",
                "-e",
                "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
                "-e",
                "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092",
                "-e",
                "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
                "-e",
                "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
                "-e",
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                "-e",
                "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
                "-e",
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
                "-e",
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
                "-e",
                "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
                "-e",
                "CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk",
                ORACLE_IMAGE,
            ])
            .output()
            .expect("spawn docker run kafka oracle");
        assert!(
            out.status.success(),
            "docker run {ORACLE_IMAGE} failed: stdout={} stderr={}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
        let oracle = Self { name };
        oracle.wait_ready();
        oracle
    }

    /// Read the oracle's own `ApiVersions` table, from inside its container.
    pub(crate) fn api_versions(&self) -> String {
        let out = self.exec_tool();
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        assert!(
            out.status.success(),
            "kafka-broker-api-versions against {ORACLE_IMAGE} failed: status={} stdout={stdout} stderr={}",
            out.status,
            String::from_utf8_lossy(&out.stderr),
        );
        stdout
    }

    fn exec_tool(&self) -> std::process::Output {
        Command::new("docker")
            .args([
                "exec",
                &self.name,
                TOOL,
                "--bootstrap-server",
                "localhost:9092",
            ])
            .output()
            .expect("spawn docker exec kafka-broker-api-versions")
    }

    fn wait_ready(&self) {
        for attempt in 0..ORACLE_BOOT_ATTEMPTS {
            if self.exec_tool().status.success() {
                eprintln!("KRABKA[test] oracle {ORACLE_IMAGE} ready after {attempt} attempts");
                return;
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        panic!(
            "{ORACLE_IMAGE} did not answer within {ORACLE_BOOT_ATTEMPTS}s; container logs:\n{}",
            self.logs()
        );
    }

    fn logs(&self) -> String {
        let out = Command::new("docker")
            .args(["logs", &self.name])
            .output()
            .expect("spawn docker logs");
        format!(
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        )
    }
}

impl Drop for OracleBroker {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}
