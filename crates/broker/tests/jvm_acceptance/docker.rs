//! Docker invocation of the Apache Kafka command-line tools.
//!
//! This module pins the cp-kafka images the suites run, wraps `docker run` so a
//! failed tool reports its captured output, and builds the host-side files that
//! those containers bind-mount.

use std::process::{Command, Stdio};

use assert2::assert;

use super::ports::host_port;

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
        "KRABKA[test] docker_run image={image} {args:?} status={} stderr_len={}",
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
        "KRABKA[test] docker_run image={image} mount={mount} {args:?} status={} stderr_len={}",
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
        "KRABKA[test] docker_run image={image} mounts={mounts:?} {args:?} status={} stderr_len={}",
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
