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

/// KIP-966 needs a `DescribeTopicPartitions` client and a `TopicCommand` that
/// renders the `Elr:` / `LastKnownElr:` columns. Neither [`KAFKA_IMAGE`] (Kafka
/// 2.7) nor [`KAFKA_IMAGE_TXN`] (Kafka 3.5) has either: their `kafka-topics`
/// still describes topics through a `Metadata` fan-out, and Kafka's `Metadata`
/// schema carries no ELR field in any version.
/// `mirror.gcr.io/apache/kafka:4.3.1` is the same image
/// `jvm_kip320_divergence` uses as a modern `AdminClient`, and it doubles as
/// the JVM broker the ELR columns are compared against. Its tools are not on
/// `PATH`; call them by their `/opt/kafka/bin` path.
pub(crate) const KAFKA_IMAGE_ELR: &str = "mirror.gcr.io/apache/kafka:4.3.1";

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
    let out = docker_run_kafka_tool_allowing_failure_with_image(image, args);
    assert!(
        out.status.success(),
        "docker run image={image} {args:?} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

/// Run a Kafka CLI tool the way [`docker_run_kafka_tool`] does, and hand back
/// what it did without asserting that it succeeded.
///
/// A case about a refusal needs this: the JVM tool exits non-zero and prints
/// the broker's error, which is the evidence, so the asserting helpers above
/// would fail the test on the very outcome it is checking. Pair it with
/// [`tool_output`] to search both streams as one text.
pub(crate) fn docker_run_kafka_tool_allowing_failure(args: &[&str]) -> std::process::Output {
    docker_run_kafka_tool_allowing_failure_with_image(KAFKA_IMAGE, args)
}

/// [`docker_run_kafka_tool_allowing_failure`] against a caller-chosen image.
pub(crate) fn docker_run_kafka_tool_allowing_failure_with_image(
    image: &str,
    args: &[&str],
) -> std::process::Output {
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
    out
}

/// A finished tool's stdout followed by its stderr, as one text.
///
/// The JVM tools split a failure across the two streams in ways that differ
/// per tool -- `kafka-configs` prints its own summary line on stdout and the
/// exception on stderr -- so a case about a refusal searches both.
pub(crate) fn tool_output(out: &std::process::Output) -> String {
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    text
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

    config.remove(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
      producer.send(new ProducerRecord<>(args[1], "after-abort")).get();
    }
    System.out.println("TXNPROBE OK");
  }
}
"#;

/// A compiled `KafkaStreams` topology, for the suite that runs the real
/// Streams runtime against krabka (`tests/jvm_streams_app.rs`).
///
/// The shape is chosen so that every internal-topic contract KIP-1071 and the
/// Streams runtime put on a broker is exercised by one run:
///
/// - `selectKey` then an explicitly named `repartition()`, so
///   `InternalTopicManager` creates `<application.id>-shuffle-repartition`
///   with `RepartitionTopicConfig`'s overrides (`cleanup.policy=delete`,
///   `retention.ms=-1`, `segment.bytes=52428800`).
/// - a time-windowed `count()` with the record cache disabled, so every
///   update is forwarded rather than the last per key per commit interval,
///   and with no `Materialized` name, so the store name
///   is the generated `KSTREAM-AGGREGATE-STATE-STORE-<n>` and the changelog
///   is created with `WindowedChangelogTopicConfig`'s
///   `cleanup.policy=compact,delete`.
/// - `processing.guarantee=exactly_once_v2`, so the single stream thread runs
///   one transactional producer and every sink and changelog write is inside
///   a transaction the consumer only sees under `read_committed`.
///
/// Both configs also carry `message.timestamp.type=CreateTime` from
/// `InternalTopicConfig.INTERNAL_TOPIC_DEFAULT_OVERRIDES`, which is the
/// registration KIP-1071's internal topics depend on.
///
/// The app seeds its own input with a plain producer at FIXED record
/// timestamps before it starts the topology. That is what makes the expected
/// output exact: every seeded record lands in the same window, so the emitted
/// running counts are a function of the input order alone and not of when the
/// test happened to run.
///
/// Arguments: `<bootstrap> <input topic> <output topic> <application.id>
/// [<group.protocol>]`. The fifth argument is set as the raw `group.protocol`
/// key rather than through a `StreamsConfig` constant, because the class is
/// compiled against Kafka 3.5 jars, which have no KIP-1071 constant, and is
/// then also run against 4.3 jars, which do.
///
/// It prints `STREAMSPROBE OK` once the topology has emitted every expected
/// output record, and `STREAMSPROBE TIMEOUT remaining=<n>` (exit 1) if it has
/// not within the latch budget.
pub(crate) const STREAMS_APP_JAVA: &str = r#"
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.Stores;

public final class StreamsApp {
  // A fixed record timestamp for every seeded input record. The window is ten
  // minutes wide and epoch-aligned, so all five records fall in the one window
  // whatever the wall clock says when the suite runs.
  private static final long BASE_TIMESTAMP = 1700000000000L;
  private static final Duration WINDOW = Duration.ofMinutes(10);
  private static final Duration RETENTION = Duration.ofMinutes(30);
  private static final List<String> WORDS =
      List.of("alpha", "alpha", "beta", "alpha", "beta");

  public static void main(String[] args) throws Exception {
    final String bootstrap = args[0];
    final String input = args[1];
    final String output = args[2];
    final String applicationId = args[3];
    final String groupProtocol = args.length > 4 ? args[4] : "classic";

    seed(bootstrap, input);

    final CountDownLatch emitted = new CountDownLatch(WORDS.size());

    StreamsBuilder builder = new StreamsBuilder();
    builder.stream(input, Consumed.with(Serdes.String(), Serdes.String()))
        .selectKey((key, value) -> value)
        .repartition(Repartitioned.<String, String>with(Serdes.String(), Serdes.String())
            .withName("shuffle")
            .withNumberOfPartitions(1))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(WINDOW))
        // An in-memory window store, not the default RocksDB one: the Apache
        // image is JRE-only and carries no `libstdc++`, so `librocksdbjni`
        // cannot load there and the topology would die on its first commit.
        // The store choice is a client-side one and changes nothing krabka
        // sees: a windowed store is still logged, so the changelog topic is
        // still created with `cleanup.policy=compact,delete`, which is what
        // this suite reads back.
        .count(Materialized.<String, Long>as(
                Stores.inMemoryWindowStore("krabka-counts", RETENTION, WINDOW, false))
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.Long()))
        .toStream()
        .map((windowedKey, count) ->
            new KeyValue<>(windowedKey.key(), windowedKey.key() + ":" + count))
        .peek((key, value) -> emitted.countDown())
        .to(output, Produced.with(Serdes.String(), Serdes.String()));

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
    config.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
    // Forward every update rather than the last one per key per commit. The
    // record cache is 10MB by default, which collapses the running counts
    // this suite reads back into one record per key -- `alpha:3`, `beta:2` --
    // and the five expected outputs would never all arrive. The raw key is
    // used rather than the `StreamsConfig` constant because the class is
    // compiled against Kafka 3.5 jars and also runs against 4.3 ones.
    config.put("statestore.cache.max.bytes", 0);
    config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/krabka-streams-state/" + applicationId);
    if (!"classic".equals(groupProtocol)) {
      config.put("group.protocol", groupProtocol);
    }

    KafkaStreams streams = new KafkaStreams(builder.build(), config);
    streams.setUncaughtExceptionHandler(error -> {
      System.err.println("STREAMSPROBE uncaught " + error);
      error.printStackTrace();
      return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
    });
    streams.start();
    boolean complete = emitted.await(180, TimeUnit.SECONDS);
    streams.close(Duration.ofSeconds(60));
    if (complete) {
      System.out.println("STREAMSPROBE OK");
    } else {
      System.out.println("STREAMSPROBE TIMEOUT remaining=" + emitted.getCount());
      System.exit(1);
    }
  }

  // Seed the input topic at fixed timestamps, with a plain producer, before
  // the topology starts. `auto.offset.reset` defaults to `earliest` under
  // Streams, so the run sees every one of them.
  private static void seed(String bootstrap, String input) throws Exception {
    Properties config = new Properties();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
      for (int i = 0; i < WORDS.size(); i++) {
        producer.send(new ProducerRecord<>(
            input, null, BASE_TIMESTAMP + i, "seed-" + i, WORDS.get(i))).get();
      }
    }
    System.out.println("STREAMSPROBE seeded=" + WORDS.size());
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
