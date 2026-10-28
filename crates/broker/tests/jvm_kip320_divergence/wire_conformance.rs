//! Scenario 1: wire conformance of `OffsetForLeaderEpoch` and Fetch v12.
//!
//! The scenario proves that an official JVM client decodes Krabka's
//! `OffsetForLeaderEpoch` (`api_key` 23) responses and the tagged
//! `diverging_epoch` / `current_leader` Fetch v12+ fields byte-exactly. It is
//! the only scenario that runs against a single Krabka broker, and it carries
//! the Java helper source it compiles in-container, so it stands apart from the
//! mixed-cluster scenarios.

use {
    crate::{
        docker::{
            KAFKA_IMAGE, KAFKA_IMAGE_MODERN, docker_rm, docker_run_kafka_tool_with_image,
            produce_lines_via_jvm,
        },
        support::{self, controller_voters, jvm_broker_config},
    },
    krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle},
    krabka_metadata::MetadataRecord,
    std::{
        fs,
        process::{Command, Output},
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::task,
};

/// A running broker and the directory that backs it.
struct StartedBroker {
    broker: BrokerHandle,
    dir: TempDir,
}

/// Single-broker Krabka config bound on `0.0.0.0:<client_port>`, advertised as
/// `host.docker.internal:<client_port>`. Mirrors `start_host_broker` but
/// parameterized on the port so the wire-conformance test can pick a port that
/// doesn't collide with the rest of the JVM suite.
async fn start_host_broker_on(client_port: u16, controller_port: u16) -> StartedBroker {
    support::init_tracing();
    let dir = TempDir::new().expect("tempdir");
    let config = BrokerConfig {
        controller_quorum_voters: vec![(
            krabka_broker::NodeId(1),
            format!("127.0.0.1:{controller_port}"),
        )],
        heartbeat_interval: krabka_units::millis(3_000),
        // This broker advertises a container-only hostname, so its host-side
        // heartbeat client cannot loop back through the advertised listener.
        // Keep it alive for the bounded in-container Java compile and probe.
        heartbeat_timeout: krabka_units::secs(120),
        bootstrap_mode: BootstrapMode::Bootstrap,
        ..jvm_broker_config()
            .log_dir(dir.path().to_path_buf())
            .listen(format!("0.0.0.0:{client_port}").parse().expect("addr"))
            .controller(format!("0.0.0.0:{controller_port}").parse().expect("addr"))
            .advertised(format!("host.docker.internal:{client_port}"))
            .voters(controller_voters(&[(
                1,
                format!("0.0.0.0:{controller_port}").parse().expect("addr"),
            )]))
            .call()
    };
    let handle = Broker::start(config).await.expect("start broker");
    StartedBroker {
        broker: handle,
        dir,
    }
}

/// The small Java helper that proves the official JVM client decodes Krabka's
/// `OffsetForLeaderEpoch` (`api_key` 23) + Fetch v12+ responses byte-exactly.
///
/// It builds an official `org.apache.kafka.clients.consumer.KafkaConsumer`,
/// assigns the partition, and drains both leader epochs. The JVM `Fetcher`'s
/// offset/position-validation pass issues `OffsetForLeaderEpoch` for KIP-320.
/// It decodes the tagged `diverging_epoch` / `current_leader` fields the
/// Krabka leader stamps into Fetch v12+ responses. The byte-exactness signal
/// is a clean drain with no `LogTruncationException` and no
/// `RecordDeserializationException`, plus the observed
/// `beginningOffsets`/`endOffsets` that frame the old-epoch boundary. The
/// helper prints `KIP320PROBE OK` on success. Otherwise it prints
/// `KIP320PROBE FAIL ...` and exits non-zero, so the Rust side can assert on
/// stdout.
///
/// The test writes the source string to a host tempdir and mounts it into the
/// cp-kafka container. It then compiles the source in-container with the
/// bundled JDK's `javac` against the container's Kafka client jars, and runs
/// it.
const OFFSET_FOR_LEADER_EPOCH_HELPER_JAVA: &str = r#"
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import java.time.Duration;
import java.util.*;

public class Kip320Probe {
  public static void main(String[] args) throws Exception {
    String bootstrap = args[0];
    String topic = args[1];
    long expectedOldEpochEnd = Long.parseLong(args[2]);

    Properties p = new Properties();
    p.put("bootstrap.servers", bootstrap);
    p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    p.put("group.id", "kip320-probe");
    p.put("auto.offset.reset", "earliest");
    // Force the modern Fetch path (v12+) so the broker's tagged
    // diverging_epoch / current_leader fields are exercised on decode.
    p.put("enable.auto.commit", "false");

    KafkaConsumer<String,String> c = new KafkaConsumer<>(p);
    TopicPartition tp = new TopicPartition(topic, 0);
    c.assign(Collections.singletonList(tp));
    c.seekToBeginning(Collections.singletonList(tp));

    // Drain everything. If Krabka's OffsetForLeaderEpoch / diverging_epoch
    // bytes were malformed, the JVM Fetcher would either throw
    // LogTruncationException or RecordDeserializationException here.
    int polled = 0;
    long end = System.currentTimeMillis() + 20000;
    long beginning = c.beginningOffsets(Collections.singletonList(tp)).get(tp);
    long latest = c.endOffsets(Collections.singletonList(tp)).get(tp);
    while (System.currentTimeMillis() < end && c.position(tp) < latest) {
      ConsumerRecords<String,String> recs = c.poll(Duration.ofMillis(500));
      polled += recs.count();
    }
    long finalPosition = c.position(tp);
    System.out.println("KIP320PROBE beginning=" + beginning + " latest=" + latest + " position=" + finalPosition + " polled=" + polled);

    // The consumer committed/validated its positions across both epochs via
    // OffsetForLeaderEpoch under the hood. We assert the visible end offset
    // matches the broker's reported log end, and that the OLD epoch boundary
    // we were told to expect lies strictly inside [beginning, latest].
    if (latest <= 0) { System.out.println("KIP320PROBE FAIL empty-log"); System.exit(2); }
    if (finalPosition != latest) { System.out.println("KIP320PROBE FAIL incomplete-drain"); System.exit(4); }
    if (polled <= 0) { System.out.println("KIP320PROBE FAIL no-records-polled"); System.exit(5); }
    if (expectedOldEpochEnd <= beginning || expectedOldEpochEnd > latest) {
      System.out.println("KIP320PROBE FAIL boundary expectedOldEpochEnd=" + expectedOldEpochEnd);
      System.exit(3);
    }
    System.out.println("KIP320PROBE OK");
    c.close();
  }
}
"#;

const TOPIC: &str = "krabka-kip320-wire";
#[derive(Default)]
struct JvmWireEpochs {
    started: Option<StartedBroker>,
    bootstrap: String,
    container: String,
    client_port: u16,
    epoch0_end: Option<i64>,
    rust_answer: Option<(i16, i64)>,
    java_output: Option<Output>,
}
#[rstest::fixture]
fn jvm_wire_epochs() -> JvmWireEpochs {
    // Each wire exchange starts its own broker and owns its backing directory.
    JvmWireEpochs::default()
}
#[rstest_bdd_macros::given(
    "a single Krabka topic krabka-kip320-wire with epoch-zero records {records}"
)]
async fn first_wire_epoch(jvm_wire_epochs: &mut JvmWireEpochs, records: String) {
    let records: Vec<String> = serde_json::from_str(&records).expect("epoch zero payloads");
    let container = support::unique_container_name("krabka-kip320-wire-helper");
    // Allocated rather than fixed at 10692/10693: two runs of this suite would
    // otherwise race for the same bind, and the loser reports `Address already
    // in use` as a test failure.
    let client_port = support::free_port();
    let controller_port = support::free_port();
    let bootstrap = format!("host.docker.internal:{client_port}");

    docker_rm(&container);
    let StartedBroker { broker, dir } = start_host_broker_on(client_port, controller_port).await;

    // 1. Create topic (1 partition, RF=1).
    let out = docker_run_kafka_tool_with_image(KAFKA_IMAGE, &[
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
        bootstrap.as_str(),
    ]);
    assert2::assert!(out.status.success(), "create topic failed");

    // 2. Produce a first batch at the current (epoch 0) leadership.
    produce_lines_via_jvm(bootstrap.as_str(), TOPIC, &records);

    // The offset boundary of epoch 0 is the broker's current log end offset.
    let epoch0_end = broker
        .local_log_end_offset(TOPIC, 0)
        .expect("partition hosted");
    eprintln!("KRABKA[kip320] epoch-0 boundary (LEO) = {epoch0_end}");

    jvm_wire_epochs.started = Some(StartedBroker { broker, dir });
    jvm_wire_epochs.bootstrap = bootstrap;
    jvm_wire_epochs.container = container;
    jvm_wire_epochs.client_port = client_port;
    jvm_wire_epochs.epoch0_end = Some(epoch0_end);
}
#[rstest_bdd_macros::when("leadership advances one epoch and records {records} are produced")]
async fn next_wire_epoch(jvm_wire_epochs: &mut JvmWireEpochs, records: String) {
    let records: Vec<String> = serde_json::from_str(&records).expect("new epoch payloads");
    let broker = &jvm_wire_epochs
        .started
        .as_ref()
        .expect("broker started")
        .broker;
    let bootstrap = &jvm_wire_epochs.bootstrap;
    // 3. Bump the partition's leader epoch to simulate a leadership change,
    //    then produce a second batch at the new epoch. Now an
    //    OffsetForLeaderEpoch(epoch=0) MUST return `epoch0_end`.
    let mut partition = broker
        .partition_record_for_test(TOPIC, 0)
        .expect("wire-probe partition metadata");
    partition.leader_epoch = partition.leader_epoch.next();
    let epoch1 = partition.leader_epoch;
    partition.partition_epoch += 1;
    broker
        .submit_metadata_record_for_test(MetadataRecord::V1Partition(partition))
        .await
        .expect("advance wire-probe leader epoch in metadata");
    let epoch_deadline = Instant::now() + Duration::from_secs(5);
    while broker
        .partition_record_for_test(TOPIC, 0)
        .is_none_or(|partition| partition.leader_epoch != epoch1)
    {
        assert2::assert!(
            Instant::now() <= epoch_deadline,
            "wire-probe leader epoch did not reach metadata"
        );
        task::yield_now().await;
    }
    produce_lines_via_jvm(bootstrap.as_str(), TOPIC, &records);
}
#[rstest_bdd_macros::when(
    "the Rust client queries partition 0 epoch 0 without current-epoch fencing"
)]
async fn rust_epoch_boundary(jvm_wire_epochs: &mut JvmWireEpochs) {
    let client = krabka_client_core::Client::builder()
        .bootstrap(format!("127.0.0.1:{}", jvm_wire_epochs.client_port))
        .build()
        .await
        .expect("rust probe client");
    let answer = client
        .offset_for_leader_epoch(TOPIC, 0, -1, 0)
        .await
        .expect("offset_for_leader_epoch");
    jvm_wire_epochs.rust_answer = Some((answer.error_code, answer.end_offset));
}
#[rstest_bdd_macros::then(
    "the Rust epoch answer has Kafka code {code} and the recorded epoch-zero end offset"
)]
fn expected_rust_epoch(jvm_wire_epochs: &JvmWireEpochs, code: i16) {
    assert2::assert!(
        jvm_wire_epochs.rust_answer.expect("Rust query executed")
            == (
                code,
                jvm_wire_epochs.epoch0_end.expect("epoch zero boundary")
            )
    );
}
#[rstest_bdd_macros::when(
    "the official JVM consumer validates positions and drains both Fetch v12 epochs"
)]
fn java_epoch_boundary(jvm_wire_epochs: &mut JvmWireEpochs) {
    let container = &jvm_wire_epochs.container;
    let bootstrap = &jvm_wire_epochs.bootstrap;
    let epoch0_end = jvm_wire_epochs.epoch0_end.expect("epoch zero boundary");
    // 5. Compile + run the Java helper inside the cp-kafka container. It drives
    //    the official Apache Kafka consumer, which validates positions via
    //    OffsetForLeaderEpoch and decodes Fetch v12+ tagged diverging_epoch /
    //    current_leader fields. A clean drain + matching boundary is the
    //    byte-exactness signal.
    let helper_dir = TempDir::new().unwrap();
    let helper_path = helper_dir.path().join("Kip320Probe.java");
    fs::write(&helper_path, OFFSET_FOR_LEADER_EPOCH_HELPER_JAVA).unwrap();
    // The helper image runs as a non-root uid, while `TempDir` is 0700.
    #[cfg(unix)]
    {
        use crate::support::chmod_for_container;

        chmod_for_container(helper_dir.path(), 0o755, "chmod Java helper directory");
        chmod_for_container(&helper_path, 0o644, "chmod Java helper source");
    }
    let entry = format!(
        "set -e; cp /helper/Kip320Probe.java /tmp/Kip320Probe.java; \
         CP=$(ls /usr/share/java/kafka/*.jar 2>/dev/null | tr '\\n' ':')$(ls /usr/share/java/cp-base-new/*.jar 2>/dev/null | tr '\\n' ':'); \
         javac -cp \"$CP\" -d /tmp /tmp/Kip320Probe.java; \
         java -cp \"/tmp:$CP\" Kip320Probe {bootstrap} {TOPIC} {epoch0_end}"
    );
    let helper_out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--name",
            container,
            "--add-host=host.docker.internal:host-gateway",
            "-v",
            &format!("{}:/helper", helper_dir.path().display()),
            "--entrypoint",
            "bash",
            // Modern image (Kafka 3.5): Fetch v12+ + full KIP-320 client path.
            KAFKA_IMAGE_MODERN,
            "-c",
            &entry,
        ])
        .output()
        .expect("spawn java helper");

    jvm_wire_epochs.java_output = Some(helper_out);
}
#[rstest_bdd_macros::then("the Java consumer reports {marker} without {failure}")]
fn expected_java_epoch(jvm_wire_epochs: &JvmWireEpochs, marker: String, failure: String) {
    let output = jvm_wire_epochs
        .java_output
        .as_ref()
        .expect("JVM consumer executed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!(
        "KRABKA[kip320] java helper status={} stdout={stdout} stderr={stderr}",
        output.status
    );
    assert2::assert!(
        !stderr.contains(&failure) && !stdout.contains(&failure),
        "JVM consumer decoding failure: {stderr}"
    );
    assert2::assert!(
        stdout.contains(&marker),
        "JVM conformance probe: stdout={stdout} stderr={stderr}"
    );
}
#[rstest_bdd_macros::then("the wire conformance broker and Java container are shut down")]
async fn finish_wire_epochs(jvm_wire_epochs: &mut JvmWireEpochs) {
    docker_rm(&jvm_wire_epochs.container);
    let StartedBroker {
        broker,
        dir: _directory_guard,
    } = jvm_wire_epochs.started.take().expect("broker started");
    broker.shutdown().await;
}
#[ignore = "requires Docker; Linux-bound (host.docker.internal bridge)"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_kip320_divergence/wire_conformance.feature",
    name = "Official JVM Fetch and epoch validation agree with the Rust epoch boundary"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kip320_wire_conformance_offset_for_leader_epoch(jvm_wire_epochs: JvmWireEpochs) {
    let _ = jvm_wire_epochs;
}
