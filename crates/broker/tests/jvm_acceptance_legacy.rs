//! Kafka 0.10.1 clients (Confluent Platform 3.1.2) against a modern broker,
//! exercising v1 `MessageSet` records and the up/down-conversion paths.
//!
//! The shared harness lives in [`jvm_acceptance`]; see it for the container
//! networking these suites depend on.

mod jvm_acceptance;
mod support;

use std::{
    io::Write,
    process::{Command, Stdio},
};

use assert2::assert;
use jvm_acceptance::*;

/// Test 1: pure-legacy round-trip.
///
/// A Kafka 0.10.1 console-producer (cp-kafka:3.1.2) sends 3 records
/// with Produce v0–2 and v1 `MessageSet` records. A Kafka 0.10.1
/// console-consumer reads them back with Fetch v0–3. The test exercises
/// both up-conversion in the Produce handler and down-conversion in the
/// Fetch handler, end-to-end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_legacy_010_round_trip() {
    const TOPIC: &str = "legacy-010-round-trip";

    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    // 1. Create the topic via the modern AdminClient. The 0.10.x-era
    //    kafka-topics tool used --zookeeper, not --bootstrap-server,
    //    so we can't drive it from a 3.1.2 image without standing up
    //    Zookeeper. Use 6.1.1's AdminClient for setup.
    docker_run_kafka_tool(&[
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
    ]);

    // 2. Produce 3 records via the 0.10.1 console-producer.
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_LEGACY,
            "kafka-console-producer",
            "--broker-list",
            broker0_advertised(),
            "--topic",
            TOPIC,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn legacy producer");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"alpha\nbravo\ncharlie\n")
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait legacy producer");
    assert!(
        producer_out.status.success(),
        "legacy producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // 3. Consume them back via the 0.10.1 console-consumer.
    //    0.10.0 added `--new-consumer` + `--bootstrap-server`; the
    //    old `--zookeeper` mode is unusable without ZK. Use the new
    //    consumer with --partition 0 to bypass group coordination.
    //    The 0.10.x console-consumer can exit non-zero after
    //    --max-messages is satisfied, so we don't assert on exit
    //    status — we only assert that stdout contains the records.
    let consumer_out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_LEGACY,
            "kafka-console-consumer",
            "--new-consumer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            "3",
            "--timeout-ms",
            "10000",
        ])
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("spawn legacy consumer");
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    let stderr = String::from_utf8_lossy(&consumer_out.stderr);
    for needle in ["alpha", "bravo", "charlie"] {
        assert!(
            s.contains(needle),
            "legacy consumer didn't emit {needle}: status={} stdout={s:?} stderr={stderr:?}",
            consumer_out.status,
        );
    }

    broker.shutdown().await;
}

/// Test 2: legacy producer, modern consumer.
///
/// A Kafka 0.10.1 console-producer sends 3 records. A Kafka 2.6
/// console-consumer (cp-kafka:6.1.1) reads them back with Fetch v11+.
/// The test validates that the up-conversion writes a well-formed v2
/// `RecordBatch` to the log that a modern client can decode, and not
/// only bytes that a Crabka broker accepts on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_legacy_010_produce_modern_consume() {
    const TOPIC: &str = "legacy-010-produce-modern-consume";

    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
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
    ]);

    // Produce via legacy.
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_LEGACY,
            "kafka-console-producer",
            "--broker-list",
            broker0_advertised(),
            "--topic",
            TOPIC,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn legacy producer");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"alpha\nbravo\ncharlie\n")
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait legacy producer");
    assert!(
        producer_out.status.success(),
        "legacy producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // Consume via modern (cp-kafka:6.1.1, uses Fetch v11+).
    let consumer_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        broker0_advertised(),
        "--topic",
        TOPIC,
        "--partition",
        "0",
        "--from-beginning",
        "--max-messages",
        "3",
        "--timeout-ms",
        "10000",
    ]);
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for needle in ["alpha", "bravo", "charlie"] {
        assert!(
            s.contains(needle),
            "modern consumer didn't emit {needle}: stdout={s:?}"
        );
    }

    broker.shutdown().await;
}

/// Test 3: modern producer, legacy consumer.
///
/// A Kafka 2.6 console-producer (cp-kafka:6.1.1) sends 3 records with
/// Produce v9. A Kafka 0.10.1 console-consumer (cp-kafka:3.1.2) reads
/// them with Fetch v0–3. The test validates that a real Kafka 0.10.x
/// client can parse the bytes `down_convert_for_fetch` emits as a v0/v1
/// `MessageSet`. That is the load-bearing concern for down-conversion
/// correctness.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_modern_produce_legacy_010_consume() {
    const TOPIC: &str = "modern-produce-legacy-010-consume";

    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
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
    ]);

    // Produce via modern (cp-kafka:6.1.1, Produce v9).
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
            TOPIC,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn modern producer");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"alpha\nbravo\ncharlie\n")
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait modern producer");
    assert!(
        producer_out.status.success(),
        "modern producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // Consume via legacy (cp-kafka:3.1.2, Fetch v0-3).
    // The 0.10.x console-consumer can exit non-zero after
    // --max-messages is satisfied, so we don't assert on exit
    // status — we only assert that stdout contains the records.
    let consumer_out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_LEGACY,
            "kafka-console-consumer",
            "--new-consumer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            "3",
            "--timeout-ms",
            "10000",
        ])
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("spawn legacy consumer");
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    let stderr = String::from_utf8_lossy(&consumer_out.stderr);
    for needle in ["alpha", "bravo", "charlie"] {
        assert!(
            s.contains(needle),
            "legacy consumer didn't emit {needle}: status={} stdout={s:?} stderr={stderr:?}",
            consumer_out.status,
        );
    }

    broker.shutdown().await;
}

/// Test 4: gzip-compressed legacy round-trip.
///
/// A Kafka 0.10.1 console-producer with `compression.type=gzip`
/// sends ~50 records as a single outer-wrapped gzip `MessageSet`. That
/// is how v0/v1 represents compressed batches. A Kafka 2.6
/// console-consumer (cp-kafka:6.1.1) reads them back. The test validates
/// the gzip path through `legacy_to_v2`, which decompresses the legacy
/// batch and re-emits it as a v2 `RecordBatch` with the same compression
/// marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_legacy_010_compressed_round_trip() {
    const TOPIC: &str = "legacy-010-compressed-round-trip";

    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
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
    ]);

    // 50 newline-separated records to give gzip something to compress.
    let mut input = String::with_capacity(50 * 12);
    {
        use std::fmt::Write as _;
        for i in 0..50 {
            writeln!(input, "record-{i:03}").unwrap();
        }
    }

    // Produce via legacy with gzip.
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_LEGACY,
            "kafka-console-producer",
            "--broker-list",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--producer-property",
            "compression.type=gzip",
            "--producer-property",
            "batch.size=131072", // 128 KiB — enough to batch all 50 records together
            "--producer-property",
            "linger.ms=100", // give the producer time to batch
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn legacy producer");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(input.as_bytes())
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait legacy producer");
    assert!(
        producer_out.status.success(),
        "legacy gzip producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // Consume all 50 via modern.
    let consumer_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        broker0_advertised(),
        "--topic",
        TOPIC,
        "--partition",
        "0",
        "--from-beginning",
        "--max-messages",
        "50",
        "--timeout-ms",
        "15000",
    ]);
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for i in 0..50 {
        let needle = format!("record-{i:03}");
        assert!(
            s.contains(&needle),
            "modern consumer didn't emit {needle} after legacy gzip produce"
        );
    }

    broker.shutdown().await;
}

/// Slice 2d follow-up: snappy-compressed legacy round-trip.
///
/// A Kafka 0.10.1 console-producer with `compression.type=snappy` sends
/// ~50 records as a single outer-wrapped snappy `MessageSet`. A Kafka 2.6
/// console-consumer (cp-kafka:6.1.1) reads them back. The test validates
/// the snappy path through `legacy_to_v2`, which converts xerial-framed
/// snappy to a v2 `RecordBatch`.
///
/// NOTE: 0.10.x-era snappy-java framing is fragile against newer JVMs. For
/// that reason slice 2d deferred this test and exercised only gzip live.
/// This test stays here as the documented follow-up. If it proves flaky in
/// CI, pin a specific snappy-java version rather than delete it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_legacy_010_snappy_round_trip() {
    const TOPIC: &str = "legacy-010-snappy-round-trip";

    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
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
    ]);

    // 50 newline-separated records to give snappy something to compress.
    let mut input = String::with_capacity(50 * 12);
    {
        use std::fmt::Write as _;
        for i in 0..50 {
            writeln!(input, "record-{i:03}").unwrap();
        }
    }

    // Produce via legacy with snappy.
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_LEGACY,
            "kafka-console-producer",
            "--broker-list",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--producer-property",
            "compression.type=snappy",
            "--producer-property",
            "batch.size=131072", // 128 KiB — enough to batch all 50 records together
            "--producer-property",
            "linger.ms=100", // give the producer time to batch
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn legacy producer");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(input.as_bytes())
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait legacy producer");
    assert!(
        producer_out.status.success(),
        "legacy snappy producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // Consume all 50 via modern.
    let consumer_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        broker0_advertised(),
        "--topic",
        TOPIC,
        "--partition",
        "0",
        "--from-beginning",
        "--max-messages",
        "50",
        "--timeout-ms",
        "15000",
    ]);
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for i in 0..50 {
        let needle = format!("record-{i:03}");
        assert!(
            s.contains(&needle),
            "modern consumer didn't emit {needle} after legacy snappy produce"
        );
    }

    broker.shutdown().await;
}
