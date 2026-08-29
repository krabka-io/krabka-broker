//! The compressed legacy batches, where v0 and v1 wrap the whole batch in one
//! outer compressed message rather than compressing each record.
//!
//! Both tests drive `legacy_to_v2`, which has to decompress the outer wrapper
//! and re-emit a v2 `RecordBatch` carrying the same compression marker; gzip
//! and snappy reach different codec paths inside it.

use std::{
    io::Write,
    process::{Command, Stdio},
};

use assert2::assert;

use crate::jvm_acceptance::{
    KAFKA_IMAGE_LEGACY, broker0_advertised, docker_run_kafka_tool, nc_check_connectivity,
    start_host_broker,
};

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
