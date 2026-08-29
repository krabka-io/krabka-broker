//! The two mixed-vintage paths, in which one side of the round trip is a Kafka
//! 0.10.1 tool and the other is a Kafka 2.6 tool.
//!
//! Together they pin the two conversions separately: a modern consumer proves
//! the up-converted v2 `RecordBatch` on disk is well formed, and a 0.10.1
//! consumer proves the down-converted v0/v1 `MessageSet` is.

use std::{
    io::Write,
    process::{Command, Stdio},
};

use assert2::assert;

use crate::jvm_acceptance::{
    KAFKA_IMAGE, KAFKA_IMAGE_LEGACY, broker0_advertised, docker_run_kafka_tool,
    nc_check_connectivity, start_host_broker,
};

/// Test 2: legacy producer, modern consumer.
///
/// A Kafka 0.10.1 console-producer sends 3 records. A Kafka 2.6
/// console-consumer (cp-kafka:6.1.1) reads them back with Fetch v11+.
/// The test validates that the up-conversion writes a well-formed v2
/// `RecordBatch` to the log that a modern client can decode, and not
/// only bytes that a Krabka broker accepts on its own.
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
