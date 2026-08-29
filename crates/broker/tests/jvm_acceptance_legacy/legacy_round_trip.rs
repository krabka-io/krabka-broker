//! The pure-legacy round trip, in which a Kafka 0.10.1 producer and a Kafka
//! 0.10.1 consumer talk to the same modern broker.
//!
//! This is the only case here that exercises up-conversion in the `Produce`
//! handler and down-conversion in the `Fetch` handler within one test, so it
//! stands on its own.

use std::{
    io::Write,
    process::{Command, Stdio},
};

use assert2::assert;

use crate::jvm_acceptance::{
    KAFKA_IMAGE_LEGACY, broker0_advertised, docker_run_kafka_tool, nc_check_connectivity,
    start_host_broker,
};

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
