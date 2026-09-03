//! `kafka-console-producer` and `kafka-console-consumer` round-trips that read
//! one partition directly, with no consumer group in the path.
//!
//! The group-driven console runs live in `console_groups`, so a failure here
//! points at the produce and fetch path and not at the coordinator.

use std::{
    io::Write,
    process::{Command, Stdio},
};

use assert2::assert;

use crate::jvm_acceptance::{
    KAFKA_IMAGE, broker0_advertised, docker_run_kafka_tool, nc_check_connectivity,
    start_host_broker,
};

// `flavor = "multi_thread"` is essential here. The test bodies make
// synchronous blocking `Command::output()` calls for each `docker run`.
// On a single-threaded runtime those calls block the only worker — which
// is also driving the broker's accept loop. Incoming TCP connections then
// complete the kernel-level handshake but the broker never reads them,
// and the Java AdminClient times out. A multi-thread runtime puts the
// broker on a separate worker so the test's blocking calls don't starve it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn console_producer_round_trip() {
    const TOPIC: &str = "krabka-broker-itest";

    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    // 1. Create the topic via the JVM client.
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

    // 2. Produce 3 records via stdin.
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
        .expect("spawn producer");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"alpha\nbravo\ncharlie\n")
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "producer failed: {}",
        String::from_utf8_lossy(&producer_out.stderr)
    );

    // 3. Consume them back via --partition 0 (bypasses groups entirely).
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
        assert!(s.contains(needle), "consumer didn't emit {needle}: {s:?}");
    }

    broker.shutdown().await;
}

// Same multi-thread runtime caveat as `console_producer_round_trip`:
// the test body makes blocking `Command::output()` calls; a
// single-threaded runtime would starve the broker's accept loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn rust_producer_to_console_consumer() {
    use krabka_client_producer::{Acks, Compression, Producer, ProducerRecord};

    const TOPIC: &str = "krabka-rust-producer-itest";

    let (broker, _dir) = start_host_broker().await;

    // 1. Create the topic.
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

    // 2. Build a Rust producer pointed at the host broker and produce 3 records.
    let producer = Producer::builder()
        .bootstrap(broker0_advertised().to_string())
        .enable_idempotence(true)
        .acks(Acks::All)
        .compression(Compression::Lz4)
        .build()
        .await
        .expect("producer");
    for v in ["x", "y", "z"] {
        let fut = producer
            .send(ProducerRecord {
                topic: TOPIC.into(),
                value: Some(bytes::Bytes::from(v)),
                ..Default::default()
            })
            .await;
        let m = fut.await.expect("oneshot").expect("ack");
        assert!(m.partition == 0);
    }
    producer.flush().await.expect("flush");
    producer.close().await.expect("close");

    // 3. Consume via kafka-console-consumer --partition 0.
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
        "20000",
    ]);
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for needle in ["x", "y", "z"] {
        assert!(s.contains(needle), "missing {needle}: {s:?}");
    }

    broker.shutdown().await;
}

/// The JVM tools read a `message.timestamp.type=LogAppendTime` topic as a
/// log-append-time topic: `kafka-console-consumer --property
/// print.timestamp=true` prints `LogAppendTime:<ms>` rather than
/// `CreateTime:<ms>`, and `GetOffsetShell --time <that ms>` resolves the stamp
/// back to the batch's offset.
///
/// The printed prefix is the claim this case exists for. It comes from
/// `ConsumerRecord.timestampType()`, which the JVM client reads out of the
/// batch's attribute bit — the one the broker patched at append. A broker that
/// answered the right `logAppendTimeMs` in the produce response but left the
/// stored bit alone still prints `CreateTime` here, and no in-process test of
/// the response row can see that.
///
/// The `--time` lookup is the second half: Kafka builds the time index from
/// the rewritten `maxTimestamp`, so `offsetsForTimes` on such a topic answers
/// in append time. A broker that stamped the header but indexed the producer's
/// timestamp passes the print check and fails this one.
///
/// This case has never been executed: the machine it was written on has no
/// working Docker daemon. Run it with
/// `cargo test -p krabka-broker --test jvm_acceptance_cli -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn console_consumer_prints_log_append_time() {
    const TOPIC: &str = "krabka-log-append-time";

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
        "--config",
        "message.timestamp.type=LogAppendTime",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

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
        .expect("spawn producer");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"stamped\n")
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "producer failed: {}",
        String::from_utf8_lossy(&producer_out.stderr)
    );

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
        "1",
        "--timeout-ms",
        "20000",
        "--property",
        "print.timestamp=true",
    ]);
    let printed = String::from_utf8_lossy(&consumer_out.stdout);
    // The default formatter prints `<timestampType>:<ms>\t<value>`.
    let line = printed
        .lines()
        .find(|line| line.contains("stamped"))
        .unwrap_or_else(|| panic!("no record printed: {printed:?}"));
    assert!(
        line.starts_with("LogAppendTime:"),
        "the JVM client must read the stored batch as log-append time: {line:?}"
    );
    let stamp: i64 = line
        .trim_start_matches("LogAppendTime:")
        .split('\t')
        .next()
        .expect("a timestamp before the tab")
        .trim()
        .parse()
        .expect("the printed stamp is a millisecond clock reading");

    // The stamp resolves back to the batch it was written into, so the time
    // index carries append time and not the producer's own timestamp.
    let offsets_out = docker_run_kafka_tool(&[
        "kafka-run-class",
        "kafka.tools.GetOffsetShell",
        "--broker-list",
        broker0_advertised(),
        "--topic",
        TOPIC,
        "--time",
        &stamp.to_string(),
    ]);
    let resolved = String::from_utf8_lossy(&offsets_out.stdout);
    assert!(
        resolved.contains(&format!("{TOPIC}:0:0")),
        "the stamp must resolve to offset 0: {resolved:?}"
    );

    broker.shutdown().await;
}
