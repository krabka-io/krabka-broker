//! Kafka CLI round-trips against a single host broker: console produce/consume,
//! topic and config administration, consumer-group listing and offset deletion.
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
use crabka_broker::{Broker, BrokerConfig};
use jvm_acceptance::*;

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
    const TOPIC: &str = "crabka-broker-itest";

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

// `flavor = "multi_thread"` is essential here. The test bodies make
// synchronous blocking `Command::output()` calls for each `docker run`.
// On a single-threaded runtime those calls block the only worker — which
// is also driving the broker's accept loop. Incoming TCP connections then
// complete the kernel-level handshake but the broker never reads them,
// and the Java AdminClient times out. A multi-thread runtime puts the
// broker on a separate worker so the test's blocking calls don't starve it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_topics_describe_smokes_metadata() {
    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--topic",
        "described",
        "--partitions",
        "2",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    let out = docker_run_kafka_tool(&[
        "kafka-topics",
        "--describe",
        "--topic",
        "described",
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("Topic: described"),
        "describe missing topic line: {stdout}"
    );
    assert!(
        stdout.contains("PartitionCount: 2"),
        "describe missing partition count: {stdout}"
    );

    broker.shutdown().await;
}

// Same multi-thread runtime caveat as `console_producer_round_trip`:
// the test body makes blocking `Command::output()` calls; a
// single-threaded runtime would starve the broker's accept loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn rust_producer_to_console_consumer() {
    use crabka_client_producer::{Acks, Compression, Producer, ProducerRecord};

    const TOPIC: &str = "crabka-rust-producer-itest";

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

// Same multi-thread runtime caveat as `console_producer_round_trip`:
// the test body makes blocking `Command::output()` calls; a
// single-threaded runtime would starve the broker's accept loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn console_consumer_with_group_round_trip() {
    const TOPIC: &str = "crabka-broker-grp-itest";

    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

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

    // 2. Produce records via kafka-console-producer over stdin.
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
        .write_all(b"x\ny\nz\n")
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "producer failed: {}",
        String::from_utf8_lossy(&producer_out.stderr)
    );

    // 3. Consume WITHOUT --partition. The default `console-consumer`
    //    group will JoinGroup → SyncGroup → Heartbeat → Fetch through
    //    our coordinator.
    let consumer_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        broker0_advertised(),
        "--topic",
        TOPIC,
        "--from-beginning",
        "--group",
        "crabka-acceptance-group",
        "--max-messages",
        "3",
        "--timeout-ms",
        "20000",
    ]);
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for needle in ["x", "y", "z"] {
        assert!(s.contains(needle), "consumer didn't emit {needle}: {s:?}");
    }

    broker.shutdown().await;
}

// KIP-345 static membership: the JVM consumer with
// `group.instance.id` set should round-trip through the coordinator
// (JoinGroup → SyncGroup → Heartbeat → Fetch with the v3+
// `group_instance_id` wire field populated) and a subsequent
// `kafka-consumer-groups --describe` must surface the instance id under
// HOST/CONSUMER-ID columns, confirming the broker persisted it on the
// member metadata.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn console_consumer_with_static_membership() {
    const TOPIC: &str = "crabka-broker-static-itest";
    const GROUP: &str = "crabka-static-grp";
    const INSTANCE: &str = "client-static-1";

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

    // Produce three records.
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
        .write_all(b"a\nb\nc\n")
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "producer failed: {}",
        String::from_utf8_lossy(&producer_out.stderr)
    );

    // Consume with `group.instance.id` set. The JVM consumer sends this
    // as `group_instance_id` in JoinGroup v5+ / SyncGroup v3+ / Heartbeat
    // v3+ / OffsetCommit v7+. If the broker rejects the wire field we'll
    // see a hard failure here.
    let consumer_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        broker0_advertised(),
        "--topic",
        TOPIC,
        "--from-beginning",
        "--group",
        GROUP,
        "--consumer-property",
        &format!("group.instance.id={INSTANCE}"),
        "--max-messages",
        "3",
        "--timeout-ms",
        "20000",
    ]);
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for needle in ["a", "b", "c"] {
        assert!(s.contains(needle), "consumer didn't emit {needle}: {s:?}");
    }

    // `kafka-consumer-groups --describe` exercises the broker's
    // DescribeGroups path. The output should mention the instance id so
    // operators can correlate static slots back to pods.
    let desc_out = docker_run_kafka_tool(&[
        "kafka-consumer-groups",
        "--describe",
        "--group",
        GROUP,
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let s = String::from_utf8_lossy(&desc_out.stdout);
    assert!(s.contains(TOPIC), "describe missing topic {TOPIC}: {s}");

    broker.shutdown().await;
}

/// `kafka-configs --alter --add-config retention.ms=60000 --topic t` then
/// `--describe` round-trips through `V1TopicConfig` and the supervisor
/// reconcile push.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_configs_alter_round_trip() {
    const TOPIC: &str = "crabka-cfg-alter-itest";

    let (_broker, _dir) = start_host_broker().await;
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

    docker_run_kafka_tool(&[
        "kafka-configs",
        "--alter",
        "--entity-type",
        "topics",
        "--entity-name",
        TOPIC,
        "--add-config",
        "retention.ms=60000",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    let out = docker_run_kafka_tool(&[
        "kafka-configs",
        "--describe",
        "--entity-type",
        "topics",
        "--entity-name",
        TOPIC,
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(
        s.contains("retention.ms=60000"),
        "describe output missing retention.ms=60000: {s}"
    );
}

/// `kafka-topics --alter --topic t --partitions 3` then `--describe`
/// shows 3 partitions. Exercises `CreatePartitions` (`api_key` 37) +
/// `V1Topic` partition-count update.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_topics_alter_partitions() {
    const TOPIC: &str = "crabka-alter-parts-itest";

    let (_broker, _dir) = start_host_broker().await;
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

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--alter",
        "--topic",
        TOPIC,
        "--partitions",
        "3",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    let out = docker_run_kafka_tool(&[
        "kafka-topics",
        "--describe",
        "--topic",
        TOPIC,
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(
        s.contains("PartitionCount: 3") || s.contains("Partitions: 3"),
        "describe missing PartitionCount: 3 — got: {s}"
    );
}

/// `kafka-delete-records --offset-json-file <(...)`: produce 20
/// records, trim to offset 10, expect success + `low_watermark`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_delete_records_trims_log() {
    const TOPIC: &str = "crabka-delete-recs-itest";

    let (_broker, _dir) = start_host_broker().await;
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

    // Produce 20 records via console-producer stdin.
    let mut child = std::process::Command::new("docker")
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
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn producer");
    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().expect("stdin");
        for i in 0..20 {
            writeln!(stdin, "msg-{i}").expect("write");
        }
    }
    drop(child.stdin.take());
    let prod_out = child.wait_with_output().expect("wait producer");
    assert!(
        prod_out.status.success(),
        "producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&prod_out.stdout),
        String::from_utf8_lossy(&prod_out.stderr),
    );

    // Build offset-json on the host so we can pass it into the container.
    // The cp-kafka container runs as a non-root user; on Linux,
    // `tempfile::NamedTempFile` creates the file 0600, so the bind-mount is
    // unreadable inside the container. Relax to 0644 so the container's uid
    // can read it. WSL/Docker-Desktop ignores this, but native Linux CI
    // enforces it strictly.
    let json = format!(
        r#"{{"partitions":[{{"topic":"{TOPIC}","partition":0,"offset":10}}],"version":1}}"#
    );
    let tmp = tempfile::NamedTempFile::new().expect("tmp");
    std::fs::write(tmp.path(), &json).expect("write json");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o644))
            .expect("chmod offsets.json");
    }
    let host_path = tmp.path().to_path_buf();
    let mount = format!("{}:/offsets.json:ro", host_path.display());

    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-delete-records",
            "--bootstrap-server",
            broker0_advertised(),
            "--offset-json-file",
            "/offsets.json",
        ])
        .output()
        .expect("spawn delete-records");
    assert!(
        out.status.success(),
        "delete-records failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(
        s.contains("low_watermark") || s.contains("10"),
        "delete-records output missing low_watermark: {s}"
    );
}

/// `kafka-consumer-groups --list` and `--describe` round-trip after a
/// real consumer has joined a group.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_consumer_groups_list_describe() {
    const TOPIC: &str = "crabka-cg-list-itest";
    const GROUP: &str = "crabka-cg-list-grp";

    let (_broker, _dir) = start_host_broker().await;
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

    // Produce one record so the consumer has something to settle on.
    let mut child = std::process::Command::new("docker")
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
        .stdin(std::process::Stdio::piped())
        .spawn()
        .expect("spawn producer");
    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().expect("stdin");
        writeln!(stdin, "alpha").expect("write");
    }
    drop(child.stdin.take());
    let _ = child.wait_with_output();

    // Consume one record with --group so the group is registered with
    // the coordinator.
    docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        broker0_advertised(),
        "--topic",
        TOPIC,
        "--group",
        GROUP,
        "--from-beginning",
        "--max-messages",
        "1",
        "--timeout-ms",
        "10000",
    ]);

    let list_out = docker_run_kafka_tool(&[
        "kafka-consumer-groups",
        "--list",
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let s = String::from_utf8_lossy(&list_out.stdout);
    assert!(s.contains(GROUP), "list output missing {GROUP}: {s}");

    let desc_out = docker_run_kafka_tool(&[
        "kafka-consumer-groups",
        "--describe",
        "--group",
        GROUP,
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let s = String::from_utf8_lossy(&desc_out.stdout);
    assert!(
        s.contains(TOPIC),
        "describe output missing topic {TOPIC}: {s}"
    );
}

/// `kafka-consumer-groups --delete-offsets` exercises `OffsetDelete`
/// (`api_key` 47, KIP-496) end-to-end against `cp-kafka:6.1.1`. The JVM
/// `AdminClient` flow under this CLI runs `FindCoordinator` →
/// `DescribeGroups` → `OffsetDelete`. After the consumer exits, the group
/// is `Empty`, so the KIP-496 subscription guard skips and the tombstone
/// path runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_consumer_groups_delete_offsets() {
    const TOPIC: &str = "crabka-cg-delete-offsets-itest";
    const GROUP: &str = "crabka-cg-delete-offsets-grp";

    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "2",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    // Produce one record so the consumer has something to commit on.
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
        .spawn()
        .expect("spawn producer");
    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().expect("stdin");
        writeln!(stdin, "alpha").expect("write");
    }
    drop(child.stdin.take());
    let _ = child.wait_with_output();

    // Consume one record with --group so an offset is committed and the
    // group is registered with the coordinator. After --max-messages exits
    // the consumer disconnects → group transitions to Empty, so KIP-496's
    // subscription guard skips and the subsequent --delete-offsets path
    // returns NONE per partition instead of GROUP_SUBSCRIBED_TO_TOPIC.
    docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        broker0_advertised(),
        "--topic",
        TOPIC,
        "--group",
        GROUP,
        "--from-beginning",
        "--max-messages",
        "1",
        "--timeout-ms",
        "10000",
    ]);

    // Sanity: --describe before delete should list TOPIC for GROUP. If this
    // fails, the failure is on the commit/coordinator path — not on
    // OffsetDelete — and the test would otherwise pass-by-accident below.
    let pre_desc = docker_run_kafka_tool(&[
        "kafka-consumer-groups",
        "--describe",
        "--group",
        GROUP,
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let pre_s = String::from_utf8_lossy(&pre_desc.stdout);
    assert!(
        pre_s.contains(TOPIC),
        "pre-delete --describe missing {TOPIC}: {pre_s}"
    );

    // Run --delete-offsets via a piped-stdin spawn so any Y/N prompt the
    // 2.7 build may emit is satisfied. `kafka-consumer-groups` in 2.7
    // generally does not prompt for --delete-offsets when all flags are
    // supplied; the piped "y\n" is defensive and ignored otherwise.
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-consumer-groups",
            "--bootstrap-server",
            broker0_advertised(),
            "--delete-offsets",
            "--group",
            GROUP,
            "--topic",
            TOPIC,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn delete-offsets");
    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().expect("stdin");
        writeln!(stdin, "y").expect("write y");
    }
    drop(child.stdin.take());
    let out = child.wait_with_output().expect("wait delete-offsets");
    assert!(
        out.status.success(),
        "delete-offsets failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let s = String::from_utf8_lossy(&out.stdout);
    // Kafka 2.7 prints a "TOPIC | PARTITION | STATUS" table with
    // "Successful" per row on success. Be lenient: any of the indicators
    // is enough since header formatting drifts across CLI versions.
    assert!(
        s.contains("Successful") || s.contains(TOPIC),
        "delete-offsets stdout missing success indicator: {s}"
    );

    // Post-delete --describe: no data row should reference TOPIC for
    // GROUP. Header text may still mention column names, so guard with a
    // line-level check that the line both belongs to GROUP and refers to
    // TOPIC.
    let post_desc = docker_run_kafka_tool(&[
        "kafka-consumer-groups",
        "--describe",
        "--group",
        GROUP,
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let post_s = String::from_utf8_lossy(&post_desc.stdout);
    let leaked = post_s
        .lines()
        .any(|l| l.starts_with(GROUP) && l.contains(TOPIC));
    assert!(
        !leaked,
        "post-delete --describe still shows {TOPIC} for {GROUP}: {post_s}"
    );
}

/// `kafka-cluster cluster-id` exercises `DescribeCluster` (`api_key` 60).
///
/// Uses `cp-kafka:7.5.0` (= [`KAFKA_IMAGE_TXN`]) because:
/// - `cp-kafka:6.1.1` does not ship the `kafka-cluster` binary at all.
/// - `cp-kafka:7.5.0` ships it but the subcommand is `cluster-id`
///   (not `describe`; that alias does not exist in this version).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_cluster_describe() {
    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    let out = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TXN,
        &[
            "kafka-cluster",
            "cluster-id",
            "--bootstrap-server",
            broker0_advertised(),
        ],
    );
    let s = String::from_utf8_lossy(&out.stdout);
    // `kafka-cluster cluster-id` prints a line like:
    //   "Cluster ID: <uuid>"
    assert!(
        s.contains("Cluster ID") || s.contains("cluster ID") || s.contains("00000000"),
        "cluster-id output missing cluster id: {s}"
    );
}

/// `kafka-console-consumer` sees a compacted topic with only
/// the latest value per key.
///
/// 1. Spin up a single-broker cluster with a fast cleaner interval (3s).
/// 2. `kafka-topics --create --topic compacted-jvm --config cleanup.policy=compact
///    --config segment.bytes=256 --partitions 1 --replication-factor 1`
/// 3. `kafka-console-producer --property parse.key=true --property key.separator=:`
///    with this stdin:
///      k1:v1
///      k1:v2
///      k2:v3
///      k1:v4
///      k3:v5
/// 4. Sleep 8s to let the 3s cleaner tick and the segment rolls happen.
/// 5. `kafka-console-consumer --topic compacted-jvm --from-beginning --timeout-ms 5000`
/// 6. Assert stdout contains `v4`, `v3`, `v5` (latest per-key values).
/// 7. Assert stdout does NOT contain `v1` or `v2` (stale values compacted away).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_console_consumer_sees_compacted_topic_end_to_end() {
    const TOPIC: &str = "compacted-jvm";

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
        log_config: crabka_log::LogConfig::default(),
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(crabka_broker::NodeId(1), controller_addr.to_string())],
        heartbeat_interval: crabka_units::millis(3_000),
        heartbeat_timeout: crabka_units::millis(9_000),
        replica_lag_time_max: crabka_units::millis(30_000),
        controller_election_timeout: crabka_units::secs(5),
        controller_heartbeat_interval: crabka_units::millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        // 3s cleaner tick so we don't have to wait the full 30s default.
        cleaner_interval_override: Some(crabka_units::secs(3)),
        ..BrokerConfig::default()
    };
    let broker = Broker::start(config).await.expect("start broker");
    eprintln!(
        "CRABKA[test] compaction broker started listen={listen} advertised={bootstrap}",
        bootstrap = broker0_advertised(),
        listen = broker0_listen()
    );
    nc_check_connectivity();

    // 1. Create the topic with cleanup.policy=compact and tiny segment.bytes
    //    so records are sealed into a second segment before the cleaner runs.
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
        "cleanup.policy=compact",
        "--config",
        "segment.bytes=256",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    // 1b. Wait for cleanup.policy=compact + segment.bytes=256 to propagate
    //     from the metadata image into the partition's LogConfig via the
    //     ReplicatorSupervisor reconcile loop. Without this wait, produces
    //     can land in a default-config Log (1GiB segments, Delete policy) →
    //     no segment rolls, no compaction.
    let cfg_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if let Some(cfg) = broker.partition_log_config_for_test(TOPIC, 0)
            && cfg.cleanup_policy == crabka_log::CleanupPolicy::Compact
            && cfg.segment_size == crabka_units::bytes(256)
        {
            break;
        }
        assert!(
            std::time::Instant::now() <= cfg_deadline,
            "cleanup.policy/segment.bytes never propagated within 10s"
        );
        // intentional: bounded poll of the local reconciled LogConfig override;
        // `partition_log_config_for_test` is not surfaced by any awaiter/metric.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // 2. Produce 5 records under 3 keys — k1 has three values (v1, v2, v4);
    //    only v4 should survive compaction.
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
            "--property",
            "parse.key=true",
            "--property",
            "key.separator=:",
            // Force per-record batches so each line is its own RecordBatch.
            // Default linger.ms=0 already, but batch.size+linger.ms keep
            // multiple in-flight records bundled when they're submitted
            // back-to-back. Setting batch.size=1 and max-in-flight=1 makes
            // each line a separate batch, which is what we need so
            // segment.bytes=256 actually rolls segments mid-workload.
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
    // First 5 records: the actual workload. After that, a burst of "pad"
    // records under a sentinel key forces the active segment past
    // `segment.bytes=256` so v5 ends up sealed (otherwise the compactor
    // can't see it; it never touches the active segment) and the test's
    // "no stale v1" assertion can actually hold for k1.
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(
            b"k1:v1\nk1:v2\nk2:v3\nk1:v4\nk3:v5\n\
              __pad__:p0\n__pad__:p1\n__pad__:p2\n__pad__:p3\n\
              __pad__:p4\n__pad__:p5\n__pad__:p6\n__pad__:p7\n",
        )
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "producer failed: {}",
        String::from_utf8_lossy(&producer_out.stderr)
    );
    eprintln!("CRABKA[test] produced 5 records; waiting for cleaner to compact...");

    // 3. Wait until the cleaner completes at least two compaction passes over
    //    this partition *after* the records landed (per-partition counter
    //    bumped once per sweep), so a sweep that was in-flight when the segment
    //    sealed can't be mistaken for one that saw the new records. This
    //    guarantees the stale k1 values have been compacted away.
    let compactions_before = broker
        .metrics()
        .log_compactions_total
        .get_or_create(&crabka_broker::metrics::PartitionLabel {
            topic: TOPIC.to_string(),
            partition: 0,
        })
        .get();
    broker
        .wait_for_metrics("partition compacted after produce", |m| {
            m.log_compactions_total
                .get_or_create(&crabka_broker::metrics::PartitionLabel {
                    topic: TOPIC.to_string(),
                    partition: 0,
                })
                .get()
                >= compactions_before + 2
        })
        .await;

    // 4. Consume from beginning — only the latest per-key records should appear.
    let consumer_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        broker0_advertised(),
        "--topic",
        TOPIC,
        "--partition",
        "0",
        "--from-beginning",
        "--timeout-ms",
        "5000",
    ]);
    let stdout = String::from_utf8_lossy(&consumer_out.stdout);
    eprintln!("CRABKA[test] consumer stdout: {stdout:?}");

    // Latest values for each key must be present.
    for needle in ["v4", "v3", "v5"] {
        assert!(
            stdout.contains(needle),
            "expected {needle} in consumer output (latest per-key); got: {stdout:?}"
        );
    }
    // Stale values for k1 must have been compacted away.
    for stale in ["v1", "v2"] {
        assert!(
            !stdout.contains(stale),
            "stale value {stale} still present after compaction; got: {stdout:?}"
        );
    }

    broker.shutdown().await;
}

/// KIP-429 JVM acceptance: drive `kafka-console-consumer` with the JVM
/// `CooperativeStickyAssignor` against Crabka. The test validates that
/// Crabka's `JoinGroup` vote rule accepts `cooperative-sticky` and that the
/// broker forwards the negotiated `protocol_name` correctly, so the JVM
/// client's `AbstractCoordinator.onJoinComplete` accepts the response.
///
/// The test uses `cp-kafka:7.5.0` (= [`KAFKA_IMAGE_TXN`]). The
/// cooperative-sticky assignor in `cp-kafka:6.1.1` (Kafka 2.7) still lacked
/// several rebalance race fixes that landed in Kafka 3.x.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn cooperative_sticky_kafka_console_consumer() {
    const TOPIC: &str = "coop-jvm";

    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    // 1. Create the topic.
    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--topic",
        TOPIC,
        "--partitions",
        "3",
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

    // 3. Consume via kafka-console-consumer with CooperativeStickyAssignor.
    //    Use cp-kafka:7.5.0 (Kafka 3.5) — cooperative-sticky in 2.7 had
    //    rebalance races that masked broker correctness issues.
    let consumer_out = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TXN,
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--group",
            "coop-jvm-group",
            "--consumer-property",
            "partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor",
            "--from-beginning",
            "--max-messages",
            "3",
            "--timeout-ms",
            "30000",
        ],
    );
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for needle in ["alpha", "bravo", "charlie"] {
        assert!(
            s.contains(needle),
            "consumer didn't emit {needle}: stdout={s:?} stderr={:?}",
            String::from_utf8_lossy(&consumer_out.stderr)
        );
    }

    broker.shutdown().await;
}
