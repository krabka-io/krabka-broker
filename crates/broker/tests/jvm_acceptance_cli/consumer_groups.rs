//! The `kafka-consumer-groups` administration tool: `--list`, `--describe`, and
//! the KIP-496 `--delete-offsets` path.
//!
//! These runs exercise the JVM `AdminClient` group APIs rather than a consumer,
//! so they stay apart from the `kafka-console-consumer` suites.

use std::process::{Command, Stdio};

use assert2::assert;

use crate::jvm_acceptance::{
    KAFKA_IMAGE, broker0_advertised, docker_run_kafka_tool, nc_check_connectivity,
    start_host_broker,
};

/// `kafka-consumer-groups --list` and `--describe` round-trip after a
/// real consumer has joined a group.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_consumer_groups_list_describe() {
    const TOPIC: &str = "krabka-cg-list-itest";
    const GROUP: &str = "krabka-cg-list-grp";

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
    const TOPIC: &str = "krabka-cg-delete-offsets-itest";
    const GROUP: &str = "krabka-cg-delete-offsets-grp";

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
