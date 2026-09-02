//! The `kafka-topics` tool: `--create` followed by `--describe`, and an
//! `--alter --partitions` growth through `CreatePartitions`.

use assert2::assert;

use crate::jvm_acceptance::{
    broker0_advertised, docker_run_kafka_tool, nc_check_connectivity, start_host_broker,
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

/// `kafka-topics --alter --topic t --partitions 3` then `--describe`
/// shows 3 partitions. Exercises `CreatePartitions` (`api_key` 37) +
/// `V1Topic` partition-count update.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_topics_alter_partitions() {
    const TOPIC: &str = "krabka-alter-parts-itest";

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

/// `kafka-topics --create`, `--list` (present), `--delete`, `--list` (gone).
///
/// The epic's criterion says create/describe/**delete**, and the only
/// `--delete` any container suite ran was
/// `jvm_acceptance_freeze/break_glass_delete.rs`, which asserts the two-person
/// rule REFUSES one. A refusal does not evidence that the ordinary path works,
/// so this pins the success case: `DeleteTopics` (`api_key` 20) over the broker
/// listener, with `--list` on both sides so the assertion is the topic's
/// disappearance rather than the tool's exit status.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_topics_delete_removes_the_topic() {
    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--topic",
        "deleted-by-cli",
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    let before = docker_run_kafka_tool(&[
        "kafka-topics",
        "--list",
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let listed = String::from_utf8_lossy(&before.stdout);
    assert!(
        listed.lines().any(|line| line.trim() == "deleted-by-cli"),
        "topic missing before the delete: {listed}"
    );

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--delete",
        "--topic",
        "deleted-by-cli",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    let after = docker_run_kafka_tool(&[
        "kafka-topics",
        "--list",
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let remaining = String::from_utf8_lossy(&after.stdout);
    assert!(
        !remaining
            .lines()
            .any(|line| line.trim() == "deleted-by-cli"),
        "topic still listed after the delete: {remaining}"
    );

    broker.shutdown().await;
}
