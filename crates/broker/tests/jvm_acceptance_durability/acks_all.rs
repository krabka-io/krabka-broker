//! The steady-state `acks=all` durability gate: 100 records written with
//! `--request-required-acks -1` must all read back from a third broker under
//! `read_committed`.
//!
//! It covers the high-watermark path with the cluster intact. The variant that
//! kills the partition leader mid-burst lives beside it in `leader_crash`.

use {
    crate::{
        durability_cluster,
        jvm_acceptance::{KAFKA_IMAGE, consume_committed_at, docker_run_kafka_tool},
        support::{jvm_acks_all_burst, jvm_output_lines},
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    std::{process::Output, time::Duration},
    tempfile::TempDir,
    tokio::time,
};
const TOPIC: &str = "krabka-acks-all-itest";
struct ReplicatedAcknowledgement {
    cluster: Vec<(BrokerHandle, TempDir)>,
    produced: Option<Output>,
    consumed: Option<Output>,
}
#[rstest::fixture]
fn acknowledgements() -> ReplicatedAcknowledgement {
    // Start this scenario with fresh inputs and observations.

    ReplicatedAcknowledgement {
        cluster: Vec::new(),
        produced: None,
        consumed: None,
    }
}
#[rstest_bdd_macros::given("three brokers host a topic replicated on all three nodes")]
async fn replicated_topic(acknowledgements: &mut ReplicatedAcknowledgement) {
    acknowledgements.cluster =
        durability_cluster([10092, 10192, 10292], [10093, 10193, 10293], |_| {}).await;
    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "3",
        "--bootstrap-server",
        "host.docker.internal:10092",
    ]);
}
#[rstest_bdd_macros::when(
    "the stock producer writes a hundred records with acknowledgements from all replicas"
)]
fn write(acknowledgements: &mut ReplicatedAcknowledgement) {
    acknowledgements.produced = Some(
        jvm_acks_all_burst(
            KAFKA_IMAGE,
            "host.docker.internal:10092",
            TOPIC,
            "msg",
            10_000,
        )
        .output()
        .expect("spawn kafka-console-producer"),
    );
}
#[rstest_bdd_macros::then("the replicated producer command succeeds")]
fn acknowledged(acknowledgements: &ReplicatedAcknowledgement) {
    let output = acknowledgements
        .produced
        .as_ref()
        .expect("producer completed");
    assert!(
        output.status.success(),
        "kafka-console-producer failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
#[rstest_bdd_macros::when(
    "a read committed stock consumer reads from the third broker after replication settles"
)]
async fn read(acknowledgements: &mut ReplicatedAcknowledgement) {
    // The follower HW/LSO has no awaiter in this original network harness.
    time::sleep(Duration::from_secs(2)).await;
    acknowledgements.consumed = Some(consume_committed_at(
        "host.docker.internal:10292",
        TOPIC,
        100,
    ));
}
#[rstest_bdd_macros::then("the committed consumer receives at least a hundred records")]
fn committed_records(acknowledgements: &ReplicatedAcknowledgement) {
    let output = acknowledgements
        .consumed
        .as_ref()
        .expect("consumer completed");
    let count = jvm_output_lines(output).len();
    assert!(
        count >= 100,
        "expected at least100 records; got {count}: stdout={}",
        String::from_utf8_lossy(&output.stdout)
    );
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_durability/acks_all.feature",
    name = "All replica acknowledgements make records visible to a committed consumer on another broker"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_all_durability(acknowledgements: ReplicatedAcknowledgement) {
    for (handle, _directory) in acknowledgements.cluster {
        handle.shutdown().await;
    }
}
