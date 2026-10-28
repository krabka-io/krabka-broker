//! The three-node quorum round-trip: produce through one broker, consume
//! through another, then kill the controller leader and confirm a survivor
//! still answers `Metadata`.
//!
//! It is the only durability case that exercises controller re-election after
//! the leader is killed, rather than the data path of a replicated partition,
//! so it stands on its own.

use {
    crate::{
        durability_cluster,
        jvm_acceptance::{KAFKA_IMAGE, create_plain_console_topic_at, docker_run_kafka_tool},
        support::{ContainerInput, jvm_docker_command, jvm_stdin_output},
    },
    assert2::assert,
    krabka_broker::{BrokerHandle, NodeId},
    std::{process::Output, time::Duration},
    tempfile::TempDir,
    tokio::time,
};
const TOPIC: &str = "krabka-quorum-itest";
const CLIENT_PORTS: [u16; 3] = [9192, 9292, 9392];
struct QuorumExchange {
    cluster: Vec<(BrokerHandle, TempDir)>,
    retired_directory: Option<TempDir>,
    produced: Option<Output>,
    consumed: Option<Output>,
    listed: Option<Output>,
}
#[rstest::fixture]
fn quorum() -> QuorumExchange {
    // Start this scenario with fresh inputs and observations.

    QuorumExchange {
        cluster: Vec::new(),
        retired_directory: None,
        produced: None,
        consumed: None,
        listed: None,
    }
}
#[rstest_bdd_macros::given(
    "a three node controller quorum has created its topic and propagated it to the second broker"
)]
async fn topic(quorum: &mut QuorumExchange) {
    quorum.cluster = durability_cluster(CLIENT_PORTS, [9193, 9293, 9393], |_| {}).await;
    create_plain_console_topic_at("host.docker.internal:9192", TOPIC);
    quorum.cluster[1]
        .0
        .wait_until_partition_present(TOPIC, 0)
        .await;
}
#[rstest_bdd_macros::when("the stock producer bootstraps through broker two and writes a b and c")]
fn produce(quorum: &mut QuorumExchange) {
    let mut command = jvm_docker_command()
        .image(KAFKA_IMAGE)
        .args(&[
            "kafka-console-producer",
            "--bootstrap-server",
            "host.docker.internal:9292",
            "--topic",
            TOPIC,
        ])
        .input(ContainerInput::Attached)
        .call();
    quorum.produced = Some(jvm_stdin_output(&mut command, b"a\nb\nc\n"));
}
#[rstest_bdd_macros::then("the cross broker stock producer succeeds")]
fn acknowledged(quorum: &QuorumExchange) {
    let output = quorum.produced.as_ref().expect("producer completed");
    assert!(
        output.status.success(),
        "JVM producer failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
#[rstest_bdd_macros::when("the stock consumer bootstraps through broker three and reads the topic")]
fn consume(quorum: &mut QuorumExchange) {
    quorum.consumed = Some(docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        "host.docker.internal:9392",
        "--topic",
        TOPIC,
        "--partition",
        "0",
        "--from-beginning",
        "--max-messages",
        "3",
        "--timeout-ms",
        "20000",
    ]));
}
#[rstest_bdd_macros::then("the consumer receives a b and c")]
fn values(quorum: &QuorumExchange) {
    let stdout =
        String::from_utf8_lossy(&quorum.consumed.as_ref().expect("consumer completed").stdout);
    for value in ["a", "b", "c"] {
        assert!(stdout.contains(value), "missing {value} in {stdout:?}");
    }
}
#[rstest_bdd_macros::when(
    "the controller leader shuts down and an operator lists topics through a surviving broker"
)]
async fn lose_leader(quorum: &mut QuorumExchange) {
    let leader_index = quorum
        .cluster
        .iter()
        .enumerate()
        .find_map(|(index, (handle, _))| {
            let id = NodeId(u64::try_from(index + 1).expect("node id fits"));
            (handle.controller_leader_id() == Some(id)).then_some(index)
        })
        .expect("a controller leader exists");
    let (leader, directory) = quorum.cluster.remove(leader_index);
    leader.shutdown().await;
    quorum.retired_directory = Some(directory);
    // Preserve the original settle window: cached leader identities can be stale.
    time::sleep(Duration::from_secs(5)).await;
    let survivor = (0..CLIENT_PORTS.len())
        .find(|index| *index != leader_index)
        .expect("surviving broker");
    let bootstrap = format!("host.docker.internal:{}", CLIENT_PORTS[survivor]);
    quorum.listed = Some(docker_run_kafka_tool(&[
        "kafka-topics",
        "--list",
        "--bootstrap-server",
        &bootstrap,
    ]));
}
#[rstest_bdd_macros::then("the surviving quorum still reports the original topic")]
fn metadata_survives(quorum: &QuorumExchange) {
    let stdout = String::from_utf8_lossy(
        &quorum
            .listed
            .as_ref()
            .expect("survivor topic list completed")
            .stdout,
    );
    assert!(
        stdout.contains(TOPIC),
        "topic missing after leader kill: {stdout:?}"
    );
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_durability/quorum_round_trip.feature",
    name = "Stock clients exchange records across brokers and metadata survives controller loss"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_jvm_round_trip(quorum: QuorumExchange) {
    for (handle, _directory) in quorum.cluster {
        handle.shutdown().await;
    }
}
