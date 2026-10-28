//! The byte-for-byte replica comparison: produce 100 records into a
//! replication-factor-three topic, then confirm `kafka-dump-log` renders every
//! broker's local segment identically.
//!
//! It is the only durability case that reads the on-disk segments back through
//! the JVM tools, so it carries the container mount handling that the other
//! cases do not need.

use {
    crate::{
        durability_cluster, jvm_acceptance::KAFKA_IMAGE, prepare_replication_topic,
        support::jvm_acks_all_producer,
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::{
        io::Write as _,
        path::PathBuf,
        process::{Command, Output},
    },
    tempfile::TempDir,
};
const TOPIC: &str = "krabka-replication-itest";
struct ReplicaDumps {
    cluster: Vec<(BrokerHandle, TempDir)>,
    bootstrap: String,
    produced: Option<Output>,
    dumps: Vec<(PathBuf, Output)>,
}
#[fixture]
fn replicas() -> ReplicaDumps {
    // Start this scenario with fresh inputs and observations.

    ReplicaDumps {
        cluster: Vec::new(),
        bootstrap: String::new(),
        produced: None,
        dumps: Vec::new(),
    }
}
#[given(
    "a three broker cluster hosts a replication factor three topic ready for all replica acknowledgements"
)]
async fn ready(replicas: &mut ReplicaDumps) {
    replicas.cluster = durability_cluster([9492, 9592, 9692], [9493, 9593, 9693], |_| {}).await;
    let (_, all) =
        prepare_replication_topic(&replicas.cluster[0].0, TOPIC, &[9492, 9592, 9692]).await;
    replicas.bootstrap = all;
}
#[when(
    "the stock console producer appends a hundred numbered records with all replica acknowledgements"
)]
fn write(replicas: &mut ReplicaDumps) {
    let mut child = jvm_acks_all_producer(KAFKA_IMAGE, &replicas.bootstrap, TOPIC);
    {
        let stdin = child.stdin.as_mut().expect("producer stdin");
        for index in 0..100 {
            writeln!(stdin, "msg-{index}").expect("write record");
        }
    }
    drop(child.stdin.take());
    replicas.produced = Some(child.wait_with_output().expect("wait producer"));
}
#[then("the all replica producer succeeds")]
fn acknowledged(replicas: &ReplicaDumps) {
    let output = replicas.produced.as_ref().expect("producer completed");
    assert!(
        output.status.success(),
        "producer failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
#[when("all three local logs reach a hundred records and kafka dump log reads each first segment")]
async fn dump(replicas: &mut ReplicaDumps) {
    for (broker, _) in &replicas.cluster {
        broker.wait_until_local_log_end_offset(TOPIC, 0, 100).await;
    }
    for (_, directory) in &replicas.cluster {
        let partition = directory.path().join(format!("{TOPIC}-0"));
        let log = partition.join("00000000000000000000.log");
        let mount = format!("{}:/data:ro", partition.display());
        let output = Command::new("docker")
            .args([
                "run",
                "--rm",
                "-v",
                &mount,
                KAFKA_IMAGE,
                "kafka-dump-log",
                "--files",
                "/data/00000000000000000000.log",
                "--print-data-log",
            ])
            .output()
            .expect("spawn dump-log");
        replicas.dumps.push((log, output));
    }
}
#[then("all three existing segments dump successfully with byte identical renderings")]
fn identical(replicas: &ReplicaDumps) {
    assert!(replicas.dumps.len() == 3);
    for (index, (log, output)) in replicas.dumps.iter().enumerate() {
        assert!(
            log.exists(),
            "broker {} missing log file: {log:?}",
            index + 1
        );
        assert!(
            output.status.success(),
            "dump-log failed for broker {}: stdout={}, stderr={}",
            index + 1,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let dumps: Vec<_> = replicas
        .dumps
        .iter()
        .map(|(_, output)| String::from_utf8_lossy(&output.stdout))
        .collect();
    assert!(dumps[0] == dumps[1], "broker1 vs broker2 dump differ");
    assert!(dumps[1] == dumps[2], "broker2 vs broker3 dump differ");
}
#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_durability/replica_byte_compare.feature",
    name = "All replicas store records with identical Kafka dump log renderings"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_replication_byte_compare(replicas: ReplicaDumps) {
    for (broker, _directory) in replicas.cluster {
        broker.shutdown().await;
    }
}
