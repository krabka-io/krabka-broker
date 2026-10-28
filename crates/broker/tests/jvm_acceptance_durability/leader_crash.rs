//! The `acks=all` durability gate across a leader crash: the partition-0 leader
//! is killed mid-burst, and the records the JVM producer retries through the
//! election must still be readable from a survivor.
//!
//! It runs with accelerated election timers and probes `Metadata` for the
//! current leader, neither of which the steady-state `acks=all` case needs.

use {
    crate::{
        durability_cluster,
        jvm_acceptance::{KAFKA_IMAGE, consume_committed_at},
        prepare_replication_topic,
        support::{
            client::connect_with_context, discovery::metadata_first_leader, jvm_acks_all_burst,
            jvm_output_lines,
        },
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    std::{process::Output, time::Duration},
    tempfile::TempDir,
    tokio::time,
};
const TOPIC: &str = "krabka-acks-all-crash-itest";
const CLIENT_PORTS: [u16; 3] = [10392, 10492, 10592];
struct CrashedLeader {
    cluster: Vec<(BrokerHandle, TempDir)>,
    bootstrap: String,
    leader_index: usize,
    consumed: Option<Output>,
}
#[rstest::fixture]
fn crash() -> CrashedLeader {
    // Start this scenario with fresh inputs and observations.

    CrashedLeader {
        cluster: Vec::new(),
        bootstrap: String::new(),
        leader_index: 0,
        consumed: None,
    }
}
#[rstest_bdd_macros::given(
    "a three replica topic and consumer coordinator are ready in a cluster with accelerated failure detection"
)]
async fn ready(crash: &mut CrashedLeader) {
    let cluster = durability_cluster(CLIENT_PORTS, [10393, 10493, 10593], |config| {
        config.heartbeat_interval = krabka_units::millis(200);
        config.heartbeat_timeout = krabka_units::millis(2_000);
        config.replica_lag_time_max = krabka_units::millis(2_000);
        config.controller_fetch_timeout = krabka_units::millis(500);
        config.controller_election_timeout = krabka_units::millis(150);
        config.controller_heartbeat_interval = krabka_units::millis(100);
    })
    .await;
    let (_, bootstrap) = prepare_replication_topic(&cluster[0].0, TOPIC, &CLIENT_PORTS).await;
    cluster[0].0.wait_until_group_coordinator_ready().await;
    let leader_node_id = {
        use krabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};
        let local_bootstrap = format!("127.0.0.1:{}", CLIENT_PORTS[0]);
        let probe = connect_with_context(local_bootstrap, None, "metadata probe").await;
        let resp = probe
            .send(MetadataRequest {
                topics: Some(vec![MetadataRequestTopic {
                    name: Some(TOPIC.into()),
                    ..Default::default()
                }]),
                ..Default::default()
            })
            .await
            .expect("metadata");
        metadata_first_leader(&resp, TOPIC, 1)
    };

    crash.leader_index = usize::try_from((leader_node_id - 1).max(0)).unwrap_or(0);
    crash.cluster = cluster;
    crash.bootstrap = bootstrap;
}
#[rstest_bdd_macros::when(
    "a hundred record all acknowledgements producer races with the partition leader shutting down after fifty milliseconds"
)]
async fn race(crash: &mut CrashedLeader) {
    let producer = jvm_acks_all_burst(KAFKA_IMAGE, &crash.bootstrap, TOPIC, "crash-msg", 30_000)
        .spawn()
        .expect("spawn kafka-console-producer");
    // This delay defines the fault injection window rather than readiness.
    time::sleep(Duration::from_millis(50)).await;
    if crash.leader_index < crash.cluster.len() {
        let (leader, _directory) = crash.cluster.remove(crash.leader_index);
        leader.shutdown().await;
    }
    let output = producer.wait_with_output().expect("wait producer");
    eprintln!(
        "producer after leader crash: status={} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}
#[rstest_bdd_macros::when(
    "a committed consumer reads one record through a surviving broker after election settles"
)]
async fn read(crash: &mut CrashedLeader) {
    // The original harness has no follower HW convergence awaiter.
    time::sleep(Duration::from_secs(3)).await;
    let port = CLIENT_PORTS
        .iter()
        .enumerate()
        .find_map(|(index, port)| (index != crash.leader_index).then_some(port))
        .expect("surviving broker");
    let bootstrap = format!("host.docker.internal:{port}");
    crash.consumed = Some(consume_committed_at(&bootstrap, TOPIC, 1));
}
#[rstest_bdd_macros::then("the surviving cluster serves at least one committed record")]
fn available(crash: &CrashedLeader) {
    let output = crash
        .consumed
        .as_ref()
        .expect("survivor consumer completed");
    let count = jvm_output_lines(output).len();
    assert!(
        count >= 1,
        "expected at least1 readable record after leader crash; got {count}: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_durability/leader_crash.feature",
    name = "Committed reads recover after partition leader loss during a producer burst"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_all_survives_leader_crash(crash: CrashedLeader) {
    for (broker, _directory) in crash.cluster {
        broker.shutdown().await;
    }
}
