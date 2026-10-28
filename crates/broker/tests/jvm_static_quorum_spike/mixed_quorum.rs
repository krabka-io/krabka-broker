//! The KIP-595 JVM mixed-quorum acceptance spike: two Krabka controllers and one JVM
//! controller form a static three-voter quorum, and the JVM joins the
//! Krabka-led quorum as a follower and replicates its committed metadata.
//!
//! This is the leader-to-follower direction of the cross-implementation goal,
//! and it needs no KIP-853 dynamic voters.

use {
    crate::{
        static_quorum_harness::{
            MixedQuorum, docker_rm, jvm_controller_properties, kafka_cluster_id_string,
            start_jvm_controller,
        },
        support,
    },
    assert2::check,
    krabka_broker::{BrokerHandle, NodeId},
    std::time::{Duration, Instant},
    tempfile::TempDir,
    tokio::time,
    uuid::Uuid,
};

const CONTAINER: &str = "krabka-kip595-slice5-spike";

struct MixedReplication {
    controllers: Option<[BrokerHandle; 2]>,
    dirs: Option<[TempDir; 2]>,
    properties: Option<TempDir>,
    ports: [u16; 3],
    cluster_id: Uuid,
    leaders: Vec<(Option<NodeId>, Option<NodeId>)>,
    logs: String,
}
#[rstest::fixture]
fn mixed() -> MixedReplication {
    // Start this scenario with fresh inputs and observations.

    MixedReplication {
        controllers: None,
        dirs: None,
        properties: None,
        ports: [0; 3],
        cluster_id: Uuid::nil(),
        leaders: Vec::new(),
        logs: String::new(),
    }
}
#[rstest_bdd_macros::given(
    "two Krabka voters reserve a three-voter static quorum with shared cluster id {id}"
)]
async fn static_voters(mixed: &mut MixedReplication, id: String) {
    support::init_tracing();
    docker_rm(CONTAINER);
    mixed.cluster_id = Uuid::parse_str(&id).expect("cluster UUID");
    let (ports, controllers, dirs) = MixedQuorum::start(mixed.cluster_id, None).await;
    mixed.ports = ports;
    mixed.controllers = Some(controllers);
    mixed.dirs = Some(dirs);
}
#[rstest_bdd_macros::when(
    "Kafka JVM voter 3 starts and the mixed quorum is observed for 50 seconds"
)]
async fn jvm_replication(mixed: &mut MixedReplication) {
    let props = jvm_controller_properties(mixed.ports, "");
    mixed.properties = Some(start_jvm_controller(
        CONTAINER,
        mixed.ports[2],
        &kafka_cluster_id_string(mixed.cluster_id),
        &props,
    ));
    let [c1, c2] = mixed.controllers.as_ref().expect("controllers");
    let deadline = Instant::now() + Duration::from_secs(50);
    let mut tick = 0u32;
    while Instant::now() < deadline {
        mixed
            .leaders
            .push((c1.controller_leader_id(), c2.controller_leader_id()));
        if tick.is_multiple_of(4) {
            let qs = c1.controller_quorum_state_for_test();
            eprintln!(
                "[t={}s] leader={:?} epoch={} hwm={} matched={:?}",
                tick / 2,
                qs.current_leader,
                qs.current_term,
                qs.last_applied_index,
                qs.per_voter_matched_index
            );
        }
        tick += 1;
        time::sleep(Duration::from_millis(500)).await;
    }
    mixed.logs = support::save_jvm_logs(CONTAINER, "/tmp/jvm_spike.log");
    support::print_log_tail(&mixed.logs, 40);
    eprintln!(
        "voter counts: {}, {}",
        c1.voter_count_for_test(),
        c2.voter_count_for_test()
    );
}
#[rstest_bdd_macros::then("the two Krabka voters observe a shared elected leader")]
fn common_leader(mixed: &MixedReplication) {
    check!(
        mixed.leaders.iter().any(|(a, b)| a.is_some() && a == b),
        "leader observations: {:?}",
        mixed.leaders
    );
}
#[rstest_bdd_macros::then(
    "the JVM joins as follower or leader and reports these replication signals without protocol failures"
)]
fn jvm_signals(mixed: &MixedReplication, datatable: Vec<Vec<String>>) {
    check!(
        mixed.logs.contains("Completed transition to FollowerState")
            || mixed.logs.contains("Completed transition to LeaderState"),
        "JVM did not join"
    );
    for row in datatable.into_iter().skip(1) {
        let [presence, signal]: [String; 2] = row.try_into().expect("presence and log signal");
        match presence.as_str() {
            "present" => {
                check!(mixed.logs.contains(&signal), "missing {signal}");
            }
            "absent" => {
                check!(!mixed.logs.contains(&signal), "unexpected {signal}");
            }
            _ => panic!("presence must be present or absent"),
        }
    }
}
#[ignore = "requires Docker + a published controller port (throwaway spike)"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_static_quorum_spike/mixed_quorum.feature",
    name = "A JVM voter joins and replicates a Krabka-led static quorum"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_mixed_jvm_krabka_quorum(mixed: MixedReplication) {
    docker_rm(CONTAINER);
    for controller in mixed.controllers.take().expect("controllers") {
        controller.shutdown().await;
    }
    drop(mixed);
}
