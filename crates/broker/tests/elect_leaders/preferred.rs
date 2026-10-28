//! The operator-triggered preferred election: `ElectLeaders` with
//! `election_type=0` moves leadership back to `replicas[0]` once that replica
//! is alive and in the ISR again.

use {
    crate::{
        OperatorElectionScenario, operator_election,
        support::{
            admin::{ElectionKind, ElectionSetup},
            topics::AssignedTopicSetup,
        },
        wait::{wait_isr_contains, wait_partition_exists, wait_partition_leader},
        wire::{
            CLIENT_ID, create_topic_plaintext, drive_elect_all_partitions, drive_elect_leaders,
        },
    },
    assert2::assert,
    krabka_broker::codes,
};
#[rstest_bdd_macros::given(
    "foo-preferred partition 0 has replicas 1 and 2, and preferred broker 1 has stopped, rejoined and become electable in the ISR"
)]
async fn rejoined_preferred_replica(operator_election: &mut OperatorElectionScenario) {
    let mut cluster = mem::take(&mut operator_election.cluster);
    // All three brokers' addresses captured before any shutdowns.
    let broker1_addr = cluster[0].1.listen_addr;

    // Create a rf=2 topic. With 3 registered brokers the scheduler assigns
    // replicas [1, 2]; broker 1 is the preferred (first) replica.
    create_topic_plaintext(broker1_addr, AssignedTopicSetup {
        name: ("foo-preferred").into(),
        replicas: vec![krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)],
    })
    .await;

    // Wait for all rf brokers to see the partition in their image.
    wait_partition_exists(&cluster[0].0, "foo-preferred", 0).await;
    wait_partition_exists(&cluster[1].0, "foo-preferred", 0).await;

    let initial_leader = cluster[0]
        .0
        .partition_leader_for_test("foo-preferred", 0)
        .unwrap_or(1);
    eprintln!("initial partition leader: {initial_leader}");

    // Kill broker 1 (index 0). Raft quorum {2, 3} can still commit.
    let (dead_h, dead_cfg, dead_dir) = cluster.remove(0);
    dead_h.shutdown().await;

    // Wait for the surviving cluster to elect a new partition leader
    // (i.e., not broker 1).
    cluster[0]
        .0
        .wait_until_partition_leader_changed("foo-preferred", 0, krabka_broker::NodeId(1))
        .await;
    let new_leader = cluster[0]
        .0
        .partition_leader_for_test("foo-preferred", 0)
        .unwrap();
    eprintln!("new partition leader after broker 1 death: {new_leader}");
    let _ = new_leader; // used for diagnostics

    // Revive broker 1 (Rejoin reads the existing raft log).
    // The surviving 2/3 quorum continues committing.
    let mut revived_cfg = dead_cfg.clone();
    revived_cfg.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
    let revived_h = krabka_broker::Broker::start(revived_cfg)
        .await
        .expect("rejoin broker 1");

    // Wait for broker 1 to be back in the ISR on broker 2's view.
    // The ISR expand is committed by the surviving raft leader so broker 2
    // (or 3) must reflect it.
    wait_isr_contains(&cluster[0].0, "foo-preferred", 0, 1).await;

    // The ElectLeaders request MUST go to the raft leader, which is the only
    // broker with an authoritative liveness state (it receives all heartbeats).
    // After broker 1's revive it heartbeats to the raft leader, which marks
    // it as Alive. We discover which surviving broker is the raft leader and
    // send there.
    //
    // Inlined here rather than extracted into a helper to avoid a complex
    // `&[(BrokerHandle, BrokerConfig, TempDir)]` function signature that
    // triggers the Rust 1.95 annotate-snippets ICE in clippy::type_complexity.
    let elect_addr = {
        let leader = cluster[0].0.wait_until_controller_leader().await.unwrap();
        let pos = cluster
            .iter()
            .position(|(_, cfg, _)| cfg.node_id == leader)
            .expect("raft leader must be one of the surviving brokers");
        // A PREFERRED election refuses with PREFERRED_NOT_ALIVE until broker 1
        // is electable, and the ISR wait above does not imply that: the ISR
        // expand is a replicated record proposed by the data leader, while a
        // revived broker becomes electable only once its first post-revive
        // heartbeat reaches the controller and the controller publishes it
        // unfenced. Settling on the ISR alone races that, which is what made
        // this test flaky under load. Settle on the predicate the election
        // actually reads -- the replicated set, which trails the controller's
        // own registry by up to one liveness tick.
        cluster[pos].0.wait_until_broker_electable(1).await;
        cluster[pos].1.listen_addr
    };
    eprintln!("sending ElectLeaders Preferred to raft leader at {elect_addr}");

    operator_election.cluster = cluster;
    operator_election.revived = Some(revived_h);
    operator_election.retired_directory = Some(dead_dir);
    operator_election.address = Some(elect_addr);
}
#[rstest_bdd_macros::when(
    "the operator requests a preferred election of foo-preferred partition 0"
)]
async fn elect_rejoined_preferred_replica(operator_election: &mut OperatorElectionScenario) {
    let result = drive_elect_leaders()
        .addr(operator_election.address())
        .client_id(CLIENT_ID)
        .election(ElectionSetup {
            topic: "foo-preferred".into(),
            ..Default::default()
        })
        .call()
        .await;
    operator_election.result = Some(result);
}
#[rstest_bdd_macros::then(
    "the surviving broker and rejoined broker both observe broker 1 as foo-preferred partition 0 leader"
)]
async fn restored_preferred_partition_leader(operator_election: &mut OperatorElectionScenario) {
    wait_partition_leader(&operator_election.cluster[0].0, "foo-preferred", 0, 1).await;
    wait_partition_leader(
        operator_election
            .revived
            .as_ref()
            .expect("rejoined preferred broker"),
        "foo-preferred",
        0,
        1,
    )
    .await;
    operator_election.shutdown().await;
}
#[rstest_bdd_macros::given(
    "foo-all-partitions partition 0 already has preferred broker 1 as leader with replicas 1 and 2"
)]
async fn already_preferred_partition(operator_election: &mut OperatorElectionScenario) {
    let address = operator_election.cluster[0].1.listen_addr;
    create_topic_plaintext(address, AssignedTopicSetup {
        name: "foo-all-partitions".into(),
        replicas: vec![krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)],
    })
    .await;
    wait_partition_exists(&operator_election.cluster[0].0, "foo-all-partitions", 0).await;
    operator_election.address = Some(address);
}
#[rstest_bdd_macros::when(
    "the operator requests preferred elections for all partitions and then explicitly names foo-all-partitions partition 0"
)]
async fn compare_all_and_named_preferred_elections(
    operator_election: &mut OperatorElectionScenario,
) {
    operator_election.all_result = Some(
        drive_elect_all_partitions(operator_election.address(), ElectionKind::Preferred).await,
    );
    operator_election.result = Some(
        drive_elect_leaders()
            .addr(operator_election.address())
            .client_id(CLIENT_ID)
            .election(ElectionSetup {
                topic: "foo-all-partitions".into(),
                ..Default::default()
            })
            .call()
            .await,
    );
}
#[rstest_bdd_macros::then(
    "the all-partitions response omits skipped rows and the named partition returns ELECTION_NOT_NEEDED code 84"
)]
async fn omitted_preferred_election_rows(operator_election: &mut OperatorElectionScenario) {
    assert!(
        operator_election
            .all_result
            .as_ref()
            .expect("all-partitions election")
            .is_empty()
    );
    assert!(
        operator_election.result.as_ref().expect("named election")
            == &[(0, codes::ELECTION_NOT_NEEDED)]
    );
    operator_election.shutdown().await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/preferred_elections.feature",
    name = "A rejoined preferred replica can regain leadership"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preferred_election_via_wire_returns_success(operator_election: OperatorElectionScenario) {}
#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/preferred_elections.feature",
    name = "All-partitions elections omit partitions already on the preferred leader"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn electing_every_partition_omits_the_ones_already_on_their_preferred_leader(
    operator_election: OperatorElectionScenario,
) {
}

use std::mem;
