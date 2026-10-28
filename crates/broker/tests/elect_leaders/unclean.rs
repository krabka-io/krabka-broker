//! The operator-triggered unclean election: `ElectLeaders` with
//! `election_type=1` promotes the first alive replica once every ISR member is
//! dead.
//!
//! The scenario forges the dead ISR with an injected `PartitionRecord` rather
//! than by stopping brokers, so the raft quorum stays intact throughout.

use {
    crate::{
        OperatorElectionScenario, operator_election,
        support::{
            admin::{ElectionKind, ElectionSetup},
            topics::AssignedTopicSetup,
        },
        wait::{
            wait_partition_exists, wait_partition_isr_contains, wait_partition_isr_only,
            wait_partition_leader, wait_partition_record_known,
        },
        wire::{CLIENT_ID, create_topic_plaintext, drive_elect_leaders},
    },
    krabka_metadata::{MetadataRecord, PartitionRecord},
};
#[rstest_bdd_macros::given(
    "foo-unclean partition 0 has live replicas 1 and 2 but its committed leader and sole ISR member are unregistered broker 99"
)]
async fn unavailable_partition_isr(operator_election: &mut OperatorElectionScenario) {
    let cluster = &operator_election.cluster;
    // Send ElectLeaders to the current raft leader. Any broker answers it the
    // same way now that the election reads replicated state, but the leader is
    // the node whose view cannot lag, so it is the one this case drives.
    let controller_id = cluster[0].0.wait_until_controller_leader().await.unwrap();
    let leader_index = cluster
        .iter()
        .position(|(_, cfg, _)| cfg.node_id == controller_id)
        .expect("raft leader must be one of the brokers");
    let addr = cluster[leader_index].1.listen_addr;
    // The UNCLEAN election promotes "the first electable replica". A committed
    // registration record is not enough on its own: a broker starts fenced and
    // is published unfenced only after the controller has taken its first
    // heartbeat and confirmed metadata catch-up. Waiting only on registration
    // would race an ELIGIBLE_LEADERS_NOT_AVAILABLE answer, so settle on the
    // predicate the election reads.
    cluster[leader_index].0.wait_until_broker_electable(1).await;
    // Keep named references to avoid chained index+tuple accesses that
    // confuse the Rust 1.95 borrow-checker span computation.
    let h0 = &cluster[0].0;
    let h1 = &cluster[1].0;

    // Create rf=2 topic. Replicas=[1,2]; broker 1 is preferred.
    create_topic_plaintext(addr, AssignedTopicSetup {
        name: ("foo-unclean").into(),
        replicas: vec![krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)],
    })
    .await;
    wait_partition_exists(h0, "foo-unclean", 0).await;
    wait_partition_exists(h1, "foo-unclean", 0).await;

    // Read the current partition record so we can preserve replicas + epoch.
    let pr_before = wait_partition_record_known(h0, "foo-unclean", 0).await;
    eprintln!("partition before injection: {pr_before:?}");

    // Inject a PartitionRecord whose leader AND ISR are the dead phantom 99.
    // Broker 99 has no registration in the image, so it is never electable.
    //
    // Crucially, the leader must be a DEAD broker, not a live replica: a live
    // leader runs ISR management and would re-admit itself / caught-up replicas
    // before the manual election ran, healing the forged state (the partition
    // would then have a live ISR member → ELECTION_NOT_NEEDED). With a dead
    // leader (99, not in replicas) no live broker owns the partition, so the
    // forged ISR=[99] persists until the operator's unclean election. (Nothing
    // auto-elects either: failover is transition-triggered on AliveToDead, and
    // 99 never went alive→dead.)
    let forged = MetadataRecord::V1Partition(PartitionRecord {
        topic: "foo-unclean".to_owned(),
        partition: 0,
        leader: krabka_broker::NodeId(99),
        isr: vec![krabka_broker::NodeId(99)],
        leader_epoch: pr_before.leader_epoch.next(),
        replicas: pr_before.replicas.clone(),
        partition_epoch: 0,
        adding_replicas: Vec::new(),
        removing_replicas: Vec::new(),
        directories: Vec::new(),
    });
    h0.submit_metadata_record_for_test(forged)
        .await
        .expect("inject forged PartitionRecord");

    // Wait for the injected ISR to propagate to the image. With a dead leader
    // it stays [99] (no live leader to repair it).
    wait_partition_isr_only(h0, "foo-unclean", 0, &[99]).await;

    operator_election.address = Some(addr);
}
#[rstest_bdd_macros::when("the operator requests an unclean election of foo-unclean partition 0")]
async fn elect_first_live_replica(operator_election: &mut OperatorElectionScenario) {
    operator_election.result = Some(
        drive_elect_leaders()
            .addr(operator_election.address())
            .client_id(CLIENT_ID)
            .election(ElectionSetup {
                topic: "foo-unclean".into(),
                kind: ElectionKind::Unclean,
                ..Default::default()
            })
            .call()
            .await,
    );
}
#[rstest_bdd_macros::then(
    "foo-unclean partition 0 is led by live replica 1 and its ISR includes broker 1"
)]
async fn unclean_partition_leader(operator_election: &mut OperatorElectionScenario) {
    let observer = &operator_election.cluster[0].0;
    wait_partition_leader(observer, "foo-unclean", 0, 1).await;
    wait_partition_isr_contains(observer, "foo-unclean", 0, 1).await;
    operator_election.shutdown().await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/unclean_election.feature",
    name = "Unclean election selects the first live replica when the ISR is unavailable"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unclean_election_via_wire_picks_alive_replica(
    operator_election: OperatorElectionScenario,
) {
}
