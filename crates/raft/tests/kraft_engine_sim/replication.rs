//! Record-carrying replication: a change submitted at a follower is refused
//! with the leader hint, and once it reaches the leader the record itself
//! travels to every follower and lands in all three metadata images.

use {
    crate::{
        harness::{
            await_single_leader, await_until, start_engines, three_voter_network, topic_record,
        },
        sim_net::SimNet,
    },
    krabka_raft::{RaftError, kraft::NodeId},
    rstest_bdd_macros::{given, then, when},
    std::time::Duration,
    tokio::time,
};
struct ReplicationCluster {
    net: SimNet,
    ids: [NodeId; 3],
    directories: Vec<tempfile::TempDir>,
    leader: Option<NodeId>,
    refusal: Option<Result<krabka_raft::SubmitChangeResult, RaftError>>,
}
#[rstest::fixture]
fn replication_cluster() -> ReplicationCluster {
    let (net, ids) = three_voter_network();
    ReplicationCluster {
        net,
        ids,
        directories: vec![],
        leader: None,
        refusal: None,
    }
}
#[given("three running voters have elected a leader for the metadata log")]
async fn replication_leader(replication_cluster: &mut ReplicationCluster) {
    replication_cluster.directories = start_engines()
        .net(&replication_cluster.net)
        .ids(&replication_cluster.ids)
        .cluster_id(uuid::Uuid::from_u128(200))
        .call();
    replication_cluster.leader = Some(
        await_single_leader(
            &replication_cluster.net,
            &replication_cluster.ids,
            Duration::from_secs(10),
        )
        .await
        .0,
    );
}
#[when("a follower receives the orders topic submission")]
async fn follower_submission(replication_cluster: &mut ReplicationCluster) {
    let leader = replication_cluster.leader.expect("leader elected");
    let follower = *replication_cluster
        .ids
        .iter()
        .find(|&&id| id != leader)
        .expect("follower");
    replication_cluster.refusal = Some(
        replication_cluster
            .net
            .get(follower)
            .expect("follower running")
            .submit_change(vec![topic_record("orders", 1)])
            .await,
    );
}
#[then("the follower refuses with NotLeader and the current leader hint")]
fn follower_hint(replication_cluster: &ReplicationCluster) {
    assert2::assert!(let Some(Err(RaftError::NotLeader { current_leader })) = replication_cluster.refusal.as_ref());
    assert2::assert!(*current_leader == replication_cluster.leader);
}
#[when("the hinted leader commits the orders topic and every voter applies it")]
async fn leader_submission(replication_cluster: &mut ReplicationCluster) {
    let leader = replication_cluster.leader.expect("leader elected");
    time::timeout(
        Duration::from_secs(10),
        replication_cluster
            .net
            .get(leader)
            .expect("leader running")
            .submit_change(vec![topic_record("orders", 1)]),
    )
    .await
    .expect("leader submit did not hang")
    .expect("leader submit committed");
    for &id in &replication_cluster.ids {
        let controller = replication_cluster.net.get(id).expect("voter running");
        await_until(Duration::from_secs(10), || {
            controller.current_image().topic("orders").map(|_| ())
        })
        .await;
    }
}
#[then("all three metadata images contain the orders topic")]
fn replicated_topic(replication_cluster: &ReplicationCluster) {
    for &id in &replication_cluster.ids {
        assert2::assert!(
            replication_cluster
                .net
                .get(id)
                .expect("voter running")
                .current_image()
                .topic("orders")
                .is_some()
        );
    }
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/kraft_engine_sim/replication.feature",
    name = "A follower hints the leader and the leader replicates the submitted topic"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_submit_change_propagates(replication_cluster: ReplicationCluster) {
    for id in replication_cluster.ids {
        replication_cluster
            .net
            .get(id)
            .expect("voter running")
            .shutdown()
            .await;
    }
}
