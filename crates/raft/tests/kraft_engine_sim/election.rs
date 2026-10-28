//! Leader election: that three engines settle on exactly one leader, and that a
//! bare majority still elects one when every voter shares an election timeout.
//!
//! The uniform-timeout case is the regression guard for the split-vote
//! livelock, which the staggered topologies cannot reach.

use {
    crate::{
        harness::{await_single_leader, build_engine, start_engines, three_voter_network, uniform},
        sim_net::SimNet,
    },
    krabka_raft::kraft::NodeId,
    krabka_units::prelude::millis,
    rstest_bdd_macros::{given, then, when},
    std::time::Duration,
};
struct ElectionCluster {
    net: SimNet,
    ids: [NodeId; 3],
    live: Vec<NodeId>,
    directories: Vec<tempfile::TempDir>,
    leader: Option<NodeId>,
    epoch: Option<u32>,
    self_leaders: Option<usize>,
}
#[rstest::fixture]
fn election_cluster() -> ElectionCluster {
    let (net, ids) = three_voter_network();
    ElectionCluster {
        net,
        ids,
        live: vec![],
        directories: vec![],
        leader: None,
        epoch: None,
        self_leaders: None,
    }
}
#[given("three voters run with staggered election timeouts")]
fn staggered_election(election_cluster: &mut ElectionCluster) {
    election_cluster.directories = start_engines()
        .net(&election_cluster.net)
        .ids(&election_cluster.ids)
        .cluster_id(uuid::Uuid::from_u128(100))
        .call();
    election_cluster.live = election_cluster.ids.to_vec();
}
#[given("only voters 1 and 2 run with uniform 200 millisecond election timeouts")]
fn uniform_election(election_cluster: &mut ElectionCluster) {
    for id in [NodeId(1), NodeId(2)] {
        let (controller, directory) = build_engine()
            .net(&election_cluster.net)
            .me(id)
            .ids(&election_cluster.ids)
            .cluster_id(uuid::Uuid::from_u128(150))
            .timeouts(uniform(millis(200)))
            .call();
        election_cluster.net.register(id, controller);
        election_cluster.directories.push(directory);
        election_cluster.live.push(id);
    }
}
#[when("the live voters converge within {seconds} seconds and report their quorum states")]
async fn elected_cluster(election_cluster: &mut ElectionCluster, seconds: u64) {
    let (leader, epoch) = await_single_leader(
        &election_cluster.net,
        &election_cluster.live,
        Duration::from_secs(seconds),
    )
    .await;
    election_cluster.leader = Some(leader);
    election_cluster.epoch = Some(epoch);
    let mut self_leaders = 0;
    for &id in &election_cluster.live {
        let state = election_cluster
            .net
            .get(id)
            .expect("live voter")
            .quorum_state()
            .await
            .expect("quorum state");
        if state.leader_id == Some(id) {
            self_leaders += 1;
        }
    }
    election_cluster.self_leaders = Some(self_leaders);
}
#[then("exactly one live voter leads at an epoch of at least one")]
fn unique_elected_leader(election_cluster: &ElectionCluster) {
    assert2::assert!(election_cluster.epoch.is_some_and(|epoch| epoch >= 1));
    assert2::assert!(
        election_cluster
            .live
            .contains(&election_cluster.leader.expect("leader elected"))
    );
    assert2::assert!(election_cluster.self_leaders == Some(1));
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/kraft_engine_sim/election.feature",
    name = "Three engines elect exactly one leader"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_engines_elect_one_leader(election_cluster: ElectionCluster) {
    for id in election_cluster.live {
        election_cluster
            .net
            .get(id)
            .expect("live voter")
            .shutdown()
            .await;
    }
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/kraft_engine_sim/election.feature",
    name = "A bare majority elects with uniform timeouts"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bare_majority_two_of_three_elects_with_uniform_timeouts(
    election_cluster: ElectionCluster,
) {
    for id in election_cluster.live {
        election_cluster
            .net
            .get(id)
            .expect("live voter")
            .shutdown()
            .await;
    }
}
