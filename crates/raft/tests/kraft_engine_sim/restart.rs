//! Restart recovery: a follower that snapshots, dies, and is reopened over its
//! own data dir rebuilds its metadata image from the checkpoint plus the log.

use {
    crate::{
        harness::{
            self as fixture, await_single_leader, await_until, reopen_controller, shutdown_nodes,
            start_cluster, three_voter_network, topic_record,
        },
        sim_net::SimNet,
    },
    krabka_raft::kraft::{KraftController, NodeId},
    rstest_bdd_macros::{given, then, when},
    std::collections::HashMap,
    tokio::time,
};
struct RestartCluster {
    net: SimNet,
    ids: [NodeId; 3],
    directories: HashMap<NodeId, tempfile::TempDir>,
    leader: Option<NodeId>,
    reopened: Option<KraftController>,
}
#[rstest::fixture]
fn restart_cluster() -> RestartCluster {
    let (net, ids) = three_voter_network();
    RestartCluster {
        net,
        ids,
        directories: HashMap::new(),
        leader: None,
        reopened: None,
    }
}
#[given("three voters have committed and replicated the persistent topic")]
async fn persistent_cluster(restart_cluster: &mut RestartCluster) {
    let net = &restart_cluster.net;
    let ids = &restart_cluster.ids;
    restart_cluster.directories = start_cluster(net, ids, uuid::Uuid::from_u128(400));
    let leader = await_single_leader(net, ids, fixture::Duration::from_secs(10))
        .await
        .0;
    time::timeout(
        fixture::Duration::from_secs(10),
        net.get(leader)
            .expect("leader")
            .submit_change(vec![topic_record("persistent", 9)]),
    )
    .await
    .expect("submit did not hang")
    .expect("submit committed");
    for &id in ids {
        let controller = net.get(id).expect("voter");
        await_until(fixture::Duration::from_secs(10), || {
            controller.current_image().topic("persistent").map(|_| ())
        })
        .await;
    }
    restart_cluster.leader = Some(leader);
}
#[when("a follower snapshots its image and restarts from its own directory")]
async fn restart_snapshotted_follower(restart_cluster: &mut RestartCluster) {
    let leader = restart_cluster.leader.expect("leader");
    let net = &restart_cluster.net;
    let victim = *restart_cluster
        .ids
        .iter()
        .find(|&&id| id != leader)
        .expect("follower");
    let controller = net.get(victim).expect("follower running");
    controller.trigger_snapshot().await.expect("snapshot");
    controller.shutdown().await;
    net.remove(victim);
    // shutdown signals the fire-and-forget loop; allow it to release KraftLog.
    time::sleep(fixture::Duration::from_millis(50)).await;
    let reopened = reopen_controller(
        net,
        restart_cluster.directories[&victim].path().to_path_buf(),
        victim,
        uuid::Uuid::from_u128(400),
        &restart_cluster.ids,
    );
    net.register(victim, reopened.clone());
    restart_cluster.reopened = Some(reopened);
}
#[then("the reopened follower immediately recovers the persistent topic")]
fn recovered_persistent_topic(restart_cluster: &RestartCluster) {
    assert2::assert!(
        restart_cluster
            .reopened
            .as_ref()
            .expect("follower reopened")
            .current_image()
            .topic("persistent")
            .is_some()
    );
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/kraft_engine_sim/restart.feature",
    name = "A follower rebuilds its image from checkpoint and log"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_recovers_image(restart_cluster: RestartCluster) {
    shutdown_nodes(&restart_cluster.net, &restart_cluster.ids).await;
}
