//! Leader failover: that the survivors of a leader kill elect a new leader at a
//! higher epoch and can still commit, that a leader killed and reopened over its
//! own data dir five times running never leaves the quorum leaderless, and that
//! a leader the network isolates gives its epoch up instead of holding it.

use {
    crate::{
        harness::{
            self as fixture, await_single_leader, await_until, reopen_controller, start_engines,
            three_voter_network, topic_record,
        },
        sim_net::SimNet,
    },
    krabka_raft::kraft::QuorumStateSnapshot,
    krabka_units::prelude,
    tokio::{
        task,
        time::{self, Instant},
    },
};

/// An elected cluster: the data directories that must outlive it, the leader
/// and the epoch it leads.
struct ElectedCluster {
    dirs: Vec<tempfile::TempDir>,
    leader: fixture::NodeId,
    epoch: u32,
}

/// The nodes left after a leader is killed, and the leader and epoch they
/// elect.
struct SurvivingLeader {
    former_epoch: u32,
    survivors: Vec<fixture::NodeId>,
    leader: fixture::NodeId,
    epoch: u32,
}

/// Start the failover fixture and retain its directories through the scenario.
#[bon::builder]
async fn elected_cluster(
    net: &SimNet,
    #[builder(default = &[fixture::NodeId(1), fixture::NodeId(2), fixture::NodeId(3)])]
    ids: &[fixture::NodeId],
    #[builder(default)] cluster_id: uuid::Uuid,
    #[builder(default = &fixture::STAGGERED_TIMEOUTS)] timeouts: &[krabka_units::Time],
    fetch_timeout: Option<krabka_units::Time>,
) -> ElectedCluster {
    let dirs = start_engines()
        .net(net)
        .ids(ids)
        .cluster_id(cluster_id)
        .timeouts(timeouts)
        .maybe_fetch_timeout(fetch_timeout)
        .call();
    let (leader, epoch) = await_single_leader(net, ids, fixture::Duration::from_secs(10)).await;
    ElectedCluster {
        dirs,
        leader,
        epoch,
    }
}

async fn elect_three_voter_cluster(
    net: &SimNet,
    ids: &[fixture::NodeId],
    cluster_id: uuid::Uuid,
) -> ElectedCluster {
    elected_cluster()
        .net(net)
        .ids(ids)
        .cluster_id(cluster_id)
        .call()
        .await
}

/// Polls `ctrl`'s quorum-state snapshot until `f` accepts it, or panics.
///
/// This is the view `DescribeQuorum`, Metadata and `BrokerHeartbeat` all serve
/// from, so it is the right place to observe whether a node still answers as
/// the controller leader.
async fn await_surviving_leader(
    net: &SimNet,
    ids: &[fixture::NodeId],
    former: fixture::NodeId,
    former_epoch: u32,
) -> SurvivingLeader {
    let survivors = ids
        .iter()
        .copied()
        .filter(|id| *id != former)
        .collect::<Vec<_>>();
    let (leader, epoch) =
        await_single_leader(net, &survivors, fixture::Duration::from_secs(15)).await;
    SurvivingLeader {
        former_epoch,
        survivors,
        leader,
        epoch,
    }
}

async fn await_quorum_state<F>(
    ctrl: &fixture::KraftController,
    timeout: fixture::Duration,
    mut f: F,
) where
    F: FnMut(&QuorumStateSnapshot) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(qs) = ctrl.quorum_state().await
            && f(&qs)
        {
            return;
        }
        assert2::assert!(
            Instant::now() < deadline,
            "quorum state never satisfied the condition"
        );
        task::yield_now().await;
    }
}

use rstest_bdd_macros::{given, then, when};
struct FailoverCluster {
    net: SimNet,
    ids: [fixture::NodeId; 3],
    cluster: uuid::Uuid,
    elected: Option<ElectedCluster>,
    replacement: Option<SurvivingLeader>,
    elapsed: Option<fixture::Duration>,
    convergence: Vec<(fixture::NodeId, u32)>,
    isolated_state: Option<QuorumStateSnapshot>,
}
#[rstest::fixture]
fn failover() -> FailoverCluster {
    let (net, ids) = three_voter_network();
    FailoverCluster {
        net,
        ids,
        cluster: uuid::Uuid::nil(),
        elected: None,
        replacement: None,
        elapsed: None,
        convergence: vec![],
        isolated_state: None,
    }
}
#[given("a three voter cluster {cluster} has elected its initial leader")]
async fn initial_failover_leader(failover: &mut FailoverCluster, cluster: u128) {
    failover.cluster = uuid::Uuid::from_u128(cluster);
    failover.elected =
        Some(elect_three_voter_cluster(&failover.net, &failover.ids, failover.cluster).await);
}
#[given(
    "cluster {cluster} has uniform 150 millisecond elections and 1500 millisecond Fetch rounds"
)]
async fn lockstep_failover_leader(failover: &mut FailoverCluster, cluster: u128) {
    failover.cluster = uuid::Uuid::from_u128(cluster);
    failover.elected = Some(
        elected_cluster()
            .net(&failover.net)
            .ids(&failover.ids)
            .cluster_id(failover.cluster)
            .timeouts(&[prelude::millis(150); 3])
            .fetch_timeout(prelude::millis(1500))
            .call()
            .await,
    );
    time::sleep(fixture::Duration::from_secs(1)).await;
}
#[when("the current leader crashes and the surviving majority elects a replacement")]
async fn crash_leader(failover: &mut FailoverCluster) {
    let initial = failover.elected.as_ref().expect("initial leader elected");
    let killed_at = Instant::now();
    failover
        .net
        .get(initial.leader)
        .expect("leader running")
        .shutdown()
        .await;
    failover.net.remove(initial.leader);
    failover.replacement = Some(
        await_surviving_leader(&failover.net, &failover.ids, initial.leader, initial.epoch).await,
    );
    failover.elapsed = Some(killed_at.elapsed());
}
#[then("a different surviving voter leads at a higher epoch")]
fn new_failover_epoch(failover: &FailoverCluster) {
    let initial = failover.elected.as_ref().expect("initial leader");
    let replacement = failover.replacement.as_ref().expect("replacement elected");
    assert2::assert!(replacement.leader != initial.leader);
    assert2::assert!(replacement.former_epoch == initial.epoch);
    assert2::assert!(replacement.epoch > replacement.former_epoch);
}
#[then("the replacement election completes in less than 2600 milliseconds")]
fn lockstep_failover_bound(failover: &FailoverCluster) {
    assert2::assert!(failover.elapsed.expect("crash timed") < fixture::Duration::from_millis(2600));
}
#[when("the replacement leader commits the post-failover topic to both survivors")]
async fn submit_after_failover(failover: &mut FailoverCluster) {
    let replacement = failover.replacement.as_ref().expect("replacement elected");
    time::timeout(
        fixture::Duration::from_secs(10),
        failover
            .net
            .get(replacement.leader)
            .expect("leader running")
            .submit_change(vec![topic_record("post-failover", 7)]),
    )
    .await
    .expect("submit did not hang")
    .expect("submit committed");
    for &id in &replacement.survivors {
        let controller = failover.net.get(id).expect("survivor");
        await_until(fixture::Duration::from_secs(10), || {
            controller
                .current_image()
                .topic("post-failover")
                .map(|_| ())
        })
        .await;
    }
}
#[then("both surviving metadata images contain the post-failover topic")]
fn failover_committed_topic(failover: &FailoverCluster) {
    for &id in &failover
        .replacement
        .as_ref()
        .expect("replacement")
        .survivors
    {
        assert2::assert!(
            failover
                .net
                .get(id)
                .expect("survivor")
                .current_image()
                .topic("post-failover")
                .is_some()
        );
    }
}
#[when("the elected leader is crashed and reopened from its directory five times")]
async fn repeated_failover(failover: &mut FailoverCluster) {
    for _ in 0..5 {
        let (leader, epoch) = await_single_leader(
            &failover.net,
            &failover.ids,
            fixture::Duration::from_secs(10),
        )
        .await;
        failover.convergence.push((leader, epoch));
        failover.net.get(leader).expect("leader").shutdown().await;
        failover.net.remove(leader);
        time::sleep(fixture::Duration::from_millis(50)).await;
        let index = failover
            .ids
            .iter()
            .position(|id| *id == leader)
            .expect("leader voter index");
        let directory = failover.elected.as_ref().expect("cluster directories").dirs[index]
            .path()
            .to_path_buf();
        let reopened = reopen_controller(
            &failover.net,
            directory,
            leader,
            failover.cluster,
            &failover.ids,
        );
        failover.net.register(leader, reopened);
    }
    failover.convergence.push(
        await_single_leader(
            &failover.net,
            &failover.ids,
            fixture::Duration::from_secs(10),
        )
        .await,
    );
}
#[then("all six observed quorums agree on a live leader at an established epoch")]
fn repeated_failover_convergence(failover: &FailoverCluster) {
    assert2::assert!(failover.convergence.len() == 6);
    for &(leader, epoch) in &failover.convergence {
        assert2::assert!(failover.ids.contains(&leader));
        assert2::assert!(epoch >= 1);
    }
}
#[when("the network isolates the leader until it resigns and the majority elects a replacement")]
async fn isolate_failover_leader(failover: &mut FailoverCluster) {
    let initial = failover.elected.as_ref().expect("initial leader");
    let isolated = failover.net.get(initial.leader).expect("leader running");
    failover.net.partition(initial.leader);
    await_quorum_state(&isolated, fixture::Duration::from_secs(10), |state| {
        state.leader_id.is_none()
    })
    .await;
    failover.isolated_state = Some(
        isolated
            .quorum_state()
            .await
            .expect("isolated quorum state"),
    );
    failover.replacement = Some(
        await_surviving_leader(&failover.net, &failover.ids, initial.leader, initial.epoch).await,
    );
}
#[then("the isolated controller no longer names itself leader")]
fn isolated_resignation(failover: &FailoverCluster) {
    assert2::assert!(
        failover
            .isolated_state
            .as_ref()
            .expect("isolation observed")
            .leader_id
            .is_none()
    );
}
#[when("the majority commits post-partition and the network heals the former leader")]
async fn heal_failover_leader(failover: &mut FailoverCluster) {
    let initial = failover.elected.as_ref().expect("initial leader");
    let replacement = failover.replacement.as_ref().expect("replacement");
    time::timeout(
        fixture::Duration::from_secs(10),
        failover
            .net
            .get(replacement.leader)
            .expect("new leader")
            .submit_change(vec![topic_record("post-partition", 8)]),
    )
    .await
    .expect("submit did not hang")
    .expect("submit committed");
    failover.net.heal(initial.leader);
    let isolated = failover.net.get(initial.leader).expect("former leader");
    await_quorum_state(&isolated, fixture::Duration::from_secs(15), |state| {
        state.leader_id == Some(replacement.leader) && state.leader_epoch >= replacement.epoch
    })
    .await;
    await_until(fixture::Duration::from_secs(15), || {
        isolated.current_image().topic("post-partition").map(|_| ())
    })
    .await;
    failover.isolated_state = Some(isolated.quorum_state().await.expect("healed quorum state"));
}
#[then("the former leader follows the replacement epoch and contains post-partition")]
fn healed_failover_image(failover: &FailoverCluster) {
    let initial = failover.elected.as_ref().expect("initial leader");
    let replacement = failover.replacement.as_ref().expect("replacement");
    let state = failover.isolated_state.as_ref().expect("healed state");
    assert2::assert!(state.leader_id == Some(replacement.leader));
    assert2::assert!(state.leader_epoch >= replacement.epoch);
    assert2::assert!(
        failover
            .net
            .get(initial.leader)
            .expect("former leader")
            .current_image()
            .topic("post-partition")
            .is_some()
    );
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/kraft_engine_sim/failover.feature",
    name = "A leader crash preserves majority commits"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failure_reelects(failover: FailoverCluster) {
    fixture::shutdown_nodes(&failover.net, &failover.ids).await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/kraft_engine_sim/failover.feature",
    name = "Five leader restarts preserve election convergence"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_leader_restart_reelects(failover: FailoverCluster) {
    fixture::shutdown_nodes(&failover.net, &failover.ids).await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/kraft_engine_sim/failover.feature",
    name = "An isolated leader resigns and rejoins after healing"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_leader_resigns_and_rejoins_after_heal(failover: FailoverCluster) {
    fixture::shutdown_nodes(&failover.net, &failover.ids).await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/kraft_engine_sim/failover.feature",
    name = "Lockstep survivors retry split votes on the election timer"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lockstep_survivors_elect_within_a_fetch_timeout_and_a_few_election_timeouts(
    failover: FailoverCluster,
    cluster: u128,
) {
    let _ = cluster;
    fixture::shutdown_nodes(&failover.net, &failover.ids).await;
}
