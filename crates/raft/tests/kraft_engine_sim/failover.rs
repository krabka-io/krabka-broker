//! Leader failover: that the survivors of a leader kill elect a new leader at a
//! higher epoch and can still commit, that a leader killed and reopened over its
//! own data dir five times running never leaves the quorum leaderless, and that
//! a leader the network isolates gives its epoch up instead of holding it.

use crate::{
    harness::{
        self as fixture, await_single_leader, await_until, reopen_controller, start_cluster,
        start_engines, three_voter_network, topic_record,
    },
    sim_net::SimNet,
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
    assert2::assert!((leader != former, epoch > former_epoch) == (true, true));
    SurvivingLeader {
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
    F: FnMut(&krabka_raft::kraft::QuorumStateSnapshot) -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(qs) = ctrl.quorum_state().await
            && f(&qs)
        {
            return;
        }
        assert2::assert!(
            tokio::time::Instant::now() < deadline,
            "quorum state never satisfied the condition"
        );
        tokio::task::yield_now().await;
    }
}

/// 3. After a kill of the leader, the remaining two re-elect a single new
///    leader, and a `submit_change` to the new leader commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failure_reelects() {
    let (net, ids) = three_voter_network();
    let cid = uuid::Uuid::from_u128(300);

    let ElectedCluster {
        dirs: _dirs,
        leader,
        epoch: epoch1,
    } = elect_three_voter_cluster(&net, &ids, cid).await;

    // Kill the leader: shut it down and remove it from the registry so peers see
    // it as unreachable.
    net.get(leader).unwrap().shutdown().await;
    net.remove(leader);

    // The two survivors must elect a NEW single leader at a higher epoch.
    let SurvivingLeader {
        survivors,
        leader: new_leader,
        epoch: _epoch2,
    } = await_surviving_leader(&net, &ids, leader, epoch1).await;

    // A submit to the new leader commits across the two survivors.
    tokio::time::timeout(
        fixture::Duration::from_secs(10),
        net.get(new_leader)
            .unwrap()
            .submit_change(vec![topic_record("post-failover", 7)]),
    )
    .await
    .expect("post-failover submit did not hang")
    .expect("post-failover submit ok");

    for &id in &survivors {
        let ctrl = net.get(id).unwrap();
        await_until(fixture::Duration::from_secs(10), || {
            ctrl.current_image().topic("post-failover").map(|_| ())
        })
        .await;
    }

    for &id in &survivors {
        net.get(id).unwrap().shutdown().await;
    }
}

/// 3b. The same leader is killed and reopened over its own data dir five times
///     in a row, and the quorum must converge on one leader after every round.
///
///     This is the acceptance for an ex-leader that comes back. Leadership is
///     volatile, so the reopened node loads `leader_id == None` while its two
///     followers still believe it leads. It must not answer their Fetches as if
///     it did. When it did, each answer scored as a live fetch, their watchdogs
///     never expired, they held their stale leader belief, and a KIP-996
///     pre-vote only grants when the voter follows no leader. Nothing could
///     ever win a pre-vote and the cluster stayed leaderless.
///
///     Wall-clock, not `start_paused`: the engines are real spawned tasks and
///     `await_single_leader` polls them with `yield_now`, so the runtime is
///     never idle and virtual time would never auto-advance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_leader_restart_reelects() {
    let (net, ids) = three_voter_network();
    let cid = uuid::Uuid::from_u128(301);
    let dirs = start_cluster(&net, &ids, cid);

    for _ in 0..5 {
        let (leader, _) = await_single_leader(&net, &ids, fixture::Duration::from_secs(10)).await;
        net.get(leader).unwrap().shutdown().await;
        net.remove(leader);
        tokio::time::sleep(fixture::Duration::from_millis(50)).await;
        let reopened =
            reopen_controller(&net, dirs[&leader].path().to_path_buf(), leader, cid, &ids);
        net.register(leader, reopened);
    }

    let _ = await_single_leader(&net, &ids, fixture::Duration::from_secs(10)).await;
    for &id in &ids {
        net.get(id).unwrap().shutdown().await;
    }
}

/// 3c. A leader that the network isolates must resign instead of holding an
///     epoch it can no longer serve, and must rejoin as a follower once the
///     partition heals.
///
///     Nothing else can tell it. The majority side elects at a higher epoch, but
///     the isolated node receives none of that traffic, and under KIP-996 the
///     new round's pre-votes never reach it either -- so with no check-quorum it
///     keeps answering `DescribeQuorum`, Metadata and `BrokerHeartbeat` as the
///     leader of the old epoch for as long as the partition lasts. The
///     `election_safety` model property cannot see this: the two leaders hold
///     different epochs.
///
///     The isolated node keeps running, which is what separates this from the
///     kill above: its whole state machine ticks on, and the only thing that
///     changes is that no voter's Fetch arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_leader_resigns_and_rejoins_after_heal() {
    let (net, ids) = three_voter_network();
    let cid = uuid::Uuid::from_u128(302);
    let ElectedCluster {
        dirs: _dirs,
        leader,
        epoch: epoch1,
    } = elect_three_voter_cluster(&net, &ids, cid).await;
    let isolated = net.get(leader).expect("leader is registered");
    net.partition(leader);

    // Check-quorum is 1.5x the fetch timeout, and the longest configured
    // timeout here is 450ms, so a resignation is due well inside this budget.
    // Until it resigns the node still names itself leader of `epoch1`.
    await_quorum_state(&isolated, fixture::Duration::from_secs(10), |qs| {
        qs.leader_id.is_none()
    })
    .await;

    // The majority side elects its own leader at a higher epoch and commits.
    let SurvivingLeader {
        survivors: _survivors,
        leader: new_leader,
        epoch: epoch2,
    } = await_surviving_leader(&net, &ids, leader, epoch1).await;
    tokio::time::timeout(
        fixture::Duration::from_secs(10),
        net.get(new_leader)
            .expect("new leader is registered")
            .submit_change(vec![topic_record("post-partition", 8)]),
    )
    .await
    .expect("post-partition submit did not hang")
    .expect("post-partition submit ok");

    // Heal: the rejoining node attaches to the new leader's epoch as a
    // follower, and replicates what it missed.
    net.heal(leader);
    await_quorum_state(&isolated, fixture::Duration::from_secs(15), |qs| {
        qs.leader_id == Some(new_leader) && qs.leader_epoch >= epoch2
    })
    .await;
    await_until(fixture::Duration::from_secs(15), || {
        isolated.current_image().topic("post-partition").map(|_| ())
    })
    .await;

    for &id in &ids {
        net.get(id).expect("still registered").shutdown().await;
    }
}

/// 3e. Two survivors whose fetch timers run in lockstep elect a new leader
///     within one fetch timeout and a few election timeouts of a leader kill
///     (#1477).
///
///     Both followers last heard from the leader in the same Fetch round, so
///     their fetch timers expire together. Each wins the other's pre-vote, both
///     stand, and the binding vote splits. Kafka retries a split vote one
///     randomized election timeout later, `controller.quorum.election.timeout.ms`
///     plus up to as much again, which is far shorter than the fetch timeout.
///     When the election timer ran on the fetch timeout, the retry came one to
///     two fetch timeouts after the split, so a crashed controller's
///     leadership took up to three fetch timeouts to move.
#[yare::parameterized(
    first = { 940 },
    second = { 941 },
    third = { 942 },
    fourth = { 943 },
    fifth = { 944 },
    sixth = { 945 },
)]
#[test_macro(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn lockstep_survivors_elect_within_a_fetch_timeout_and_a_few_election_timeouts(
    cluster: u128,
) {
    let fetch_timeout = krabka_units::prelude::millis(1_500);
    let election_timeout = krabka_units::prelude::millis(150);
    let (net, ids) = three_voter_network();
    let ElectedCluster {
        dirs: _dirs,
        leader,
        epoch,
    } = elected_cluster()
        .net(&net)
        .ids(&ids)
        .cluster_id(uuid::Uuid::from_u128(cluster))
        .timeouts(&[election_timeout; 3])
        .fetch_timeout(fetch_timeout)
        .call()
        .await;
    // Let both followers settle into the same long-poll Fetch rounds.
    tokio::time::sleep(fixture::Duration::from_secs(1)).await;

    let killed_at = tokio::time::Instant::now();
    net.get(leader).unwrap().shutdown().await;
    net.remove(leader);
    let SurvivingLeader { survivors, .. } = await_surviving_leader(&net, &ids, leader, epoch).await;
    let elapsed = killed_at.elapsed();

    // One fetch timeout to notice, a split round, and one retry drawn from at
    // most two election timeouts, with room for a loaded runner. A retry on the
    // fetch timeout lands at least two fetch timeouts in, past this bound.
    // 1500 ms, plus four 150 ms election timeouts, plus 500 ms of slack.
    let bound = fixture::Duration::from_millis(2_600);
    assert2::assert!(elapsed < bound);
    for id in survivors {
        net.get(id).unwrap().shutdown().await;
    }
}
