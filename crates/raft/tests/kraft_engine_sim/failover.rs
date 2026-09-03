//! Leader failover: that the survivors of a leader kill elect a new leader at a
//! higher epoch and can still commit, that a leader killed and reopened over its
//! own data dir five times running never leaves the quorum leaderless, and that
//! a leader the network isolates gives its epoch up instead of holding it.

use std::{collections::HashMap, sync::Arc, time::Duration};

use krabka_raft::{
    ControllerFetchMissLimit, MetadataRaftCommandQueueCapacity, MetadataRaftFetchMax,
    kraft::{KraftController, NodeId, snapshot_fetch::MetadataSnapshotFetchMax},
};

use crate::{
    harness::{
        STAGGERED_TIMEOUTS, await_single_leader, await_until, build_engine, topic_record, voter_set,
    },
    sim_net::SimNet,
};

/// Polls `ctrl`'s quorum-state snapshot until `f` accepts it, or panics.
///
/// This is the view `DescribeQuorum`, Metadata and `BrokerHeartbeat` all serve
/// from, so it is the right place to observe whether a node still answers as
/// the controller leader.
async fn await_quorum_state<F>(ctrl: &KraftController, timeout: Duration, mut f: F)
where
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
    let net = SimNet::new();
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cid = uuid::Uuid::from_u128(300);

    let timeouts = STAGGERED_TIMEOUTS;
    let mut dirs = Vec::new();
    for (i, &id) in ids.iter().enumerate() {
        let (ctrl, dir) = build_engine(id, &ids, cid, timeouts[i], &net);
        net.register(id, ctrl);
        dirs.push(dir);
    }

    let (leader, epoch1) = await_single_leader(&net, &ids, Duration::from_secs(10)).await;

    // Kill the leader: shut it down and remove it from the registry so peers see
    // it as unreachable.
    net.get(leader).unwrap().shutdown().await;
    net.remove(leader);

    // The two survivors must elect a NEW single leader at a higher epoch.
    let survivors: Vec<NodeId> = ids.iter().copied().filter(|&id| id != leader).collect();
    let (new_leader, epoch2) = await_single_leader(&net, &survivors, Duration::from_secs(15)).await;
    assert2::assert!(new_leader != leader);
    assert2::assert!(epoch2 > epoch1);

    // A submit to the new leader commits across the two survivors.
    tokio::time::timeout(
        Duration::from_secs(10),
        net.get(new_leader)
            .unwrap()
            .submit_change(vec![topic_record("post-failover", 7)]),
    )
    .await
    .expect("post-failover submit did not hang")
    .expect("post-failover submit ok");

    for &id in &survivors {
        let ctrl = net.get(id).unwrap();
        await_until(Duration::from_secs(10), || {
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
    let net = SimNet::new();
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cid = uuid::Uuid::from_u128(301);
    let mut dirs: HashMap<NodeId, tempfile::TempDir> = HashMap::new();
    for (i, &id) in ids.iter().enumerate() {
        let (ctrl, dir) = build_engine(id, &ids, cid, STAGGERED_TIMEOUTS[i], &net);
        net.register(id, ctrl);
        dirs.insert(id, dir);
    }

    for _ in 0..5 {
        let (leader, _) = await_single_leader(&net, &ids, Duration::from_secs(10)).await;
        net.get(leader).unwrap().shutdown().await;
        net.remove(leader);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let reopened = KraftController::open(
            dirs[&leader].path().to_path_buf(),
            leader,
            cid,
            voter_set(&ids),
            STAGGERED_TIMEOUTS[usize::try_from(leader.0 - 1).unwrap()],
            None,
            ControllerFetchMissLimit::default(),
            MetadataRaftCommandQueueCapacity::default(),
            MetadataRaftFetchMax::default(),
            Arc::new(net.as_peer(leader)),
            0,
            krabka_units::prelude::bytes(0),
            krabka_units::prelude::millis(0),
            MetadataSnapshotFetchMax::default(),
        )
        .expect("reopen leader");
        net.register(leader, reopened);
    }

    let _ = await_single_leader(&net, &ids, Duration::from_secs(10)).await;
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
    let net = SimNet::new();
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cid = uuid::Uuid::from_u128(302);
    let mut dirs = Vec::new();
    for (i, &id) in ids.iter().enumerate() {
        let (ctrl, dir) = build_engine(id, &ids, cid, STAGGERED_TIMEOUTS[i], &net);
        net.register(id, ctrl);
        dirs.push(dir);
    }

    let (leader, epoch1) = await_single_leader(&net, &ids, Duration::from_secs(10)).await;
    let isolated = net.get(leader).expect("leader is registered");
    net.partition(leader);

    // Check-quorum is 1.5x the fetch timeout, and the longest configured
    // timeout here is 450ms, so a resignation is due well inside this budget.
    // Until it resigns the node still names itself leader of `epoch1`.
    await_quorum_state(&isolated, Duration::from_secs(10), |qs| {
        qs.leader_id.is_none()
    })
    .await;

    // The majority side elects its own leader at a higher epoch and commits.
    let survivors: Vec<NodeId> = ids.iter().copied().filter(|&id| id != leader).collect();
    let (new_leader, epoch2) = await_single_leader(&net, &survivors, Duration::from_secs(15)).await;
    assert2::assert!((new_leader != leader, epoch2 > epoch1) == (true, true));
    tokio::time::timeout(
        Duration::from_secs(10),
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
    await_quorum_state(&isolated, Duration::from_secs(15), |qs| {
        qs.leader_id == Some(new_leader) && qs.leader_epoch >= epoch2
    })
    .await;
    await_until(Duration::from_secs(15), || {
        isolated.current_image().topic("post-partition").map(|_| ())
    })
    .await;

    for &id in &ids {
        net.get(id).expect("still registered").shutdown().await;
    }
}
