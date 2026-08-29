//! Leader failover: that the survivors of a leader kill elect a new leader at a
//! higher epoch and can still commit, and that a leader killed and reopened
//! over its own data dir five times running never leaves the quorum leaderless.

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
            Arc::new(net.clone()),
            0,
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
