//! Restart recovery: a follower that snapshots, dies, and is reopened over its
//! own data dir rebuilds its metadata image from the checkpoint plus the log.

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

/// 4. Restart recovery: commit, snapshot, drop one engine, reopen it over its
///    dir, then assert that the image is rebuilt from the checkpoint and the
///    log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_recovers_image() {
    let net = SimNet::new();
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cid = uuid::Uuid::from_u128(400);

    let timeouts = STAGGERED_TIMEOUTS;
    // Keep per-node data dirs so we can reopen one.
    let mut dirs: HashMap<NodeId, tempfile::TempDir> = HashMap::new();
    for (i, &id) in ids.iter().enumerate() {
        let (ctrl, dir) = build_engine(id, &ids, cid, timeouts[i], &net);
        net.register(id, ctrl);
        dirs.insert(id, dir);
    }

    let (leader, _epoch) = await_single_leader(&net, &ids, Duration::from_secs(10)).await;

    // Commit a topic and ensure it is replicated everywhere.
    tokio::time::timeout(
        Duration::from_secs(10),
        net.get(leader)
            .unwrap()
            .submit_change(vec![topic_record("persistent", 9)]),
    )
    .await
    .expect("submit did not hang")
    .expect("submit ok");

    for &id in &ids {
        let ctrl = net.get(id).unwrap();
        await_until(Duration::from_secs(10), || {
            ctrl.current_image().topic("persistent").map(|_| ())
        })
        .await;
    }

    // Pick a follower to restart so the cluster keeps a leader meanwhile.
    let victim = *ids.iter().find(|&&id| id != leader).unwrap();
    let victim_ctrl = net.get(victim).unwrap();
    // Snapshot the victim's image, then drop it.
    victim_ctrl.trigger_snapshot().await.unwrap();
    victim_ctrl.shutdown().await;
    net.remove(victim);
    // intentional: let the shutdown-signalled engine task exit and drop its
    // KraftLog before we reopen the same data dir. `shutdown()` only sends
    // `Command::Shutdown`; the loop is spawned fire-and-forget with no JoinHandle,
    // so there is no accessor to await loop teardown / log-handle release.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let victim_dir = dirs.get(&victim).unwrap().path().to_path_buf();
    let reopened = KraftController::open(
        victim_dir,
        victim,
        cid,
        voter_set(&ids),
        timeouts[usize::try_from(victim.0 - 1).unwrap()],
        None,
        ControllerFetchMissLimit::default(),
        MetadataRaftCommandQueueCapacity::default(),
        MetadataRaftFetchMax::default(),
        Arc::new(net.as_peer(victim)),
        0,
        krabka_units::prelude::bytes(0),
        krabka_units::prelude::millis(0),
        MetadataSnapshotFetchMax::default(),
    )
    .expect("reopen");
    // The recovered image must contain the committed topic.
    assert2::assert!(reopened.current_image().topic("persistent").is_some());
    net.register(victim, reopened);

    for &id in &ids {
        if let Some(c) = net.get(id) {
            c.shutdown().await;
        }
    }
}
