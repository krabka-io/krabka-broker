//! KIP-630 snapshot catch-up: a voter that joins with an empty log, far behind
//! the leader's pruned `log_start`, converges on the leader's metadata image
//! through `FetchSnapshot` rather than through log replication.

use std::{collections::HashMap, time::Duration};

use krabka_raft::kraft::NodeId;

use crate::{
    harness::{
        STAGGERED_TIMEOUTS, await_single_leader, await_until, build_engine_with_snapshot_interval,
        topic_record,
    },
    sim_net::SimNet,
};

/// 5. KIP-630 snapshot catch-up, the Slice-4 acceptance. A lagging controller
///    follower whose own log is empty and far behind the leader's pruned
///    `log_start` catches up purely through `FetchSnapshot`, and not through log
///    replication.
///
///    Topology: all three voters are configured up front, so an election can
///    reach a majority, but the lagging node's engine starts LATE, on a fresh
///    empty tempdir. The two timely voters, the leader and one follower, commit
///    a burst of distinct metadata records larger than
///    `snapshot_interval_records`. That forces the leader to write a checkpoint
///    and prune its log, so its `log_start_offset` advances past 0. When the
///    lagging node finally joins, its `LEO == 0 < leader.log_start`, so a
///    `snapshot_id` answers its first Fetch. The engine then runs the
///    `FetchSnapshot` loop, installs the snapshot, and resumes a normal Fetch
///    from the snapshot boundary. The follower's published `MetadataImage` must
///    converge to the leader's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_follower_catches_up_via_snapshot() {
    let net = SimNet::new();
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cid = uuid::Uuid::from_u128(500);
    // Snapshot after every 5 committed records past the last checkpoint.
    let interval = 5u64;

    // Start only TWO voters (leader + one follower): that is a majority of three,
    // so submits commit while the third node stays down. Staggered timeouts so
    // node 1 reliably wins. Node 3 is the lagging node, started later.
    let timeouts = STAGGERED_TIMEOUTS;
    let mut dirs: HashMap<NodeId, tempfile::TempDir> = HashMap::new();
    for &id in &[NodeId(1), NodeId(2)] {
        let idx = usize::try_from(id.0 - 1).unwrap();
        let (ctrl, dir) = build_engine_with_snapshot_interval(
            id,
            &ids, // full voter set: the quorum is three even though one is down
            cid,
            timeouts[idx],
            &net,
            interval,
        );
        net.register(id, ctrl);
        dirs.insert(id, dir);
    }

    // The two live voters elect a leader among themselves (two of three is a
    // majority). The lagging node 3 is down, so only poll the live pair.
    let live = [NodeId(1), NodeId(2)];
    let (leader, _epoch) = await_single_leader(&net, &live, Duration::from_secs(10)).await;

    // Commit MORE than `interval` distinct topics so the leader snapshots and
    // prunes at least once. Distinct names make the image grow per record.
    let burst = usize::try_from(interval).unwrap() * 3; // comfortably past the threshold
    for i in 0..burst {
        tokio::time::timeout(
            Duration::from_secs(10),
            net.get(leader)
                .unwrap()
                .submit_change(vec![topic_record(&format!("t{i}"), 1000 + i as u128)]),
        )
        .await
        .expect("burst submit did not hang")
        .expect("burst submit ok");
    }

    // The leader must have snapshotted and pruned: its log_start advanced past 0.
    // Poll briefly — the prune happens on the apply that crosses the threshold,
    // which is synchronous with the last submit's commit, but give the watch a
    // moment to republish the quorum snapshot.
    let leader_ctrl = net.get(leader).unwrap();
    await_until(Duration::from_secs(10), || {
        (leader_ctrl.quorum_snapshot().log_start_offset > 0).then_some(())
    })
    .await;
    let leader_log_start = leader_ctrl.quorum_snapshot().log_start_offset;
    assert2::assert!(leader_log_start > 0);

    // Capture the leader's converged image to compare against.
    let leader_image = leader_ctrl.current_image();
    // Sanity: every burst topic is in the leader image.
    for i in 0..burst {
        assert2::assert!(leader_image.topic(&format!("t{i}")).is_some());
    }

    // Now bring the lagging node 3 up on a FRESH empty tempdir: its LEO is 0,
    // far below the leader's pruned log_start, so it can ONLY catch up by
    // fetching the snapshot.
    let (lag_ctrl, lag_dir) =
        build_engine_with_snapshot_interval(NodeId(3), &ids, cid, timeouts[2], &net, interval);
    net.register(NodeId(3), lag_ctrl);
    dirs.insert(NodeId(3), lag_dir);

    // Wait until the lagging follower's image equals the leader's. Catch-up runs
    // through the FetchSnapshot path (its LEO 0 < leader.log_start), reassembling
    // and installing the snapshot, then resuming normal fetch.
    let lag = net.get(NodeId(3)).unwrap();
    let want = leader_image.clone();
    await_until(Duration::from_secs(10), || {
        (*lag.current_image() == *want).then_some(())
    })
    .await;

    assert2::assert!(*lag.current_image() == *leader_image);
    // It really used a snapshot: the follower's log_start is at the snapshot
    // boundary, not 0 (a pure log replication from 0 would leave it at 0).
    let lag_snap = lag.quorum_snapshot();
    assert2::assert!(lag_snap.log_start_offset > 0);

    for &id in &ids {
        if let Some(c) = net.get(id) {
            c.shutdown().await;
        }
    }
}
