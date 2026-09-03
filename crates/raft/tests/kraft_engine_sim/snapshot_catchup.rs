//! KIP-630 snapshot catch-up: a voter that joins with an empty log, far behind
//! the leader's pruned `log_start`, converges on the leader's metadata image
//! through `FetchSnapshot` rather than through log replication.

use std::{collections::HashMap, time::Duration};

use krabka_raft::kraft::{
    NodeId, PeerSender, checkpoint_dir,
    transport::{api_key, wire},
};

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

/// Every voter snapshots and prunes on its own (#364): a follower that never
/// held leadership still checkpoints once the committed offset advances past
/// `snapshot_interval_records`, and the resulting on-disk checkpoint is
/// enough for it to serve a lagging peer's `Fetch`/`FetchSnapshot` directly —
/// the leader stays up and reachable throughout, so this isolates the
/// follower's own serve path from election/discovery timing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_that_pruned_independently_still_serves_a_lagging_fetch() {
    let net = SimNet::new();
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cid = uuid::Uuid::from_u128(501);
    let interval = 5u64;

    let mut dirs: HashMap<NodeId, tempfile::TempDir> = HashMap::new();
    for &id in &[NodeId(1), NodeId(2)] {
        let idx = usize::try_from(id.0 - 1).unwrap();
        let (ctrl, dir) = build_engine_with_snapshot_interval(
            id,
            &ids,
            cid,
            STAGGERED_TIMEOUTS[idx],
            &net,
            interval,
        );
        net.register(id, ctrl);
        dirs.insert(id, dir);
    }

    let live = [NodeId(1), NodeId(2)];
    let (leader, _epoch) = await_single_leader(&net, &live, Duration::from_secs(10)).await;
    assert2::assert!(leader == NodeId(1));
    let follower = NodeId(2);

    let burst = usize::try_from(interval).unwrap() * 3;
    for i in 0..burst {
        tokio::time::timeout(
            Duration::from_secs(10),
            net.get(leader)
                .unwrap()
                .submit_change(vec![topic_record(&format!("f{i}"), 2000 + i as u128)]),
        )
        .await
        .expect("burst submit did not hang")
        .expect("burst submit ok");
    }

    // The follower must have pruned on its own — the direct fix for #364:
    // `maybe_snapshot_and_prune` no longer gates on `is_leader()`, so a
    // follower's HWM advance on its applied Fetch response checkpoints and
    // prunes exactly as the leader's does, with the leader never touched.
    let follower_ctrl = net.get(follower).unwrap();
    await_until(Duration::from_secs(10), || {
        (follower_ctrl.quorum_snapshot().log_start_offset > 0).then_some(())
    })
    .await;
    // `checkpoint_dir` is also the metadata log's own segment directory (it
    // holds `.log`/`.index`/`leader-epoch-checkpoint` alongside `.checkpoint`
    // files), so filter for the checkpoint retention keeps to exactly one.
    let follower_dir = dirs[&follower].path().to_path_buf();
    let checkpoint_entries: Vec<_> = std::fs::read_dir(checkpoint_dir(&follower_dir))
        .expect("read checkpoint dir")
        .collect::<Result<Vec<_>, _>>()
        .expect("read checkpoint dir entries")
        .into_iter()
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "checkpoint"))
        .collect();
    assert2::assert!(checkpoint_entries.len() == 1);
    let want_bytes =
        std::fs::read(checkpoint_entries[0].path()).expect("read the follower's checkpoint file");

    // A lagging peer (node 3, never registered — this exercises the wire
    // protocol directly rather than through election/discovery) asks the
    // FOLLOWER, not the leader, for records from offset 0. Below the
    // follower's own pruned log_start, so it must point back at its own
    // checkpoint rather than serve records (only a leader serves records).
    let fetch_req = wire::PeerRequest::Fetch {
        from: NodeId(3),
        fetch_epoch: 0,
        fetch_offset: 0,
    }
    .encode();
    let fetch_resp_body = net
        .send(follower, api_key::FETCH, fetch_req)
        .await
        .expect("fetch to the follower succeeds");
    let Some(wire::PeerResponse::Fetch {
        snapshot_id,
        records,
        ..
    }) = wire::PeerResponse::decode_fetch(&fetch_resp_body)
    else {
        panic!("follower did not return a decodable Fetch response");
    };
    let (end_offset, epoch) =
        snapshot_id.expect("fetch below the follower's own log_start returns a snapshot id");
    assert2::assert!(records.is_empty());

    // Fetch the whole snapshot from the follower directly, exactly as a
    // lagging peer's `FetchSnapshot` loop would, and confirm it reassembles
    // to the follower's own on-disk checkpoint byte-for-byte.
    let mut got_bytes = Vec::new();
    loop {
        let position = i64::try_from(got_bytes.len()).unwrap();
        let req = wire::PeerRequest::FetchSnapshot {
            from: NodeId(3),
            snapshot_id: (end_offset, epoch),
            position,
            max_bytes: i32::MAX,
        }
        .encode();
        let resp_body = net
            .send(follower, api_key::FETCH_SNAPSHOT, req)
            .await
            .expect("fetch snapshot chunk from the follower succeeds");
        let Some(wire::PeerResponse::FetchSnapshot {
            size,
            bytes,
            error_code,
            ..
        }) = wire::PeerResponse::decode_fetch_snapshot(&resp_body)
        else {
            panic!("follower did not return a decodable FetchSnapshot response");
        };
        assert2::assert!(error_code == 0);
        got_bytes.extend_from_slice(&bytes);
        if i64::try_from(got_bytes.len()).unwrap() >= size {
            break;
        }
    }
    assert2::assert!(got_bytes == want_bytes);

    for &id in &live {
        if let Some(c) = net.get(id) {
            c.shutdown().await;
        }
    }
}
