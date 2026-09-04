//! KIP-630 snapshot catch-up: a voter that joins with an empty log, far behind
//! the leader's pruned `log_start`, converges on the leader's metadata image
//! through `FetchSnapshot` rather than through log replication, and keeps that
//! transfer alive when the node serving it rolls to a newer checkpoint.

use std::{
    collections::{BTreeSet, HashMap},
    time::Duration,
};

use krabka_protocol::records::RecordBatch;
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

/// The `.checkpoint` artifacts a node currently holds, by file name. The
/// checkpoint directory is also the metadata log's own segment directory, so
/// the extension filter is what separates checkpoints from `.log` / `.index`.
fn checkpoint_names(dir: &std::path::Path) -> BTreeSet<String> {
    std::fs::read_dir(checkpoint_dir(dir))
        .expect("read checkpoint dir")
        .flatten()
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|ext| ext == "checkpoint")
        })
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect()
}

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
    // Retention keeps the latest checkpoint and the one before it (#365), so a
    // node that has rolled at least once holds two and never more. Names are
    // fixed-width and zero-padded, so the greatest name is the latest id, and
    // that is the one a Fetch below the log start points at.
    let follower_dir = dirs[&follower].path().to_path_buf();
    let names = checkpoint_names(&follower_dir);
    assert2::assert!((1..=2).contains(&names.len()), "{names:?}");
    let latest = names.last().expect("a checkpoint exists");
    let want_bytes = std::fs::read(checkpoint_dir(&follower_dir).join(latest))
        .expect("read the follower's latest checkpoint file");

    // A lagging peer (node 3, never registered — this exercises the wire
    // protocol directly rather than through election/discovery) asks the
    // FOLLOWER, not the leader, for records from offset 0. Below the
    // follower's own pruned log_start, so it must point back at its own
    // checkpoint rather than serve records (only a leader serves records).
    let fetch_req = wire::PeerRequest::Fetch {
        from: NodeId(3),
        fetch_epoch: 0,
        fetch_offset: 0,
        replica_directory_id: uuid::Uuid::nil(),
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

/// A `FetchSnapshot` that is mid-transfer when the serving node rolls to a
/// newer checkpoint finishes on the id it started on (#365).
///
/// Before retention kept the previous checkpoint, the roll deleted the id
/// under the reader: `load_checkpoint_by_id` missed, the leader answered
/// `SNAPSHOT_NOT_FOUND` (98), and the reader dropped its reassembly and began
/// again from position 0 against the newer id. A reader slower than one
/// `metadata_snapshot_interval_records` never escapes that, which is exactly
/// the reader snapshots exist for. Kafka has no reference count either — the
/// previous snapshot simply stays until retention expires it.
///
/// The lagging peer here drives the wire directly, chunk by chunk, which is
/// the same `FetchSnapshot` loop `Engine::on_fetch_snapshot_response` runs;
/// stepping it by hand is what makes "the leader rolled between two chunks"
/// exact rather than a race.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_fetch_in_flight_survives_the_leader_rolling_to_a_new_checkpoint() {
    let net = SimNet::new();
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cid = uuid::Uuid::from_u128(502);
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
    let leader_dir = dirs[&leader].path().to_path_buf();

    let submit = async |name: String, id: u128| {
        tokio::time::timeout(
            Duration::from_secs(10),
            net.get(leader)
                .unwrap()
                .submit_change(vec![topic_record(&name, id)]),
        )
        .await
        .expect("submit did not hang")
        .expect("submit ok");
    };

    // A first checkpoint, with distinct topics so the image it holds is
    // recognizable when the transfer below reassembles it.
    for i in 0..usize::try_from(interval).unwrap() * 3 {
        submit(format!("r{i}"), 3000 + i as u128).await;
    }
    await_until(Duration::from_secs(10), || {
        (!checkpoint_names(&leader_dir).is_empty()).then_some(())
    })
    .await;
    let before_roll = checkpoint_names(&leader_dir);

    // The lagging peer asks for records from 0, below the leader's pruned
    // log start, and is pointed at that checkpoint.
    let fetch = wire::PeerRequest::Fetch {
        from: NodeId(3),
        fetch_epoch: 0,
        fetch_offset: 0,
        replica_directory_id: uuid::Uuid::nil(),
    }
    .encode();
    let body = net
        .send(leader, api_key::FETCH, fetch)
        .await
        .expect("fetch to the leader succeeds");
    let Some(wire::PeerResponse::Fetch { snapshot_id, .. }) =
        wire::PeerResponse::decode_fetch(&body)
    else {
        panic!("leader did not return a decodable Fetch response");
    };
    let id = snapshot_id.expect("a fetch below the leader's log start returns a snapshot id");

    // One chunk, small enough that the transfer is unmistakably incomplete.
    // It is cut on the snapshot header batch's own boundary: KIP-595 names the
    // field `unalignedRecords`, but this codec decodes it leniently as record
    // batches and drops a trailing fragment, so a chunk ending mid-batch would
    // arrive empty. What this test needs is a transfer left open across the
    // roll, not a particular chunk size.
    let checkpoint_file = checkpoint_dir(&leader_dir).join(
        before_roll
            .iter()
            .next_back()
            .expect("the leader wrote a checkpoint"),
    );
    let on_disk = std::fs::read(&checkpoint_file).expect("read the leader's checkpoint");
    let chunk_bytes = {
        let mut cursor: &[u8] = &on_disk;
        RecordBatch::decode(&mut cursor).expect("decode the snapshot header batch");
        i32::try_from(on_disk.len() - cursor.len()).expect("a header batch fits an i32")
    };
    let mut assembled = Vec::new();
    let chunk = async |position: usize, max_bytes: i32| {
        let req = wire::PeerRequest::FetchSnapshot {
            from: NodeId(3),
            snapshot_id: id,
            position: i64::try_from(position).unwrap(),
            max_bytes,
        }
        .encode();
        let body = net
            .send(leader, api_key::FETCH_SNAPSHOT, req)
            .await
            .expect("fetch snapshot chunk from the leader succeeds");
        let Some(wire::PeerResponse::FetchSnapshot {
            snapshot_id,
            size,
            bytes,
            error_code,
            ..
        }) = wire::PeerResponse::decode_fetch_snapshot(&body)
        else {
            panic!("leader did not return a decodable FetchSnapshot response");
        };
        // The id must never change under the reader, and 98 is the
        // `SNAPSHOT_NOT_FOUND` that used to send it back to position 0.
        assert2::assert!((snapshot_id, error_code) == (id, 0));
        (size, bytes)
    };
    let (size, first) = chunk(0, chunk_bytes).await;
    assembled.extend_from_slice(&first);
    let total = usize::try_from(size).unwrap();
    assert2::assert!(assembled.len() < total, "the transfer is partway");

    // Now roll the leader onto a NEW checkpoint while that transfer is open.
    // One record at a time, so the roll lands as soon as the interval is
    // crossed and the id being read is one behind the latest, not two.
    for i in 0..usize::try_from(interval).unwrap() * 2 {
        submit(format!("s{i}"), 4000 + i as u128).await;
        if checkpoint_names(&leader_dir) != before_roll {
            break;
        }
    }
    let after_roll = checkpoint_names(&leader_dir);
    assert2::assert!(
        after_roll != before_roll,
        "the leader rolled: {after_roll:?}"
    );

    // Finish the original transfer against the rolled leader.
    while assembled.len() < total {
        let (_, bytes) = chunk(assembled.len(), i32::MAX).await;
        assert2::assert!(!bytes.is_empty(), "the transfer made progress");
        assembled.extend_from_slice(&bytes);
    }

    // What it reassembled is a real snapshot, and it is the image the id names
    // (the pre-roll topics) rather than a truncated or mixed artifact.
    let records = krabka_raft::deserialize_metadata_snapshot(&assembled)
        .expect("the reassembled snapshot decodes");
    let topics: BTreeSet<String> = records
        .iter()
        .filter_map(|record| match record {
            krabka_metadata::MetadataRecord::V1Topic(topic) => Some(topic.name.clone()),
            _ => None,
        })
        .collect();
    // The id names a boundary from before the roll, so it holds the burst's
    // topics and none of the records committed to force the roll. A restart
    // onto the newer id would have carried those.
    assert2::assert!(topics.contains("r0"), "{topics:?}");
    assert2::assert!(
        topics.len() >= usize::try_from(interval).unwrap(),
        "{topics:?}"
    );
    assert2::assert!(
        topics.iter().all(|name| name.starts_with('r')),
        "{topics:?}"
    );

    for &id in &live {
        if let Some(c) = net.get(id) {
            c.shutdown().await;
        }
    }
}
