//! Tests for the Fetch and `FetchSnapshot` paths: the predicates that classify
//! a response, the replies a follower and a leaderless node serve, and the
//! requests the engine emits while replicating or transferring a snapshot.

use std::time::Duration as StdDuration;

use assert2::assert;

use super::*;
use crate::kraft::{
    controller::{
        offsets::metadata_fetch_offset_below_log_start,
        records::{decode_batches, encode_batches},
        replication::{
            FetchBatchDisposition, classify_fetch_batch, fetch_epoch_for_request,
            should_serve_fetch_records, should_start_snapshot_fetch,
            snapshot_fetch_response_invalid,
        },
        test_support::{
            build_engine_only, build_engine_only_with_policy, elect_single_voter_engine,
            one_offset_batch, record_peer_sends, recv_peer_send, recv_peer_send_with_api,
            topic_record,
        },
    },
    types::LogOffsetMetadata,
};

#[test]
fn snapshot_install_admission_rejects_malformed_stale_and_pending_cases() {
    use krabka_verified::{SnapshotInstallDecision, snapshot_install_decision};

    for (pending, end, epoch, log_end, expected) in [
        (false, 11, 3, 10, SnapshotInstallDecision::Install),
        (false, 10, 3, 10, SnapshotInstallDecision::Stale),
        (false, 9, 3, 10, SnapshotInstallDecision::Stale),
        (true, 11, 3, 10, SnapshotInstallDecision::Reject),
        (false, -1, 3, 10, SnapshotInstallDecision::Reject),
        (false, 11, -1, 10, SnapshotInstallDecision::Reject),
    ] {
        assert2::assert!(snapshot_install_decision(pending, end, epoch, log_end) == expected);
    }
}

fn become_follower(engine: &mut Engine, leader_id: NodeId, leader_epoch: Epoch) {
    engine.on_event(Event::ReceiveBeginQuorumEpoch {
        leader_id,
        leader_epoch,
    });
    assert2::assert!(matches!(
        engine.core.role(),
        Role::Follower { leader_id: active, .. } if *active == leader_id
    ));
}

#[test]
fn fetch_records_are_served_only_by_clean_leader_fetches() {
    for (_case, has_snapshot, has_divergence, is_leader, want) in [
        ("clean leader", false, false, true, true),
        ("snapshot response", true, false, true, false),
        ("divergence response", false, true, true, false),
        ("clean follower", false, false, false, false),
    ] {
        assert2::assert!(
            should_serve_fetch_records(has_snapshot, has_divergence, is_leader) == want
        );
    }
}

#[test]
fn fetch_epoch_uses_installed_snapshot_epoch_only_at_empty_boundary() {
    for (_case, installed, log_start, log_end, last_epoch, want) in [
        ("empty log with installed snapshot", Some(7), 10, 10, 3, 7),
        ("non-empty log with snapshot", Some(7), 10, 11, 3, 3),
        ("empty log without snapshot", None, 10, 10, 3, 3),
    ] {
        assert2::assert!(
            fetch_epoch_for_request(installed, Offset(log_start), Offset(log_end), last_epoch)
                == want
        );
    }
}

#[test]
fn fetch_batch_classifier_separates_duplicate_append_and_gap() {
    for (_case, base_offset, log_end, want) in [
        (
            "duplicate batch",
            4,
            5,
            FetchBatchDisposition::AlreadyPresent,
        ),
        ("contiguous append", 5, 5, FetchBatchDisposition::Append),
        ("offset gap", 6, 5, FetchBatchDisposition::Gap),
    ] {
        assert2::assert!(classify_fetch_batch(Offset(base_offset), Offset(log_end)) == want);
    }
}

#[test]
fn snapshot_fetch_hint_starts_only_for_future_non_duplicate_snapshots() {
    for (_case, snapshot_id, log_end, in_flight, want) in [
        ("future snapshot", (11, 2), 10, None, true),
        ("snapshot at log end", (10, 2), 10, None, false),
        (
            "duplicate in-flight snapshot",
            (11, 2),
            10,
            Some((11, 2)),
            false,
        ),
        ("newer in-flight snapshot", (12, 2), 10, Some((11, 2)), true),
    ] {
        assert2::assert!(
            should_start_snapshot_fetch(snapshot_id, Offset(log_end), in_flight) == want
        );
    }
}

#[test]
fn snapshot_fetch_response_is_invalid_unless_success_from_active_leader() {
    for (_case, error_code, response_epoch, current_epoch, want) in [
        ("successful active leader", 0, 2, 2, false),
        ("error from active leader", 1, 2, 2, true),
        ("success from wrong epoch", 0, 3, 2, true),
        ("error from wrong epoch", 1, 3, 2, true),
    ] {
        assert2::assert!(
            snapshot_fetch_response_invalid(
                error_code,
                NodeId(response_epoch),
                NodeId(current_epoch)
            ) == want
        );
    }
}

#[tokio::test]
async fn follower_fetch_redirects_to_current_leader() {
    let (mut follower, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    follower.on_event(Event::ReceiveBeginQuorumEpoch {
        leader_id: NodeId(2),
        leader_epoch: 1,
    });
    let (reply, mut response) = oneshot::channel();

    follower.on_inbound(Inbound::Fetch {
        req: wire::PeerRequest::Fetch {
            from: NodeId(3),
            fetch_epoch: 1,
            fetch_offset: 0,
        }
        .encode(),
        reply,
    });

    let body = response
        .try_recv()
        .expect("follower returned Fetch redirect");
    let decoded = wire::PeerResponse::decode_fetch(&body).expect("decode Fetch redirect");
    assert2::assert!(matches!(
        decoded,
        wire::PeerResponse::Fetch {
            leader_id: NodeId(2),
            leader_epoch: 1,
            records,
            ..
        } if records.is_empty()
    ));
}

#[tokio::test]
async fn fetch_without_leader_returns_error_instead_of_dropping_reply() {
    let (mut follower, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let leader_epoch = follower.core.quorum_state().leader_epoch;
    let (reply, mut response) = oneshot::channel();

    follower.on_inbound(Inbound::Fetch {
        req: wire::PeerRequest::Fetch {
            from: NodeId(2),
            fetch_epoch: leader_epoch,
            fetch_offset: 0,
        }
        .encode(),
        reply,
    });

    let body = response
        .try_recv()
        .expect("leaderless Fetch returned an error");
    assert2::assert!(matches!(
        wire::PeerResponse::decode_fetch(&body),
        Some(wire::PeerResponse::FetchError {
            leader_epoch: response_epoch,
            error_code: wire::NOT_LEADER_OR_FOLLOWER,
        }) if response_epoch == leader_epoch
    ));
}

#[tokio::test]
async fn broadcast_end_quorum_epoch_sends_to_every_other_voter() {
    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let mut sends = record_peer_sends(&mut engine, wire::PeerResponse::Ack { epoch: 4 }.encode());

    engine.broadcast_end_quorum_epoch(4);

    let mut peers = Vec::new();
    for _ in 0..2 {
        let send = recv_peer_send(&mut sends).await;
        assert2::assert!(send.api_key == api_key::END_QUORUM_EPOCH);
        match wire::decode_end(&send.body) {
            Some(wire::PeerRequest::EndQuorumEpoch {
                leader_id,
                leader_epoch,
            }) => {
                assert2::assert!(leader_id == NodeId(1));
                assert2::assert!(leader_epoch == 4);
            }
            other => panic!("unexpected end quorum request: {other:?}"),
        }
        peers.push(send.peer);
    }
    peers.sort_unstable();
    assert2::assert!(peers == vec![NodeId(2), NodeId(3)]);
}

#[tokio::test]
async fn send_fetch_uses_snapshot_epoch_only_until_log_extends_past_boundary() {
    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2)]);
    engine
        .log
        .install_snapshot(Offset(10))
        .expect("install snapshot");
    engine.installed_snapshot_epoch = Some(7);
    let fetch_response = wire::PeerResponse::Fetch {
        leader_id: NodeId(2),
        leader_epoch: 7,
        diverging: None,
        snapshot_id: None,
        hwm: 10,
        records: bytes::Bytes::new(),
    }
    .encode();
    let mut sends = record_peer_sends(&mut engine, fetch_response.clone());

    engine.send_fetch(NodeId(2));
    let send = recv_peer_send(&mut sends).await;
    match wire::decode_fetch(&send.body) {
        Some(wire::PeerRequest::Fetch {
            fetch_epoch,
            fetch_offset,
            ..
        }) => {
            assert2::assert!(fetch_epoch == 7);
            assert2::assert!(fetch_offset == 10);
        }
        other => panic!("unexpected fetch request: {other:?}"),
    }

    let mut batch = one_offset_batch(10, 9, b"after-snapshot");
    engine
        .log
        .append_at(&mut batch, Offset(10))
        .expect("append after snapshot");
    engine.send_fetch(NodeId(2));
    let send = recv_peer_send(&mut sends).await;
    match wire::decode_fetch(&send.body) {
        Some(wire::PeerRequest::Fetch {
            fetch_epoch,
            fetch_offset,
            ..
        }) => {
            assert2::assert!(fetch_epoch == 9);
            assert2::assert!(fetch_offset == 11);
        }
        other => panic!("unexpected fetch request: {other:?}"),
    }
}

#[test]
fn serve_fetch_records_returns_batches_only_for_offsets_inside_log() {
    let (mut engine, _dir) = build_engine_only_with_policy(
        NodeId(1),
        &[NodeId(1)],
        ControllerFetchMissLimit::default(),
        MetadataRaftFetchMax::try_from(krabka_units::bytes(1))
            .expect("one byte still serves the first batch"),
    );
    let mut batch = one_offset_batch(0, 1, b"a");
    engine.log.append(&mut batch, 0).expect("append");
    let mut batch = one_offset_batch(1, 1, b"b");
    engine.log.append(&mut batch, 0).expect("append");

    assert2::assert!(engine.serve_fetch_records(Offset(-1)).is_empty());
    assert2::assert!(engine.serve_fetch_records(Offset(2)).is_empty());
    let records = engine.serve_fetch_records(Offset(0));
    let decoded = decode_batches(&records).expect("decode served records");
    assert2::assert!(
        decoded
            .iter()
            .map(|batch| batch.base_offset)
            .collect::<Vec<_>>()
            == vec![0]
    );
}

#[tokio::test]
async fn fetch_response_snapshot_hint_starts_once_and_ignores_stale_hint() {
    let (mut engine, _dir) = build_engine_only_with_policy(
        NodeId(1),
        &[NodeId(1), NodeId(2)],
        ControllerFetchMissLimit::default(),
        MetadataRaftFetchMax::try_from(krabka_units::bytes(512)).expect("positive fetch maximum"),
    );
    let fetch_snapshot_response = wire::PeerResponse::FetchSnapshot {
        snapshot_id: (11, 3),
        size: 0,
        position: 0,
        bytes: bytes::Bytes::new(),
        error_code: 0,
    }
    .encode();
    let mut sends = record_peer_sends(&mut engine, fetch_snapshot_response);
    become_follower(&mut engine, NodeId(2), 3);

    let body = wire::PeerResponse::Fetch {
        leader_id: NodeId(2),
        leader_epoch: 3,
        diverging: None,
        snapshot_id: Some((11, 3)),
        hwm: 11,
        records: bytes::Bytes::new(),
    }
    .encode();
    engine.on_fetch_response(NodeId(2), &body);
    let send = recv_peer_send_with_api(&mut sends, api_key::FETCH_SNAPSHOT).await;
    match wire::decode_fetch_snapshot(&send.body) {
        Some(wire::PeerRequest::FetchSnapshot {
            snapshot_id,
            position,
            max_bytes,
            ..
        }) => {
            assert2::assert!(snapshot_id == (11, 3));
            assert2::assert!(position == 0);
            assert2::assert!(max_bytes == 512);
        }
        other => panic!("unexpected fetch snapshot request: {other:?}"),
    }
    assert2::assert!(
        engine
            .snapshot_fetch
            .as_ref()
            .is_some_and(|s| s.snapshot_id == (11, 3))
    );

    engine.on_fetch_response(NodeId(2), &body);
    assert2::assert!(
        tokio::time::timeout(StdDuration::from_millis(20), async {
            loop {
                let send = recv_peer_send(&mut sends).await;
                if send.api_key == api_key::FETCH_SNAPSHOT {
                    return send;
                }
            }
        })
        .await
        .is_err()
    );

    engine
        .log
        .install_snapshot(Offset(11))
        .expect("install snapshot");
    engine.snapshot_fetch = None;
    engine.on_fetch_response(NodeId(2), &body);
    assert2::assert!(engine.snapshot_fetch.is_none());
}

#[tokio::test]
async fn leaderless_observer_discovers_before_applying_fetch_content() {
    let (mut observer, _dir) = build_engine_only(NodeId(3), &[NodeId(1), NodeId(2)]);
    assert2::assert!(matches!(
        observer.core.role(),
        Role::Observer {
            leader_id: None,
            ..
        }
    ));

    let body = wire::PeerResponse::Fetch {
        leader_id: NodeId(2),
        leader_epoch: 1,
        diverging: None,
        snapshot_id: None,
        hwm: 1,
        records: encode_batches(&[one_offset_batch(0, 1, b"replicated")]),
    }
    .encode();

    // The bootstrap peer redirects us to node 2. Content on this discovery
    // response is ignored until node 2 answers under the attached fence.
    observer.on_fetch_response(NodeId(1), &body);
    assert2::assert!(matches!(
        observer.core.role(),
        Role::Observer {
            leader_id: Some(NodeId(2)),
            ..
        }
    ));
    assert2::assert!(observer.log.log_end_offset() == Offset(0));
    assert2::assert!(observer.log.hwm() == Offset(0));

    observer.on_fetch_response(NodeId(2), &body);
    assert2::assert!(observer.log.log_end_offset() == Offset(1));
    assert2::assert!(observer.log.hwm() == Offset(1));
}

#[tokio::test]
async fn rejected_fetch_responses_leave_log_watermark_and_snapshot_unchanged() {
    let body = wire::PeerResponse::Fetch {
        leader_id: NodeId(2),
        leader_epoch: 3,
        diverging: None,
        snapshot_id: Some((11, 3)),
        hwm: 11,
        records: encode_batches(&[one_offset_batch(0, 3, b"foreign")]),
    }
    .encode();

    for (case, setup, from) in [
        ("stale epoch", 0u8, NodeId(2)),
        ("wrong sender", 1, NodeId(3)),
        ("changed role", 2, NodeId(2)),
    ] {
        let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        become_follower(&mut engine, NodeId(2), if setup == 0 { 4 } else { 3 });
        if setup == 2 {
            engine.on_event(Event::ReceiveEndQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 3,
            });
            assert2::assert!(matches!(engine.core.role(), Role::Prospective { .. }));
        }

        engine.on_fetch_response(from, &body);

        assert2::assert!(engine.log.log_end_offset() == Offset(0), "{case}");
        assert2::assert!(engine.log.hwm() == Offset(0), "{case}");
        assert2::assert!(engine.snapshot_fetch.is_none(), "{case}");
    }
}

#[tokio::test]
async fn admitted_fetch_selects_truncate_append_or_high_watermark_path() {
    // Truncation does not also append or advance the HWM.
    let (mut truncating, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2)]);
    for offset in 0..2 {
        let mut batch = one_offset_batch(offset, 2, b"local");
        truncating
            .log
            .append(&mut batch, 0)
            .expect("append local batch");
    }
    become_follower(&mut truncating, NodeId(2), 3);
    let truncate = wire::PeerResponse::Fetch {
        leader_id: NodeId(2),
        leader_epoch: 3,
        diverging: Some(LogOffsetMetadata {
            offset: 1,
            epoch: 2,
        }),
        snapshot_id: None,
        hwm: 2,
        records: encode_batches(&[one_offset_batch(2, 3, b"must-not-append")]),
    }
    .encode();
    truncating.on_fetch_response(NodeId(2), &truncate);
    assert2::assert!(truncating.log.log_end_offset() == Offset(1));
    assert2::assert!(truncating.log.hwm() == Offset(0));

    // Append advances the HWM only after the carried batch reaches the log.
    let (mut appending, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2)]);
    become_follower(&mut appending, NodeId(2), 3);
    let append = wire::PeerResponse::Fetch {
        leader_id: NodeId(2),
        leader_epoch: 3,
        diverging: None,
        snapshot_id: None,
        hwm: 1,
        records: encode_batches(&[one_offset_batch(0, 3, b"replicated")]),
    }
    .encode();
    appending.on_fetch_response(NodeId(2), &append);
    assert2::assert!(appending.log.log_end_offset() == Offset(1));
    assert2::assert!(appending.log.hwm() == Offset(1));

    // An empty response can advance only the watermark over existing data.
    let (mut advancing, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2)]);
    let mut local = one_offset_batch(0, 2, b"already-replicated");
    advancing
        .log
        .append(&mut local, 0)
        .expect("append local batch");
    become_follower(&mut advancing, NodeId(2), 3);
    let watermark = wire::PeerResponse::Fetch {
        leader_id: NodeId(2),
        leader_epoch: 3,
        diverging: None,
        snapshot_id: None,
        hwm: 1,
        records: bytes::Bytes::new(),
    }
    .encode();
    advancing.on_fetch_response(NodeId(2), &watermark);
    assert2::assert!(advancing.log.log_end_offset() == Offset(1));
    assert2::assert!(advancing.log.hwm() == Offset(1));
}

#[tokio::test]
async fn fetch_snapshot_response_error_or_wrong_leader_aborts_transfer() {
    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2)]);
    let fetch_response = wire::PeerResponse::Fetch {
        leader_id: NodeId(2),
        leader_epoch: 3,
        diverging: None,
        snapshot_id: None,
        hwm: 0,
        records: bytes::Bytes::new(),
    }
    .encode();
    let mut sends = record_peer_sends(&mut engine, fetch_response);

    engine.snapshot_fetch = Some(SnapshotFetchState::new((12, 3), NodeId(2)));
    let error_body = wire::PeerResponse::FetchSnapshot {
        snapshot_id: (12, 3),
        size: 0,
        position: 0,
        bytes: bytes::Bytes::new(),
        error_code: 99,
    }
    .encode();
    engine.on_fetch_snapshot_response(NodeId(2), &error_body);
    assert2::assert!(engine.snapshot_fetch.is_none());
    let send = recv_peer_send_with_api(&mut sends, api_key::FETCH).await;
    assert2::assert!(send.peer == 2);

    engine.snapshot_fetch = Some(SnapshotFetchState::new((12, 3), NodeId(2)));
    let ok_body = wire::PeerResponse::FetchSnapshot {
        snapshot_id: (12, 3),
        size: 0,
        position: 0,
        bytes: bytes::Bytes::new(),
        error_code: 0,
    }
    .encode();
    engine.on_fetch_snapshot_response(NodeId(3), &ok_body);
    assert2::assert!(engine.snapshot_fetch.is_none());
    let send = recv_peer_send_with_api(&mut sends, api_key::FETCH).await;
    assert2::assert!(send.peer == 3);
}

/// A follower clamps its own high watermark to its log end, so that value
/// alone cannot say how far behind the quorum this node is. The snapshot
/// therefore carries the leader's watermark separately, and it never goes
/// backwards when a later response reports a lower one.
#[tokio::test]
async fn quorum_high_watermark_keeps_the_leader_s_watermark_past_the_local_clamp() {
    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2)]);

    // A node that has heard from nobody reports its own watermark.
    assert!(engine.quorum_state_snapshot().quorum_high_watermark == 0);

    // Only a response that clears the leader/epoch fence is admitted, so put
    // the node behind node 2 at the epoch the responses below carry.
    become_follower(&mut engine, NodeId(2), 3);

    let response = |hwm: i64| {
        wire::PeerResponse::Fetch {
            leader_id: NodeId(2),
            leader_epoch: 3,
            diverging: None,
            snapshot_id: None,
            hwm,
            records: bytes::Bytes::new(),
        }
        .encode()
    };

    // The leader has committed 10 000 records and this node has none of them.
    engine.on_fetch_response(NodeId(2), &response(10_000));
    let snapshot = engine.quorum_state_snapshot();
    assert!(snapshot.high_watermark == 0);
    assert!(snapshot.quorum_high_watermark == 10_000);

    // A stale response cannot walk the quorum's committed offset back: every
    // watermark a leader reports is committed, so the highest one seen is a
    // lower bound on what the quorum has.
    engine.on_fetch_response(NodeId(2), &response(9_000));
    assert!(engine.quorum_state_snapshot().quorum_high_watermark == 10_000);
}

/// A node whose fetch offset is below the leader's pruned log start is told to
/// take a snapshot instead, and that response carries the leader's watermark
/// like any other. It is also the node furthest behind the quorum, so dropping
/// the watermark on that path would report the worst laggard as caught up.
#[tokio::test]
async fn quorum_high_watermark_is_recorded_from_a_snapshot_redirect_too() {
    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2)]);
    // Only a response that clears the leader/epoch fence is admitted, so put
    // the node behind node 2 at the epoch the responses below carry.
    become_follower(&mut engine, NodeId(2), 3);

    let redirect = wire::PeerResponse::Fetch {
        leader_id: NodeId(2),
        leader_epoch: 3,
        diverging: None,
        snapshot_id: Some((20_000, 3)),
        hwm: 20_000,
        records: bytes::Bytes::new(),
    }
    .encode();

    engine.on_fetch_response(NodeId(2), &redirect);
    let snapshot = engine.quorum_state_snapshot();
    assert!(snapshot.high_watermark == 0);
    assert!(snapshot.quorum_high_watermark == 20_000);
}

/// Every controller serves an observer's metadata fetch, not only the leader,
/// so the slice says both how far this node has committed -- which bounds the
/// records it can hand over -- and how far the quorum has. A follower that is
/// itself catching up would otherwise report its own clamped watermark as the
/// quorum's, and an observer that drew level with it would call itself ready.
#[tokio::test]
async fn a_lagging_follower_serves_the_quorums_committed_offset() {
    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2)]);
    // Only a response that clears the leader/epoch fence is admitted, so put
    // the node behind node 2 at the epoch the responses below carry.
    become_follower(&mut engine, NodeId(2), 3);

    engine.on_fetch_response(
        NodeId(2),
        &wire::PeerResponse::Fetch {
            leader_id: NodeId(2),
            leader_epoch: 3,
            diverging: None,
            snapshot_id: None,
            hwm: 10_000,
            records: bytes::Bytes::new(),
        }
        .encode(),
    );

    let slice = engine.metadata_fetch_slice(0, DEFAULT_METADATA_RAFT_FETCH_MAX);
    assert!(slice.high_watermark == 0);
    assert!(slice.quorum_high_watermark == 10_000);
}

#[test]
fn a_metadata_fetch_needs_a_snapshot_only_below_the_retained_log() {
    // Half-open: a fetch *at* the log start still reads from the log, and a
    // node that has never pruned (log start 0) never redirects. Widening this
    // to `<=` would answer a caught-up observer with a snapshot on every poll.
    for (_case, fetch_offset, log_start, want) in [
        ("pruned away", 0, 4_096, true),
        ("one below the start", 4_095, 4_096, true),
        ("at the start", 4_096, 4_096, false),
        ("past the start", 4_097, 4_096, false),
        ("never pruned", 0, 0, false),
        ("negative offset", -1, 4_096, false),
    ] {
        assert!(
            metadata_fetch_offset_below_log_start(Offset(fetch_offset), Offset(log_start)) == want,
            "fetch {fetch_offset} against log start {log_start}"
        );
    }
}

/// An observer that asks for an offset the controller has already pruned is
/// pointed at the latest checkpoint instead of being served an empty slice.
///
/// This is the restart case: a broker-only node comes back at offset 0, the
/// controller has snapshotted and pruned past it, and without the snapshot id
/// the observer re-asks for the same gone offset on every poll, never builds
/// an image, and never registers.
#[tokio::test]
async fn a_metadata_fetch_below_the_pruned_log_start_returns_the_snapshot_id() {
    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
    elect_single_voter_engine(&mut engine);
    for name in ["a", "b", "c"] {
        let (reply, mut rx) = oneshot::channel();
        engine.on_submit_change(&topic_record(name), reply);
        assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    }
    engine
        .write_snapshot_and_prune()
        .expect("snapshot and prune");
    let log_start = engine.log.log_start_offset();
    assert!(log_start > 0, "the log must have been pruned");
    let snapshot_id = engine.latest_snapshot_id().expect("a checkpoint exists");

    let pruned = engine.metadata_fetch_slice(0, DEFAULT_METADATA_RAFT_FETCH_MAX);
    assert!(pruned.snapshot_id == Some(snapshot_id));
    assert!(pruned.records.is_empty());
    assert!(pruned.log_start_offset == log_start.0);

    // At the retained boundary the log still answers, so the observer keeps
    // fetching records rather than re-installing a snapshot it already has.
    let retained = engine.metadata_fetch_slice(log_start.0, DEFAULT_METADATA_RAFT_FETCH_MAX);
    assert!(retained.snapshot_id == None);
}
