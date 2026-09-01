//! Tests for KIP-630 snapshotting: the interval-driven checkpoint and prune, the
//! checkpoint directory's id ordering and single-snapshot retention, and the
//! fact that an ordinary snapshot never reloads the live image.

use assert2::assert;

use super::*;
use crate::kraft::controller::{
    checkpoint::{
        checkpoint_id_is_newer, latest_checkpoint_id, load_checkpoint_by_id,
        load_latest_checkpoint, parse_checkpoint_name, retain_latest_checkpoint, write_checkpoint,
    },
    test_support::{
        await_leader, build_engine_only, build_with_snapshot_interval, elect_single_voter_engine,
        submit_change_with_timeout, topic_record,
    },
};

#[test]
fn snapshot_pruning_stays_at_or_below_the_committed_frontier() {
    assert2::assert!(krabka_verified::snapshot_prune_admission(10, 10));
    assert2::assert!(krabka_verified::snapshot_prune_admission(9, 10));
    assert2::assert!(!krabka_verified::snapshot_prune_admission(11, 10));
    assert2::assert!(!krabka_verified::snapshot_prune_admission(-1, 10));
}

#[test]
fn checkpoint_id_ordering_prefers_higher_offset_then_epoch_without_equal_replacement() {
    for (_case, candidate, current, want) in [
        ("higher offset", (11, 1), (10, 9), true),
        ("same offset higher epoch", (10, 9), (10, 2), true),
        ("same offset lower epoch", (10, 2), (10, 9), false),
        ("equal checkpoint", (10, 9), (10, 9), false),
    ] {
        assert2::assert!(checkpoint_id_is_newer(candidate, current) == want);
    }
}

#[test]
fn checkpoint_names_must_use_the_canonical_fixed_width_encoding() {
    assert2::assert!(
        parse_checkpoint_name("00000000000000000010-0000000002.checkpoint") == Some((10, 2))
    );
    for malformed in [
        "10-2.checkpoint",
        "00000000000000000010-0000000002",
        "00000000000000000010-0000000002.checkpoint.tmp",
        "-0000000000000000001-0000000002.checkpoint",
        "00000000000000000010--000000001.checkpoint",
        "not-a-checkpoint",
    ] {
        assert2::assert!(parse_checkpoint_name(malformed).is_none(), "{malformed}");
    }
}

#[test]
fn ordinary_snapshot_does_not_reload_the_live_image() {
    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
    elect_single_voter_engine(&mut engine);
    let (reply, mut rx) = oneshot::channel();
    engine.on_submit_change(&topic_record("ordinary-snapshot"), reply);
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));

    // Replication factor is derived from PartitionRecord on the wire. A
    // normal KIP-630 checkpoint must not replace unrelated in-memory state
    // with that canonicalized representation; only a downgrade reloads.
    let mut in_memory_only = engine.image.topic("ordinary-snapshot").unwrap().clone();
    in_memory_only.replication_factor = 99;
    engine.image.apply(&MetadataRecord::V1Topic(in_memory_only));

    engine
        .write_snapshot_and_prune()
        .expect("ordinary snapshot");

    assert2::assert!(
        engine
            .image
            .topic("ordinary-snapshot")
            .unwrap()
            .replication_factor
            == 99
    );
}

/// A single-voter leader with `snapshot_interval_records = 3` snapshots and
/// prunes once the committed offset has advanced past the threshold. After
/// committing four distinct topics, a checkpoint exists on disk and the log
/// has been pruned (its log-start offset rose above 0).
#[tokio::test]
async fn leader_snapshots_and_prunes_at_threshold() {
    let (ctrl, dir) = build_with_snapshot_interval(NodeId(1), &[NodeId(1)], 3);
    ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
    await_leader(&ctrl, Some(NodeId(1))).await;

    // Four distinct topics, each committed immediately (single voter). Each
    // commit advances the HWM well past the 3-record interval, so a
    // snapshot+prune fires.
    for name in ["a", "b", "c", "d"] {
        submit_change_with_timeout(&ctrl, topic_record(name), "snapshot threshold submit")
            .await
            .unwrap();
    }

    // A checkpoint was written.
    let cp = load_latest_checkpoint(&checkpoint_dir(dir.path()))
        .expect("scan checkpoints")
        .expect("a checkpoint exists");
    assert2::assert!(!cp.is_empty());

    // The log was pruned: log-start advanced past 0.
    let qs = ctrl.quorum_state().await.unwrap();
    assert2::assert!(qs.log_start_offset > 0);
    ctrl.shutdown().await;
}

#[test]
fn latest_checkpoint_id_picks_highest_offset_then_epoch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cp_dir = checkpoint_dir(dir.path());
    write_checkpoint(&cp_dir, 10, 2, b"ten-two").expect("write checkpoint 10/2");
    write_checkpoint(&cp_dir, 10, 9, b"ten-nine").expect("write checkpoint 10/9");
    write_checkpoint(&cp_dir, 11, 1, b"eleven-one").expect("write checkpoint 11/1");

    assert2::assert!(latest_checkpoint_id(&cp_dir) == Some((11, 1)));
    let latest = load_latest_checkpoint(&cp_dir)
        .expect("load latest")
        .expect("latest exists");
    assert2::assert!(latest == b"eleven-one");
}

#[test]
fn retain_latest_checkpoint_deletes_older_checkpoints_only() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cp_dir = checkpoint_dir(dir.path());
    write_checkpoint(&cp_dir, 5, 1, b"old").expect("write old");
    write_checkpoint(&cp_dir, 6, 1, b"new").expect("write new");
    write_checkpoint(&cp_dir, 6, 0, b"older-same-offset").expect("write older same offset");

    retain_latest_checkpoint(&cp_dir);

    for (_case, end_offset, epoch, want_present) in [
        ("matching checkpoint", 6, 1, true),
        ("wrong end offset", 5, 1, false),
        ("wrong epoch", 6, 0, false),
    ] {
        assert2::assert!(
            load_checkpoint_by_id(&cp_dir, end_offset, epoch).is_some() == want_present
        );
    }
    let entries: Vec<_> = std::fs::read_dir(&cp_dir)
        .expect("read checkpoint dir")
        .collect::<Result<Vec<_>, _>>()
        .expect("read entries");
    assert2::assert!(entries.len() == 1);
}
