//! Tests for KIP-630 snapshotting: the interval-driven checkpoint and prune,
//! the checkpoint directory's id ordering and two-snapshot retention, the
//! header timestamp each checkpoint is stamped with, and the fact that an
//! ordinary snapshot never reloads the live image.

use {
    super::*,
    crate::{
        kraft::{
            controller::{
                checkpoint::{
                    CheckpointChunk, CheckpointFile, checkpoint_id_is_newer, latest_checkpoint_id,
                    load_checkpoint_by_id, load_latest_checkpoint, open_checkpoint,
                    parse_checkpoint_name, retain_latest_checkpoint, retain_recent_checkpoints,
                    write_checkpoint,
                },
                records::decode_control_record,
                test_support::{
                    ControllerInDir, await_leader, build, build_engine_only,
                    elect_single_voter_engine, one_offset_batch, open_test_controller_with,
                    submit_change_with_timeout, topic_record, voter_set,
                },
            },
            snapshot_fetch::SnapshotId,
        },
        snapshot::{SnapshotReader, SnapshotWriter},
    },
    assert2::assert,
};

async fn assert_checkpoint_pruned(ctrl: &KraftController, dir: &std::path::Path) {
    let quorum = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let quorum = ctrl.quorum_state().await.unwrap();
            if quorum.log_start_offset > 0 {
                break quorum;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("snapshot completion");
    let checkpoint = load_latest_checkpoint(dir)
        .expect("scan checkpoints")
        .expect("a checkpoint exists");
    assert2::assert!(!checkpoint.is_empty());
    assert2::assert!(quorum.log_start_offset > 0);
}

/// The `last_contained_log_timestamp` a checkpoint's KIP-630
/// `SnapshotHeaderRecord` is stamped with. The header is the first control
/// batch of the artifact, so decoding it is how a reader — the JVM
/// `kafka-dump-log --cluster-metadata-decoder` included — sees the value.
fn latest_header_timestamp(checkpoint_dir: &std::path::Path) -> i64 {
    let bytes = load_latest_checkpoint(checkpoint_dir)
        .expect("scan checkpoints")
        .expect("a checkpoint exists");
    snapshot_header_timestamp(&bytes)
}

fn snapshot_header_timestamp(bytes: &[u8]) -> i64 {
    let mut cursor = bytes;
    let header = RecordBatch::decode(&mut cursor).expect("decode the header batch");
    let record = header.records.first().expect("a header record");
    match decode_control_record(record).expect("decode the header control record") {
        ControlRecord::SnapshotHeader(header) => header.last_contained_log_timestamp,
        other => panic!("expected a snapshot header control record, got {other:?}"),
    }
}

#[test]
fn snapshot_pruning_stays_at_or_below_the_committed_frontier() {
    assert2::assert!(krabka_verified::snapshot_prune_admission(10, 10));
    assert2::assert!(krabka_verified::snapshot_prune_admission(9, 10));
    assert2::assert!(!krabka_verified::snapshot_prune_admission(11, 10));
    assert2::assert!(!krabka_verified::snapshot_prune_admission(-1, 10));
}

fn delayed_snapshot_engine(topic: &str) -> (Engine, tempfile::TempDir) {
    let (mut engine, dir) = build_engine_only().ids(&[NodeId(1)]).call();
    elect_single_voter_engine(&mut engine);
    let mut committed = super::test_support::submit_on_engine(&mut engine, &topic_record(topic));
    assert2::assert!(matches!(committed.try_recv(), Ok(Ok(_))));
    engine.snapshot_interval_records = 1;
    engine.snapshot_write_delay = Duration::from_millis(300);
    (engine, dir)
}

fn assert_checkpoint_contains_topic(dir: &std::path::Path, topic: &str) {
    let bytes = load_latest_checkpoint(dir).unwrap().unwrap();
    let contents = SnapshotReader::read(&bytes).unwrap();
    let image = MetadataImage::from_records(uuid::Uuid::nil(), &contents.metadata_records);
    assert2::assert!(image.topic(topic).is_some());
}

/// A deliberately slow durable checkpoint must not occupy the owning engine
/// task. The second command is handled while the sole worker is still asleep.
#[tokio::test]
async fn delayed_regular_checkpoint_keeps_engine_commands_moving() {
    let (mut engine, dir) = delayed_snapshot_engine("snapshot-boundary");

    let (tx, rx) = CommandSender::channel(4);
    engine.cmd_tx = tx.clone();
    let task = tokio::spawn(engine.run(rx));

    // This first query completes its turn and starts the regular worker.
    let (reply, answer) = tokio::sync::oneshot::channel();
    tx.send(Command::QuorumStateSnapshot { reply })
        .await
        .unwrap();
    answer.await.expect("first query");

    let (reply, answer) = tokio::sync::oneshot::channel();
    tx.send(Command::QuorumStateSnapshot { reply })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_millis(100), answer)
        .await
        .expect("engine was blocked by checkpoint")
        .expect("second query");
    assert2::assert!(latest_checkpoint_id(dir.path()).is_none());

    tokio::time::timeout(Duration::from_secs(2), async {
        while latest_checkpoint_id(dir.path()).is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("worker checkpoint");
    assert_checkpoint_contains_topic(dir.path(), "snapshot-boundary");
    tx.send(Command::Halt).await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn regular_worker_excludes_other_checkpoint_writers_until_completion() {
    let (mut engine, _dir) = delayed_snapshot_engine("snapshot-writer");
    let bytes = SnapshotWriter::serialize(&engine.image, 0).unwrap();
    let id = (engine.log.hwm().0, 1);
    engine.maybe_snapshot();
    assert2::assert!(engine.snapshot_job.is_some());
    assert2::assert!(engine.do_trigger_snapshot().is_err());
    assert2::assert!(engine.write_snapshot_checkpoint(&bytes).is_err());
    assert2::assert!(engine.install_fetched_snapshot(id, &bytes).is_err());
    let job = engine.snapshot_job.take().unwrap();
    engine.finish_snapshot_job(job.await);
    assert2::assert!(engine.do_trigger_snapshot().is_ok());
    assert2::assert!(engine.write_snapshot_checkpoint(&bytes).is_ok());
    assert2::assert!(engine.install_fetched_snapshot(id, &bytes).is_ok());
}

#[tokio::test]
async fn halting_joins_regular_checkpoint_before_releasing_the_directory() {
    let (mut engine, dir) = delayed_snapshot_engine("snapshot-halt");
    engine.maybe_snapshot();
    let (tx, rx) = CommandSender::channel(4);
    engine.cmd_tx = tx.clone();
    tx.send(Command::Halt).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), engine.run(rx))
        .await
        .expect("halt joins the checkpoint worker");
    assert_checkpoint_contains_topic(dir.path(), "snapshot-halt");
}

#[yare::parameterized(
    higher_offset = { (11, 1), (10, 9), true },
    same_offset_higher_epoch = { (10, 9), (10, 2), true },
    same_offset_lower_epoch = { (10, 2), (10, 9), false },
    equal_checkpoint = { (10, 9), (10, 9), false },
)]
fn checkpoint_id_ordering_prefers_higher_offset_then_epoch_without_equal_replacement(
    candidate: SnapshotId,
    current: SnapshotId,
    want: bool,
) {
    assert2::assert!(checkpoint_id_is_newer(candidate, current) == want);
}

#[test]
fn checkpoint_names_use_canonical_fixed_width_encoding() {
    assert2::assert!(
        parse_checkpoint_name("00000000000000000010-0000000002.checkpoint") == Some((10, 2))
    );
    assert2::assert!(
        parse_checkpoint_name("00000000000000000000-0000000000.checkpoint") == Some((0, 0))
    );
}

#[yare::parameterized(
    short_widths = { "10-2.checkpoint" },
    missing_suffix = { "00000000000000000010-0000000002" },
    extra_suffix = { "00000000000000000010-0000000002.checkpoint.tmp" },
    negative_offset = { "-0000000000000000001-0000000002.checkpoint" },
    malformed_epoch = { "00000000000000000010--000000001.checkpoint" },
    not_a_checkpoint = { "not-a-checkpoint" }
)]
fn malformed_checkpoint_names_are_rejected(malformed: &str) {
    assert2::assert!(parse_checkpoint_name(malformed).is_none());
}

#[test]
fn ordinary_snapshot_does_not_reload_the_live_image() {
    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    let mut rx =
        super::test_support::submit_on_engine(&mut engine, &topic_record("ordinary-snapshot"));
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

/// Elect the single-voter fixture before snapshot submissions.
async fn single_leader_snapshot_fixture() -> ControllerInDir {
    let (ctrl, dir) = build()
        .ids(&[NodeId(1)])
        .snapshot_interval_records(3)
        .call();
    super::test_support::elect_single_voter_controller(&ctrl).await;
    ControllerInDir {
        controller: ctrl,
        dir,
    }
}

/// A single-voter leader with `snapshot_interval_records = 3` snapshots each
/// time the committed offset advances past the threshold. The test engine
/// keeps no log beyond its newest snapshot, so the cleaning after the second
/// snapshot moves the log start up. After four distinct topics, a checkpoint
/// exists on disk and the log start has risen above 0.
#[tokio::test]
async fn leader_snapshots_and_prunes_at_threshold() {
    let ControllerInDir {
        controller: ctrl,
        dir,
    } = single_leader_snapshot_fixture().await;

    // Four distinct topics, each committed immediately (single voter). Each
    // commit advances the HWM well past the 3-record interval, so a snapshot
    // fires, and the second one lets the cleaning move the log start.
    for name in ["a", "b", "c", "d"] {
        submit_change_with_timeout(&ctrl, topic_record(name), "snapshot threshold submit")
            .await
            .unwrap();
    }

    // A checkpoint was written.
    // The log was cleaned: log-start advanced past 0.
    assert_checkpoint_pruned(&ctrl, dir.path()).await;
    ctrl.shutdown().await;
}

/// The byte-size cap (`max_bytes_between_snapshots`) fires once enough
/// records have committed, even though no single commit's batch reaches the
/// threshold on its own and the threshold falls strictly between batch
/// boundaries. `bytes_since_snapshot` is tracked incrementally from applied
/// batch lengths rather than re-derived from a bounded log read, which would
/// under-count whenever a batch that would cross the cap doesn't fit the
/// remaining read budget and gets silently excluded (never re-included on a
/// later, identical read of the same unchanged range).
#[tokio::test]
async fn leader_snapshots_and_prunes_at_byte_threshold_across_many_small_commits() {
    let (ctrl, dir) = build()
        .ids(&[NodeId(1)])
        .max_bytes_between_snapshots(krabka_units::prelude::bytes(200))
        .call();
    ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
    await_leader(&ctrl, Some(NodeId(1))).await;

    // Commit distinct single-topic batches, each comfortably under the
    // 200-byte cap on its own, until the cumulative bytes cross it.
    for i in 0..50 {
        submit_change_with_timeout(
            &ctrl,
            topic_record(&format!("bytes-cap-{i}")),
            "byte threshold submit",
        )
        .await
        .unwrap();
        if ctrl.quorum_state().await.unwrap().log_start_offset > 0 {
            break;
        }
    }

    assert_checkpoint_pruned(&ctrl, dir.path()).await;
    ctrl.shutdown().await;
}

#[test]
fn latest_checkpoint_id_picks_highest_offset_then_epoch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cp_dir = dir.path().to_path_buf();
    write_checkpoint(&cp_dir, 10, 2, b"ten-two").expect("write checkpoint 10/2");
    write_checkpoint(&cp_dir, 10, 9, b"ten-nine").expect("write checkpoint 10/9");
    write_checkpoint(&cp_dir, 11, 1, b"eleven-one").expect("write checkpoint 11/1");

    assert2::assert!(latest_checkpoint_id(&cp_dir) == Some((11, 1)));
    let latest = load_latest_checkpoint(&cp_dir)
        .expect("load latest")
        .expect("latest exists");
    assert2::assert!(latest == b"eleven-one");
}

fn checkpoint_entry_count(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .expect("read checkpoint dir")
        .collect::<Result<Vec<_>, _>>()
        .expect("read entries")
        .len()
}

#[test]
fn retain_recent_checkpoints_keeps_the_two_newest_ids_and_deletes_the_rest() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cp_dir = dir.path().to_path_buf();
    write_checkpoint(&cp_dir, 5, 1, b"oldest").expect("write oldest");
    write_checkpoint(&cp_dir, 6, 0, b"older-same-offset").expect("write older same offset");
    write_checkpoint(&cp_dir, 6, 1, b"newest").expect("write newest");

    retain_recent_checkpoints(&cp_dir);

    for (_case, end_offset, epoch, want_present) in [
        ("the latest checkpoint", 6, 1, true),
        ("the one before it", 6, 0, true),
        ("older than both", 5, 1, false),
    ] {
        assert2::assert!(
            load_checkpoint_by_id(&cp_dir, end_offset, epoch).is_some() == want_present
        );
    }
    assert2::assert!(checkpoint_entry_count(&cp_dir) == 2);
}

#[test]
fn retain_recent_checkpoints_keeps_a_lone_checkpoint() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cp_dir = dir.path().to_path_buf();
    write_checkpoint(&cp_dir, 7, 2, b"only").expect("write only");

    retain_recent_checkpoints(&cp_dir);

    assert2::assert!(load_checkpoint_by_id(&cp_dir, 7, 2) == Some(b"only".to_vec()));
}

#[test]
fn retain_latest_checkpoint_keeps_only_the_single_newest_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cp_dir = dir.path().to_path_buf();
    write_checkpoint(&cp_dir, 5, 1, b"oldest").expect("write oldest");
    write_checkpoint(&cp_dir, 6, 0, b"older").expect("write older");
    write_checkpoint(&cp_dir, 6, 1, b"newest").expect("write newest");

    retain_latest_checkpoint(&cp_dir);

    assert2::assert!(load_checkpoint_by_id(&cp_dir, 6, 1) == Some(b"newest".to_vec()));
    assert2::assert!(load_checkpoint_by_id(&cp_dir, 6, 0).is_none());
    assert2::assert!(load_checkpoint_by_id(&cp_dir, 5, 1).is_none());
    assert2::assert!(checkpoint_entry_count(&cp_dir) == 1);

    // Empty dir no-ops
    let empty_dir = tempfile::tempdir().expect("tempdir");
    retain_latest_checkpoint(empty_dir.path());
}

/// A snapshot roll leaves the checkpoint it replaces on disk, and only the
/// roll after that removes it. That one-snapshot grace is what a follower
/// chunking a `FetchSnapshot` through the previous id needs: Kafka keeps the
/// previous snapshot servable the same way, through retention rather than by
/// tracking in-flight readers.
#[test]
fn a_snapshot_roll_keeps_the_checkpoint_it_replaces_until_the_next_one() {
    let (mut engine, dir) = build_engine_only().ids(&[NodeId(1)]).call();
    elect_single_voter_engine(&mut engine);
    let cp_dir = dir.path().to_path_buf();

    let mut ids = Vec::new();
    for name in ["first", "second", "third"] {
        let mut rx = super::test_support::submit_on_engine(&mut engine, &topic_record(name));
        assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));
        engine
            .write_snapshot_and_prune()
            .expect("snapshot and prune");
        ids.push(latest_checkpoint_id(&cp_dir).expect("a checkpoint exists"));

        // Every roll must land on a genuinely new id, or retention would have
        // nothing to keep and this test would pass vacuously.
        assert2::assert!(ids.iter().filter(|id| **id == ids[ids.len() - 1]).count() == 1);

        if ids.len() == 2 {
            assert2::assert!(load_checkpoint_by_id(&cp_dir, ids[0].0, ids[0].1).is_some());
        }
    }

    assert2::assert!(
        (
            load_checkpoint_by_id(&cp_dir, ids[0].0, ids[0].1).is_some(),
            load_checkpoint_by_id(&cp_dir, ids[1].0, ids[1].1).is_some(),
            load_checkpoint_by_id(&cp_dir, ids[2].0, ids[2].1).is_some(),
        ) == (false, true, true)
    );
}

/// The KIP-630 header carries the create-time of the last batch the snapshot
/// contains, the way Kafka's `MetadataLoader` hands `createSnapshot` the
/// append time of the last batch it folded in. Before this the engine passed a
/// literal `0` and every checkpoint claimed 1970.
#[test]
fn the_checkpoint_header_carries_the_last_contained_batch_create_time() {
    let (mut engine, dir) = build_engine_only().ids(&[NodeId(1)]).call();
    elect_single_voter_engine(&mut engine);

    // Append below the engine's own submit path so the create-times are
    // chosen, not read off the wall clock. The snapshot covers both, so it
    // must name the newer one.
    let older = 1_700_000_000_000;
    let newer = 1_700_000_111_222;
    for (value, stamp) in [(b"a", older), (b"b", newer)] {
        let mut batch = one_offset_batch(0, 1, value);
        engine.log.append(&mut batch, stamp).expect("append");
    }
    engine.log.advance_hwm(engine.log.log_end_offset());

    engine
        .write_snapshot_and_prune()
        .expect("snapshot and prune");

    let bytes = load_latest_checkpoint(dir.path())
        .expect("scan checkpoints")
        .expect("a checkpoint exists");
    assert2::assert!(snapshot_header_timestamp(&bytes) == newer);
}

/// A checkpoint rewritten at a boundary the previous one already pruned to
/// keeps that boundary's create-time. The prune moves the log start up to the
/// boundary, so `hwm - 1` is no longer readable; the last record the snapshot
/// contains has not changed, and reverting its header to the epoch would undo
/// the stamp on disk. A time-cap fire on an idle log and an explicit trigger
/// both land here.
#[test]
fn a_snapshot_at_an_already_pruned_boundary_keeps_the_header_timestamp() {
    let (mut engine, dir) = build_engine_only().ids(&[NodeId(1)]).call();
    elect_single_voter_engine(&mut engine);
    let stamp = 1_700_000_222_333;
    let mut batch = one_offset_batch(0, 1, b"only");
    engine.log.append(&mut batch, stamp).expect("append");
    engine.log.advance_hwm(engine.log.log_end_offset());

    let cp_dir = dir.path().to_path_buf();
    let mut stamps = Vec::new();
    engine.write_snapshot_and_prune().expect("first snapshot");
    stamps.push(latest_header_timestamp(&cp_dir));
    // Nothing committed in between, so both of these rewrite the same
    // checkpoint id over the pruned boundary.
    engine.write_snapshot_and_prune().expect("second snapshot");
    stamps.push(latest_header_timestamp(&cp_dir));
    engine.do_trigger_snapshot().expect("explicit trigger");
    stamps.push(latest_header_timestamp(&cp_dir));

    assert2::assert!(stamps == vec![stamp; 3]);
}

/// A follower that installed a snapshot has no log below the boundary either,
/// so the create-time it carries forward comes from the installed artifact's
/// own header — the one place the record's stamp still exists on this node.
#[test]
fn an_installed_snapshot_hands_its_header_timestamp_to_the_next_checkpoint() {
    let (mut engine, dir) = build_engine_only()
        .me(2)
        .ids(&[NodeId(1), NodeId(2)])
        .call();
    let stamp = 1_700_000_444_555;
    let mut image = engine.image.clone();
    image.apply(&MetadataRecord::V1Voters(VotersRecord {
        voters: voter_set(&[NodeId(1), NodeId(2)]),
    }));
    let bytes = SnapshotWriter::serialize(&image, stamp).expect("serialize");

    engine
        .install_fetched_snapshot((7, 0), &bytes)
        .expect("install the fetched snapshot");
    engine
        .do_trigger_snapshot()
        .expect("checkpoint after install");

    assert2::assert!(latest_header_timestamp(dir.path()) == stamp);
}

/// The same across a restart: the recovered checkpoint's header is the only
/// surviving record of the create-time, and a checkpoint written before any
/// new records commit must not drop it.
#[tokio::test]
async fn a_restart_recovers_the_header_timestamp_from_the_checkpoint() {
    let stamp = 1_700_000_666_777;
    // Snapshot and prune without an election, so the reopened controller's
    // bootstrap epoch matches and its checkpoint lands on the same id.
    let (mut engine, dir) = build_engine_only().ids(&[NodeId(1)]).call();
    let mut batch = one_offset_batch(0, 0, b"restart");
    engine.log.append(&mut batch, stamp).expect("append");
    engine.log.advance_hwm(engine.log.log_end_offset());
    engine
        .write_snapshot_and_prune()
        .expect("snapshot and prune");
    let cp_dir = dir.path().to_path_buf();
    assert2::assert!(latest_header_timestamp(&cp_dir) == stamp);
    drop(engine);

    let reopened = open_test_controller_with()
        .data_dir(dir.path().to_path_buf())
        .cluster_id(uuid::Uuid::nil())
        .voters(voter_set(&[NodeId(1)]))
        .call()
        .expect("reopen over the same data dir");
    reopened.trigger_snapshot().await.expect("trigger snapshot");

    assert2::assert!(latest_header_timestamp(&cp_dir) == stamp);
    reopened.shutdown().await;
}

/// The same stamp end to end through the engine's own leader append path: a
/// submitted change is stamped with the wall clock, and the checkpoint written
/// over it names that instant rather than the epoch.
#[tokio::test]
async fn a_submitted_change_stamps_its_checkpoint_with_the_append_wall_clock() {
    let ControllerInDir {
        controller: ctrl,
        dir,
    } = single_leader_snapshot_fixture().await;

    let before = KraftController::wall_clock_ms();
    for name in ["a", "b", "c", "d"] {
        submit_change_with_timeout(&ctrl, topic_record(name), "snapshot threshold submit")
            .await
            .unwrap();
    }
    let after = KraftController::wall_clock_ms();

    let bytes = load_latest_checkpoint(dir.path())
        .expect("scan checkpoints")
        .expect("a checkpoint exists");
    let stamped = snapshot_header_timestamp(&bytes);
    assert2::assert!(before <= stamped && stamped <= after, "{stamped}");
    ctrl.shutdown().await;
}

/// A reader that counts the bytes it hands out, to show what a chunk read
/// costs.
struct CountingReader {
    inner: std::io::Cursor<Vec<u8>>,
    read: u64,
}

impl std::io::Read for CountingReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.read += u64::try_from(n).expect("a read fits in u64");
        Ok(n)
    }
}

impl std::io::Seek for CountingReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> { self.inner.seek(pos) }
}

/// A chunk read validates the position against the length, clamps the chunk
/// to the end, and reads only the bytes it returns (#1610). The earlier serve
/// path read the whole checkpoint for every chunk.
#[yare::parameterized(
    first_chunk = { 0, 4, Some(b"0123".as_slice()) },
    middle_chunk = { 4, 3, Some(b"456".as_slice()) },
    clamped_to_the_end = { 7, 100, Some(b"789".as_slice()) },
    last_byte = { 9, 4, Some(b"9".as_slice()) },
    zero_max_bytes = { 3, 0, Some(b"".as_slice()) },
    negative_max_bytes = { 3, -1, Some(b"".as_slice()) },
    whole_file = { 0, i32::MAX, Some(b"0123456789".as_slice()) },
    position_at_the_end = { 10, 4, None },
    position_past_the_end = { 11, 4, None },
    negative_position = { -1, 4, None },
)]
fn checkpoint_chunk_reads_only_the_requested_range(
    position: i64,
    max_bytes: i32,
    expected: Option<&'static [u8]>,
) {
    let mut file = CheckpointFile::new(
        CountingReader {
            inner: std::io::Cursor::new(b"0123456789".to_vec()),
            read: 0,
        },
        10,
    );

    let chunk = file.read_chunk(position, max_bytes).expect("read");

    let expected_read = expected.map_or(0, |bytes| u64::try_from(bytes.len()).expect("fits"));
    assert2::assert!(
        (chunk, file.len(), file.reader().read)
            == (
                expected.map_or(CheckpointChunk::OutOfRange, |bytes| CheckpointChunk::Bytes(
                    bytes.to_vec()
                )),
                10,
                expected_read,
            )
    );
}

/// A checkpoint that retention deletes while a `FetchSnapshot` holds it open
/// still serves every chunk from the open handle, at its open length. One
/// deleted before the open is not found.
#[test]
fn an_open_checkpoint_survives_its_deletion() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_checkpoint(dir.path(), 6, 1, b"0123456789").expect("write");
    let mut open = open_checkpoint(dir.path(), 6, 1).expect("open");

    // Retention keeps the two newest ids, so two newer ones delete (6, 1).
    write_checkpoint(dir.path(), 7, 1, b"newer").expect("write newer");
    write_checkpoint(dir.path(), 8, 1, b"newest").expect("write newest");
    retain_recent_checkpoints(dir.path());
    assert!(open_checkpoint(dir.path(), 6, 1).is_none());

    let chunks = [
        open.read_chunk(0, 6).expect("first chunk"),
        open.read_chunk(6, 6).expect("second chunk"),
    ];
    assert!(
        (open.len(), chunks)
            == (10, [
                CheckpointChunk::Bytes(b"012345".to_vec()),
                CheckpointChunk::Bytes(b"6789".to_vec()),
            ])
    );
}

#[test]
fn open_checkpoint_misses_an_absent_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_checkpoint(dir.path(), 6, 1, b"present").expect("write");

    assert!(
        (
            open_checkpoint(dir.path(), 6, 1).map(|file| file.len()),
            open_checkpoint(dir.path(), 6, 2).is_none(),
            open_checkpoint(&dir.path().join("absent"), 6, 1).is_none(),
        ) == (Some(7), true, true)
    );
}
