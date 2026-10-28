//! Exercise real appends against a disk-sync gate, not a timing-only workload.

use {
    crate::{
        CompactionOutcome, Log, LogConfig, LogError, LogIo, Segment,
        log::test_support::{
            EncodedVerbatim, TempLog, append_keyed_samples, append_samples, compacting_segments,
            compaction_ctx, install_stamps, sample_batch, tiny_segments, verbatim_from,
        },
        name,
        producer_snapshot::{self, LoadedSnapshot, ProducerSnapshotEntry},
        test_support::open_log,
    },
    assert2::assert,
    bytes::Bytes,
    krabka_ids::{Offset, ProducerId},
    krabka_protocol::records::{Record, RecordBatch},
    krabka_units::{bytes, prelude::ByteSizeExt as _},
    std::{
        collections::HashSet,
        fs::{self, File},
        io, iter, mem,
        path::Path,
        sync::{
            Arc, Condvar, Mutex,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        thread::{self, JoinHandle, ThreadId},
        time::{Duration, SystemTime},
    },
    tempfile::tempdir,
};

fn check_boundary_snapshot(log: &Log, directory: &Path, expected: &[ProducerSnapshotEntry]) {
    let copy = tempdir().unwrap();
    fs::copy(
        name::producer_snapshot_path(directory, 1),
        name::producer_snapshot_path(copy.path(), 1),
    )
    .unwrap();
    let LoadedSnapshot { state, .. } =
        producer_snapshot::reload(copy.path(), log.producer_reload_range(Offset(1)))
            .unwrap()
            .unwrap();
    assert!(state.into_values().collect::<Vec<_>>() == expected);
}

#[derive(Debug)]
struct GatedIo {
    started: Mutex<Option<mpsc::Sender<ThreadId>>>,
    released: Mutex<bool>,
    changed: Condvar,
    fail: bool,
}

/// A [`GatedIo`] and the channel that reports the thread that reached it.
struct Gate {
    io: Arc<GatedIo>,
    started: mpsc::Receiver<ThreadId>,
}

impl GatedIo {
    fn gate(fail: bool) -> Gate {
        let (tx, rx) = mpsc::channel();
        Gate {
            io: Arc::new(Self {
                started: Mutex::new(Some(tx)),
                released: Mutex::new(false),
                changed: Condvar::new(),
                fail,
            }),
            started: rx,
        }
    }

    fn open_gate(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

impl LogIo for GatedIo {
    fn sync_data(&self, file: &File) -> io::Result<()> {
        let started = self.started.lock().unwrap().take();
        if let Some(started) = started {
            let _ = started.send(thread::current().id());
            drop(
                self.changed
                    .wait_while(self.released.lock().unwrap(), |released| !*released)
                    .unwrap(),
            );
            if self.fail {
                return Err(io::ErrorKind::StorageFull.into());
            }
        }
        file.sync_data()
    }
}

fn first_append(dir: &Path, strict: bool) -> Log {
    let mut log = Log::open(dir, LogConfig {
        flush_on_append: strict,
        retention: None,
        ..tiny_segments()
    })
    .unwrap();
    let mut first = sample_batch(1);
    first.producer_id = 41;
    first.producer_epoch = 0;
    first.base_sequence = 0;
    log.append(&mut first).unwrap();
    // Disk gates must not occupy the process-wide pool shared by other tests.
    log.rollover_flusher.executor =
        Some(super::executor::Executor::new(2, 256, 64 * 1024 * 1024).unwrap());
    log
}

fn install_gate(log: &mut Log, fail: bool) -> Gate {
    let gate = GatedIo::gate(fail);
    log.test_set_io(Arc::<GatedIo>::clone(&gate.io));
    gate
}

fn await_progress<T>(receiver: &mpsc::Receiver<T>) -> Result<T, mpsc::RecvTimeoutError> {
    receiver.recv_timeout(Duration::from_secs(5))
}

fn observe_pending<T>(receiver: &mpsc::Receiver<T>) -> Result<T, mpsc::RecvTimeoutError> {
    receiver.recv_timeout(Duration::from_millis(100))
}

/// A thread that writes to a log and hands the log back, and the channel that
/// carries what the write returned.
struct SpawnedWriter<T> {
    writer: JoinHandle<Log>,
    received: mpsc::Receiver<T>,
}

fn spawn_writer<T: Send + 'static>(
    mut log: Log,
    write: impl FnOnce(&mut Log) -> T + Send + 'static,
) -> SpawnedWriter<T> {
    let (sent, received) = mpsc::channel();
    let writer = thread::spawn(move || {
        sent.send(write(&mut log)).unwrap();
        log
    });
    SpawnedWriter { writer, received }
}

fn partition_with_executor(executor: impl FnOnce() -> Arc<super::executor::Executor>) -> TempLog {
    let directory = tempdir().unwrap();
    let mut log = first_append(directory.path(), false);
    log.rollover_flusher.executor = Some(executor());
    TempLog {
        dir: directory,
        log,
    }
}

fn check_flushed_boundary(log: &Log, directory: &Path) {
    assert!(log.log_end_offset() == Offset(2));
    assert!(name::producer_snapshot_path(directory, 1).exists());
}

/// The gate of a rollover that has started, and whether its flush job began.
struct GatedRollover {
    gate: Arc<GatedIo>,
    began: Result<ThreadId, mpsc::RecvTimeoutError>,
}

fn begin_gated_rollover(log: &mut Log) -> GatedRollover {
    let Gate { io: gate, started } = install_gate(log, false);
    log.append(&mut sample_batch(1)).unwrap();
    let began = await_progress(&started);
    GatedRollover { gate, began }
}

fn join_blocked_writer<T, U>(
    gate: &GatedIo,
    writer: JoinHandle<T>,
    began: Result<ThreadId, mpsc::RecvTimeoutError>,
    pending: &Result<U, mpsc::RecvTimeoutError>,
) -> T {
    gate.open_gate();
    let log = writer.join().unwrap();
    began.unwrap();
    assert!(matches!(pending, Err(mpsc::RecvTimeoutError::Timeout)));
    log
}

#[test]
fn buffered_rollovers_allow_appends_reads_and_idle_maintenance_during_disk_sync() {
    let dir = tempdir().unwrap();
    let mut log = first_append(dir.path(), false);
    let first_state = log.producer_state_snapshot();
    let Gate { io: gate, started } = install_gate(&mut log, false);
    let (tx, rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        for sequence in [1, 2] {
            let mut batch = sample_batch(1);
            batch.producer_id = 41;
            batch.producer_epoch = 0;
            batch.base_sequence = sequence;
            log.append(&mut batch).unwrap();
        }
        log.tick(SystemTime::now(), Offset(3)).unwrap();
        assert!(log.read(Offset(0), bytes(4096)).unwrap().batches.len() == 3);
        tx.send(log).unwrap();
    });
    let began = await_progress(&started);
    let returned = rx.recv_timeout(Duration::from_secs(2));
    // Always release before asserting, so the synchronous negative control
    // fails without leaving a worker or Drop waiting forever.
    let unpublished = !name::producer_snapshot_path(dir.path(), 1).exists();
    let untierable = returned
        .as_ref()
        .is_ok_and(|log| log.tierable_segments().is_empty());
    gate.open_gate();
    writer.join().unwrap();
    began.unwrap();
    let mut log = returned.expect("buffered roll must not wait for disk sync");
    assert!(unpublished, "the snapshot must wait for its record flush");
    assert!(untierable, "tiering must wait for the boundary snapshot");
    log.sync().unwrap();
    check_boundary_snapshot(&log, dir.path(), &first_state);
    assert!(
        log.producer_state_entry(ProducerId(41))
            .unwrap()
            .last_sequence
            == 2
    );
    assert!(log.tierable_segments().len() == 2);
    drop(log);
    let recovered = open_log(dir.path());
    assert!(recovered.log_end_offset() == Offset(3));
    assert!(
        recovered
            .producer_state_entry(ProducerId(41))
            .unwrap()
            .last_sequence
            == 2
    );
}

#[test]
fn stamped_appends_wait_for_earlier_rollovers_in_both_append_paths() {
    for verbatim in [false, true] {
        let dir = tempdir().unwrap();
        let mut log = first_append(dir.path(), false);
        let GatedRollover { gate, began } = begin_gated_rollover(&mut log);
        install_stamps(&mut log, 100, 1);
        let SpawnedWriter {
            writer,
            received: rx,
        } = spawn_writer(log, move |log| {
            if verbatim {
                let EncodedVerbatim { batch, .. } =
                    verbatim_from(&sample_batch(1), krabka_ids::LeaderEpoch(1));
                log.append_verbatim(&batch).unwrap();
            } else {
                log.append(&mut sample_batch(1)).unwrap();
            }
            log.stamp_for_offset(Offset(2))
        });
        let pending = observe_pending(&rx);
        let log = join_blocked_writer(&gate, writer, began, &pending);
        assert!(rx.recv().unwrap() == Some(100));
        assert!(name::producer_snapshot_path(dir.path(), 1).exists());
        assert!(log.log_end_offset() == Offset(3));
    }
}

#[test]
fn slow_disk_bounds_the_rollover_queue_without_losing_boundary_state() {
    for (max_jobs, budget) in [
        (256, 3 * producer_snapshot::allocation_size(1).unwrap()),
        (3, 64 * 1024 * 1024),
    ] {
        let dir = tempdir().unwrap();
        let mut log = first_append(dir.path(), false);
        // Bound job counts and bytes independently, including the running
        // snapshot: only two more rolls may be queued.
        log.rollover_flusher.executor =
            Some(super::executor::Executor::new(2, max_jobs, budget).unwrap());
        let GatedRollover { gate, began } = begin_gated_rollover(&mut log);
        let (tx, rx) = mpsc::channel();
        let writer = thread::spawn(move || {
            for offset in 2..=4 {
                log.append(&mut sample_batch(1)).unwrap();
                tx.send(offset).unwrap();
            }
            log
        });
        let queued: Result<Vec<_>, _> = iter::repeat_with(|| await_progress(&rx)).take(2).collect();
        let pending = observe_pending(&rx);
        gate.open_gate();
        let mut log = writer.join().unwrap();
        began.unwrap();
        assert!(queued.unwrap() == (2..=3).collect::<Vec<_>>());
        assert!(matches!(pending, Err(mpsc::RecvTimeoutError::Timeout)));
        assert!(rx.recv().unwrap() == 4);
        log.sync().unwrap();
        assert!(log.log_end_offset() == Offset(5));
        assert!(producer_snapshot::list(dir.path()).unwrap().len() == 4);
        assert!(log.tierable_segments().len() == 4);
    }
}

#[test]
fn strict_rollover_waits_for_disk_before_acknowledging() {
    let dir = tempdir().unwrap();
    let mut log = first_append(dir.path(), true);
    let Gate { io: gate, started } = install_gate(&mut log, false);
    let (tx, rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        log.append(&mut sample_batch(1)).unwrap();
        tx.send(log).unwrap();
    });
    let began = await_progress(&started);
    let pending = rx.try_recv();
    gate.open_gate();
    writer.join().unwrap();
    began.unwrap();
    assert!(matches!(pending, Err(mpsc::TryRecvError::Empty)));
    let log = rx.recv().unwrap();
    check_flushed_boundary(&log, dir.path());
}

#[test]
fn failed_background_flush_is_reported_by_sync_and_both_append_paths() {
    let dir = tempdir().unwrap();
    let mut log = first_append(dir.path(), false);
    let Gate { io: gate, started } = install_gate(&mut log, true);
    log.append(&mut sample_batch(1)).unwrap();
    let began = await_progress(&started);
    log.append(&mut sample_batch(1)).unwrap();
    gate.open_gate();
    began.unwrap();
    assert!(matches!(log.sync(), Err(LogError::Io(e)) if e.kind() == io::ErrorKind::StorageFull));
    assert!(!name::producer_snapshot_path(dir.path(), 1).exists());
    assert!(!name::producer_snapshot_path(dir.path(), 2).exists());
    assert!(matches!(
        log.append(&mut sample_batch(1)),
        Err(LogError::Io(_))
    ));
    let EncodedVerbatim { batch, .. } = verbatim_from(&sample_batch(1), krabka_ids::LeaderEpoch(1));
    assert!(matches!(log.append_verbatim(&batch), Err(LogError::Io(_))));
}

#[test]
fn reset_and_truncation_wait_and_cannot_recreate_stale_snapshots() {
    for reset in [false, true] {
        let dir = tempdir().unwrap();
        let mut log = first_append(dir.path(), false);
        let GatedRollover { gate, began } = begin_gated_rollover(&mut log);
        let SpawnedWriter {
            writer,
            received: rx,
        } = spawn_writer(log, move |log| {
            if reset {
                log.reset_to(Offset(0))
            } else {
                log.truncate_to(Offset(0))
            }
        });
        let pending = observe_pending(&rx);
        let log = join_blocked_writer(&gate, writer, began, &pending);
        rx.recv().unwrap().unwrap();
        assert!(log.log_end_offset() == Offset(0));
        assert!(producer_snapshot::list(dir.path()).unwrap().is_empty());
        drop(log);
        let recovered = open_log(dir.path());
        assert!(recovered.producer_state_snapshot().is_empty());
    }
}

#[test]
fn many_partitions_share_a_fixed_worker_pool() {
    let shared = super::executor::Executor::shared().unwrap();
    for _ in 0..24 {
        let mut flusher = super::Flusher::default();
        let permit = flusher.reserve(10).unwrap();
        assert!(Arc::ptr_eq(flusher.executor.as_ref().unwrap(), &shared));
        drop(permit);
    }
    // Isolate disk gates from other tests, using the same executor constructor.
    let executor = super::executor::Executor::new(4, 256, 64 * 1024 * 1024).unwrap();
    let mut partitions = Vec::new();
    for _ in 0..24 {
        let TempLog { dir, mut log } = partition_with_executor(|| Arc::clone(&executor));
        let Gate { io: gate, started } = install_gate(&mut log, false);
        log.append(&mut sample_batch(1)).unwrap();
        partitions.push((dir, log, gate, started));
    }
    let started: Vec<_> = partitions[..4]
        .iter()
        .map(|(_, _, _, started)| await_progress(started))
        .collect();
    let pending: Vec<_> = partitions[4..]
        .iter()
        .map(|(_, _, _, started)| started.recv_timeout(Duration::from_millis(10)))
        .collect();
    for (_, _, gate, _) in &partitions {
        gate.open_gate();
    }
    let mut threads = HashSet::new();
    for thread in started {
        threads.insert(thread.unwrap());
    }
    assert!(
        pending
            .iter()
            .all(|result| matches!(result, Err(mpsc::RecvTimeoutError::Timeout)))
    );
    for (_, log, _, _) in &mut partitions {
        log.sync().unwrap();
    }
    for (_, _, _, started) in &partitions[4..] {
        threads.insert(started.recv().unwrap());
    }
    assert!(
        threads.len() == 4,
        "all 24 partitions must use the same four native workers"
    );
    for (dir, log, _, _) in &partitions {
        assert!(name::producer_snapshot_path(dir.path(), 1).exists());
        assert!(log.tierable_segments().len() == 1);
    }
}

#[test]
fn snapshot_byte_backpressure_is_shared_between_partitions_and_admits_oversized_jobs_exclusively() {
    for budget in [producer_snapshot::allocation_size(1).unwrap(), 1] {
        let executor = super::executor::Executor::new(2, 256, budget).unwrap();
        let TempLog {
            dir: _first_dir,
            log: mut first,
        } = partition_with_executor(|| Arc::clone(&executor));
        let TempLog {
            dir: second_dir,
            log: mut second,
        } = partition_with_executor(|| executor);
        let Gate { io: gate, started } = install_gate(&mut first, false);
        first.append(&mut sample_batch(1)).unwrap();
        let began = await_progress(&started);
        let (tx, rx) = mpsc::channel();
        let (preparing, prepared) = mpsc::channel();
        let writer = thread::spawn(move || {
            producer_snapshot::test_observe_prepare(preparing);
            second.append(&mut sample_batch(1)).unwrap();
            tx.send(()).unwrap();
            second
        });
        let pending = observe_pending(&rx);
        let not_prepared = observe_pending(&prepared);
        let uncaptured = !name::producer_snapshot_path(second_dir.path(), 1).exists();
        gate.open_gate();
        began.unwrap();
        let mut second = writer.join().unwrap();
        assert!(matches!(pending, Err(mpsc::RecvTimeoutError::Timeout)));
        assert!(uncaptured);
        assert!(
            matches!(not_prepared, Err(mpsc::RecvTimeoutError::Timeout)),
            "backpressure must apply before allocation"
        );
        prepared.recv().unwrap();
        rx.recv().unwrap();
        first.sync().unwrap();
        second.sync().unwrap();
        assert!(first.tierable_segments().len() == 1);
        assert!(second.tierable_segments().len() == 1);
    }
}

#[test]
fn sealed_segment_flush_survives_each_post_roll_setup_failure() {
    for extension in ["log", "txnindex", "stampindex"] {
        let dir = tempdir().unwrap();
        let mut log = first_append(dir.path(), false);
        let first_state = log.producer_state_snapshot();
        if extension == "stampindex" {
            install_stamps(&mut log, 100, 1);
        }
        let blocked = dir.path().join(format!("{:020}.{extension}", 1));
        fs::create_dir(&blocked).unwrap();
        let result = log.append(&mut sample_batch(1));
        // Flush even when setup failed before there was a new active segment.
        log.rollover_flusher.finish().unwrap();
        assert!(matches!(result, Err(LogError::Io(_))), "{extension}");
        check_boundary_snapshot(&log, dir.path(), &first_state);
        if extension == "stampindex" {
            assert!(log.tierable_segments().len() == 1);
            fs::remove_dir(&blocked).unwrap();
            // A retry and another roll must not leave a gap in the archive.
            append_samples(&mut log, 2, 1);
            log.sync().unwrap();
            assert!(log.tierable_segments().len() == 2);
        }
    }
}

#[test]
fn synchronous_rollovers_do_not_clone_flush_handles_and_clone_failure_does_not_seal() {
    for strict in [false, true] {
        let dir = tempdir().unwrap();
        let mut log = first_append(dir.path(), strict);
        Segment::test_fail_flush_handles(true);
        let result = log.append(&mut sample_batch(1));
        Segment::test_fail_flush_handles(false);
        if strict {
            result.unwrap();
            check_flushed_boundary(&log, dir.path());
        } else {
            assert!(matches!(result, Err(LogError::Io(_))));
            assert!(!log.active.as_ref().unwrap().is_sealed());
            log.append(&mut sample_batch(1)).unwrap();
            log.sync().unwrap();
            assert!(log.log_end_offset() == Offset(2));
        }
    }
}

#[test]
fn a_busy_partition_yields_between_flushes_without_reordering_its_boundaries() {
    let executor = super::executor::Executor::new(1, 256, 64 * 1024 * 1024).unwrap();
    let TempLog {
        dir: first_dir,
        log: mut first,
    } = partition_with_executor(|| Arc::clone(&executor));
    let Gate {
        io: first_gate,
        started: first_started,
    } = install_gate(&mut first, false);
    first.append(&mut sample_batch(1)).unwrap();
    let first_began = await_progress(&first_started);
    first.append(&mut sample_batch(1)).unwrap();
    let TempLog {
        dir: _second_dir,
        log: mut second,
    } = partition_with_executor(|| executor);
    let Gate {
        io: second_gate,
        started: second_started,
    } = install_gate(&mut second, false);
    second.append(&mut sample_batch(1)).unwrap();
    first_gate.open_gate();
    let second_began = await_progress(&second_started);
    let first_boundary = name::producer_snapshot_path(first_dir.path(), 1).exists();
    let later_boundary = name::producer_snapshot_path(first_dir.path(), 2).exists();
    second_gate.open_gate();
    first_began.unwrap();
    second_began.unwrap();
    first.sync().unwrap();
    second.sync().unwrap();
    assert!(first_boundary);
    assert!(
        !later_boundary,
        "the second partition must run before the first partition's backlog"
    );
    assert!(first.tierable_segments().len() == 2);
    assert!(second.tierable_segments().len() == 1);
}

/// A compacted log of five keyed segments, with flushes on a private pool.
fn compacted_log(dir: &Path) -> Log {
    let mut log = Log::open(dir, LogConfig {
        flush_on_append: false,
        retention: None,
        ..compacting_segments()
    })
    .unwrap();
    append_keyed_samples(&mut log, 5, |i| format!("k{}", i % 2));
    log.rollover_flusher.executor =
        Some(super::executor::Executor::new(2, 256, 64 * 1024 * 1024).unwrap());
    log
}

/// A compaction pass is prepared and built while the flush of a segment it
/// consumes is still on the disk, and installed only once that flush ends:
/// the flush holds the segment's files and writes the snapshot at its end.
#[test]
fn compaction_builds_during_a_rollover_flush_and_installs_after_it() {
    let dir = tempdir().unwrap();
    let mut log = compacted_log(dir.path());
    let Gate { io: gate, started } = install_gate(&mut log, false);
    append_keyed_samples(&mut log, 1, |_| "k1".to_owned());
    let began = await_progress(&started);
    let (built_tx, built) = mpsc::channel();
    let SpawnedWriter {
        writer,
        received: installed,
    } = spawn_writer(log, move |log| {
        let job = log.prepare_compaction(&compaction_ctx()).unwrap().unwrap();
        let pass = job.build();
        built_tx.send(()).unwrap();
        log.finish_compaction(pass)
    });

    let built_meanwhile = await_progress(&built);
    let pending = observe_pending(&installed);
    let log = join_blocked_writer(&gate, writer, began, &pending);

    assert!(built_meanwhile.is_ok());
    assert!(matches!(
        installed.recv().unwrap(),
        Ok(CompactionOutcome::Installed)
    ));
    assert!(log.log_end_offset() == Offset(6));
}

/// A ticket covers the flushes submitted before it, not those after.
#[test]
fn a_ticket_waits_only_for_the_flushes_before_it() {
    let dir = tempdir().unwrap();
    let mut log = first_append(dir.path(), false);
    let before = log.rollover_flusher.ticket();
    let GatedRollover { gate, began } = begin_gated_rollover(&mut log);
    let after = log.rollover_flusher.ticket();
    let wait_for = |ticket| {
        let (tx, waited) = mpsc::channel();
        let shared = Arc::clone(&log.rollover_flusher.shared);
        thread::spawn(move || tx.send(shared.finish_through(ticket)).unwrap());
        waited
    };
    let (earlier, later) = (wait_for(before), wait_for(after));

    let unblocked = await_progress(&earlier);
    let pending = observe_pending(&later);
    gate.open_gate();

    began.unwrap();
    assert!(matches!(unblocked, Ok(Ok(()))));
    assert!(matches!(pending, Err(mpsc::RecvTimeoutError::Timeout)));
    assert!(matches!(await_progress(&later), Ok(Ok(()))));
}

/// Performs every `sync_data` and counts the ones on the `.log` file, and
/// records the `(offset, len)` of every range dropped from the page cache
/// instead of dropping it.
#[derive(Debug, Default)]
struct CountingIo {
    syncs: AtomicUsize,
    drops: Mutex<Vec<(u64, u64)>>,
}

impl LogIo for CountingIo {
    fn write_back(&self, file: &File, offset: u64, len: u64) -> io::Result<()> {
        assert!(
            offset % 4096 == 0 && len % 4096 == 0,
            "a write-out range covers whole pages"
        );
        self.syncs.fetch_add(1, Ordering::SeqCst);
        file.sync_data()
    }

    fn drop_cached(&self, _file: &File, offset: u64, len: u64) {
        self.drops.lock().unwrap().push((offset, len));
    }
}

/// A log with `window` as its writeback window, `budget` as its page-cache
/// budget and a private flush pool, after `appends` appends of one 1 KiB
/// record each.
fn appended_through_page_cache(
    dir: &Path,
    window: u32,
    budget: Option<u32>,
    appends: usize,
) -> (Log, Arc<CountingIo>) {
    appended_with(
        dir,
        LogConfig {
            writeback_window: bytes(window),
            page_cache_budget: budget.map(bytes),
            retention: None,
            ..LogConfig::default()
        },
        appends,
    )
}

/// A log opened with `config`, a counting I/O and a private flush pool,
/// after `appends` appends of one 1 KiB record each.
fn appended_with(dir: &Path, config: LogConfig, appends: usize) -> (Log, Arc<CountingIo>) {
    let mut log = Log::open(dir, config).unwrap();
    let io = Arc::new(CountingIo::default());
    log.test_set_io(Arc::<CountingIo>::clone(&io));
    log.rollover_flusher.executor =
        Some(super::executor::Executor::new(2, 256, 64 * 1024 * 1024).unwrap());
    for _ in 0..appends {
        log.append(&mut kib_batch()).unwrap();
    }
    (log, io)
}

/// One batch of one 1 KiB record, the unit the pacing tests append.
fn kib_batch() -> RecordBatch {
    RecordBatch {
        records: vec![Record {
            value: Some(Bytes::from(vec![7u8; 1024])),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Through the page cache, every `writeback_window` appended bytes start one
/// background sync of the active segment, and nothing else syncs it.
#[test]
fn appends_through_the_page_cache_start_a_sync_every_writeback_window() {
    let dir = tempdir().unwrap();
    // 32 KiB of records through a 4 KiB window: at least 7 windows filled.
    let (log, io) = appended_through_page_cache(dir.path(), 4096, None, 32);
    log.rollover_flusher.finish().unwrap();
    let syncs = io.syncs.load(Ordering::SeqCst);
    assert!((7..=8).contains(&syncs), "{syncs} syncs");
    // Without a budget, no page is recorded or dropped.
    assert!(io.drops.lock().unwrap().is_empty());
    let ledger = log.rollover_flusher.executor.as_ref().unwrap().page_cache();
    assert!(ledger.recorded_bytes() == 0);
}

/// A window of zero leaves write-back to the kernel.
#[test]
fn a_zero_writeback_window_starts_no_sync() {
    let dir = tempdir().unwrap();
    let (log, io) = appended_through_page_cache(dir.path(), 0, Some(4096), 32);
    log.rollover_flusher.finish().unwrap();
    assert!(io.syncs.load(Ordering::SeqCst) == 0);
    assert!(io.drops.lock().unwrap().is_empty());
}

/// Every sync hands the whole pages it covered to the page-cache ledger,
/// and the ledger drops the oldest pages beyond the budget, from the start
/// of the segment, in order, and never a page the next append writes into.
#[test]
fn a_page_cache_budget_drops_the_oldest_synced_pages() {
    let dir = tempdir().unwrap();
    // 32 KiB of records through 4 KiB windows under an 8 KiB budget.
    let (log, io) = appended_through_page_cache(dir.path(), 4096, Some(8192), 32);
    log.rollover_flusher.finish().unwrap();
    let drops = io.drops.lock().unwrap().clone();
    let handed = log.writeback_handed;
    assert!(
        handed % 4096 == 0 && handed >= 28 * 1024,
        "{handed} bytes handed over"
    );
    let dropped: u64 = drops.iter().map(|(_, len)| len).sum();
    assert!(
        dropped == handed - 8192,
        "{dropped} of {handed} dropped: {drops:?}"
    );
    let mut next = 0;
    for (offset, len) in drops {
        assert!(
            offset == next && len % 4096 == 0,
            "{offset}+{len} after {next}"
        );
        next = offset + len;
    }
    let ledger = log.rollover_flusher.executor.as_ref().unwrap().page_cache();
    assert!(ledger.recorded_bytes() == 8192);
}

/// An explicit sync hands the whole pages written since the last window to
/// the ledger before it syncs everything, so a flush between windows leaves
/// no page outside the budget's accounting, and the frontier stays on a
/// page boundary.
#[test]
fn an_explicit_sync_hands_its_pages_to_the_ledger() {
    let dir = tempdir().unwrap();
    let (mut log, io) = appended_through_page_cache(dir.path(), 1 << 20, Some(1 << 20), 10);
    assert!(log.writeback_handed == 0);
    let before = io.syncs.load(Ordering::SeqCst);

    log.sync().unwrap();
    log.rollover_flusher.finish().unwrap();

    let active = log.active.as_ref().unwrap();
    let size = active.size().bytes_u64();
    assert!(
        log.writeback_handed == size & !4095 && log.writeback_handed > 0,
        "{} handed of {size}",
        log.writeback_handed
    );
    assert!(io.syncs.load(Ordering::SeqCst) == before + 1);
    let ledger = log.rollover_flusher.executor.as_ref().unwrap().page_cache();
    assert!(ledger.recorded_bytes() == log.writeback_handed);
}

/// Cutting the log in place or replacing its active segment restarts the
/// pacing from the segment that is active then: the next window's pages are
/// written out, instead of waiting until the new segment grows past the old
/// frontier.
#[test]
fn pacing_restarts_from_a_cut_or_reset_active_segment() {
    let dir = tempdir().unwrap();
    let (mut log, io) = appended_through_page_cache(dir.path(), 4096, None, 64);
    assert!(log.writeback_handed >= 32 * 1024);

    log.truncate_to(Offset(0)).unwrap();
    assert!(log.writeback_handed == 0);
    let before = io.syncs.load(Ordering::SeqCst);
    for _ in 0..8 {
        log.append(&mut kib_batch()).unwrap();
    }
    log.rollover_flusher.finish().unwrap();
    assert!(
        io.syncs.load(Ordering::SeqCst) > before,
        "a window after the cut is written out"
    );

    log.reset_to(Offset(1_000)).unwrap();
    assert!(log.writeback_handed == 0);
    let before = io.syncs.load(Ordering::SeqCst);
    for _ in 0..8 {
        log.append(&mut kib_batch()).unwrap();
    }
    log.rollover_flusher.finish().unwrap();
    assert!(
        io.syncs.load(Ordering::SeqCst) > before,
        "a window after the reset is written out"
    );
}

/// The drops recorded so far, cleared for the next step.
fn take_drops(io: &CountingIo) -> Vec<(u64, u64)> { mem::take(&mut *io.drops.lock().unwrap()) }

/// 64 appends of one 1 KiB record through 4 KiB windows under an 8 KiB
/// budget, synced: every handed-over page but the newest two has been
/// dropped, and the drops so far are cleared.
fn cold_log(dir: &Path) -> (Log, Arc<CountingIo>) {
    let (log, io) = appended_through_page_cache(dir, 4096, Some(8192), 64);
    log.rollover_flusher.finish().unwrap();
    take_drops(&io);
    (log, io)
}

/// A fetch of bytes the budget has dropped drops the pages behind it, where
/// its reader's previous fetch was, and a byte-copy fetch its own pages too,
/// so a consumer reading an old backlog leaves no page cache in its wake. A
/// fetch of the newest bytes, which the ledger holds, drops nothing.
#[test]
fn a_cold_fetch_drops_the_pages_behind_it() {
    let dir = tempdir().unwrap();
    let (log, io) = cold_log(dir.path());
    let active = log.active.as_ref().unwrap();
    let cold_before = log
        .rollover_flusher
        .page_cache_retained_from(active.log_file())
        .unwrap();
    assert!(
        cold_before == log.writeback_handed - 8192,
        "{cold_before} cold"
    );

    // Every batch is the same length, so the batch at offset `n` starts at
    // `n * batch_len`.
    let first = log.read_raw(Offset(0), Offset(64), bytes(4096)).unwrap();
    let batches = first.last_offset.unwrap().0 - first.start_offset.0 + 1;
    let batch_len = u64::try_from(first.total).unwrap() / u64::try_from(batches).unwrap();
    // The oldest records are cold; nothing is behind them, so the byte-copy
    // read drops only its own pages, from the start of the file.
    assert!(take_drops(&io) == vec![(0, batch_len * u64::try_from(batches).unwrap())]);

    // Further on, the read drops the pages back to twice its size behind it,
    // from a page boundary, through its own.
    let read = log.read_raw(Offset(16), Offset(64), bytes(4096)).unwrap();
    let start = 16 * batch_len;
    let end = start + u64::try_from(read.total).unwrap();
    assert!(end <= cold_before, "{end} is not cold before {cold_before}");
    let from = (start - 8192) & !4095;
    assert!(take_drops(&io) == vec![(from, end - from)]);

    // The newest records are in the ledger: nothing is dropped for them.
    let tail = 60 * batch_len;
    assert!(tail >= cold_before, "{tail} is cold before {cold_before}");
    log.read_raw(Offset(60), Offset(64), bytes(4096)).unwrap();
    assert!(take_drops(&io).is_empty());
}

/// The `sendfile` fetch sends its bytes after it is described, so it drops
/// only the pages behind a cold region, up to the page the region starts in.
#[krabka_macros::sendfile_cfg]
#[test]
fn a_cold_sendfile_fetch_drops_the_pages_behind_its_region() {
    let dir = tempdir().unwrap();
    let (log, io) = cold_log(dir.path());
    let desc = log
        .read_raw_desc(Offset(16), Offset(64), bytes(4096))
        .unwrap();
    let region = desc.regions.first().expect("a cached run is described");
    let from = (region.offset - 8192) & !4095;
    let to = region.offset & !4095;
    assert!(take_drops(&io) == vec![(from, to - from)]);
    // The first region of the file has nothing behind it.
    log.read_raw_desc(Offset(0), Offset(64), bytes(4096))
        .unwrap();
    assert!(take_drops(&io).is_empty());
}

/// Without a budget no fetch is cold, and a fetch of the active segment
/// before any of its pages was handed over is not either.
#[test]
fn fetches_without_a_budget_or_before_a_handover_drop_nothing() {
    let dir = tempdir().unwrap();
    let (log, io) = appended_through_page_cache(dir.path(), 4096, None, 64);
    log.rollover_flusher.finish().unwrap();
    log.read_raw(Offset(0), Offset(64), bytes(4096)).unwrap();
    assert!(take_drops(&io).is_empty());

    let dir = tempdir().unwrap();
    // A 1 MiB window: 64 KiB of appends hand nothing over.
    let (log, io) = appended_through_page_cache(dir.path(), 1 << 20, Some(8192), 64);
    log.rollover_flusher.finish().unwrap();
    assert!(log.writeback_handed == 0);
    log.read_raw(Offset(0), Offset(64), bytes(4096)).unwrap();
    assert!(take_drops(&io).is_empty());
}

/// A sealed segment with no page left in the ledger is cold throughout: a
/// fetch of it drops its pages even where nothing behind it was handed over.
#[test]
fn a_fetch_of_a_sealed_segment_the_ledger_forgot_is_cold() {
    let dir = tempdir().unwrap();
    // 8 KiB segments: the 64 appends roll seven times or more.
    let (log, io) = appended_with(
        dir.path(),
        LogConfig {
            writeback_window: bytes(4096),
            page_cache_budget: Some(bytes(8192)),
            segment_size: bytes(8 * 1024),
            retention: None,
            ..LogConfig::default()
        },
        64,
    );
    log.rollover_flusher.finish().unwrap();
    assert!(
        log.segments.len() >= 4,
        "{} sealed segments",
        log.segments.len()
    );
    let oldest = &log.segments[0];
    assert!(
        log.rollover_flusher
            .page_cache_retained_from(oldest.log_file())
            .is_none()
    );
    take_drops(&io);
    let read = log.read_raw(Offset(0), Offset(1), bytes(4096)).unwrap();
    assert!(take_drops(&io) == vec![(0, u64::try_from(read.total).unwrap())]);
}
