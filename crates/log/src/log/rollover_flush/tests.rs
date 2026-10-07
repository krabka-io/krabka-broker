//! Exercise real appends against a disk-sync gate, not a timing-only workload.

use std::{
    collections::HashSet,
    fs::File,
    io,
    sync::{Arc, Condvar, Mutex, mpsc},
    time::Duration,
};

use assert2::assert;
use krabka_ids::{Offset, ProducerId};
use krabka_units::bytes;
use tempfile::tempdir;

use crate::{
    Log, LogConfig, LogError, LogIo,
    log::test_support::{sample_batch, tiny_segments, verbatim_from},
    name, producer_snapshot,
};

fn check_boundary_snapshot(
    log: &Log,
    directory: &std::path::Path,
    expected: &[crate::producer_snapshot::ProducerSnapshotEntry],
) {
    let copy = tempdir().unwrap();
    std::fs::copy(
        name::producer_snapshot_path(directory, 1),
        name::producer_snapshot_path(copy.path(), 1),
    )
    .unwrap();
    let (_, state) = producer_snapshot::reload(copy.path(), log.producer_reload_range(Offset(1)))
        .unwrap()
        .unwrap();
    assert!(state.into_values().collect::<Vec<_>>() == expected);
}

#[derive(Debug)]
struct GatedIo {
    started: Mutex<Option<mpsc::Sender<std::thread::ThreadId>>>,
    released: Mutex<bool>,
    changed: Condvar,
    fail: bool,
}

impl GatedIo {
    fn new(fail: bool) -> (Arc<Self>, mpsc::Receiver<std::thread::ThreadId>) {
        let (tx, rx) = mpsc::channel();
        (
            Arc::new(Self {
                started: Mutex::new(Some(tx)),
                released: Mutex::new(false),
                changed: Condvar::new(),
                fail,
            }),
            rx,
        )
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

impl LogIo for GatedIo {
    fn sync_data(&self, file: &File) -> io::Result<()> {
        let started = self.started.lock().unwrap().take();
        if let Some(started) = started {
            let _ = started.send(std::thread::current().id());
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.changed.wait(released).unwrap();
            }
            if self.fail {
                return Err(io::ErrorKind::StorageFull.into());
            }
        }
        file.sync_data()
    }
}

fn first_append(dir: &std::path::Path, strict: bool) -> Log {
    let mut log = Log::open(
        dir,
        LogConfig {
            flush_on_append: strict,
            retention: None,
            ..tiny_segments()
        },
    )
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

#[test]
fn buffered_rollovers_allow_appends_reads_and_idle_maintenance_during_disk_sync() {
    let dir = tempdir().unwrap();
    let mut log = first_append(dir.path(), false);
    let first_state = log.producer_state_snapshot();
    let (gate, started) = GatedIo::new(false);
    log.test_set_io(gate.clone());
    let (tx, rx) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        for sequence in [1, 2] {
            let mut batch = sample_batch(1);
            batch.producer_id = 41;
            batch.producer_epoch = 0;
            batch.base_sequence = sequence;
            log.append(&mut batch).unwrap();
        }
        log.tick(std::time::SystemTime::now(), Offset(3)).unwrap();
        assert!(log.read(Offset(0), bytes(4096)).unwrap().batches.len() == 3);
        tx.send(log).unwrap();
    });
    let began = started.recv_timeout(Duration::from_secs(5));
    let returned = rx.recv_timeout(Duration::from_secs(2));
    // Always release before asserting, so the synchronous negative control
    // fails without leaving a worker or Drop waiting forever.
    let unpublished = !name::producer_snapshot_path(dir.path(), 1).exists();
    let untierable = returned
        .as_ref()
        .is_ok_and(|log| log.tierable_segments().is_empty());
    gate.release();
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
    let recovered = Log::open(dir.path(), LogConfig::default()).unwrap();
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
        let (gate, started) = GatedIo::new(false);
        log.test_set_io(gate.clone());
        log.append(&mut sample_batch(1)).unwrap();
        let began = started.recv_timeout(Duration::from_secs(5));
        log.set_stamp_source(Arc::new(crate::stamp_source::MonotonicStampSource::new(
            100, 1,
        )))
        .unwrap();
        let (tx, rx) = mpsc::channel();
        let writer = std::thread::spawn(move || {
            if verbatim {
                let (_, batch) = verbatim_from(&sample_batch(1), krabka_ids::LeaderEpoch(1));
                log.append_verbatim(&batch).unwrap();
            } else {
                log.append(&mut sample_batch(1)).unwrap();
            }
            tx.send(log.stamp_for_offset(Offset(2))).unwrap();
            log
        });
        let pending = rx.recv_timeout(Duration::from_millis(100));
        gate.release();
        let log = writer.join().unwrap();
        began.unwrap();
        assert!(matches!(pending, Err(mpsc::RecvTimeoutError::Timeout)));
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
        let (gate, started) = GatedIo::new(false);
        log.test_set_io(gate.clone());
        log.append(&mut sample_batch(1)).unwrap();
        let began = started.recv_timeout(Duration::from_secs(5));
        let (tx, rx) = mpsc::channel();
        let writer = std::thread::spawn(move || {
            for offset in 2..=4 {
                log.append(&mut sample_batch(1)).unwrap();
                tx.send(offset).unwrap();
            }
            log
        });
        let queued: Vec<_> = (2..=3)
            .map(|_| rx.recv_timeout(Duration::from_secs(5)))
            .collect();
        let pending = rx.recv_timeout(Duration::from_millis(100));
        gate.release();
        let mut log = writer.join().unwrap();
        began.unwrap();
        assert!(
            queued.into_iter().collect::<Result<Vec<_>, _>>().unwrap()
                == (2..=3).collect::<Vec<_>>()
        );
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
    let (gate, started) = GatedIo::new(false);
    log.test_set_io(gate.clone());
    let (tx, rx) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        log.append(&mut sample_batch(1)).unwrap();
        tx.send(log).unwrap();
    });
    let began = started.recv_timeout(Duration::from_secs(5));
    let pending = rx.try_recv();
    gate.release();
    writer.join().unwrap();
    began.unwrap();
    assert!(matches!(pending, Err(mpsc::TryRecvError::Empty)));
    let log = rx.recv().unwrap();
    assert!(log.log_end_offset() == Offset(2));
    assert!(name::producer_snapshot_path(dir.path(), 1).exists());
}

#[test]
fn failed_background_flush_is_reported_by_sync_and_both_append_paths() {
    let dir = tempdir().unwrap();
    let mut log = first_append(dir.path(), false);
    let (gate, started) = GatedIo::new(true);
    log.test_set_io(gate.clone());
    log.append(&mut sample_batch(1)).unwrap();
    let began = started.recv_timeout(Duration::from_secs(5));
    log.append(&mut sample_batch(1)).unwrap();
    gate.release();
    began.unwrap();
    assert!(matches!(log.sync(), Err(LogError::Io(e)) if e.kind() == io::ErrorKind::StorageFull));
    assert!(!name::producer_snapshot_path(dir.path(), 1).exists());
    assert!(!name::producer_snapshot_path(dir.path(), 2).exists());
    assert!(matches!(
        log.append(&mut sample_batch(1)),
        Err(LogError::Io(_))
    ));
    let (_, batch) = verbatim_from(&sample_batch(1), krabka_ids::LeaderEpoch(1));
    assert!(matches!(log.append_verbatim(&batch), Err(LogError::Io(_))));
}

#[test]
fn reset_and_truncation_wait_and_cannot_recreate_stale_snapshots() {
    for reset in [false, true] {
        let dir = tempdir().unwrap();
        let mut log = first_append(dir.path(), false);
        let (gate, started) = GatedIo::new(false);
        log.test_set_io(gate.clone());
        log.append(&mut sample_batch(1)).unwrap();
        let began = started.recv_timeout(Duration::from_secs(5));
        let (tx, rx) = mpsc::channel();
        let writer = std::thread::spawn(move || {
            let result = if reset {
                log.reset_to(Offset(0))
            } else {
                log.truncate_to(Offset(0))
            };
            tx.send(result).unwrap();
            log
        });
        let pending = rx.recv_timeout(Duration::from_millis(100));
        gate.release();
        let log = writer.join().unwrap();
        began.unwrap();
        assert!(matches!(pending, Err(mpsc::RecvTimeoutError::Timeout)));
        rx.recv().unwrap().unwrap();
        assert!(log.log_end_offset() == Offset(0));
        assert!(producer_snapshot::list(dir.path()).unwrap().is_empty());
        drop(log);
        let recovered = Log::open(dir.path(), LogConfig::default()).unwrap();
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
        let dir = tempdir().unwrap();
        let mut log = first_append(dir.path(), false);
        log.rollover_flusher.executor = Some(executor.clone());
        let (gate, started) = GatedIo::new(false);
        log.test_set_io(gate.clone());
        log.append(&mut sample_batch(1)).unwrap();
        partitions.push((dir, log, gate, started));
    }
    let started: Vec<_> = partitions[..4]
        .iter()
        .map(|(_, _, _, started)| started.recv_timeout(Duration::from_secs(5)))
        .collect();
    let pending: Vec<_> = partitions[4..]
        .iter()
        .map(|(_, _, _, started)| started.recv_timeout(Duration::from_millis(10)))
        .collect();
    for (_, _, gate, _) in &partitions {
        gate.release();
    }
    let mut threads: HashSet<_> = started.into_iter().map(Result::unwrap).collect();
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
        let first_dir = tempdir().unwrap();
        let mut first = first_append(first_dir.path(), false);
        first.rollover_flusher.executor = Some(executor.clone());
        let second_dir = tempdir().unwrap();
        let mut second = first_append(second_dir.path(), false);
        second.rollover_flusher.executor = Some(executor);
        let (gate, started) = GatedIo::new(false);
        first.test_set_io(gate.clone());
        first.append(&mut sample_batch(1)).unwrap();
        let began = started.recv_timeout(Duration::from_secs(5));
        let (tx, rx) = mpsc::channel();
        let (preparing, prepared) = mpsc::channel();
        let writer = std::thread::spawn(move || {
            producer_snapshot::test_observe_prepare(preparing);
            second.append(&mut sample_batch(1)).unwrap();
            tx.send(()).unwrap();
            second
        });
        let pending = rx.recv_timeout(Duration::from_millis(100));
        let not_prepared = prepared.recv_timeout(Duration::from_millis(100));
        let uncaptured = !name::producer_snapshot_path(second_dir.path(), 1).exists();
        gate.release();
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
            log.set_stamp_source(Arc::new(crate::stamp_source::MonotonicStampSource::new(
                100, 1,
            )))
            .unwrap();
        }
        let blocked = dir.path().join(format!("{:020}.{extension}", 1));
        std::fs::create_dir(&blocked).unwrap();
        let result = log.append(&mut sample_batch(1));
        // Flush even when setup failed before there was a new active segment.
        log.rollover_flusher.finish().unwrap();
        assert!(matches!(result, Err(LogError::Io(_))), "{extension}");
        check_boundary_snapshot(&log, dir.path(), &first_state);
        if extension == "stampindex" {
            assert!(log.tierable_segments().len() == 1);
            std::fs::remove_dir(&blocked).unwrap();
            // A retry and another roll must not leave a gap in the archive.
            log.append(&mut sample_batch(1)).unwrap();
            log.append(&mut sample_batch(1)).unwrap();
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
        crate::Segment::test_fail_flush_handles(true);
        let result = log.append(&mut sample_batch(1));
        crate::Segment::test_fail_flush_handles(false);
        if strict {
            result.unwrap();
            assert!(log.log_end_offset() == Offset(2));
            assert!(name::producer_snapshot_path(dir.path(), 1).exists());
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
    let first_dir = tempdir().unwrap();
    let mut first = first_append(first_dir.path(), false);
    first.rollover_flusher.executor = Some(executor.clone());
    let (first_gate, first_started) = GatedIo::new(false);
    first.test_set_io(first_gate.clone());
    first.append(&mut sample_batch(1)).unwrap();
    let first_began = first_started.recv_timeout(Duration::from_secs(5));
    first.append(&mut sample_batch(1)).unwrap();
    let second_dir = tempdir().unwrap();
    let mut second = first_append(second_dir.path(), false);
    second.rollover_flusher.executor = Some(executor);
    let (second_gate, second_started) = GatedIo::new(false);
    second.test_set_io(second_gate.clone());
    second.append(&mut sample_batch(1)).unwrap();
    first_gate.release();
    let second_began = second_started.recv_timeout(Duration::from_secs(5));
    let first_boundary = name::producer_snapshot_path(first_dir.path(), 1).exists();
    let later_boundary = name::producer_snapshot_path(first_dir.path(), 2).exists();
    second_gate.release();
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
