//! Exercise real appends against a disk-sync gate, not a timing-only workload.

use std::{
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
    log::test_support::{sample_batch, verbatim_from},
    name, producer_snapshot,
};

#[derive(Debug)]
struct GatedIo {
    started: Mutex<Option<mpsc::Sender<()>>>,
    released: Mutex<bool>,
    changed: Condvar,
    fail: bool,
}

impl GatedIo {
    fn new(fail: bool) -> (Arc<Self>, mpsc::Receiver<()>) {
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
            let _ = started.send(());
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
            segment_size: bytes(1),
            flush_on_append: strict,
            retention: None,
            ..LogConfig::default()
        },
    )
    .unwrap();
    let mut first = sample_batch(1);
    first.producer_id = 41;
    first.producer_epoch = 0;
    first.base_sequence = 0;
    log.append(&mut first).unwrap();
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
    let copy = tempdir().unwrap();
    std::fs::copy(
        name::producer_snapshot_path(dir.path(), 1),
        name::producer_snapshot_path(copy.path(), 1),
    )
    .unwrap();
    let (_, state) = producer_snapshot::reload(copy.path(), log.producer_reload_range(Offset(1)))
        .unwrap()
        .unwrap();
    assert!(state.into_values().collect::<Vec<_>>() == first_state);
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
    let dir = tempdir().unwrap();
    let mut log = first_append(dir.path(), false);
    let (gate, started) = GatedIo::new(false);
    log.test_set_io(gate.clone());
    log.append(&mut sample_batch(1)).unwrap();
    let began = started.recv_timeout(Duration::from_secs(5));
    let (tx, rx) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        for offset in 2..=18 {
            log.append(&mut sample_batch(1)).unwrap();
            tx.send(offset).unwrap();
        }
        log
    });
    let queued: Vec<_> = (2..=17)
        .map(|_| rx.recv_timeout(Duration::from_secs(5)))
        .collect();
    let pending = rx.recv_timeout(Duration::from_millis(100));
    gate.release();
    let mut log = writer.join().unwrap();
    began.unwrap();
    assert!(
        queued.into_iter().collect::<Result<Vec<_>, _>>().unwrap() == (2..=17).collect::<Vec<_>>()
    );
    assert!(matches!(pending, Err(mpsc::RecvTimeoutError::Timeout)));
    assert!(rx.recv().unwrap() == 18);
    log.sync().unwrap();
    assert!(log.log_end_offset() == Offset(19));
    assert!(producer_snapshot::list(dir.path()).unwrap().len() == 18);
    assert!(log.tierable_segments().len() == 18);
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
    gate.release();
    began.unwrap();
    assert!(matches!(log.sync(), Err(LogError::Io(e)) if e.kind() == io::ErrorKind::StorageFull));
    assert!(!name::producer_snapshot_path(dir.path(), 1).exists());
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
