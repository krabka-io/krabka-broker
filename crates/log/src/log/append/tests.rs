//! Unit tests for the owned append paths: offset assignment, the
//! caller-supplied `append_at` offset, the leader-epoch transitions all
//! four append paths record, and the size-driven segment roll.

use assert2::{assert, check};
use krabka_units::prelude::bytes;
use tempfile::tempdir;

use super::*;
use crate::{
    config::LogConfig,
    io::LogIo,
    leader_epoch_checkpoint::EpochEntry,
    log::test_support::{
        abort_marker, sample_batch, sample_batch_with_epoch, test_batch_at, test_log,
        transactional_batch, verbatim_from,
    },
    stamp_index::{StampEntry, StampIndex},
};

#[derive(Debug)]
struct FailAfterBytes(std::sync::Mutex<usize>);

impl LogIo for FailAfterBytes {
    fn write(&self, file: &std::fs::File, buf: &[u8]) -> std::io::Result<usize> {
        use std::io::Write;

        let mut remaining = self.0.lock().unwrap();
        if *remaining == 0 {
            return Err(std::io::ErrorKind::StorageFull.into());
        }
        let written = (&*file).write(&buf[..buf.len().min(*remaining)])?;
        *remaining -= written;
        Ok(written)
    }
}

#[derive(Debug)]
struct FailFirstSync(std::sync::atomic::AtomicUsize);

impl LogIo for FailFirstSync {
    fn sync_data(&self, file: &std::fs::File) -> std::io::Result<()> {
        if self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
            Err(std::io::Error::other("injected sync_data failure"))
        } else {
            file.sync_data()
        }
    }
}

#[derive(Debug)]
struct CountSync(std::sync::atomic::AtomicUsize);

impl LogIo for CountSync {
    fn sync_data(&self, file: &std::fs::File) -> std::io::Result<()> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        file.sync_data()
    }
}

#[derive(Debug)]
struct FailSyncAt {
    calls: std::sync::atomic::AtomicUsize,
    fail_at: usize,
}

impl LogIo for FailSyncAt {
    fn sync_data(&self, file: &std::fs::File) -> std::io::Result<()> {
        let call = self
            .calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        if call == self.fail_at {
            Err(std::io::Error::other("injected ordered sync failure"))
        } else {
            file.sync_data()
        }
    }
}

#[test]
fn partial_write_rolls_back_cursor_and_recovers_pre_append_state() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    log.test_set_io(std::sync::Arc::new(FailAfterBytes(std::sync::Mutex::new(
        16,
    ))));

    let error = log.append(&mut sample_batch(2)).unwrap_err();

    assert!(
        matches!(error, LogError::Io(error) if error.kind() == std::io::ErrorKind::StorageFull)
    );
    assert!(log.log_end_offset() == Offset(0));
    assert!(log.producer_state_snapshot().is_empty());
    drop(log);
    let recovered = Log::open(dir.path(), LogConfig::default()).unwrap();
    assert!(recovered.log_end_offset() == Offset(0));
}

#[test]
fn shorter_append_after_partial_write_leaves_no_physical_tail() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    let mut first = sample_batch(1);
    log.append(&mut first).unwrap();
    let log_path = crate::name::log_path(dir.path(), 0);
    let position = std::fs::metadata(&log_path).unwrap().len();

    let mut next = sample_batch(1);
    let mut failed = sample_batch(10);
    let partial_len = next.encoded_len() + 1;
    assert!(partial_len < failed.encoded_len());
    log.test_set_io(std::sync::Arc::new(FailAfterBytes(std::sync::Mutex::new(
        partial_len,
    ))));

    let error = log.append(&mut failed).unwrap_err();

    assert!(
        matches!(error, LogError::Io(error) if error.kind() == std::io::ErrorKind::StorageFull)
    );
    assert!(std::fs::metadata(&log_path).unwrap().len() == position);

    log.test_set_io(std::sync::Arc::new(crate::io::FileIo));
    let next_len = next.encoded_len() as u64;
    log.append(&mut next).unwrap();

    assert!(std::fs::metadata(log_path).unwrap().len() == position + next_len);
    assert!(
        log.read(Offset(0), crate::log::test_support::NO_LIMIT)
            .unwrap()
            .batches
            == vec![first, next]
    );
}

#[test]
fn sync_failure_rolls_back_leo_producer_and_transaction_state() {
    let dir = tempdir().unwrap();
    let config = LogConfig {
        flush_on_append: true,
        ..LogConfig::default()
    };
    let mut log = Log::open(dir.path(), config.clone()).unwrap();
    let io = std::sync::Arc::new(FailFirstSync(std::sync::atomic::AtomicUsize::new(0)));
    log.test_set_io(io.clone());
    let producer = ProducerId(41);

    let error = log
        .append(&mut transactional_batch(producer.get(), 0, &["value"]))
        .unwrap_err();

    assert!(matches!(error, LogError::Io(_)));
    assert!(io.0.load(std::sync::atomic::Ordering::Relaxed) == 2);
    assert!(log.log_end_offset() == Offset(0));
    assert!(log.lso() == Offset(0));
    assert!(log.producer_state_snapshot().is_empty());
    assert!(log.pending_transaction_start(producer) == None);
    drop(log);
    let recovered = Log::open(dir.path(), config).unwrap();
    assert!(recovered.log_end_offset() == Offset(0));
    assert!(recovered.producer_state_snapshot().is_empty());
    assert!(recovered.pending_transaction_start(producer) == None);
}

/// A leader-epoch transition is recorded only when the epoch is known and
/// only when it advances past the one already recorded.
///
/// Four append paths carry the same two-part guard, and each half is silent
/// on its own: joined with `||` an unknown epoch gets recorded, and relaxed
/// from `>` to `>=` the same epoch is recorded twice. Neither shows up in
/// the appended data -- only in the epoch checkpoint.
#[test]
fn an_epoch_transition_is_recorded_once_and_only_when_it_advances() {
    // Offered in order. -1 is KIP-320's "unknown"; 5 repeats; 6 advances.
    const OFFERED: [i32; 5] = [-1, 5, 5, 6, -1];
    // Only the two advances belong in the checkpoint.
    const RECORDED: [i32; 2] = [5, 6];

    fn recorded(log: &Log) -> Vec<i32> {
        log.epoch_checkpoint
            .entries()
            .iter()
            .map(|e| e.epoch.0)
            .collect()
    }

    // `append`: the log assigns the base offset.
    {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        for epoch in OFFERED {
            let mut batch = sample_batch(1);
            batch.partition_leader_epoch = epoch;
            log.append(&mut batch).expect("append");
        }
        check!(recorded(&log) == RECORDED, "append: {:?}", recorded(&log));
    }

    // `append_at`: the caller supplies the offset.
    {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        for (i, epoch) in OFFERED.iter().enumerate() {
            let mut batch = sample_batch(1);
            batch.partition_leader_epoch = *epoch;
            log.append_at(&mut batch, Offset(i64::try_from(i).unwrap()))
                .expect("append_at");
        }
        check!(
            recorded(&log) == RECORDED,
            "append_at: {:?}",
            recorded(&log)
        );
    }

    // `append_verbatim`: producer-supplied bytes, log-assigned offset.
    {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        for epoch in OFFERED {
            let producer = test_batch_at(0);
            let (_wire, vb) = verbatim_from(&producer, LeaderEpoch(epoch));
            log.append_verbatim(&vb).expect("append_verbatim");
        }
        check!(
            recorded(&log) == RECORDED,
            "append_verbatim: {:?}",
            recorded(&log)
        );
    }

    // `append_verbatim_at`: producer-supplied bytes and offset.
    {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        for (i, epoch) in OFFERED.iter().enumerate() {
            let producer = test_batch_at(0);
            let (_wire, vb) = verbatim_from(&producer, LeaderEpoch(*epoch));
            log.append_verbatim_at(&vb, Offset(i64::try_from(i).unwrap()))
                .expect("append_verbatim_at");
        }
        check!(
            recorded(&log) == RECORDED,
            "append_verbatim_at: {:?}",
            recorded(&log)
        );
    }
}

#[test]
fn append_at_uses_reconciled_frontier_floor() {
    let (dir, mut log) = test_log();
    let mut prefix = test_batch_at(0);
    log.append(&mut prefix).unwrap();

    log.reconcile_next_offset(Offset(3));
    let mut gap_batch = test_batch_at(0);
    let mut rejected = gap_batch.clone();
    let err = log.append_at(&mut rejected, Offset(1)).unwrap_err();
    assert!(matches!(
        err,
        LogError::OffsetMismatch {
            expected: Offset(3),
            actual: Offset(1)
        }
    ));

    log.append_at(&mut gap_batch, Offset(3)).unwrap();
    assert!(log.log_end_offset() == Offset(4));
    drop(dir);
}

#[test]
fn assigned_append_uses_reconciled_frontier_floor() {
    let (_dir, mut log) = test_log();
    log.append(&mut test_batch_at(0)).unwrap();
    log.reconcile_next_offset(Offset(3));

    let mut batch = test_batch_at(0);
    let assigned = log.append(&mut batch).unwrap();

    assert!(assigned == Offset(3));
    assert!(batch.base_offset == 3);
    assert!(log.log_end_offset() == Offset(4));
}

#[test]
fn invalid_interval_is_write_free_and_a_corrected_retry_succeeds() {
    let (_dir, mut log) = test_log();
    let mut invalid = sample_batch(1);
    invalid.last_offset_delta = -1;

    let error = log.append(&mut invalid).unwrap_err();

    assert!(matches!(error, LogError::InvalidArgument(_)));
    assert!(log.log_end_offset() == Offset(0));
    assert!(log.lso() == Offset(0));
    assert!(log.producer_state_snapshot().is_empty());

    let mut retry = sample_batch(1);
    assert!(log.append(&mut retry).unwrap() == Offset(0));
    assert!(log.log_end_offset() == Offset(1));
}

#[test]
fn successor_overflow_is_rejected_before_log_mutation() {
    let (_dir, mut log) = test_log();
    log.reconcile_next_offset(Offset(i64::MAX));
    let mut batch = sample_batch(1);

    let error = log.append(&mut batch).unwrap_err();

    assert!(matches!(error, LogError::InvalidArgument(_)));
    assert!(log.log_end_offset() == Offset(0));
    assert!(log.lso() == Offset(0));
    assert!(log.producer_state_snapshot().is_empty());
}

#[test]
fn sidecar_failure_rolls_back_bytes_frontiers_and_allows_retry() {
    let (_dir, mut log) = test_log();
    log.set_stamp_source(std::sync::Arc::new(
        crate::stamp_source::MonotonicStampSource::new(100, 1),
    ))
    .unwrap();
    log.stamp_indexes.clear();

    let error = log.append(&mut sample_batch(1)).unwrap_err();

    assert!(matches!(error, LogError::Corrupt(_)));
    assert!(log.log_end_offset() == Offset(0));
    assert!(log.lso() == Offset(0));
    assert!(log.producer_state_snapshot().is_empty());
    assert!(log.active_txn_index.entries().is_empty());
    assert!(log.stamp_for_offset(Offset(0)).is_none());

    assert!(log.append(&mut sample_batch(1)).unwrap() == Offset(0));
    assert!(log.log_end_offset() == Offset(1));
    assert!(log.lso() == Offset(1));
    assert!(log.stamp_for_offset(Offset(0)).is_some());
}

#[test]
fn segment_bytes_are_synced_before_durable_sidecars() {
    let (_dir, mut stamped_log) = test_log();
    stamped_log
        .set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(100, 1),
        ))
        .unwrap();
    let stamp_syncs = std::sync::Arc::new(CountSync(std::sync::atomic::AtomicUsize::new(0)));
    stamped_log.test_set_io(stamp_syncs.clone());

    stamped_log.append(&mut sample_batch(1)).unwrap();
    assert!(stamp_syncs.0.load(std::sync::atomic::Ordering::Relaxed) == 1);

    let (_dir, mut transaction_log) = test_log();
    let txn_syncs = std::sync::Arc::new(CountSync(std::sync::atomic::AtomicUsize::new(0)));
    transaction_log.test_set_io(txn_syncs.clone());
    transaction_log
        .append(&mut transactional_batch(7, 0, &["value"]))
        .unwrap();
    assert!(txn_syncs.0.load(std::sync::atomic::Ordering::Relaxed) == 0);

    transaction_log.append(&mut abort_marker(7, 0)).unwrap();
    assert!(txn_syncs.0.load(std::sync::atomic::Ordering::Relaxed) == 1);
}

#[test]
fn post_roll_failure_restores_the_previous_active_segment() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(
        dir.path(),
        LogConfig {
            segment_size: bytes(1),
            ..LogConfig::default()
        },
    )
    .unwrap();
    log.append(&mut sample_batch(1)).unwrap();
    log.set_stamp_source(std::sync::Arc::new(
        crate::stamp_source::MonotonicStampSource::new(100, 1),
    ))
    .unwrap();
    let io = std::sync::Arc::new(FailSyncAt {
        calls: std::sync::atomic::AtomicUsize::new(0),
        fail_at: 2,
    });
    log.test_set_io(io.clone());

    let error = log.append(&mut sample_batch(1)).unwrap_err();

    assert!(matches!(error, LogError::Io(_)));
    assert!(io.calls.load(std::sync::atomic::Ordering::Relaxed) == 3);
    assert!(log.log_end_offset() == Offset(1));
    assert!(log.lso() == Offset(1));
    assert!(log.segments.is_empty());
    assert!(log.active.as_ref().unwrap().base_offset() == Offset(0));
    assert!(log.stamp_for_offset(Offset(1)).is_none());

    assert!(log.append(&mut sample_batch(1)).unwrap() == Offset(1));
    assert!(log.log_end_offset() == Offset(2));
}

#[test]
fn post_roll_partial_write_restores_the_previous_active_segment() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(
        dir.path(),
        LogConfig {
            segment_size: bytes(1),
            ..LogConfig::default()
        },
    )
    .unwrap();
    log.append(&mut sample_batch(1)).unwrap();
    log.test_set_io(std::sync::Arc::new(FailAfterBytes(std::sync::Mutex::new(
        16,
    ))));

    let error = log.append(&mut sample_batch(2)).unwrap_err();

    assert!(matches!(error, LogError::Io(_)));
    assert!(log.log_end_offset() == Offset(1));
    assert!(log.segments.is_empty());
    assert!(log.active.as_ref().unwrap().base_offset() == Offset(0));

    log.test_set_io(std::sync::Arc::new(crate::io::FileIo));
    assert!(log.append(&mut sample_batch(1)).unwrap() == Offset(1));
    assert!(log.log_end_offset() == Offset(2));
}

#[test]
fn append_assigns_monotonic_offsets() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    let mut b1 = sample_batch(3);
    let mut b2 = sample_batch(2);
    let first_offset = log.append(&mut b1).unwrap();
    let second_offset = log.append(&mut b2).unwrap();
    assert2::assert!(first_offset == Offset(0));
    assert2::assert!(second_offset == Offset(3));
    assert2::assert!(log.log_end_offset() == Offset(5));
}

#[test]
fn append_at_matching_offset_preserves_caller_offset() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    let mut b = sample_batch(3);
    // Pretend the caller (a replicator) already knows the leader's
    // assigned offset for this batch is 0.
    log.append_at(&mut b, Offset(0)).unwrap();
    assert2::assert!(b.base_offset == 0);
    assert2::assert!(log.log_end_offset() == Offset(3));

    let mut b2 = sample_batch(2);
    log.append_at(&mut b2, Offset(3)).unwrap();
    assert2::assert!(b2.base_offset == 3);
    assert2::assert!(log.log_end_offset() == Offset(5));
}

#[test]
fn append_at_with_mismatched_offset_errors() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    let mut b = sample_batch(2);
    let err = log.append_at(&mut b, Offset(7)).unwrap_err();
    assert2::assert!(matches!(
        err,
        LogError::OffsetMismatch {
            expected: Offset(0),
            actual: Offset(7)
        }
    ));
    // Failure must not advance the log.
    assert2::assert!(log.log_end_offset() == 0);
}

#[test]
fn segment_rolls_when_bytes_exceeded() {
    let dir = tempdir().unwrap();
    let config = LogConfig {
        segment_size: bytes(200), // tiny so we roll fast
        ..LogConfig::default()
    };
    let mut log = Log::open(dir.path(), config).unwrap();
    for _ in 0..5 {
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
    }
    // Multiple .log files should exist now.
    let log_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("log"))
        .collect();
    assert2::assert!(log_files.len() >= 2);
}

/// A roll to a new segment reopens the active `.stampindex` at the
/// sidecar of the new segment. The entry for the post-roll batch lands in
/// the new segment's file and does not leak back into the sealed
/// segment's file. This test guards the reopen. A reopen that did nothing
/// would keep the stamps in the sealed segment's index.
#[test]
fn roll_reopens_stampindex_for_new_segment() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(
        dir.path(),
        LogConfig {
            segment_size: bytes(1), // roll on every append after the first
            ..LogConfig::default()
        },
    )
    .unwrap();
    log.set_stamp_source(std::sync::Arc::new(
        crate::stamp_source::MonotonicStampSource::new(100, 5),
    ))
    .unwrap();

    log.append(&mut sample_batch(1)).unwrap(); // offset 0, segment 0, stamp 100
    log.append(&mut sample_batch(1)).unwrap(); // rolls: segment @ base 1, stamp 105

    // The new segment's own sidecar holds exactly its post-roll entry.
    let seg1 = StampIndex::open(dir.path().join("00000000000000000001.stampindex")).unwrap();
    assert2::assert!(
        seg1.entries()
            == [StampEntry {
                base_offset: Offset(1),
                last_offset: Offset(1),
                stamp: 105,
            }]
    );
    // The sealed segment kept only its pre-roll entry — nothing leaked back.
    let seg0 = StampIndex::open(dir.path().join("00000000000000000000.stampindex")).unwrap();
    assert2::assert!(
        seg0.entries()
            == [StampEntry {
                base_offset: Offset(0),
                last_offset: Offset(0),
                stamp: 100,
            }]
    );
}

#[test]
fn append_records_epoch_transition() {
    use tempfile::TempDir;
    let dir = TempDir::new().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    let mut b = sample_batch_with_epoch(3, 0);
    log.append(&mut b).unwrap();
    let mut b2 = sample_batch_with_epoch(2, 1); // 2 records at epoch 1
    log.append(&mut b2).unwrap();
    assert2::assert!(
        log.epoch_checkpoint().entries()
            == &[
                EpochEntry {
                    epoch: LeaderEpoch(0),
                    start_offset: Offset(0)
                },
                EpochEntry {
                    epoch: LeaderEpoch(1),
                    start_offset: Offset(3)
                }
            ]
    );
}
