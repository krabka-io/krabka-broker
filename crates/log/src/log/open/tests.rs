//! Unit tests for opening a log directory and for the producer,
//! transaction, and snapshot state that recovery rebuilds from a
//! partially written tail.

use assert2::{assert, check};
use krabka_ids::LeaderEpoch;
use krabka_units::prelude::bytes;
use tempfile::tempdir;

use super::*;
use crate::log::test_support::{
    NO_LIMIT, commit_marker, sample_batch, sample_batch_with_epoch, transactional_batch,
};

#[test]
fn open_empty_dir_creates_first_segment() {
    let dir = tempdir().unwrap();
    let log = Log::open(dir.path(), LogConfig::default()).unwrap();
    assert2::assert!(log.log_start_offset() == Offset(0));
    assert2::assert!(log.log_end_offset() == Offset(0));
    log.close();
}

#[test]
fn open_creates_log_file() {
    let dir = tempdir().unwrap();
    let log = Log::open(dir.path(), LogConfig::default()).unwrap();
    drop(log);
    let log_path = dir.path().join("00000000000000000000.log");
    assert2::assert!(log_path.exists());
}

#[test]
fn open_rejects_a_negative_segment_base() {
    let dir = tempdir().unwrap();
    std::fs::File::create(name::log_path(dir.path(), -1)).unwrap();

    assert2::assert!(matches!(
        Log::open(dir.path(), LogConfig::default()),
        Err(LogError::Corrupt(message)) if message.contains("segment bases")
    ));
}

#[test]
fn open_recovers_partial_trailing_batch() {
    let dir = tempdir().unwrap();
    {
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b1 = sample_batch(3);
        let mut b2 = sample_batch(2);
        log.append(&mut b1).unwrap();
        log.append(&mut b2).unwrap();
    }
    // Append 10 bytes of garbage to the .log file.
    let log_path = dir.path().join("00000000000000000000.log");
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&log_path)
        .unwrap();
    std::io::Write::write_all(&mut f, &[0xAB; 10]).unwrap();
    f.sync_data().unwrap();
    drop(f);
    let log = Log::open(dir.path(), LogConfig::default()).unwrap();
    assert2::assert!(log.log_end_offset() == 5);
}

#[test]
fn open_truncates_epoch_checkpoint_to_recovered_leo() {
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("00000000000000000000.log");
    let first_batch_len = {
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut first = sample_batch_with_epoch(1, 1);
        log.append(&mut first).unwrap();
        let first_batch_len = log.read_raw(Offset(0), Offset(1), NO_LIMIT).unwrap().total;
        let mut torn = sample_batch_with_epoch(1, 7);
        log.append(&mut torn).unwrap();
        assert!(log.epoch_checkpoint().latest_epoch() == Some(LeaderEpoch(7)));
        first_batch_len
    };

    std::fs::OpenOptions::new()
        .write(true)
        .open(&log_path)
        .unwrap()
        .set_len(u64::try_from(first_batch_len + 5).unwrap())
        .unwrap();

    let cfg = LogConfig {
        validate_on_open: true,
        ..LogConfig::default()
    };
    let reopened = Log::open(dir.path(), cfg).unwrap();

    assert!(reopened.log_end_offset() == Offset(1));
    assert!(
        reopened
            .epoch_checkpoint()
            .entries()
            .iter()
            .all(|entry| entry.start_offset < reopened.log_end_offset())
    );
    assert!(reopened.epoch_checkpoint().latest_epoch() == Some(LeaderEpoch(1)));
}

#[test]
fn producer_snapshot_survives_local_segment_deletion_and_restart() {
    let dir = tempdir().unwrap();
    let config = LogConfig {
        segment_size: bytes(1),
        ..LogConfig::default()
    };
    let mut log = Log::open(dir.path(), config.clone()).unwrap();
    let mut producer = sample_batch(2);
    producer.producer_id = 42;
    producer.producer_epoch = 3;
    producer.base_sequence = 7;
    producer.max_timestamp = 1_234;
    log.append(&mut producer).unwrap();

    // The second append rolls the producer batch into a sealed segment and
    // writes the snapshot at the new segment's base offset.
    log.append(&mut sample_batch(1)).unwrap();
    let export = log.tierable_segments().into_iter().next().unwrap();
    check!(export.producer_snapshot_path.exists());
    let local_start = export.last_offset + 1;
    check!(log.delete_local_segments_through(local_start).unwrap() == 1);
    drop(log);

    let reopened = Log::open(dir.path(), config).unwrap();
    let entry = reopened
        .producer_state_snapshot()
        .into_iter()
        .find(|entry| entry.producer_id == 42)
        .unwrap();
    check!(entry.producer_epoch == 3);
    check!(entry.last_sequence == 8);
    check!(entry.last_offset == 1);
    check!(entry.offset_delta == 1);
    check!(entry.timestamp == 1_234);
}

#[test]
fn snapshot_recovery_preserves_open_and_completed_transaction_fields() {
    let dir = tempdir().unwrap();
    let config = LogConfig {
        segment_size: bytes(1),
        ..LogConfig::default()
    };
    let mut log = Log::open(dir.path(), config.clone()).unwrap();
    let mut data = transactional_batch(77, 4, &["a", "b"]);
    data.base_sequence = 10;
    log.append(&mut data).unwrap();
    log.append(&mut sample_batch(1)).unwrap();

    let open = log
        .producer_state_snapshot()
        .into_iter()
        .find(|entry| entry.producer_id == 77)
        .unwrap();
    check!(open.current_txn_first_offset == Some(Offset(0)));

    log.append(&mut commit_marker(77, 4)).unwrap();
    // Roll once more so the newest snapshot, rather than the replay tail,
    // is the only durable source of the completed coordinator epoch.
    log.append(&mut sample_batch(1)).unwrap();
    drop(log);
    let reopened = Log::open(dir.path(), config).unwrap();
    let completed = reopened
        .producer_state_snapshot()
        .into_iter()
        .find(|entry| entry.producer_id == 77)
        .unwrap();
    check!(completed.last_sequence == 11);
    check!(completed.current_txn_first_offset == None);
    check!(completed.coordinator_epoch == 17);
    check!(reopened.producer_transaction_state(ProducerId(77)) == (17, None));
}

#[test]
fn recovery_recreates_missing_producer_snapshot_at_segment_boundary() {
    let dir = tempdir().unwrap();
    let config = LogConfig {
        segment_size: bytes(1),
        ..LogConfig::default()
    };
    {
        let mut log = Log::open(dir.path(), config.clone()).unwrap();
        for (base_sequence, timestamp) in [(0, 10), (2, 20), (4, 30)] {
            let mut batch = sample_batch(2);
            batch.producer_id = 42;
            batch.producer_epoch = 3;
            batch.base_sequence = base_sequence;
            batch.max_timestamp = timestamp;
            log.append(&mut batch).unwrap();
        }
    }

    let missing = producer_snapshot::path(dir.path(), Offset(4));
    check!(missing.exists());
    std::fs::remove_file(&missing).unwrap();

    let reopened = Log::open(dir.path(), config).unwrap();
    check!(missing.exists());
    let (_, boundary_state) = producer_snapshot::latest_at_or_before(dir.path(), Offset(4))
        .unwrap()
        .unwrap();
    let boundary = boundary_state.get(&ProducerId(42)).unwrap();
    check!(boundary.last_sequence == 3);
    check!(boundary.last_offset == Offset(3));
    let recovered = reopened
        .producer_state_snapshot()
        .into_iter()
        .find(|entry| entry.producer_id == 42)
        .unwrap();
    check!(recovered.last_sequence == 5);
    check!(recovered.last_offset == Offset(5));
}

#[test]
fn producer_tail_accepts_zero_and_rejects_negative_identity_fields() {
    assert2::assert!(
        Log::data_producer_tail(ProducerId(0), 0, 1, Offset(10)).unwrap() == Some((1, Offset(11)))
    );
    assert2::assert!(
        Log::data_producer_tail(ProducerId(1), 5, 1, Offset(10)).unwrap() == Some((6, Offset(11)))
    );
    assert2::assert!(Log::data_producer_tail(ProducerId(-2), 0, 0, Offset(10)).unwrap() == None);
    assert2::assert!(Log::data_producer_tail(ProducerId(1), -2, 0, Offset(10)).unwrap() == None);
}

#[test]
fn producer_tail_wraps_sequence_at_signed_maximum() {
    assert2::assert!(
        Log::data_producer_tail(ProducerId(1), i32::MAX - 1, 2, Offset(10)).unwrap()
            == Some((0, Offset(12)))
    );
}

#[test]
fn recovered_batch_offsets_require_checked_progress() {
    let mut batch = sample_batch(3);
    batch.base_offset = 10;
    assert2::assert!(
        Log::recovered_batch_offsets(Offset(10), Offset(13), &batch).unwrap()
            == (Offset(12), Offset(13))
    );

    batch.last_offset_delta = -1;
    assert2::assert!(matches!(
        Log::recovered_batch_offsets(Offset(10), Offset(20), &batch),
        Err(LogError::Corrupt(message)) if message.contains("rejected batch")
    ));

    batch.base_offset = i64::MAX;
    batch.last_offset_delta = 1;
    assert2::assert!(matches!(
        Log::recovered_batch_offsets(Offset(i64::MAX - 2), Offset(i64::MAX), &batch),
        Err(LogError::Corrupt(message)) if message.contains("rejected batch")
    ));
}

#[test]
fn producer_sequence_rollover_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut batch = sample_batch(3);
        batch.producer_id = 1;
        batch.producer_epoch = 0;
        batch.base_sequence = i32::MAX - 1;
        log.append(&mut batch).unwrap();

        let entry = log.producer_state_snapshot().into_iter().next().unwrap();
        assert2::assert!(entry.last_sequence == 0);
        assert2::assert!(entry.last_offset == Offset(2));
        assert2::assert!(entry.offset_delta == 2);
    }

    let reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
    let entry = reopened
        .producer_state_snapshot()
        .into_iter()
        .next()
        .unwrap();
    assert2::assert!(entry.last_sequence == 0);
    assert2::assert!(entry.last_offset == Offset(2));
    assert2::assert!(entry.offset_delta == 2);
}

#[test]
fn higher_epoch_control_marker_clears_data_batch_metadata() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut data = transactional_batch(88, 4, &["a", "b"]);
        data.base_sequence = 10;
        log.append(&mut data).unwrap();
        log.append(&mut commit_marker(88, 5)).unwrap();

        let entry = log
            .producer_state_snapshot()
            .into_iter()
            .find(|entry| entry.producer_id == 88)
            .unwrap();
        check!(entry.producer_epoch == 5);
        check!(entry.last_sequence == -1);
        check!(entry.last_offset == Offset(-1));
        check!(entry.offset_delta == 0);
        check!(entry.current_txn_first_offset == None);
        check!(entry.coordinator_epoch == 17);
    }

    let reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
    let entry = reopened
        .producer_state_snapshot()
        .into_iter()
        .find(|entry| entry.producer_id == 88)
        .unwrap();
    check!(entry.producer_epoch == 5);
    check!(entry.last_sequence == -1);
    check!(entry.last_offset == Offset(-1));
    check!(entry.offset_delta == 0);
    check!(entry.current_txn_first_offset == None);
    check!(entry.coordinator_epoch == 17);
}

#[test]
fn zero_producer_id_and_only_transactional_ranges_survive_recovery() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();

        let mut zero_pid = transactional_batch(0, 3, &["a", "b"]);
        zero_pid.base_sequence = 5;
        log.append(&mut zero_pid).unwrap();

        let mut ordinary = sample_batch(1);
        ordinary.producer_id = 1;
        ordinary.producer_epoch = 0;
        ordinary.base_sequence = 7;
        log.append(&mut ordinary).unwrap();

        let mut negative_pid = transactional_batch(-2, 0, &["ignored"]);
        negative_pid.base_sequence = 0;
        log.append(&mut negative_pid).unwrap();

        let state = log.producer_state_snapshot();
        let zero = state.iter().find(|entry| entry.producer_id == 0).unwrap();
        assert2::assert!(zero.last_sequence == 6);
        assert2::assert!(zero.current_txn_first_offset == Some(Offset(0)));
    }

    let reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
    let state = reopened.producer_state_snapshot();
    let zero = state.iter().find(|entry| entry.producer_id == 0).unwrap();
    assert2::assert!(zero.last_sequence == 6);
    assert2::assert!(zero.current_txn_first_offset == Some(Offset(0)));
    assert2::assert!(reopened.lso() == Offset(0));
    assert2::assert!(reopened.pending_stamp_ranges.len() == 1);
    assert2::assert!(
        reopened.pending_stamp_ranges.get(&ProducerId(0)) == Some(&vec![(Offset(0), Offset(1))])
    );
}

#[test]
fn reopen_rebuilds_pending_transactions_and_lso() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&mut transactional_batch(1000, 4, &["a", "b"]))
            .unwrap();
        assert2::assert!(log.pending_transaction_start(ProducerId(1000)) == Some(Offset(0)));
        assert2::assert!(log.lso() == Offset(0));
    }

    let mut reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
    assert2::assert!(reopened.pending_transaction_start(ProducerId(1000)) == Some(Offset(0)));
    assert2::assert!(reopened.lso() == Offset(0));
    reopened
        .set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(30, 1),
        ))
        .unwrap();

    reopened.append(&mut commit_marker(1000, 4)).unwrap();
    check!(reopened.stamp_for_offset(Offset(0)) == Some(30));
    check!(reopened.stamp_for_offset(Offset(1)) == Some(30));
    check!(reopened.stamp_for_offset(Offset(2)) == None);
    assert2::assert!(reopened.producer_transaction_state(ProducerId(1000)) == (17, None));
    assert2::assert!(
        reopened
            .pending_transaction_start(ProducerId(1000))
            .is_none()
    );
    assert2::assert!(reopened.lso() == reopened.log_end_offset());

    reopened
        .append(&mut transactional_batch(1000, 4, &["next"]))
        .unwrap();
    assert2::assert!(
        reopened.producer_transaction_state(ProducerId(1000)) == (17, Some(Offset(3)))
    );
    drop(reopened);

    let recovered_again = Log::open(dir.path(), LogConfig::default()).unwrap();
    assert2::assert!(
        recovered_again.producer_transaction_state(ProducerId(1000)) == (17, Some(Offset(3)))
    );
}

#[test]
fn reopen_does_not_treat_non_transactional_producer_data_as_pending() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut batch = sample_batch(1);
        batch.producer_id = 42;
        log.append(&mut batch).unwrap();
    }

    let reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
    check!(reopened.pending_transaction_start(ProducerId(42)) == None);
    check!(reopened.lso() == reopened.log_end_offset());
}

/// On reopen, each recovered sealed segment's `last_offset` is set to
/// `next_base - 1` (line: `seg.seal_at(Offset(base_offsets[i + 1] - 1))`).
/// Multi-record segments give non-consecutive bases so the `- 1` is
/// observable: for consecutive exports `last_offset + 1 == next_base`.
/// Mutating `- 1`→`+ 1` sets `last_offset = next_base + 1` (so
/// `last_offset + 1 == next_base + 2`); mutating `- 1`→`/ 1` sets
/// `last_offset = next_base` (so `last_offset + 1 == next_base + 1`).
#[test]
fn reopen_seals_recovered_segments_at_next_base_minus_one() {
    use tempfile::TempDir;
    let dir = TempDir::new().unwrap();
    let cfg = LogConfig {
        segment_size: bytes(1), // roll on every append
        ..LogConfig::default()
    };
    {
        let mut log = Log::open(dir.path(), cfg.clone()).unwrap();
        // Multi-record batches → segment bases are 0, 2, 4, ... (each
        // sealed segment spans two offsets), so next_base - base == 2.
        for _ in 0..4 {
            log.append(&mut sample_batch(2)).unwrap();
        }
        assert2::assert!(log.segments.len() >= 2);
    }
    // Reopen: sealed segments recovered via no-scan open + seal_at(next-1).
    let reopened = Log::open(dir.path(), cfg).unwrap();
    let exports = reopened.tierable_segments();
    assert2::assert!(exports.len() >= 2);
    for pair in exports.windows(2) {
        // last_offset must be exactly one below the next segment's base.
        assert2::assert!(pair[0].last_offset + 1 == pair[1].base_offset);
    }
}

#[test]
fn open_restores_a_log_start_trimmed_inside_the_active_segment() {
    let dir = tempdir().unwrap();
    {
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&mut sample_batch(5)).unwrap();
        // One segment holds every record, so no segment name witnesses the
        // trim: only the checkpoint can carry it across the reopen.
        log.set_log_start_offset(Offset(3)).unwrap();
        log.sync().unwrap();
    }

    let log = Log::open(dir.path(), LogConfig::default()).unwrap();

    assert!(log.log_start_offset() == Offset(3));
    assert!(log.log_end_offset() == Offset(5));
}

#[test]
fn open_rewrites_a_checkpoint_past_the_log_end_so_appends_cannot_revive_it() {
    // The hazard: a checkpoint above the log end is inert against the log that
    // reads it, but appends move the log end. Left on disk, it comes back in
    // range on a later open and hides records appended after it was written.
    let dir = tempdir().unwrap();
    {
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.set_log_start_offset(Offset(7)).unwrap();
        log.sync().unwrap();
    }

    // Reopen empty: 7 is past the log end, so it resolves to the log end.
    {
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert!(log.log_start_offset() == Offset(0));
        // Records the stale checkpoint would have hidden.
        log.append(&mut sample_batch(9)).unwrap();
        log.sync().unwrap();
    }

    let log = Log::open(dir.path(), LogConfig::default()).unwrap();

    assert!(log.log_start_offset() == Offset(0));
    assert!(log.log_end_offset() == Offset(9));
}

#[test]
fn open_resolves_a_checkpoint_against_what_the_log_holds() {
    // Reopening caps the checkpoint at the log end and leaves it alone below
    // the derived start.
    //
    // Past the log end, every record still present is below a start that was
    // already acknowledged -- they are all trimmed -- so the log start is the
    // log end and the log reads empty. A crash between a trim and the fsync of
    // the records it trimmed past arrives there.
    //
    // Below the derived start the checkpoint stands, because KIP-405 gives a
    // tiered partition a global floor that belongs under its oldest local
    // segment: raising it to meet the surviving files would hide the band the
    // archive serves. `local_log_start_offset` is the floor that follows the
    // files, and it is what a local read is measured against either way.
    enum Case {
        BelowDerivedStart,
        PastLogEnd,
    }

    for (case, expected_start, expected_local_start) in [
        (Case::BelowDerivedStart, Offset(1), Offset(2)),
        (Case::PastLogEnd, Offset(3), Offset(3)),
    ] {
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open(
                dir.path(),
                LogConfig {
                    segment_size: bytes(1),
                    ..LogConfig::default()
                },
            )
            .unwrap();
            log.append(&mut sample_batch(1)).unwrap();
            log.append(&mut sample_batch(1)).unwrap();
            log.append(&mut sample_batch(1)).unwrap();
            // Drops the sealed segments below offset 2, so the derived start
            // is 2 and the log end is 3.
            log.trim_to_offset(Offset(2)).unwrap();
            log.sync().unwrap();
        }
        let checkpointed = match case {
            Case::BelowDerivedStart => Offset(1),
            Case::PastLogEnd => Offset(9),
        };
        crate::log_start_offset_checkpoint::write(&crate::io::FileIo, dir.path(), checkpointed)
            .unwrap();

        let log = Log::open(dir.path(), LogConfig::default()).unwrap();

        assert!(log.log_start_offset() == expected_start);
        assert!(log.local_log_start_offset() == expected_local_start);
        // Whatever it resolved to is what is now on disk, so the next open
        // reads a value that already agrees with the log.
        drop(log);
        assert!(
            crate::log_start_offset_checkpoint::read(dir.path()).unwrap() == Some(expected_start)
        );
    }
}

#[test]
fn reset_to_drops_the_checkpoint_so_a_reopen_starts_at_the_new_base() {
    let dir = tempdir().unwrap();
    {
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&mut sample_batch(5)).unwrap();
        log.set_log_start_offset(Offset(3)).unwrap();
        log.reset_to(Offset(100)).unwrap();
        log.sync().unwrap();
        assert!(!name::log_start_offset_checkpoint_path(dir.path()).exists());
    }

    let log = Log::open(dir.path(), LogConfig::default()).unwrap();

    assert!(log.log_start_offset() == Offset(100));
    assert!(log.log_end_offset() == Offset(100));
}
