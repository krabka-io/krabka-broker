//! What a reopened log knows about the segments it did not write.

use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use assert2::check;
use bytes::Bytes;
use krabka_ids::Offset;
use krabka_log::{CleanupPolicy, CompactionContext, Log, LogConfig};
use krabka_protocol::records::{Record, RecordBatch};
use krabka_units::prelude::{TimeExt as _, bytes, days, gibibytes, hours};
use tempfile::tempdir;

fn epoch_millis(at: SystemTime) -> i64 {
    i64::try_from(
        at.duration_since(UNIX_EPOCH)
            .expect("test clock is after the epoch")
            .as_millis(),
    )
    .expect("epoch millis fit in i64")
}

/// A one-record batch under `key`, stamped at `ts`.
fn keyed_batch_at(key: &str, ts: i64) -> RecordBatch {
    RecordBatch {
        base_timestamp: ts,
        max_timestamp: ts,
        last_offset_delta: 0,
        records: vec![Record {
            offset_delta: 0,
            key: Some(Bytes::from(key.to_owned())),
            value: Some(Bytes::from(vec![b'v'; 96])),
            ..Record::default()
        }],
        ..RecordBatch::default()
    }
}

/// Compaction rewrites sealed segments, and the rewritten segment has the
/// same recovery gap: its tail scan starts at the newest index entry, so a
/// maximum recorded earlier in the segment is lost and retention deletes a
/// segment full of fresh records.
#[test]
fn a_compacted_segment_keeps_the_maximum_of_the_records_it_kept() {
    let dir = tempdir().unwrap();
    let now = SystemTime::now();
    let now_ms = epoch_millis(now);
    let stale_ms = now_ms - hours(2).millis_i64();

    let config = LogConfig {
        cleanup_policy: CleanupPolicy::Compact,
        // One batch per segment, so compaction has sealed segments to merge.
        segment_size: bytes(1),
        // Index every batch: the rewritten segment's unindexed tail is its
        // last batch alone, and that batch is not the newest one.
        index_interval: bytes(1),
        retention: Some(hours(1)),
        ..LogConfig::default()
    };
    let mut log = Log::open(dir.path(), config).unwrap();
    for (key, ts) in [
        ("k0", now_ms),
        ("k1", stale_ms),
        ("k2", stale_ms),
        ("k3", now_ms),
    ] {
        log.append(&mut keyed_batch_at(key, ts)).unwrap();
    }

    log.compact(&CompactionContext {
        now,
        active_producers: HashMap::new(),
    })
    .unwrap();
    check!(log.tierable_segments().len() == 1);

    log.tick(now + Duration::from_secs(1)).unwrap();

    check!(log.log_start_offset() == Offset(0));
    check!(log.tierable_segments().len() == 1);
}

/// A two-record batch stamped at `ts`, sized so a 256-byte segment holds one.
fn batch_at(ts: i64) -> RecordBatch {
    let mut batch = RecordBatch {
        base_timestamp: ts,
        max_timestamp: ts,
        last_offset_delta: 1,
        ..RecordBatch::default()
    };
    for delta in 0..2 {
        batch.records.push(Record {
            offset_delta: delta,
            key: Some(Bytes::from(format!("k{delta}"))),
            value: Some(Bytes::from(vec![b'v'; 96])),
            ..Record::default()
        });
    }
    batch
}

/// Retention must not mistake a reopened sealed segment for an ancient one.
///
/// `Segment::open` does no scan, so a sealed segment arrives with no
/// timestamp of its own. If the log leaves it that way, every sealed segment
/// reports a timestamp below any retention cutoff, and the first `tick` after
/// a restart deletes all of them. Only the "never drop the last segment"
/// guard keeps the partition from emptying. The records here are seconds old
/// against a seven-day window, so nothing may be evicted.
#[test]
fn retention_after_a_restart_keeps_segments_inside_the_window() {
    let dir = tempdir().unwrap();
    let config = LogConfig {
        // Small enough that each append rolls a new segment.
        segment_size: bytes(256),
        retention: Some(days(7)),
        ..LogConfig::default()
    };

    let now = SystemTime::now();
    let now_ms = epoch_millis(now);
    {
        let mut log = Log::open(dir.path(), config.clone()).unwrap();
        for i in 0..5 {
            log.append(&mut batch_at(now_ms - i64::from(i))).unwrap();
        }
        check!(log.log_end_offset() == Offset(10));
        log.sync().unwrap();
    }

    let mut log = Log::open(dir.path(), config).unwrap();
    let sealed_before = log.tierable_segments().len();
    check!(
        sealed_before >= 2,
        "the fixture needs several sealed segments, got {sealed_before}"
    );

    log.tick(now + Duration::from_secs(1)).unwrap();

    check!(log.log_start_offset() == Offset(0));
    check!(log.tierable_segments().len() == sealed_before);
    let read = log.read(Offset(0), gibibytes(1)).unwrap();
    check!(read.start_offset == Offset(0));
    check!(read.batches.len() == 5);
}

/// The other half of the restore: a maximum that sits *below* the newest
/// index entry.
///
/// With a dense index the walk over the unindexed tail sees only the last
/// batch, so a restore built on that walk alone reports the last batch's
/// timestamp. The newest time-index entry carries the running maximum as of
/// the batch it indexes, which is where the real maximum comes from.
#[test]
fn a_reopened_segment_keeps_a_maximum_that_predates_its_newest_batch() {
    let dir = tempdir().unwrap();
    let config = LogConfig {
        // Three batches per sealed segment.
        segment_size: bytes(600),
        // Index every batch, so the unindexed tail is the last batch alone.
        index_interval: bytes(1),
        retention: Some(hours(1)),
        ..LogConfig::default()
    };

    let now = SystemTime::now();
    let now_ms = epoch_millis(now);
    let stale_ms = now_ms - hours(2).millis_i64();
    {
        let mut log = Log::open(dir.path(), config.clone()).unwrap();
        // The newest record of the first segment is the oldest one in it.
        for ts in [now_ms, stale_ms, stale_ms, now_ms, now_ms] {
            log.append(&mut batch_at(ts)).unwrap();
        }
        check!(log.tierable_segments().len() == 1);
        log.sync().unwrap();
    }

    let mut log = Log::open(dir.path(), config).unwrap();
    log.tick(now + Duration::from_secs(1)).unwrap();

    check!(log.log_start_offset() == Offset(0));
    check!(log.tierable_segments().len() == 1);
}

/// The same restore has to survive a log whose sparse index is coarser than
/// its segments: the newest batches sit past the last index entry, so a
/// restore that trusts only the time index reports a stale maximum.
#[test]
fn a_reopened_segment_reports_the_timestamp_of_its_newest_batch() {
    let dir = tempdir().unwrap();
    let config = LogConfig {
        segment_size: bytes(4096),
        // One index entry per segment: every batch after the first lands past
        // the last entry the time index holds.
        index_interval: bytes(1 << 20),
        retention: Some(days(7)),
        ..LogConfig::default()
    };

    let now = SystemTime::now();
    let now_ms = epoch_millis(now);
    // The oldest batch of the first segment is well outside the window; the
    // newest batch of that same segment is inside it. Retention must read the
    // newest one and keep the segment.
    let stale_ms = now_ms - days(30).millis_i64();
    {
        let mut log = Log::open(dir.path(), config.clone()).unwrap();
        log.append(&mut batch_at(stale_ms)).unwrap();
        log.append(&mut batch_at(now_ms)).unwrap();
        // Fill past `segment_size` so the pair above ends up sealed together.
        for _ in 0..24 {
            log.append(&mut batch_at(now_ms)).unwrap();
        }
        log.sync().unwrap();
    }

    let mut log = Log::open(dir.path(), config).unwrap();
    let sealed_before = log.tierable_segments().len();
    check!(sealed_before >= 1);

    log.tick(now + Duration::from_secs(1)).unwrap();

    check!(log.log_start_offset() == Offset(0));
    check!(log.tierable_segments().len() == sealed_before);
}
