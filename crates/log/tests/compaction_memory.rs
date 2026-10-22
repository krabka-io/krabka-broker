//! Compaction's memory is bounded by a batch, not by what a group expands to.
//!
//! A rewrite group is bounded by `segment.bytes` on disk, and a compressed
//! batch can expand to hundreds of times its stored size. A pass that decoded
//! a whole segment or group at once, or whose offset map kept each batch's
//! decompressed body alive through the keys it stored, held all of that
//! expansion at the same time.
//!
//! The test measures the process's peak resident set (`VmHWM`) across one
//! `compact` call, after resetting it through `/proc/self/clear_refs`. It is
//! the only test in its binary, so no other test's allocations land in the
//! window.

#![cfg(target_os = "linux")]

use {
    assert2::assert,
    bytes::Bytes,
    krabka_compression::CompressionType,
    krabka_ids::Offset,
    krabka_log::{CleanupPolicy, CompactionContext, Log, LogConfig},
    krabka_protocol::records::{Attributes, Record, RecordBatch},
    krabka_units::prelude::{ByteSize, ByteSizeExt as _, bytes, gibibytes, mebibytes},
    std::time::SystemTime,
    tempfile::tempdir,
};

/// Sealed segments the pass compacts, one batch each.
const SEGMENTS: i64 = 64;

/// Records per batch. Each key is distinct, so the pass keeps every record
/// and its offset map holds one entry per record.
const RECORDS_PER_BATCH: i32 = 4;

/// Each record's value: one repeated byte, which gzip stores in about a
/// thousandth of its size.
const VALUE_LEN: usize = 256 * 1024;

/// What the compacted range expands to: 64 MiB.
const EXPANDED: ByteSize = mebibytes(64);

/// The pass's allowed peak above the resident set it starts from. One batch
/// expands to 1 MiB; the bound leaves room for the decoded batch, its
/// re-encoding and the allocator, and stays well under [`EXPANDED`].
const PEAK_BOUND: ByteSize = mebibytes(16);

fn compressed_batch(segment: i64) -> RecordBatch {
    let records = (0..RECORDS_PER_BATCH)
        .map(|delta| Record {
            offset_delta: delta,
            key: Some(Bytes::from(format!("key-{segment}-{delta}"))),
            value: Some(Bytes::from(vec![b'v'; VALUE_LEN])),
            ..Record::default()
        })
        .collect();
    RecordBatch {
        last_offset_delta: RECORDS_PER_BATCH - 1,
        attributes: Attributes::default().with_compression(CompressionType::Gzip),
        records,
        ..RecordBatch::default()
    }
}

/// A `/proc/self/status` memory field, such as `VmRSS` or `VmHWM` (the peak
/// resident set since the last reset).
fn status_field(field: &str) -> ByteSize {
    let status = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    let kib: u64 = status
        .lines()
        .find_map(|line| line.strip_prefix(field)?.strip_prefix(':'))
        .and_then(|value| value.trim().strip_suffix("kB"))
        .and_then(|kib| kib.trim().parse().ok())
        .unwrap_or_else(|| panic!("{field} in kB in /proc/self/status"));
    ByteSize::from_bytes(kib * 1024)
}

/// Reset `VmHWM` to the current resident set (`clear_refs` value 5).
fn reset_peak_resident() { std::fs::write("/proc/self/clear_refs", "5").expect("reset VmHWM"); }

/// Every record of the log, as `(offset, key)`, read a few batches at a time.
fn surviving_keys(log: &Log) -> Vec<(i64, Bytes)> {
    let mut keys = Vec::new();
    let mut next = Offset(0);
    loop {
        let read = log.read(next, mebibytes(1)).unwrap();
        let Some(last) = read.batches.last() else {
            return keys;
        };
        next = Offset(last.base_offset + i64::from(last.last_offset_delta) + 1);
        for batch in &read.batches {
            keys.extend(batch.records.iter().map(|record| {
                (
                    batch.base_offset + i64::from(record.offset_delta),
                    record.key.clone().expect("every record is keyed"),
                )
            }));
        }
    }
}

#[test]
fn compacting_a_compressed_log_holds_one_batch_not_the_group() {
    let dir = tempdir().unwrap();
    let config = LogConfig {
        cleanup_policy: CleanupPolicy::Compact,
        // One batch per segment.
        segment_size: bytes(1),
        ..LogConfig::default()
    };
    let mut log = Log::open(dir.path(), config).unwrap();
    // The last append opens the active segment the pass leaves alone.
    for segment in 0..=SEGMENTS {
        log.append(&mut compressed_batch(segment)).unwrap();
    }
    // One group for the whole range: the on-disk size bound alone would let
    // the pass hold all of it.
    let mut merged = log.config_snapshot();
    merged.segment_size = gibibytes(1);
    log.set_config(merged);

    reset_peak_resident();
    let start = status_field("VmRSS");
    log.compact(&CompactionContext {
        now: SystemTime::now(),
        last_stable_offset: Offset(i64::MAX),
    })
    .unwrap();
    let growth = status_field("VmHWM") - start;

    assert!(
        growth < PEAK_BOUND,
        "compacting {} MiB of expanded records raised the peak resident set by {} KiB",
        EXPANDED.bytes_u64() >> 20,
        growth.bytes_u64() >> 10,
    );

    let expected: Vec<(i64, Bytes)> = (0..=SEGMENTS)
        .flat_map(|segment| {
            (0..RECORDS_PER_BATCH).map(move |delta| {
                (
                    segment * i64::from(RECORDS_PER_BATCH) + i64::from(delta),
                    Bytes::from(format!("key-{segment}-{delta}")),
                )
            })
        })
        .collect();
    assert!(surviving_keys(&log) == expected);
}
