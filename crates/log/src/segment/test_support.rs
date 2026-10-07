//! Fixture builders and budget constants that the segment modules' unit tests
//! share.

use bytes::Bytes;
use krabka_ids::Offset;
use krabka_protocol::records::RecordBatch;
use krabka_units::prelude::{ByteSize, ByteSizeExt, gibibytes};
use tempfile::tempdir;

use super::Segment;

/// Index every batch but the first. Every later batch is more than `0` bytes
/// past the last entry, and the first is `0` bytes past the segment start,
/// which Kafka's `bytesSinceLastIndexEntry > indexIntervalBytes` does not index.
pub(super) const DENSE_INDEX: ByteSize = ByteSize::ZERO;

/// A read budget larger than anything these tests write, so the byte
/// budget never clips the result.
pub(super) const NO_LIMIT: ByteSize = gibibytes(4);

pub(super) fn sample_batch(base_offset: i64, n: i32, ts_base: i64) -> RecordBatch {
    let mut b = RecordBatch {
        base_offset,
        base_timestamp: ts_base,
        max_timestamp: ts_base + i64::from(n - 1),
        last_offset_delta: n - 1,
        ..RecordBatch::default()
    };
    for i in 0..n {
        b.records
            .push(crate::test_support::numbered_record(i, i64::from(i)));
    }
    b
}

pub(super) fn test_segment() -> (tempfile::TempDir, Segment) {
    segment_at(0)
}

pub(super) fn segment_at(base: i64) -> (tempfile::TempDir, Segment) {
    let dir = tempdir().unwrap();
    let seg = Segment::create(dir.path(), Offset(base)).unwrap();
    (dir, seg)
}

pub(super) fn test_batch_at(off: i64) -> RecordBatch {
    crate::test_support::single_record_batch(off, 1_000, Bytes::from(format!("v{off}")))
}

pub(super) fn seeded_segment(
    dir: &std::path::Path,
    base_offset: i64,
    batches: &[(i64, i32, i64)],
) -> Segment {
    populated_segment(dir, base_offset, batches, DENSE_INDEX)
}

pub(super) fn populated_segment(
    dir: &std::path::Path,
    base_offset: i64,
    batches: &[(i64, i32, i64)],
    interval: ByteSize,
) -> Segment {
    let mut segment = Segment::create(dir, Offset(base_offset)).unwrap();
    for &(base, count, timestamp) in batches {
        segment
            .append(&sample_batch(base, count, timestamp), interval)
            .unwrap();
    }
    segment
}

pub(super) fn indexed_segment() -> (tempfile::TempDir, Segment) {
    let dir = tempdir().unwrap();
    let segment = seeded_segment(
        dir.path(),
        100,
        &[(100, 3, 100), (103, 2, 200), (105, 1, 300)],
    );
    (dir, segment)
}

pub(super) fn sample_batches(batches: &[(i64, i32, i64)]) -> Vec<RecordBatch> {
    batches
        .iter()
        .map(|&(base, count, timestamp)| sample_batch(base, count, timestamp))
        .collect()
}
