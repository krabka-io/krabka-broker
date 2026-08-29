//! Fixture builders and budget constants that the segment modules' unit tests
//! share.

use bytes::Bytes;
use krabka_ids::Offset;
use krabka_protocol::records::{Record, RecordBatch};
use krabka_units::prelude::{ByteSize, ByteSizeExt, gibibytes};
use tempfile::tempdir;

use super::Segment;

/// Index every batch. No batch is ever `0` bytes past the last entry.
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
        b.records.push(Record {
            offset_delta: i,
            timestamp_delta: i64::from(i),
            key: Some(Bytes::from(format!("k{i}"))),
            value: Some(Bytes::from(format!("v{i}"))),
            ..Default::default()
        });
    }
    b
}

pub(super) fn test_segment() -> (tempfile::TempDir, Segment) {
    let dir = tempdir().unwrap();
    let seg = Segment::create(dir.path(), Offset(0)).unwrap();
    (dir, seg)
}

pub(super) fn test_batch_at(off: i64) -> RecordBatch {
    let mut b = RecordBatch {
        base_offset: off,
        base_timestamp: 1_000,
        max_timestamp: 1_000,
        last_offset_delta: 0,
        ..RecordBatch::default()
    };
    b.records.push(Record {
        offset_delta: 0,
        timestamp_delta: 0,
        value: Some(Bytes::from(format!("v{off}"))),
        ..Default::default()
    });
    b
}
