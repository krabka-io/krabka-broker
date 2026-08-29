//! Fixtures the compaction unit tests share: the record and sealed-segment
//! builders, and the two configuration constants every rewrite test passes
//! through.

use std::path::Path;

use bytes::Bytes;
use krabka_ids::Offset;
use krabka_protocol::records::{Attributes, Record, RecordBatch};
use krabka_units::prelude::{ByteSize, Time};

use crate::segment::Segment;

/// Kafka's default `index.interval.bytes`. The compaction tests do not
/// exercise sparse-index density, so they all pass the default value
/// through.
pub(super) const INDEX_INTERVAL: ByteSize = krabka_units::kibibytes(4);

/// The `delete.retention.ms` the rewrite tests share.
pub(super) const RETENTION: Time = krabka_units::secs(1);

pub(super) fn make_record(offset_delta: i32, key: Option<&[u8]>, value: Option<&[u8]>) -> Record {
    Record {
        offset_delta,
        key: key.map(Bytes::copy_from_slice),
        value: value.map(Bytes::copy_from_slice),
        ..Default::default()
    }
}

pub(super) fn write_sealed_segment(dir: &Path, base_offset: i64, records: Vec<Record>) -> Segment {
    let mut seg = Segment::create(dir, Offset(base_offset)).unwrap();
    let n = i32::try_from(records.len()).expect("record count fits i32");
    let max_ts = records.iter().map(|r| r.timestamp_delta).max().unwrap_or(0);
    let batch = RecordBatch {
        base_offset,
        last_offset_delta: n - 1,
        max_timestamp: max_ts,
        records,
        attributes: Attributes::default(),
        ..RecordBatch::default()
    };
    seg.append(&batch, INDEX_INTERVAL).unwrap();
    seg.seal();
    seg
}

/// Write a sealed segment that holds the given batches verbatim, with
/// `base_offset`, attributes, and `producer_id` preserved. Tests use it to
/// build control batches and mixed data and control layouts.
pub(super) fn write_sealed_batches(dir: &Path, batches: &[RecordBatch]) -> Segment {
    let base = batches.first().map_or(0, |b| b.base_offset);
    let mut seg = Segment::create(dir, Offset(base)).unwrap();
    for batch in batches {
        seg.append(batch, INDEX_INTERVAL).unwrap();
    }
    seg.seal();
    seg
}

/// A control batch that carries a single commit or abort marker record.
/// The marker key is `(version: i16, marker_type: i16)` big-endian.
pub(super) fn control_batch(base_offset: i64, producer_id: i64, marker_type: i16) -> RecordBatch {
    let mut key = [0u8; 4];
    key[2..4].copy_from_slice(&marker_type.to_be_bytes());
    RecordBatch {
        base_offset,
        last_offset_delta: 0,
        producer_id,
        attributes: Attributes::default()
            .with_transactional(true)
            .with_control(true),
        records: vec![Record {
            offset_delta: 0,
            key: Some(Bytes::copy_from_slice(&key)),
            ..Default::default()
        }],
        ..RecordBatch::default()
    }
}
