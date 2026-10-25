//! Accumulator that turns a run of coordinator records into one
//! `__consumer_offsets` [`RecordBatch`].
//!
//! Every coordinator write path — classic, next-gen, share, and streams —
//! assembles its records through this builder, so the offset-delta
//! bookkeeping of a batch has one home.

use {
    bytes::Bytes,
    krabka_protocol::records::{Record, RecordBatch},
};

#[derive(Default)]
pub struct OffsetRecordBatchBuilder {
    records: Vec<Record>,
}

impl OffsetRecordBatchBuilder {
    pub(crate) fn push(&mut self, key: Bytes, value: Option<Bytes>) {
        self.records.push(Record {
            key: Some(key),
            value,
            ..Default::default()
        });
    }

    pub(crate) fn finish(mut self, now_ms: i64) -> RecordBatch {
        for (delta, record) in (0_i32..).zip(&mut self.records) {
            record.offset_delta = delta;
        }
        let last_delta = self.records.last().map_or(0, |record| record.offset_delta);
        RecordBatch {
            base_timestamp: now_ms,
            max_timestamp: now_ms,
            records: self.records,
            last_offset_delta: last_delta,
            ..RecordBatch::default()
        }
    }
}
