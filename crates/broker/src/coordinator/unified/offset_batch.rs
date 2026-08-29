//! Accumulator that turns a run of coordinator records into one
//! `__consumer_offsets` [`RecordBatch`].
//!
//! Every coordinator write path — classic, next-gen, share, and streams —
//! assembles its records through this builder, so the offset-delta
//! bookkeeping of a batch has one home.

use bytes::Bytes;
use krabka_protocol::records::{Record, RecordBatch};

#[derive(Default)]
pub(crate) struct OffsetRecordBatchBuilder {
    records: Vec<Record>,
}

impl OffsetRecordBatchBuilder {
    pub(crate) fn push(&mut self, key: Bytes, value: Option<Bytes>) {
        let delta = i32::try_from(self.records.len()).expect("batch size fits i32");
        self.records.push(Record {
            offset_delta: delta,
            key: Some(key),
            value,
            ..Default::default()
        });
    }

    pub(crate) fn finish(self, now_ms: i64) -> RecordBatch {
        let last_delta = i32::try_from(self.records.len().saturating_sub(1)).unwrap_or(0);
        RecordBatch {
            max_timestamp: now_ms,
            records: self.records,
            last_offset_delta: last_delta,
            ..RecordBatch::default()
        }
    }
}
