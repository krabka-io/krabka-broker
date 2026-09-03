//! Fixtures shared by the WAL quorum unit tests: a synthetic record batch, and
//! an append that reaches the source log through the real produce path.

use krabka_ids::Offset;
use krabka_protocol::records::{Record, RecordBatch};

use super::QuorumWalStore;
use crate::error::BrokerError;

pub(super) fn batch(records: i32) -> RecordBatch {
    let mut batch = RecordBatch {
        last_offset_delta: records - 1,
        ..RecordBatch::default()
    };
    for offset_delta in 0..records {
        batch.records.push(Record {
            offset_delta,
            ..Record::default()
        });
    }
    batch
}

pub(super) async fn append_source(
    store: &QuorumWalStore,
    records: i32,
) -> (
    Vec<Result<crate::partition::AppendedBatch, BrokerError>>,
    Offset,
) {
    crate::partition_writer::run_produce_append_batch(
        store.source.clone(),
        vec![crate::partition::ProduceData::Owned(batch(records))],
    )
    .await
    .unwrap()
}
