//! Fixtures the bound unit tests share: a `RestoreArgs` parsed from the bound
//! flags under test, the records and batches a test judges, and the harness
//! that runs `Predicates::decide_batch` and `Predicates::decide_record` over
//! one batch the way `materialize` does.

use bytes::{Bytes, BytesMut};
use clap::Parser as _;
use krabka_ids::ProducerId;
use krabka_protocol::{
    DecodeBorrow as _,
    records::{Attributes, Record, RecordBatch, RecordBatchBorrowed, RecordHeader},
};

use crate::{
    args::{PartitionRef, RestoreArgs},
    bound::{BatchDecision, Predicates, RecordDecision, record_coordinates},
    error::RestoreError,
};

const BASE_OFFSET: i64 = 1_000;
pub(super) const BASE_TIMESTAMP: i64 = 1_700_000_000_000;

pub(super) fn partition(topic: &str, index: i32) -> PartitionRef {
    PartitionRef {
        topic: topic.to_owned(),
        partition: index,
    }
}

/// Parses `RestoreArgs` the same way the binary does, with a fixed
/// archive source and target so a test only has to state the bound
/// flags under test.
pub(super) fn args_from(extra: &[&str]) -> RestoreArgs {
    let mut argv = vec![
        "krabka-restore",
        "--archive-local",
        "/archive",
        "--log-dir",
        "/target",
    ];
    argv.extend_from_slice(extra);
    crate::Cli::try_parse_from(argv)
        .expect("valid command line")
        .args
}

pub(super) fn predicates(extra: &[&str]) -> Predicates {
    Predicates::from_args(&args_from(extra)).expect("valid predicates")
}

/// A minimal record at `offset_delta`, with an arbitrary value and no key
/// or headers. Override fields with struct-update syntax.
pub(super) fn record(offset_delta: i32) -> Record {
    Record {
        offset_delta,
        value: Some(Bytes::from_static(b"v")),
        ..Record::default()
    }
}

pub(super) fn header(name: &str, value: &[u8]) -> RecordHeader {
    RecordHeader {
        key: name.to_owned(),
        value: Some(Bytes::copy_from_slice(value)),
    }
}

// Every header field holds a distinctive value, matching
// `krabka_log::filter::tests::batch`'s convention, so a mistaken swap
// between two header fields would show up as a wrong test outcome.
pub(super) fn batch(producer_id: i64, records: Vec<Record>) -> RecordBatch {
    RecordBatch {
        base_offset: BASE_OFFSET,
        partition_leader_epoch: 3,
        attributes: Attributes::default(),
        last_offset_delta: records.iter().map(|r| r.offset_delta).max().unwrap_or(0),
        base_timestamp: BASE_TIMESTAMP,
        max_timestamp: BASE_TIMESTAMP
            + records.iter().map(|r| r.timestamp_delta).max().unwrap_or(0),
        producer_id,
        producer_epoch: 0,
        base_sequence: 0,
        records,
    }
}

fn encode(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(batch.encoded_len());
    batch.encode(&mut buf).expect("encode");
    buf.to_vec()
}

fn borrow(bytes: &[u8]) -> RecordBatchBorrowed<'_> {
    let mut cursor = bytes;
    // `version` is unused by the v2 batch decoder; any value does.
    RecordBatchBorrowed::decode_borrow(&mut cursor, 0)
        .expect("decode a borrowed batch back out of its own encoding")
}

/// Runs both `decide_batch` and `decide_record` for every record, the way
/// `materialize.rs` would: a batch-level verdict, and the per-record
/// verdicts in batch order.
pub(super) fn decide(
    predicates: &Predicates,
    partition: &PartitionRef,
    owned: &RecordBatch,
) -> (BatchDecision, Vec<RecordDecision>) {
    try_decide(predicates, partition, owned).expect("valid record coordinates")
}

pub(super) fn try_decide(
    predicates: &Predicates,
    partition: &PartitionRef,
    owned: &RecordBatch,
) -> Result<(BatchDecision, Vec<RecordDecision>), RestoreError> {
    let encoded = encode(owned);
    let borrowed = borrow(&encoded);
    let header = borrowed.header();
    let producer_id = ProducerId(header.producer_id.get());

    let batch_decision = predicates.decide_batch(partition, &borrowed)?;
    let record_decisions = borrowed
        .iter()
        .map(|parsed| {
            let record = parsed?;
            let (offset, timestamp_ms) = record_coordinates(header, &record)?;
            Ok(predicates.decide_record(partition, offset, timestamp_ms, producer_id, &record))
        })
        .collect::<Result<Vec<_>, RestoreError>>()?;
    Ok((batch_decision, record_decisions))
}
