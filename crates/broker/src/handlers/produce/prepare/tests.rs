//! Tests for the verbatim-versus-owned prepare decision.

use assert2::assert;
use bytes::BytesMut;
use krabka_compression::CompressionType;
use krabka_ids::Offset;
use krabka_protocol::records::Record;
use krabka_units::{bytes, fraction};

use super::*;
use crate::handlers::produce::test_support::encode_batch;

#[test]
fn record_decompression_policy_limits_owned_and_verbatim_produce() {
    let policy = RecordDecompressionPolicy::new(fraction(1.0), bytes(1), bytes(32)).unwrap();
    let metrics = crate::metrics::BrokerMetrics::new();

    let v2 = RecordBatch {
        attributes: Attributes::default().with_compression(CompressionType::Lz4),
        records: vec![Record {
            value: Some(Bytes::from(vec![b'x'; 4096])),
            ..Default::default()
        }],
        ..Default::default()
    };
    let wire = encode_batch(&v2);
    let error = prepare_batch(
        PartitionPayload::Slice(wire.clone()),
        None,
        "t",
        &metrics,
        policy,
    )
    .unwrap_err();
    assert!(error == crate::codes::INVALID_RECORD);

    let error = prepare_batch(
        PartitionPayload::Slice(wire.clone()),
        Some(CompressionType::Zstd),
        "t",
        &metrics,
        policy,
    )
    .unwrap_err();
    assert!(error == crate::codes::INVALID_RECORD);
    assert!(
        prepare_batch(
            PartitionPayload::Slice(wire),
            Some(CompressionType::Zstd),
            "t",
            &metrics,
            RecordDecompressionPolicy::default(),
        )
        .is_ok()
    );

    let records = vec![krabka_records_legacy::ParsedRecord {
        offset: Offset(0),
        timestamp: Some(1),
        key: None,
        value: Some(Bytes::from(vec![b'x'; 4096])),
    }];
    let mut legacy = BytesMut::new();
    krabka_records_legacy::encode_compressed_message_set(
        &records,
        krabka_records_legacy::Magic::V1,
        CompressionType::Lz4,
        &mut legacy,
    )
    .unwrap();
    let error = decode_owned_batch(
        RecordsPayload::Legacy(legacy.freeze()),
        "t",
        &metrics,
        policy,
    )
    .unwrap_err();
    assert!(error == crate::codes::INVALID_RECORD);
}

// ── verbatim passthrough predicate (prepare_batch + build_produce_data) ──
//
// These drive the zero-copy dispatch end to end: `prepare_batch` validates
// the v2 batch and decides verbatim-vs-owned; `build_produce_data` maps the
// result to the writer's `ProduceData`, stamping the leader epoch.
mod verbatim;
