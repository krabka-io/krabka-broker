//! Tests that drive the zero-copy dispatch end to end, from the verbatim
//! passthrough predicate to the writer's `ProduceData`.

use {
    super::super::{DecodeEnv, PartitionPayload, PreparedBatch, PreparedSource, prepare_batch},
    crate::{
        codes,
        handlers::produce::{append::build_produce_data, topic_settings::TimestampPolicy},
        metrics::BrokerMetrics,
        partition::ProduceData,
    },
    assert2::{assert, check},
    bytes::{Bytes, BytesMut},
    krabka_compression::{CompressionType, RecordDecompressionPolicy},
    krabka_protocol::records::{
        Attributes, CRC_COVERAGE_START, HEADER_LEN, Record, RecordBatch, RecordsPayload,
        TimestampType,
    },
    std::{mem, sync::Arc},
};

/// Prepare against the fixture topic with the caller's compression, metrics
/// and version; each borrow remains valid throughout preparation.
fn prepare_for_topic(
    payload: PartitionPayload,
    compression: Option<CompressionType>,
    metrics: &BrokerMetrics,
    version: i16,
) -> Result<PreparedBatch, i16> {
    prepare_batch(
        payload,
        compression,
        TimestampPolicy::default(),
        false,
        DecodeEnv {
            topic_name: &topic(),
            metrics,
            policy: RecordDecompressionPolicy::default(),
        },
        version,
    )
}

/// The topic name these cases record under, as the shared handle the metric
/// label sets clone.
fn topic() -> Arc<str> { Arc::from("t") }

fn encode(b: &RecordBatch) -> Bytes {
    let mut buf = BytesMut::new();
    b.encode_batch(&mut buf).unwrap();
    buf.freeze()
}

fn refresh_batch_crc(encoded: &mut [u8]) {
    let crc = crc32c::crc32c(&encoded[CRC_COVERAGE_START..]);
    encoded[CRC_COVERAGE_START - 4..CRC_COVERAGE_START].copy_from_slice(&crc.to_be_bytes());
}

fn plain_batch() -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        partition_leader_epoch: -1,
        last_offset_delta: 0,
        base_timestamp: 42,
        max_timestamp: 42,
        producer_id: -1,
        records: vec![Record {
            value: Some(Bytes::from_static(b"hello")),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn rejected_batch(batch: &RecordBatch) -> i16 {
    prepare_for_topic(
        PartitionPayload::Slice(encode(batch)),
        None,
        &BrokerMetrics::new(),
        13,
    )
    .unwrap_err()
}

#[test]
fn message_count_reports_v2_record_total() {
    // Multi-record batch so the count can't be mistaken for a constant.
    let batch = RecordBatch {
        last_offset_delta: 2,
        records: vec![
            Record {
                value: Some(Bytes::from_static(b"a")),
                ..Default::default()
            },
            Record {
                value: Some(Bytes::from_static(b"b")),
                ..Default::default()
            },
            Record {
                value: Some(Bytes::from_static(b"c")),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let wire = encode(&batch);
    // A null field and a non-v2 (zeroed) slice both contribute zero.
    let cases = [
        (PartitionPayload::Slice(wire), 3, "v2 slice with 3 records"),
        (PartitionPayload::Null, 0, "null records field"),
        (
            PartitionPayload::Slice(Bytes::from_static(&[0u8; 64])),
            0,
            "non-v2 zeroed slice",
        ),
    ];
    for (payload, want, label) in cases {
        assert!(payload.message_count() == want, "case: {label}");
    }
}

/// Run the full dispatch over a v≥3 records slice: first
/// `prepare_batch`, then `build_produce_data` with the given leader
/// epoch.
fn dispatch_slice(
    slice: Bytes,
    topic_compression: Option<CompressionType>,
    leader_epoch: i32,
) -> ProduceData {
    let m = BrokerMetrics::new();
    let prepared =
        prepare_for_topic(PartitionPayload::Slice(slice), topic_compression, &m, 13).unwrap();
    build_produce_data(prepared, leader_epoch)
}

#[test]
fn passthrough_when_all_conditions_hold() {
    let b = plain_batch();
    let wire = encode(&b);
    let data = dispatch_slice(wire.clone(), None, 7);
    match data {
        ProduceData::Verbatim(v) => {
            check!(&v.bytes[..] == &wire[..]);
            check!(v.leader_epoch == 7);
            check!(v.max_timestamp == 42);
            check!(v.last_offset_delta == 0);
        }
        ProduceData::Owned(_) | ProduceData::OwnedEncoded { .. } => panic!("expected Verbatim"),
        ProduceData::OwnedCommitMarker { .. } | ProduceData::OwnedControl(_) => {
            panic!("expected producer data")
        }
    }
}

#[test]
fn passthrough_when_target_codec_equals_current() {
    // Topic forces lz4; batch is already lz4 → no recompression needed.
    let mut b = plain_batch();
    b.attributes = b.attributes.with_compression(CompressionType::Lz4);
    let wire = encode(&b);
    let data = dispatch_slice(wire, Some(CompressionType::Lz4), 1);
    assert!(matches!(data, ProduceData::Verbatim(_)));
}

#[test]
fn fallback_when_null_field() {
    // A wire-null records field is rejected as INVALID_REQUEST.
    let m = BrokerMetrics::new();
    let err = prepare_for_topic(PartitionPayload::Null, None, &m, 13).unwrap_err();
    assert!(err == codes::INVALID_REQUEST);
}

#[test]
fn fallback_on_recompression_to_different_codec() {
    // Batch uncompressed, topic forces zstd → must recompress (owned).
    let b = plain_batch();
    let wire = encode(&b);
    let data = dispatch_slice(wire, Some(CompressionType::Zstd), 0);
    assert!(matches!(data, ProduceData::Owned(_)));
}

#[test]
fn rejects_client_log_append_time() {
    let mut b = plain_batch();
    b.attributes = b
        .attributes
        .with_timestamp_type(TimestampType::LogAppendTime);
    let err = rejected_batch(&b);
    assert!(err == codes::INVALID_TIMESTAMP);
}

#[test]
fn rejects_client_control_batch() {
    let mut b = plain_batch();
    b.attributes = Attributes::default().with_control(true);
    let err = rejected_batch(&b);
    assert!(err == codes::INVALID_RECORD);
}

/// `ProduceRequest.validateRecords` refuses a zstd batch below `Produce` v7
/// with `UNSUPPORTED_COMPRESSION_TYPE`, on both append shapes and whatever the
/// topic's own `compression.type` asks for, because the refusal is about
/// whether the REQUESTING client's version can decode zstd, not about what
/// the broker stores it as.
#[yare::parameterized(
    v0_slice = { 0, false, false },
    v6_slice = { 6, false, false },
    v7_slice = { 7, false, true },
    v13_slice = { 13, false, true },
    v0_owned = { 0, true, false },
    v6_owned = { 6, true, false },
    v7_owned = { 7, true, true },
    v13_owned = { 13, true, true },
)]
fn rejects_zstd_below_v7_and_admits_it_from_v7_on_both_paths(
    version: i16,
    owned_payload: bool,
    admitted: bool,
) {
    let mut b = plain_batch();
    b.attributes = b.attributes.with_compression(CompressionType::Zstd);
    let wire = encode(&b);

    let payload = if owned_payload {
        PartitionPayload::Owned(RecordsPayload::V2(vec![b]))
    } else {
        PartitionPayload::Slice(wire)
    };
    let result = prepare_for_topic(payload, None, &BrokerMetrics::new(), version);
    if admitted {
        assert2::assert!(result.is_ok(), "version {version}: {result:?}");
    } else {
        assert2::assert!(
            result.as_ref().err() == Some(&codes::UNSUPPORTED_COMPRESSION_TYPE),
            "version {version}: {result:?}"
        );
    }
}

/// Kafka does not inspect `baseOffset` at all: `LogValidator.validateBatch`
/// checks only that the record count is positive and agrees with the offset
/// range. A nonzero `base_offset` is therefore admitted, on both append
/// shapes, exactly like every other batch this file drives through
/// `prepare_batch`.
#[test]
fn admits_a_nonzero_base_offset_on_both_paths() {
    let mut b = plain_batch();
    b.base_offset = 1;
    let payloads = [
        PartitionPayload::Slice(encode(&b)),
        PartitionPayload::Owned(RecordsPayload::V2(vec![b])),
    ];
    for payload in payloads {
        let prepared = prepare_for_topic(payload, None, &BrokerMetrics::new(), 13);
        assert!(prepared.is_ok());
    }
}

#[test]
fn rejects_invalid_client_batch_metadata_on_header_and_owned_paths() {
    let mut invalid_offset_range = plain_batch();
    invalid_offset_range.last_offset_delta = -1;

    let mut inconsistent_count = plain_batch();
    inconsistent_count.last_offset_delta = 1;

    let mut overflowing_offset_count = plain_batch();
    overflowing_offset_count.last_offset_delta = i32::MAX;
    overflowing_offset_count.records = vec![Record::default(); 1];

    let mut empty = plain_batch();
    empty.records.clear();

    let mut invalid_sequence = plain_batch();
    invalid_sequence.producer_id = 7;
    invalid_sequence.producer_epoch = 0;
    invalid_sequence.base_sequence = -1;

    for (name, batch) in [
        ("invalid offset range", invalid_offset_range),
        ("inconsistent count", inconsistent_count),
        ("overflowing offset count", overflowing_offset_count),
        ("empty batch", empty),
        ("invalid producer sequence", invalid_sequence),
    ] {
        let payloads = [
            PartitionPayload::Slice(encode(&batch)),
            PartitionPayload::Owned(RecordsPayload::V2(vec![batch])),
        ];
        for payload in payloads {
            let err = prepare_for_topic(payload, None, &BrokerMetrics::new(), 13).unwrap_err();
            assert!(err == codes::INVALID_RECORD, "case: {name}");
        }
    }
}

#[derive(Default)]
struct InvalidProduceSliceScenario {
    wire: Vec<u8>,
    result: Option<Result<PreparedBatch, i16>>,
}
#[rstest::fixture]
fn invalid_produce_slice() -> InvalidProduceSliceScenario {
    let wire = Vec::new();
    InvalidProduceSliceScenario {
        wire,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given("a Produce records slice contains {shape}")]
fn malformed_produce_records(
    invalid_produce_slice: &mut InvalidProduceSliceScenario,
    shape: String,
) {
    let clean = encode(&plain_batch()).to_vec();
    let mut wire = clean.clone();
    match shape.as_str() {
        "a CRC mismatch" => wire[HEADER_LEN] ^= 0xFF,
        "a CRC mismatch followed by a second batch" => {
            wire[HEADER_LEN] ^= 0xFF;
            wire.extend_from_slice(&clean);
        }
        "two valid batches" => wire.extend_from_slice(&clean),
        "batch size 13" => wire[8..12].copy_from_slice(&13i32.to_be_bytes()),
        "batch size -1" => wire[8..12].copy_from_slice(&(-1i32).to_be_bytes()),
        "batch size 20 truncated to 32 bytes" => {
            wire[8..12].copy_from_slice(&20i32.to_be_bytes());
            wire.truncate(32);
        }
        "magic 3" => wire[16] = 3,
        "magic 255" => wire[16] = 0xFF,
        "one byte missing from the record body" => {
            wire.pop();
        }
        "only the first 11 bytes" => wire.truncate(11),
        _ => panic!("unknown record slice shape: {shape}"),
    }
    invalid_produce_slice.wire = wire;
}
#[rstest_bdd_macros::when("Produce version 13 prepares that records slice without recompression")]
fn prepare_invalid_produce_slice(invalid_produce_slice: &mut InvalidProduceSliceScenario) {
    invalid_produce_slice.result = Some(prepare_for_topic(
        PartitionPayload::Slice(Bytes::from(mem::take(&mut invalid_produce_slice.wire))),
        None,
        &BrokerMetrics::new(),
        13,
    ));
}
#[rstest_bdd_macros::then("the records slice is rejected with Kafka error {code}")]
fn check_invalid_produce_slice(invalid_produce_slice: &InvalidProduceSliceScenario, code: i16) {
    assert!(matches!(invalid_produce_slice.result.as_ref().unwrap(),Err(error)if *error==code));
}
rstest_bdd_macros::scenarios!("tests/features/handler_invalid_produce_slice",fixtures=[invalid_produce_slice:InvalidProduceSliceScenario]);

#[test]
fn rejects_crc_valid_malformed_record_body() {
    let mut wire = encode(&plain_batch()).to_vec();
    wire[HEADER_LEN] = 0; // zero-length first record body
    refresh_batch_crc(&mut wire);

    let error = prepare_for_topic(
        PartitionPayload::Slice(Bytes::from(wire)),
        None,
        &BrokerMetrics::new(),
        13,
    )
    .unwrap_err();
    assert!(error == codes::INVALID_RECORD);
}

#[test]
fn fallback_on_multiple_batches_in_slice() {
    // Kafka v2 records fields contain exactly one batch. A second
    // batch is invalid and must never be silently discarded.
    let b = plain_batch();
    let mut two = BytesMut::new();
    b.encode_batch(&mut two).unwrap();
    b.encode_batch(&mut two).unwrap();
    let err = prepare_for_topic(
        PartitionPayload::Slice(two.freeze()),
        None,
        &BrokerMetrics::new(),
        13,
    )
    .unwrap_err();
    assert!(err == codes::INVALID_RECORD);
}

#[test]
fn transactional_batch_can_pass_through() {
    let mut b = plain_batch();
    b.producer_id = 100;
    b.producer_epoch = 0;
    b.base_sequence = 0;
    b.attributes = b.attributes.with_transactional(true);
    let wire = encode(&b);
    let data = dispatch_slice(wire, None, 0);
    match data {
        ProduceData::Verbatim(v) => {
            assert!(v.is_transactional);
            assert!(v.producer_id == krabka_log::ProducerId(100));
        }
        ProduceData::Owned(_) | ProduceData::OwnedEncoded { .. } => {
            panic!("transactional data batch should pass through")
        }
        ProduceData::OwnedCommitMarker { .. } | ProduceData::OwnedControl(_) => {
            panic!("expected producer data")
        }
    }
}

/// A producer-LZ4-compressed batch stays verbatim after structural
/// validation, even when its decompressed form is 100 KiB and its
/// compressed wire bytes are tiny.
///
/// The stored `Verbatim.bytes` equal the compressed wire bytes, which
/// are much smaller than the decompressed payload. The header fields
/// `last_offset_delta` and `max_timestamp` come straight from the v2
/// header, which agrees with the batch's one record, so the bytes need no
/// `max_timestamp` rewrite. This test pins the no-reencoding guarantee.
#[test]
fn lz4_batch_passes_through_without_reencoding() {
    // 100 KiB of highly-compressible payload across many records.
    let big = vec![b'A'; 100 * 1024];
    let mut b = RecordBatch {
        last_offset_delta: 0,
        base_timestamp: 7_777,
        max_timestamp: 7_777,
        producer_id: -1,
        ..RecordBatch::default()
    };
    b.attributes = b.attributes.with_compression(CompressionType::Lz4);
    b.records.push(Record {
        value: Some(Bytes::from(big.clone())),
        ..Default::default()
    });
    let wire = encode(&b);
    // The compressed wire bytes must be far smaller than the raw payload,
    // so an accidental re-encode to an uncompressed batch is obvious.
    assert!(
        wire.len() < big.len() / 4,
        "lz4 wire ({} B) should be much smaller than raw ({} B)",
        wire.len(),
        big.len()
    );

    let data = dispatch_slice(wire.clone(), None, 3);
    match data {
        ProduceData::Verbatim(v) => {
            // Stored bytes are the COMPRESSED wire bytes — verbatim, not
            // re-encoded from decompressed records ("must stay compressed").
            // Header fields came from the v2 header, no record decode.
            check!(&v.bytes[..] == &wire[..]);
            check!(v.bytes.len() == wire.len());
            check!(v.bytes.len() < big.len());
            check!(v.max_timestamp == 7_777);
            check!(v.last_offset_delta == 0);
            check!(v.leader_epoch == 3);
        }
        ProduceData::Owned(_) | ProduceData::OwnedEncoded { .. } => {
            panic!("lz4 producer batch must pass through verbatim")
        }
        ProduceData::OwnedCommitMarker { .. } | ProduceData::OwnedControl(_) => {
            panic!("expected producer data")
        }
    }
}

/// HEADER fields drive the idempotent dedup over the verbatim path.
///
/// `prepare_batch` exposes `producer_id`, `producer_epoch`,
/// `base_sequence`, and `last_offset_delta`. It reads them from the v2
/// header without materializing owned records. The values match what
/// an owned decode of the same bytes would give.
#[test]
fn header_fields_drive_dedup_on_verbatim_path() {
    let mut b = plain_batch();
    b.producer_id = 4242;
    b.producer_epoch = 9;
    b.base_sequence = 17;
    b.last_offset_delta = 2;
    b.base_timestamp = 555;
    b.max_timestamp = 555;
    b.records.extend([
        Record {
            value: Some(Bytes::from_static(b"second")),
            ..Default::default()
        },
        Record {
            value: Some(Bytes::from_static(b"third")),
            ..Default::default()
        },
    ]);
    // Force lz4 so validation must decompress while the append still
    // retains the producer's exact compressed bytes.
    b.attributes = b.attributes.with_compression(CompressionType::Lz4);
    let wire = encode(&b);

    let m = BrokerMetrics::new();
    let prepared = prepare_for_topic(PartitionPayload::Slice(wire.clone()), None, &m, 13).unwrap();
    assert!(matches!(prepared.source, PreparedSource::Verbatim(_)));
    check!(prepared.producer_id == 4242);
    check!(prepared.producer_epoch == 9);
    check!(prepared.base_sequence == 17);
    check!(prepared.last_offset_delta == 2);
    check!(prepared.max_timestamp == 555);

    // Cross-check: an owned decode of the same compressed bytes yields
    // the same header identity (proving the header read is correct).
    let mut cur: &[u8] = &wire;
    let owned = RecordBatch::decode_batch(&mut cur).unwrap();
    check!(owned.producer_id == prepared.producer_id);
    check!(owned.producer_epoch == prepared.producer_epoch);
    check!(owned.base_sequence == prepared.base_sequence);
    check!(owned.last_offset_delta == prepared.last_offset_delta);
}
