//! Down-conversion of a zstd-compressed v2 batch, which the legacy
//! `MessageSet` format cannot express and the broker therefore re-compresses
//! as snappy.
//!
//! The test reads the codec bits straight out of the wrapper message's
//! attributes byte, so it pins the codec id (2) that a legacy consumer sees
//! rather than only the decoded records.

use {
    crate::{
        harness::{fetch_legacy_raw, legacy_bytes, produce_batch},
        support::{self, topics::create_named_topic_request},
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_compression::CompressionType,
    krabka_protocol::{
        Decode,
        kafka_3_6_2::owned::fetch_response::FetchResponse as LegacyFetchResponse,
        records::{Attributes, Record, RecordBatch},
    },
    krabka_records_legacy::decode_message_set,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    support::InProcess,
};

struct LegacyCompression {
    process: Option<InProcess>,
    response: Option<LegacyFetchResponse>,
}

#[fixture]
fn compression() -> LegacyCompression {
    // Start this scenario with fresh inputs and observations.

    LegacyCompression {
        process: None,
        response: None,
    }
}

#[given("a modern topic holds fifty zstd-compressed records with distinct boundary keys")]
async fn zstd_records(compression: &mut LegacyCompression) {
    let process = support::start_legacy().await;
    let created = process
        .client
        .send(
            create_named_topic_request()
                .name("legacy_fetch_zstd")
                .call(),
        )
        .await
        .expect("CreateTopics");
    assert!(
        created.topics[0].error_code == 0,
        "CreateTopics error: {}",
        created.topics[0].error_code
    );
    let records = (0_i32..50)
        .map(|index| Record {
            offset_delta: index,
            timestamp_delta: i64::from(index) * 1000,
            key: Some(Bytes::from(format!("key-{index:04}"))),
            value: Some(Bytes::from(format!(
                "val-{index:04} hello world this is a repeated test value"
            ))),
            ..Default::default()
        })
        .collect();
    let batch = RecordBatch {
        attributes: Attributes::default().with_compression(CompressionType::Zstd),
        last_offset_delta: 49,
        records,
        ..Default::default()
    };
    produce_batch(process.broker.listen_addr(), "legacy_fetch_zstd", batch).await;
    compression.process = Some(process);
}

#[when("a Fetch v3 consumer reads the zstd-compressed modern batch")]
async fn fetch_zstd(compression: &mut LegacyCompression) {
    let process = compression
        .process
        .as_ref()
        .expect("zstd records were produced");
    let body = fetch_legacy_raw(process.broker.listen_addr(), "legacy_fetch_zstd", 3).await;
    compression.response = Some(
        LegacyFetchResponse::decode(&mut body.as_slice(), 3)
            .expect("decode legacy Fetch v3 response"),
    );
}

fn converted_payload(compression: &LegacyCompression) -> Bytes {
    let response = compression.response.as_ref().expect("Fetch completed");
    let partition = &response.responses[0].partitions[0];
    legacy_bytes(
        partition.error_code,
        partition.records.as_ref(),
        "Fetch v3 must return a Legacy MessageSet",
    )
}

#[then("the legacy wrapper advertises snappy compression codec two")]
fn snappy_wrapper(compression: &LegacyCompression) {
    let bytes = converted_payload(compression);
    assert!(
        bytes.len() > 17,
        "expected nonempty legacy bytes, got len={}",
        bytes.len()
    );
    let codec = bytes[17] & 0x07;
    assert!(codec == 2, "expected snappy codec id two, got {codec}");
}

#[then("all fifty records decode with boundary keys key-0000 and key-0049")]
fn compression_boundaries(compression: &LegacyCompression) {
    let bytes = converted_payload(compression);
    let records = decode_message_set(&mut bytes.as_ref(), bytes.len())
        .expect("decode snappy-recompressed MessageSet");
    assert!(
        records.len() == 50,
        "expected fifty records after snappy decompression"
    );
    check!(
        records[0].key.as_deref() == Some(b"key-0000".as_ref()),
        "first record key mismatch"
    );
    check!(
        records[49].key.as_deref() == Some(b"key-0049".as_ref()),
        "last record key mismatch"
    );
}

#[scenario(
    path = "tests/features/migrated/tests/legacy_fetch/recompression.feature",
    name = "A legacy consumer receives a snappy rewrite of modern zstd records"
)]
#[tokio::test]
async fn fetch_v3_recompresses_zstd_as_snappy(compression: LegacyCompression) {
    compression
        .process
        .expect("a broker was started")
        .broker
        .shutdown()
        .await;
}
