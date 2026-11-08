//! Down-conversion of a zstd-compressed v2 batch, which the legacy
//! `MessageSet` format cannot express.
//!
//! As in Kafka 3.9, the broker does not re-compress the batch with another
//! codec. A partition whose first batch is zstd answers with
//! `UNSUPPORTED_COMPRESSION_TYPE` and no records, which is Kafka's
//! `FetchResponse.partitionResponse`. Kafka leaves that row's records null; the
//! broker encodes every row with nothing to serve as an empty record set
//! instead, because a null one drops sarama connections. A partition that holds convertible
//! batches before the zstd batch answers with those batches alone, as Kafka's
//! lazy down-conversion stops at the zstd batch.
//!
//! The module keeps its earlier name, `recompression`, because it still holds
//! the compression scenarios of the legacy Fetch path.

use {
    crate::{
        harness::{fetch_legacy_raw, legacy_bytes, produce_batch},
        support::{self, topics::create_named_topic_request},
    },
    assert2::assert,
    bytes::Bytes,
    krabka_broker::codes,
    krabka_compression::CompressionType,
    krabka_ids::Offset,
    krabka_protocol::{
        Decode,
        kafka_3_6_2::owned::fetch_response::FetchResponse as LegacyFetchResponse,
        records::{Attributes, Record, RecordBatch, RecordsPayload},
    },
    krabka_records_legacy::{ParsedRecord, decode_message_set},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    support::InProcess,
};

/// The `CreateTime` of the uncompressed record, which Fetch v3 carries as the
/// legacy message's timestamp.
const PLAIN_TIMESTAMP: i64 = 1_700_000_000_000;

#[derive(Default)]
struct LegacyCompression {
    process: Option<InProcess>,
    response: Option<LegacyFetchResponse>,
}

#[fixture]
fn compression() -> LegacyCompression {
    LegacyCompression {
        ..LegacyCompression::default()
    }
}

/// Starts a legacy-enabled broker with one single-partition topic.
async fn start_with_topic(topic: &str) -> InProcess {
    let process = support::start_legacy().await;
    let created = process
        .client
        .send(create_named_topic_request().name(topic).call())
        .await
        .expect("CreateTopics");
    assert!(
        created.topics[0].error_code == 0,
        "CreateTopics error: {}",
        created.topics[0].error_code
    );
    process
}

fn zstd_batch() -> RecordBatch {
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
    RecordBatch {
        attributes: Attributes::default().with_compression(CompressionType::Zstd),
        last_offset_delta: 49,
        records,
        ..Default::default()
    }
}

fn plain_batch() -> RecordBatch {
    RecordBatch {
        base_timestamp: PLAIN_TIMESTAMP,
        max_timestamp: PLAIN_TIMESTAMP,
        records: vec![Record {
            key: Some(Bytes::from_static(b"plain-key")),
            value: Some(Bytes::from_static(b"plain-value")),
            ..Default::default()
        }],
        ..Default::default()
    }
}

async fn fetch_v3(compression: &mut LegacyCompression, topic: &str) {
    let process = compression
        .process
        .as_ref()
        .expect("the topic was produced to");
    let body = fetch_legacy_raw(process.broker.listen_addr(), topic, 3).await;
    compression.response = Some(
        LegacyFetchResponse::decode(&mut body.as_slice(), 3)
            .expect("decode legacy Fetch v3 response"),
    );
}

#[given("a modern topic holds fifty zstd-compressed records")]
async fn zstd_records(compression: &mut LegacyCompression) {
    let process = start_with_topic("legacy_fetch_zstd").await;
    produce_batch(
        process.broker.listen_addr(),
        "legacy_fetch_zstd",
        zstd_batch(),
    )
    .await;
    compression.process = Some(process);
}

#[given("a modern topic holds an uncompressed record followed by fifty zstd-compressed records")]
async fn mixed_records(compression: &mut LegacyCompression) {
    let process = start_with_topic("legacy_fetch_mixed").await;
    let addr = process.broker.listen_addr();
    produce_batch(addr, "legacy_fetch_mixed", plain_batch()).await;
    produce_batch(addr, "legacy_fetch_mixed", zstd_batch()).await;
    compression.process = Some(process);
}

#[when("a Fetch v3 consumer reads the zstd-compressed partition from its start")]
async fn fetch_zstd(compression: &mut LegacyCompression) {
    fetch_v3(compression, "legacy_fetch_zstd").await;
}

#[when("a Fetch v3 consumer reads the mixed partition from its start")]
async fn fetch_mixed(compression: &mut LegacyCompression) {
    fetch_v3(compression, "legacy_fetch_mixed").await;
}

#[then("the partition reports UNSUPPORTED_COMPRESSION_TYPE with an empty record set")]
fn unsupported_compression(compression: &LegacyCompression) {
    let response = compression.response.as_ref().expect("Fetch completed");
    let partition = &response.responses[0].partitions[0];
    assert!(
        (partition.error_code, &partition.records)
            == (
                codes::UNSUPPORTED_COMPRESSION_TYPE,
                &Some(RecordsPayload::Legacy(Bytes::new()))
            ),
        "expected UNSUPPORTED_COMPRESSION_TYPE with an empty record set"
    );
}

#[then("the partition succeeds with only the uncompressed record")]
fn only_plain_record(compression: &LegacyCompression) {
    let response = compression.response.as_ref().expect("Fetch completed");
    let partition = &response.responses[0].partitions[0];
    let bytes = legacy_bytes(
        partition.error_code,
        partition.records.as_ref(),
        "Fetch v3 must return a Legacy MessageSet",
    );
    let records =
        decode_message_set(&mut bytes.as_ref(), bytes.len()).expect("decode legacy MessageSet");
    assert!(
        records
            == vec![ParsedRecord {
                offset: Offset(0),
                timestamp: Some(PLAIN_TIMESTAMP),
                key: Some(Bytes::from_static(b"plain-key")),
                value: Some(Bytes::from_static(b"plain-value")),
            }]
    );
}

#[scenario(
    path = "tests/features/migrated/tests/legacy_fetch/recompression.feature",
    name = "A legacy consumer gets UNSUPPORTED_COMPRESSION_TYPE for a partition of zstd records"
)]
#[tokio::test]
async fn fetch_v3_refuses_zstd_partition(compression: LegacyCompression) {
    compression
        .process
        .expect("a broker was started")
        .broker
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/legacy_fetch/recompression.feature",
    name = "A legacy consumer receives the records written before the first zstd batch"
)]
#[tokio::test]
async fn fetch_v3_stops_before_zstd_batch(compression: LegacyCompression) {
    compression
        .process
        .expect("a broker was started")
        .broker
        .shutdown()
        .await;
}
