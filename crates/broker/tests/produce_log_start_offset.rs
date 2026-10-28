//! The `log_start_offset` an accepted `Produce` answers with.
//!
//! Kafka fills `LogAppendInfo.logStartOffset` from `UnifiedLog`'s own pointer
//! at append time, and `ReplicaManager` copies it straight into the partition
//! row, so an accepted produce reports where the partition's log actually
//! starts. Only the rows that refuse *before* any append carry the -1 that
//! `LogAppendInfo.UNKNOWN_LOG_APPEND_INFO` supplies.
//!
//! # Why the log is trimmed first
//!
//! A partition nobody has trimmed starts at 0, and 0 is also what a broker
//! that never filled the field at all would answer once the wire default
//! stopped being -1. So the case moves the pointer off 0 with a
//! `DeleteRecords` before it produces, and pins the produced row against the
//! low watermark that trim reported. A hardcoded 0 fails it, and so does the
//! -1 this suite was written for.
//!
//! # The values
//!
//! Settled against the pinned `mirror.gcr.io/apache/kafka:4.3.1` with a raw
//! `Produce v8`. Four single-record batches, then a
//! `kafka-delete-records --offset-json-file` to offset 3, which reported
//! `low_watermark: 3`, then one more produce:
//!
//! ```text
//! error_code=0 base_offset=4 log_append_time_ms=-1 log_start_offset=3
//! ```
//!
//! Those are the numbers this case asserts.
//!
//! Replaying the fourth batch's exact producer id, epoch and sequence — the
//! idempotent-retry path, which answers the already-assigned offset rather
//! than appending — answered with the same real `log_start_offset`, so the
//! dedup row carries it too. The dedup row also carries the retained batch's
//! timestamp in `log_append_time_ms`: `UnifiedLog.append` sets
//! `logAppendTime` from `BatchMetadata.timestamp`, which is the batch max
//! timestamp on a `CreateTime` topic.
#![cfg(test)]

use {
    crate::support::{
        client::produce_batch,
        offsets::{delete_records_partition, delete_records_request, delete_records_topic},
        records::{batch_from_records, value_record},
        topics::{creatable_topic, create_topic_request},
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_broker::codes,
    krabka_client_core::Client,
    krabka_protocol::{
        owned::{
            delete_records_response::DeleteRecordsPartitionResult,
            produce_response::PartitionProduceResponse,
        },
        primitives::uuid::Uuid as WireUuid,
        records::RecordBatch,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

mod support;

/// Records appended before the trim, so the partition ends at offset 4.
const APPENDED_BEFORE_TRIM: i64 = 4;

/// Where the `DeleteRecords` moves the partition's log start. It is neither 0
/// nor the -1 sentinel, which is the whole point of the fixture.
const TRIM_TO: i64 = 3;

/// The idempotent producer id the retry case sends under. Any non-negative
/// value works; the broker learns it from the first batch that carries it.
const PRODUCER_ID: i64 = 777;

/// The timestamp of every record this suite sends, and so its batches' max
/// timestamp. A duplicate row answers with it in `log_append_time_ms`.
const BATCH_MAX_TIMESTAMP: i64 = 12_345;

/// A broker holding the `orders` topic with its first records appended.
struct SeededOrders {
    process: support::InProcess,
    topic_id: WireUuid,
}

async fn seeded_orders() -> SeededOrders {
    let process = Box::pin(support::start()).await;
    let topic = create_topic(&process.broker, &process.client, "orders").await;
    for offset in 0..APPENDED_BEFORE_TRIM {
        check!(
            produce(&process.client, "orders", topic).await == accepted(offset, 0),
            "the untrimmed partition starts at 0"
        );
    }
    SeededOrders {
        process,
        topic_id: topic,
    }
}

struct LogStartScenario {
    seeded: Option<SeededOrders>,
    trimmed: Option<DeleteRecordsPartitionResult>,
    appended: Option<PartitionProduceResponse>,
    retried: Option<PartitionProduceResponse>,
}

#[fixture]
fn log_start() -> LogStartScenario {
    // Start this scenario with fresh inputs and observations.

    LogStartScenario {
        seeded: None,
        trimmed: None,
        appended: None,
        retried: None,
    }
}

#[given("the orders partition contains four acknowledged records")]
async fn seeded_partition(log_start: &mut LogStartScenario) {
    log_start.seeded = Some(seeded_orders().await);
}

#[when("DeleteRecords trims the partition to offset three")]
async fn trim_partition(log_start: &mut LogStartScenario) {
    let seeded = log_start.seeded.as_ref().expect("records were seeded");
    log_start.trimmed = Some(trim(&seeded.process.client, "orders", TRIM_TO).await);
}

#[then("the trim acknowledges low watermark three")]
fn trimmed_watermark(log_start: &LogStartScenario) {
    check!(
        log_start.trimmed.as_ref().expect("DeleteRecords completed")
            == &DeleteRecordsPartitionResult {
                partition_index: 0,
                low_watermark: TRIM_TO,
                error_code: codes::NONE,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            }
    );
}

#[when("a nonidempotent record is appended to the trimmed partition")]
async fn append_record(log_start: &mut LogStartScenario) {
    let seeded = log_start.seeded.as_ref().expect("records were seeded");
    log_start.appended = Some(produce(&seeded.process.client, "orders", seeded.topic_id).await);
}

#[when("producer 777 appends its first record to the trimmed partition")]
async fn append_idempotent(log_start: &mut LogStartScenario) {
    let seeded = log_start.seeded.as_ref().expect("records were seeded");
    log_start.appended = Some(
        produce_as(
            &seeded.process.client,
            "orders",
            seeded.topic_id,
            PRODUCER_ID,
            0,
        )
        .await,
    );
}

#[then("the accepted append reports base offset four and log start offset three")]
fn appended_watermark(log_start: &LogStartScenario) {
    assert!(
        log_start.appended.as_ref().expect("a record was produced")
            == &accepted(APPENDED_BEFORE_TRIM, TRIM_TO)
    );
}

#[when("producer 777 retries the identical epoch and sequence")]
async fn retry_record(log_start: &mut LogStartScenario) {
    let seeded = log_start.seeded.as_ref().expect("records were seeded");
    log_start.retried = Some(
        produce_as(
            &seeded.process.client,
            "orders",
            seeded.topic_id,
            PRODUCER_ID,
            0,
        )
        .await,
    );
}

#[then("the retry preserves the assigned offset timestamp and trimmed log start")]
fn retry_watermark(log_start: &LogStartScenario) {
    assert!(
        log_start.retried.as_ref().expect("the record was retried")
            == &PartitionProduceResponse {
                log_append_time_ms: BATCH_MAX_TIMESTAMP,
                ..accepted(APPENDED_BEFORE_TRIM, TRIM_TO)
            }
    );
    let seeded = log_start.seeded.as_ref().expect("records were seeded");
    check!(
        seeded.process.broker.local_log_end_offset("orders", 0) == Some(APPENDED_BEFORE_TRIM + 1)
    );
}

async fn stop_log_start(log_start: LogStartScenario) {
    log_start
        .seeded
        .expect("a partition was seeded")
        .process
        .broker
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/produce_log_start_offset.feature",
    name = "An appended record reports the real trimmed start of its partition"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_accepted_produce_reports_the_trimmed_log_start_offset(log_start: LogStartScenario) {
    stop_log_start(log_start).await;
}

#[scenario(
    path = "tests/features/migrated/tests/produce_log_start_offset.feature",
    name = "An idempotent retry reports the same trimmed start without appending"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idempotent_retry_reports_the_trimmed_log_start_offset(log_start: LogStartScenario) {
    stop_log_start(log_start).await;
}

/// The partition row an accepted produce answers with.
use crate::support::produce::expected_partition_success as accepted;

/// Create a one-partition topic and wait for its partition to exist locally.
async fn create_topic(
    broker: &krabka_broker::BrokerHandle,
    client: &Client,
    name: &str,
) -> WireUuid {
    let response = client
        .send(create_topic_request(
            creatable_topic().name(name.to_owned()).call(),
        ))
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == codes::NONE, "{response:?}");
    broker.wait_until_partition_present(name, 0).await;
    support::topic_id_for(client, name).await
}

/// Produce one single-record batch and hand back the whole partition row.
async fn produce(client: &Client, topic: &str, topic_id: WireUuid) -> PartitionProduceResponse {
    produce_as(client, topic, topic_id, -1, -1).await
}

/// Produce one single-record batch under the idempotent identity
/// `(producer_id, base_sequence)` and hand back the whole partition row.
///
/// A `producer_id` of -1 is "not idempotent", which is what [`produce`] sends.
async fn produce_as(
    client: &Client,
    topic: &str,
    topic_id: WireUuid,
    producer_id: i64,
    base_sequence: i32,
) -> PartitionProduceResponse {
    let batch = RecordBatch {
        last_offset_delta: 0,
        base_timestamp: BATCH_MAX_TIMESTAMP,
        max_timestamp: BATCH_MAX_TIMESTAMP,
        producer_id,
        producer_epoch: 0,
        base_sequence,
        ..batch_from_records(vec![value_record(0, Some(Bytes::from_static(b"frame")))])
    };
    produce_batch()
        .client(client)
        .batch(batch)
        .topic(topic)
        .topic_id(topic_id)
        .call()
        .await
}

/// Trim partition 0 of `topic` to `offset` and hand back the whole row.
async fn trim(client: &Client, topic: &str, offset: i64) -> DeleteRecordsPartitionResult {
    let response = client
        .send(delete_records_request(
            vec![delete_records_topic(topic.to_owned(), vec![
                delete_records_partition(0, offset),
            ])],
            5_000,
        ))
        .await
        .expect("DeleteRecords");
    response.topics[0].partitions[0].clone()
}
