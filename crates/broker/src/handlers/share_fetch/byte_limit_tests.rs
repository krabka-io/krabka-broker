//! Handler tests for how the `ShareFetch` byte limit bounds acquisition.
//!
//! Kafka reads the log first and acquires second: `SharePartition.acquire`
//! acquires no offset past the last batch that the read returned. Every
//! offset in `AcquiredRecords` therefore has its record in `Records`. The Java
//! share consumer treats an acquired offset with no record as a gap, and
//! acknowledges it as `Gap`, which archives the record without delivery.

use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
        share_fetch_request::{
            AcknowledgementBatch, FetchPartition, FetchTopic, ShareFetchRequest,
        },
        share_fetch_response::{PartitionData, ShareFetchResponse},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch, RecordsPayload},
};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};

use super::handle;
use crate::{
    authorizer::AllowAllAuthorizer,
    broker::BrokerHandle,
    codes,
    test_support::{
        decode_response, encode_request, peer, principal, request_context, start_broker_with,
    },
};

/// The number of batches that each topic holds.
const BATCHES: i64 = 4;

/// The records in each batch.
const RECORDS_PER_BATCH: i64 = 2;

/// Produce v12 names the topic.
const PRODUCE_VERSION: i16 = 12;

async fn start() -> (BrokerHandle, tempfile::TempDir) {
    start_with_delivery_attempts(5).await
}

async fn start_with_delivery_attempts(attempts: i16) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        cfg.share_group.enable = true;
        cfg.share_group.max_delivery_attempts = attempts;
    })
    .await
}

async fn create_topic(broker: &BrokerHandle, name: &str) -> WireUuid {
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("share-fetch-byte-limit-test")
        .build()
        .await
        .expect("client build");
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.to_string(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == codes::NONE, "{response:?}");
    broker.wait_until_partition_present(name, 0).await;
    let image = broker.controller_image_for_test();
    let topic = image.topic(name).expect("created topic in the image");
    WireUuid(topic.topic_id.into_bytes())
}

/// Appends [`BATCHES`] batches of the same size, each in its own produce.
async fn produce_batches(broker: &BrokerHandle, topic: &str) {
    let shared = broker.broker_arc_for_test();
    let user = principal("producer");
    let address = peer();
    let ctx = request_context(&user, &address, "producer-client");
    for _ in 0..BATCHES {
        let request = ProduceRequest {
            acks: -1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: topic.to_string(),
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(RecordsPayload::V2(vec![RecordBatch {
                        last_offset_delta: i32::try_from(RECORDS_PER_BATCH - 1)
                            .expect("small batch"),
                        records: (0..RECORDS_PER_BATCH)
                            .map(|delta| Record {
                                offset_delta: i32::try_from(delta).expect("small batch"),
                                value: Some(Bytes::from(vec![b'v'; 256])),
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }])),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let request_bytes = encode_request(&request, PRODUCE_VERSION);
        let response_bytes = crate::handlers::produce::handle(
            &shared,
            PRODUCE_VERSION,
            7,
            &request_bytes,
            request_bytes.clone(),
            &ctx,
        )
        .await
        .expect("handle produce");
        let response: ProduceResponse = decode_response(&response_bytes, PRODUCE_VERSION);
        let partition = &response.responses[0].partition_responses[0];
        assert!(partition.error_code == codes::NONE, "{response:?}");
    }
}

/// The size in bytes of the first batch of `topic`. Every batch has this size.
fn batch_size(broker: &BrokerHandle, topic: &str) -> i32 {
    let shared = broker.broker_arc_for_test();
    let partition = shared
        .partitions
        .get(topic, PartitionIndex(0))
        .expect("local partition");
    let log = partition.log.lock().expect("log lock");
    let read = log
        .read_raw(Offset(0), Offset(1), ByteSize::from_bytes(0))
        .expect("read the first batch");
    i32::try_from(read.total).expect("small batch")
}

async fn share_fetch(
    broker: &BrokerHandle,
    group: &str,
    member: &str,
    epoch: i32,
    topic_id: WireUuid,
    (max_records, max_bytes): (i32, i32),
    acknowledgements: &[(i64, i64, i8)],
) -> ShareFetchResponse {
    let version = krabka_protocol::owned::share_fetch_request::MAX_VERSION;
    let request = ShareFetchRequest {
        group_id: Some(group.into()),
        member_id: Some(member.into()),
        share_session_epoch: epoch,
        max_wait_ms: 0,
        min_bytes: 0,
        max_bytes,
        max_records,
        batch_size: max_records,
        topics: vec![FetchTopic {
            topic_id,
            partitions: vec![FetchPartition {
                partition_index: 0,
                acknowledgement_batches: acknowledgements
                    .iter()
                    .map(
                        |&(first_offset, last_offset, ack_type)| AcknowledgementBatch {
                            first_offset,
                            last_offset,
                            acknowledge_types: vec![ack_type],
                            ..Default::default()
                        },
                    )
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let shared = broker.broker_arc_for_test();
    let user = principal("share-consumer");
    let address = peer();
    let ctx = request_context(&user, &address, "share-client");
    let request_bytes = encode_request(&request, version);
    let response = handle(&shared, version, 7, &request_bytes, &ctx)
        .await
        .expect("handle share fetch");
    decode_response(&response, version)
}

fn partition(response: &ShareFetchResponse) -> &PartitionData {
    &response.responses[0].partitions[0]
}

/// The `(first, last)` offsets of every acquired row.
fn acquired(row: &PartitionData) -> Vec<(i64, i64)> {
    row.acquired_records
        .iter()
        .map(|range| (range.first_offset, range.last_offset))
        .collect()
}

/// The offsets of every record that the row carries.
fn record_offsets(row: &PartitionData) -> Vec<i64> {
    row.records
        .as_ref()
        .and_then(RecordsPayload::as_v2)
        .map(|batches| {
            batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .records
                        .iter()
                        .map(move |record| batch.base_offset + i64::from(record.offset_delta))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// What one scenario observes: the offsets that the limited fetch acquired,
/// the record offsets that it carried, and the offsets that a later unlimited
/// fetch by another member acquired.
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    acquired: Vec<(i64, i64)>,
    records: Vec<i64>,
    remainder: Vec<(i64, i64)>,
}

/// One scenario: the record and byte limits of the first fetch, in batch
/// sizes, and the outcome that Kafka gives.
struct Case {
    name: &'static str,
    max_records: i32,
    /// The byte limit as `(numerator, denominator)` of one batch size.
    max_bytes_in_batches: (i32, i32),
    expected: Outcome,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "all-batches-fit",
            max_records: 500,
            max_bytes_in_batches: (4, 1),
            expected: Outcome {
                acquired: vec![(0, 7)],
                records: (0..=7).collect(),
                remainder: Vec::new(),
            },
        },
        Case {
            name: "one-batch-fits",
            max_records: 500,
            max_bytes_in_batches: (1, 1),
            expected: Outcome {
                acquired: vec![(0, 1)],
                records: vec![0, 1],
                remainder: vec![(2, 7)],
            },
        },
        Case {
            name: "less-than-one-batch",
            max_records: 500,
            max_bytes_in_batches: (1, 2),
            expected: Outcome {
                acquired: vec![(0, 1)],
                records: vec![0, 1],
                remainder: vec![(2, 7)],
            },
        },
        Case {
            name: "records-limit-below-bytes-limit",
            max_records: 3,
            max_bytes_in_batches: (4, 1),
            expected: Outcome {
                acquired: vec![(0, 2)],
                records: (0..=3).collect(),
                remainder: vec![(3, 7)],
            },
        },
    ]
}

#[tokio::test]
async fn every_acquired_offset_has_its_record_in_the_response() {
    let (broker, _dir) = start().await;

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for case in cases() {
        let topic = format!("byte-limit-{}", case.name);
        let group = format!("group-{}", case.name);
        let topic_id = create_topic(&broker, &topic).await;
        crate::test_support::initialize_share_state(
            &broker,
            &group,
            uuid::Uuid::from_bytes(topic_id.0),
            0,
        )
        .await;
        // Open both sessions on the empty log, so the share partition starts
        // at offset 0 under the default `latest` reset.
        for member in ["limited", "unlimited"] {
            let opened =
                share_fetch(&broker, &group, member, 0, topic_id, (500, 1 << 20), &[]).await;
            assert!(partition(&opened).error_code == codes::NONE, "{opened:?}");
        }
        produce_batches(&broker, &topic).await;
        let size = batch_size(&broker, &topic);
        let (numerator, denominator) = case.max_bytes_in_batches;

        let limited = share_fetch(
            &broker,
            &group,
            "limited",
            1,
            topic_id,
            (case.max_records, size * numerator / denominator),
            &[],
        )
        .await;
        let rest = share_fetch(
            &broker,
            &group,
            "unlimited",
            1,
            topic_id,
            (500, 1 << 20),
            &[],
        )
        .await;

        actual.push((
            case.name,
            Outcome {
                acquired: acquired(partition(&limited)),
                records: record_offsets(partition(&limited)),
                remainder: acquired(partition(&rest)),
            },
        ));
        expected.push((case.name, case.expected));
    }

    assert!(actual == expected);
    broker.shutdown().await;
}

/// The acknowledge type `Release`.
const RELEASE: i8 = 2;

/// The log read starts at the first record that the partition can still
/// deliver, so a released record at the delivery limit must be archived
/// before the read. Otherwise it stays `Available`, the window cannot grow
/// past it, and the partition never delivers a later record.
#[tokio::test]
async fn a_record_at_the_delivery_limit_does_not_stall_the_partition() {
    let (broker, _dir) = start_with_delivery_attempts(1).await;
    let topic_id = create_topic(&broker, "delivery-limit").await;
    crate::test_support::initialize_share_state(
        &broker,
        "g",
        uuid::Uuid::from_bytes(topic_id.0),
        0,
    )
    .await;
    let opened = share_fetch(&broker, "g", "m", 0, topic_id, (500, 1 << 20), &[]).await;
    assert!(partition(&opened).error_code == codes::NONE, "{opened:?}");
    produce_batches(&broker, "delivery-limit").await;
    let first = share_fetch(&broker, "g", "m", 1, topic_id, (500, 1 << 20), &[]).await;
    produce_batches(&broker, "delivery-limit").await;

    // Release every record at its only delivery attempt, and fetch.
    let after_release = share_fetch(
        &broker,
        "g",
        "m",
        2,
        topic_id,
        (500, 1 << 20),
        &[(0, 7, RELEASE)],
    )
    .await;

    assert!(
        (
            acquired(partition(&first)),
            acquired(partition(&after_release))
        ) == (vec![(0, 7)], vec![(8, 15)])
    );
    broker.shutdown().await;
}
