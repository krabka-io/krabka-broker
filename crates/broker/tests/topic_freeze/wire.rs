//! The request shapes every case sends, and the vocabulary it judges a produce by.
//!
//! A freeze case asks "produce once, then tell me what happened", and
//! [`ProduceOutcome`] answers in the four terms that together decide the
//! feature: the error code, the `error_message` the producer's on-call reads,
//! the `base_offset` the row reports, and the partition's log end offset
//! afterwards. [`accepted`] and [`refused`] build the two expected values, so
//! a case compares one whole struct instead of four fields that could each
//! pass while the outcome is wrong.

use std::time::{SystemTime, UNIX_EPOCH};

use assert2::assert;
use bytes::Bytes;
use krabka_broker::{BrokerHandle, codes};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::PartitionProduceResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch, RecordsPayload},
};

use crate::support;

/// The unfrozen topic that every case produces to beside the frozen one.
pub(super) const CONTROL: &str = "control";

/// The wire's "no offset assigned", which is `ProduceResponse.INVALID_OFFSET`.
/// A row refused before any append carries it in `base_offset`.
const INVALID_OFFSET: i64 = -1;

/// Milliseconds since the Unix epoch, which is what `set_at_ms` carries.
pub(super) fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("a clock after 1970")
            .as_millis(),
    )
    .expect("a timestamp inside i64")
}

/// Create a one-partition topic and wait for its partition to exist locally.
pub(super) async fn create_topic(broker: &BrokerHandle, client: &Client, name: &str) -> WireUuid {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    let created = &resp.topics[0];
    assert!(
        created.error_code == 0,
        "create {name}: {:?}",
        created.error_message
    );
    broker.wait_until_partition_present(name, 0).await;
    support::topic_id_for(client, name).await
}

/// A single-record batch, in the shape a plain (non-idempotent) producer sends.
fn one_record(value: &str) -> RecordBatch {
    let mut batch = RecordBatch {
        last_offset_delta: 0,
        max_timestamp: 12_345,
        producer_id: -1,
        ..RecordBatch::default()
    };
    batch.records.push(Record {
        offset_delta: 0,
        value: Some(Bytes::from(value.to_owned())),
        ..Default::default()
    });
    batch
}

/// Produce one record and hand back the partition row.
pub(super) async fn produce(
    client: &Client,
    topic: &str,
    topic_id: WireUuid,
) -> PartitionProduceResponse {
    let resp = client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: topic.into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(RecordsPayload::V2(vec![one_record("v")])),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce");
    resp.responses[0].partition_responses[0].clone()
}

/// What one produce did, in the four terms a freeze case cares about.
///
/// The log end offset is part of the value rather than a second assertion,
/// because the two have to be read together: a `POLICY_VIOLATION` that still
/// moved the log is a pass on the error code and a catastrophe on the feature.
///
/// `base_offset` is read off the wire for the same reason. A refused row has
/// no offset to report, and Kafka says so with -1; a row that answered
/// `POLICY_VIOLATION` and still claimed offset 0 would tell a producer its
/// batch landed at the head of the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProduceOutcome {
    error_code: i16,
    error_message: Option<String>,
    base_offset: i64,
    log_end_offset: Option<i64>,
}

/// Produce one record and read the partition's log end offset afterwards.
pub(super) async fn produce_outcome(
    broker: &BrokerHandle,
    client: &Client,
    topic: &str,
    topic_id: WireUuid,
) -> ProduceOutcome {
    let response = produce(client, topic, topic_id).await;
    ProduceOutcome {
        error_code: response.error_code,
        error_message: response.error_message,
        base_offset: response.base_offset,
        log_end_offset: broker.local_log_end_offset(topic, 0),
    }
}

/// The outcome of an accepted produce that leaves the log at `log_end_offset`.
///
/// Every produce here sends one single-record batch, so the offset it was
/// assigned is the one the log ended just past.
pub(super) fn accepted(log_end_offset: i64) -> ProduceOutcome {
    ProduceOutcome {
        error_code: codes::NONE,
        error_message: None,
        base_offset: log_end_offset - 1,
        log_end_offset: Some(log_end_offset),
    }
}

/// The outcome of a produce that a freeze on `scope` refused, with the log
/// still at `log_end_offset`.
///
/// The message is spelled out rather than matched loosely. It is the only thing
/// KIP-108's `POLICY_VIOLATION` gives the producer's on-call engineer, and the
/// whole argument for reusing code 44 instead of minting a private one is that
/// the message carries the detail. A message that stopped naming the scope
/// would leave an operator with a non-retriable failure and no next step.
pub(super) fn refused(
    kind: &str,
    scope: &str,
    reason: &str,
    log_end_offset: i64,
) -> ProduceOutcome {
    ProduceOutcome {
        error_code: codes::POLICY_VIOLATION,
        error_message: Some(format!(
            "a write freeze on the {kind} scope {scope:?} refuses this write: {reason}"
        )),
        base_offset: INVALID_OFFSET,
        log_end_offset: Some(log_end_offset),
    }
}
