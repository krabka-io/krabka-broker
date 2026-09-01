//! The requests every case sends, and the batch builder that makes a record
//! batch of an exact wire length.

use assert2::assert;
use bytes::Bytes;
use krabka_broker::{BrokerHandle, codes};
use krabka_client_core::Client;
use krabka_compression::CompressionType;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::{LeaderIdAndEpoch, PartitionProduceResponse},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch, RecordsPayload},
};

/// Kafka's `max.message.bytes`, and its broker-wide default
/// `message.max.bytes`, which a topic that sets neither inherits.
pub(super) const MAX_MESSAGE_BYTES: &str = "max.message.bytes";

/// Kafka's `compression.type`, whose non-`producer` values make the broker
/// re-encode a batch whose codec differs before it stores it.
pub(super) const COMPRESSION_TYPE: &str = "compression.type";

/// Kafka's default for both keys: 1 MiB of records plus the 12-byte
/// `Records.LOG_OVERHEAD`. Read out of `apache/kafka:4.1.0` as the
/// `DEFAULT_CONFIG` synonym of an unset `max.message.bytes`.
pub(super) const KAFKA_DEFAULT: usize = 1_048_588;

/// Create a one-partition topic with `configs` and wait for its partition to
/// exist locally.
///
/// The `CreateTopics` row is asserted rather than ignored: an unrecognized
/// config key comes back as `INVALID_CONFIG` (40) there, which is the failure
/// this whole feature exists to remove.
pub(super) async fn create_topic(
    broker: &BrokerHandle,
    client: &Client,
    name: &str,
    configs: &[(&str, &str)],
) -> WireUuid {
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.to_owned(),
                num_partitions: 1,
                replication_factor: 1,
                configs: configs
                    .iter()
                    .map(|(name, value)| CreatableTopicConfig {
                        name: (*name).to_owned(),
                        value: Some((*value).to_owned()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    let created = &response.topics[0];
    assert!(
        created.error_code == codes::NONE,
        "create {name} with {configs:?}: {created:?}"
    );
    broker.wait_until_partition_present(name, 0).await;
    crate::support::topic_id_for(client, name).await
}

/// A single-record batch whose complete v2 wire encoding is exactly
/// `target` bytes.
///
/// The record's value carries the slack. Its length is a varint, so growing
/// the value can grow the encoding by more than one byte; the loop re-measures
/// and converges rather than assuming a fixed overhead. `target` must leave
/// room for the 61-byte batch header and the record framing around an empty
/// value, which every caller's cap does.
pub(super) fn batch_of_wire_len(target: usize) -> RecordBatch {
    let mut value_len = target.saturating_sub(70);
    for _ in 0..8 {
        let batch = one_record(value_len);
        let encoded = batch.encoded_len();
        if encoded == target {
            return batch;
        }
        value_len = value_len
            .checked_add(target)
            .and_then(|grown| grown.checked_sub(encoded))
            .expect("a target that leaves room for the batch framing");
    }
    panic!("no value length encodes a batch of exactly {target} bytes");
}

fn one_record(value_len: usize) -> RecordBatch {
    RecordBatch {
        last_offset_delta: 0,
        max_timestamp: 12_345,
        producer_id: -1,
        records: vec![Record {
            offset_delta: 0,
            value: Some(Bytes::from(vec![b'x'; value_len])),
            ..Default::default()
        }],
        ..RecordBatch::default()
    }
}

/// A single-record gzip batch carrying `value_len` repeated bytes.
///
/// Repeated bytes are the point: gzip shrinks them by three orders of
/// magnitude, so the batch that arrives on the wire is tiny and the batch a
/// topic with `compression.type=uncompressed` stores is not.
pub(super) fn gzip_batch(value_len: usize) -> RecordBatch {
    let mut batch = one_record(value_len);
    batch.attributes = batch.attributes.with_compression(CompressionType::Gzip);
    batch
}

/// Bytes `batch` occupies on the wire, with its own compression applied.
pub(super) fn wire_len(batch: &RecordBatch) -> usize {
    let mut buf = bytes::BytesMut::new();
    batch.encode(&mut buf).expect("encode batch");
    buf.len()
}

/// Produce one batch of exactly `wire_len` bytes and hand back the whole
/// partition row.
pub(super) async fn produce_batch_of_wire_len(
    client: &Client,
    topic: &str,
    topic_id: WireUuid,
    wire_len: usize,
) -> PartitionProduceResponse {
    produce_batch(client, topic, topic_id, batch_of_wire_len(wire_len)).await
}

/// Produce `batch` and hand back the whole partition row.
pub(super) async fn produce_batch(
    client: &Client,
    topic: &str,
    topic_id: WireUuid,
    batch: RecordBatch,
) -> PartitionProduceResponse {
    let response = client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: topic.to_owned(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(RecordsPayload::V2(vec![batch])),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce");
    response.responses[0].partition_responses[0].clone()
}

/// The partition row an accepted produce answers with, at `base_offset`.
pub(super) fn accepted(base_offset: i64) -> PartitionProduceResponse {
    PartitionProduceResponse {
        index: 0,
        error_code: codes::NONE,
        base_offset,
        log_append_time_ms: -1,
        log_start_offset: -1,
        record_errors: vec![],
        error_message: None,
        current_leader: LeaderIdAndEpoch::default(),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    }
}

/// The partition row an oversized batch answers with.
///
/// `error_message` is `None` on purpose. Kafka does not attach a custom
/// message to `RecordTooLargeException`, so a JVM producer renders
/// `Errors.MESSAGE_TOO_LARGE`'s own text -- "The request included a message
/// larger than the max message size the server will accept." -- which is what
/// `apache/kafka:4.1.0` printed for the refused batch this suite is modelled
/// on. A broker that sent its own message would replace that familiar line
/// with an unfamiliar one.
///
/// `base_offset` is -1, the "no offset assigned" sentinel. The refusal lands
/// before any append, so Kafka fills the row from
/// `LogAppendInfo.UNKNOWN_LOG_APPEND_INFO` and every offset in it is -1. A raw
/// `Produce v9` against `apache/kafka:4.3.1` with `max.message.bytes=2048`
/// answers a 2049-byte batch with exactly this row: error code 10,
/// `base_offset=-1`, `log_append_time_ms=-1`, `log_start_offset=-1`, no record
/// errors and no error message.
pub(super) fn too_large() -> PartitionProduceResponse {
    PartitionProduceResponse {
        index: 0,
        error_code: codes::MESSAGE_TOO_LARGE,
        base_offset: -1,
        log_append_time_ms: -1,
        log_start_offset: -1,
        record_errors: vec![],
        error_message: None,
        current_leader: LeaderIdAndEpoch::default(),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    }
}
