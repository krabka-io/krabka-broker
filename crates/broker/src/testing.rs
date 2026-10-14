//! Builders for requests shared by integration and unit tests.

/// Build a barrier marker batch using the broker's production wire encoder.
///
/// # Errors
///
/// Returns an error if the marker cannot be encoded as a record batch.
pub fn barrier_marker_batch(
    group: &str,
    epoch: i64,
    triggered_at: i64,
    base_offset: i64,
) -> Result<krabka_protocol::records::RecordBatch, crate::error::BrokerError> {
    crate::barrier::marker::build_barrier_batch(
        &crate::barrier::marker::BarrierMarker {
            group: group.to_owned(),
            epoch,
            triggered_at,
        },
        krabka_log::Offset(base_offset),
        0,
    )
}

/// Build the one-topic, one-partition Produce request shared by live broker
/// integration fixtures.
#[must_use]
pub fn single_partition_produce_request(
    topic: impl Into<String>,
    topic_id: uuid::Uuid,
    partition_index: i32,
    batch: krabka_protocol::records::RecordBatch,
    acks: i16,
    timeout_ms: i32,
) -> krabka_protocol::owned::produce_request::ProduceRequest {
    use krabka_protocol::{
        owned::produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        primitives::uuid::Uuid as WireUuid,
    };

    ProduceRequest {
        transactional_id: None,
        acks,
        timeout_ms,
        topic_data: vec![TopicProduceData {
            name: topic.into(),
            topic_id: WireUuid(*topic_id.as_bytes()),
            partition_data: vec![PartitionProduceData {
                index: partition_index,
                records: Some(batch.into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Build one ordinary record batch for fixtures that need only its offset,
/// leader epoch, timestamp, and value.
#[bon::builder]
pub fn single_value_batch(
    value: bytes::Bytes,
    #[builder(default)] base_offset: i64,
    #[builder(default)] leader_epoch: i32,
    #[builder(default)] timestamp_ms: i64,
) -> krabka_protocol::records::RecordBatch {
    use krabka_protocol::records::{Attributes, Record, RecordBatch};

    RecordBatch {
        base_offset,
        partition_leader_epoch: leader_epoch,
        attributes: Attributes::default(),
        last_offset_delta: 0,
        base_timestamp: timestamp_ms,
        max_timestamp: timestamp_ms,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: vec![Record {
            attributes: 0,
            offset_delta: 0,
            timestamp_delta: 0,
            key: None,
            value: Some(value),
            headers: vec![],
        }],
    }
}

/// Build the one-partition `ListOffsets` requests used by the broker and
/// restore round-trip suites.
#[bon::builder]
pub fn list_offsets_request(
    topic: String,
    partition_index: i32,
    timestamp: i64,
    #[builder(default)] current_leader_epoch: i32,
    #[builder(default)] isolation_level: i8,
) -> krabka_protocol::owned::list_offsets_request::ListOffsetsRequest {
    use krabka_protocol::owned::list_offsets_request::{
        ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
    };

    ListOffsetsRequest {
        replica_id: -1,
        isolation_level,
        topics: vec![ListOffsetsTopic {
            name: topic,
            partitions: vec![ListOffsetsPartition {
                partition_index,
                current_leader_epoch,
                timestamp,
                ..Default::default()
            }],
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    }
}

/// Build a Produce request whose topic contains one partition row per entry.
#[bon::builder]
pub fn produce_partitions_request(
    acks: i16,
    timeout_ms: i32,
    topic: String,
    topic_id: krabka_protocol::primitives::uuid::Uuid,
    partitions: Vec<krabka_protocol::owned::produce_request::PartitionProduceData>,
) -> krabka_protocol::owned::produce_request::ProduceRequest {
    use krabka_protocol::owned::produce_request::{ProduceRequest, TopicProduceData};

    ProduceRequest {
        acks,
        timeout_ms,
        topic_data: vec![TopicProduceData {
            name: topic,
            topic_id,
            partition_data: partitions,
            ..Default::default()
        }],
        ..Default::default()
    }
}
