//! Exact wire requests shared by transaction coordinator scenarios.

use assert2::assert;
use bytes::Bytes;
use krabka_protocol::{
    owned::{
        add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction},
        add_partitions_to_txn_response::AddPartitionsToTxnResponse,
        common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
        init_producer_id_request::InitProducerIdRequest,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid,
    records::{Attributes, Record, RecordBatch},
};

pub(crate) fn init_producer_request(transactional_id: &str) -> InitProducerIdRequest {
    InitProducerIdRequest {
        transactional_id: Some(transactional_id.into()),
        transaction_timeout_ms: 60_000,
        producer_id: -1,
        producer_epoch: -1,
        ..Default::default()
    }
}

/// Supply both layouts so the client can negotiate either protocol version.
pub(crate) fn add_partition_request(
    transactional_id: &str,
    topic: &str,
    (producer_id, epoch): (i64, i16),
) -> AddPartitionsToTxnRequest {
    let topic = AddPartitionsToTxnTopic {
        name: topic.into(),
        partitions: vec![0],
        ..Default::default()
    };
    AddPartitionsToTxnRequest {
        transactions: vec![AddPartitionsToTxnTransaction {
            transactional_id: transactional_id.into(),
            producer_id,
            producer_epoch: epoch,
            topics: vec![topic.clone()],
            ..Default::default()
        }],
        v3_and_below_transactional_id: transactional_id.into(),
        v3_and_below_producer_id: producer_id,
        v3_and_below_producer_epoch: epoch,
        v3_and_below_topics: vec![topic],
        ..Default::default()
    }
}

pub(crate) fn assert_partition_added(response: &AddPartitionsToTxnResponse) {
    let code = response
        .results_by_transaction
        .first()
        .and_then(|transaction| transaction.topic_results.first())
        .and_then(|row| row.results_by_partition.first())
        .map_or(response.error_code, |row| row.partition_error_code);
    assert!(code == 0, "AddPartitionsToTxn: {response:?}");
}

pub(crate) fn produce_request(
    transactional_id: &str,
    topic: &str,
    topic_id: Uuid,
    producer: Option<(i64, i16)>,
    values: &[&'static str],
) -> ProduceRequest {
    let records = i32::try_from(values.len()).expect("record count");
    let batch = RecordBatch {
        attributes: Attributes::default().with_transactional(producer.is_some()),
        producer_id: producer.map_or(-1, |(producer_id, _)| producer_id),
        producer_epoch: producer.map_or(-1, |(_, epoch)| epoch),
        base_sequence: if producer.is_some() { 0 } else { -1 },
        last_offset_delta: records - 1,
        max_timestamp: 1,
        records: values
            .iter()
            .zip(0..)
            .map(|(value, offset_delta)| Record {
                offset_delta,
                value: Some(Bytes::from_static(value.as_bytes())),
                ..Record::default()
            })
            .collect(),
        ..RecordBatch::default()
    };
    ProduceRequest {
        // KIP-890 verification needs the transactional id for transactional batches.
        transactional_id: producer.map(|_| transactional_id.to_string()),
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(batch.into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}
