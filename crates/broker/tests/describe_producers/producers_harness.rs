//! The setup every `DescribeProducers` test shares: creating a topic, reading
//! back its id, claiming a producer id, and building the record batches the
//! produce calls send.
//!
//! Both batch builders live here because the transactional one is the
//! idempotent one with the transactional attribute bit set.

use assert2::assert;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        init_producer_id_request::InitProducerIdRequest,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
    },
    records::{Attributes, Record, RecordBatch},
};

use crate::support;

pub(crate) async fn topic_id_for(
    p: &support::InProcess,
    name: &str,
) -> krabka_protocol::primitives::uuid::Uuid {
    let resp = p
        .client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

pub(crate) async fn create_topic(client: &Client, name: &str, partitions: i32) {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(resp.topics[0].error_code == 0, "{name} create: {resp:?}");
}

pub(crate) async fn init_producer(p: &support::InProcess) -> (i64, i16) {
    let init = p
        .client
        .send(InitProducerIdRequest::default())
        .await
        .expect("InitProducerId");
    (init.producer_id, init.producer_epoch)
}

pub(crate) fn batch(pid: i64, epoch: i16, base_seq: i32, values: &[&str]) -> RecordBatch {
    let n = i32::try_from(values.len()).expect("values.len fits i32");
    let records = values
        .iter()
        .enumerate()
        .map(|(i, v)| Record {
            offset_delta: i32::try_from(i).expect("index fits i32"),
            value: Some(bytes::Bytes::from(v.to_string())),
            ..Default::default()
        })
        .collect();
    RecordBatch {
        producer_id: pid,
        producer_epoch: epoch,
        base_sequence: base_seq,
        last_offset_delta: n - 1,
        max_timestamp: i64::from(n),
        records,
        ..Default::default()
    }
}

pub(crate) fn transactional_batch(
    pid: i64,
    epoch: i16,
    base_seq: i32,
    values: &[&str],
) -> RecordBatch {
    RecordBatch {
        attributes: Attributes::default().with_transactional(true),
        ..batch(pid, epoch, base_seq, values)
    }
}
