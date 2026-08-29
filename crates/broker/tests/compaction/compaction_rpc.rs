//! The typed requests that set the scenario up: creating the compacted topic
//! with its config overrides, resolving its `topic_id`, and producing one
//! keyed record.
//!
//! Each helper encodes a request body, hands it to `round_trip`, and decodes
//! the matching response, so the API versions this suite pins are all stated
//! in one file.

use std::net::SocketAddr;

use assert2::assert;
use bytes::{Bytes, BytesMut};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::MetadataResponse,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid,
    records::{Record, RecordBatch},
};
use tokio::net::TcpStream;

use crate::compaction_wire::round_trip;

/// Create a topic with config overrides, on PLAINTEXT and with no SASL.
pub(crate) async fn create_topic_with_configs(
    addr: SocketAddr,
    topic: &str,
    partitions: i32,
    rf: i16,
    configs: Vec<(&str, &str)>,
) {
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.to_string(),
            num_partitions: partitions,
            replication_factor: rf,
            configs: configs
                .into_iter()
                .map(|(name, value)| CreatableTopicConfig {
                    name: name.to_string(),
                    value: Some(value.to_string()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };

    let version: i16 = 7; // flexible
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, version).expect("encode CreateTopics");
    let resp_bytes = round_trip(&mut stream, 19, version, 1, true, &body)
        .await
        .expect("CreateTopics round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp =
        CreateTopicsResponse::decode(&mut cur, version).expect("decode CreateTopicsResponse");
    assert!(resp.topics.len() == 1);
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics({topic}) must succeed: {:?}",
        resp.topics[0].error_message
    );
}

/// Get `topic_id` with Metadata. Produce and Fetch v9+ need it.
pub(crate) async fn get_topic_id(addr: SocketAddr, topic: &str) -> Uuid {
    let req = MetadataRequest {
        topics: Some(vec![MetadataRequestTopic {
            name: Some(topic.to_string()),
            ..Default::default()
        }]),
        ..Default::default()
    };
    let version: i16 = 12; // flexible
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, version).expect("encode Metadata");
    let resp_bytes = round_trip(&mut stream, 3, version, 1, true, &body)
        .await
        .expect("Metadata round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = MetadataResponse::decode(&mut cur, version).expect("decode MetadataResponse");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

/// Produce one record with an explicit key and value to (topic, partition 0).
pub(crate) async fn produce_record(
    addr: SocketAddr,
    topic: &str,
    topic_id: Uuid,
    key: &[u8],
    value: &[u8],
) {
    let record = Record {
        offset_delta: 0,
        key: Some(Bytes::copy_from_slice(key)),
        value: Some(Bytes::copy_from_slice(value)),
        ..Default::default()
    };
    let batch = RecordBatch {
        last_offset_delta: 0,
        records: vec![record],
        ..Default::default()
    };

    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(batch.into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let version: i16 = 9; // flexible, pre-KIP-516 (no topic_id required on the wire at v9)
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, version).expect("encode Produce");
    let resp_bytes = round_trip(&mut stream, 0, version, 1, true, &body)
        .await
        .expect("Produce round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = ProduceResponse::decode(&mut cur, version).expect("decode ProduceResponse");
    let part = &resp.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "Produce must succeed: error_code={}",
        part.error_code
    );
}
