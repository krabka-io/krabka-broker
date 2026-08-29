//! The client-facing requests the witness tests send and the one partition
//! view they compare against.
//!
//! `CreateTopics`, the `acks=all` `Produce`, and the rack-carrying consumer
//! `Fetch` are shared by more than one test, and `PartitionView` is the
//! whole-struct shape of the `Metadata` answer that the ISR assertions compare
//! in one piece. Keeping them together puts every wire shape this suite pins
//! down in one file.

use std::collections::BTreeSet;

use assert2::assert;
use krabka_broker::codes;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};

use crate::TOPIC;

/// Create `TOPIC` with one partition and rf=3, and return its id.
pub(crate) async fn create_topic(client: &Client) -> WireUuid {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: TOPIC.into(),
                num_partitions: 1,
                replication_factor: 3,
                ..Default::default()
            }],
            timeout_ms: 10_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == codes::NONE,
        "CreateTopics {TOPIC}: error_code={}",
        resp.topics[0].error_code
    );
    resp.topics[0].topic_id
}

fn record_batch(n: i32) -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        last_offset_delta: (n - 1).max(0),
        records: (0..n)
            .map(|i| Record {
                offset_delta: i,
                value: Some(bytes::Bytes::from(format!("v{i}"))),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn produce_request(topic_id: WireUuid, n: i32) -> ProduceRequest {
    ProduceRequest {
        acks: -1,
        timeout_ms: 10_000,
        topic_data: vec![TopicProduceData {
            name: TOPIC.into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(record_batch(n).into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// The partition-level error code of an `acks=all` produce of `n` records.
pub(crate) async fn produce_error(client: &Client, topic_id: WireUuid, n: i32) -> i16 {
    let resp = client
        .send(produce_request(topic_id, n))
        .await
        .expect("Produce round-trip");
    resp.responses[0].partition_responses[0].error_code
}

/// A consumer `Fetch` (`replica_id` = -1) carrying `rack`.
pub(crate) fn consumer_fetch(topic_id: WireUuid, rack: &str) -> FetchRequest {
    FetchRequest {
        replica_id: -1,
        max_wait_ms: 800,
        min_bytes: 0,
        max_bytes: 10_485_760,
        session_id: 0,
        session_epoch: -1, // sessionless full fetch
        rack_id: rack.to_string(),
        topics: vec![FetchTopic {
            topic: TOPIC.into(),
            topic_id,
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset: 0,
                current_leader_epoch: -1,
                partition_max_bytes: 1_048_576,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// The metadata a Kafka admin tool renders for one partition, as one value.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PartitionView {
    pub(crate) error_code: i16,
    pub(crate) partition_index: i32,
    pub(crate) leader_id: i32,
    pub(crate) replica_nodes: Vec<i32>,
    pub(crate) isr_nodes: BTreeSet<i32>,
    pub(crate) offline_replicas: Vec<i32>,
}

pub(crate) async fn partition_view(client: &Client) -> PartitionView {
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(TOPIC.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata for the topic");
    let partition = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(TOPIC))
        .and_then(|t| t.partitions.first())
        .expect("the topic has partition 0");
    PartitionView {
        error_code: partition.error_code,
        partition_index: partition.partition_index,
        leader_id: partition.leader_id,
        replica_nodes: partition.replica_nodes.clone(),
        isr_nodes: partition.isr_nodes.iter().copied().collect(),
        offline_replicas: partition.offline_replicas.clone(),
    }
}
