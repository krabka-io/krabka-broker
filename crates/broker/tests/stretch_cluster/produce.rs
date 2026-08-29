//! The client side of the suite: a bootstrapped `Client`, the topic every test
//! writes to, and the `acks=all` produce whose partition-level error code is
//! what most of the claims are asserted on.

use std::time::{Duration, Instant};

use assert2::assert;
use krabka_broker::codes;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};

use crate::{N_RECORDS, TOPIC};

pub async fn client_at(addr: &str) -> Client {
    Client::builder()
        .bootstrap(addr.to_string())
        .client_id("stretch-cluster-test")
        .build()
        .await
        .expect("client build")
}

/// Create `TOPIC` with one partition and rf=3, and return its id.
pub async fn create_topic(client: &Client) -> WireUuid {
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

/// The partition-level error code of one `acks=all` produce.
pub async fn produce_once(client: &Client, topic_id: WireUuid, timeout_ms: i32) -> i16 {
    let resp = client
        .send(ProduceRequest {
            acks: -1,
            timeout_ms,
            topic_data: vec![TopicProduceData {
                name: TOPIC.into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(record_batch(N_RECORDS).into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce round-trip");
    resp.responses[0].partition_responses[0].error_code
}

/// `acks=all` against `addr`, retried until it commits or the bound expires.
///
/// The retry covers only the window in which the surviving replicas have not
/// yet been dropped from the ISR — the leader answers `REQUEST_TIMED_OUT` or
/// `NOT_ENOUGH_REPLICAS` until the controller commits the shrink. It never
/// turns a persistent refusal into a pass: the last code is what the caller
/// asserts on.
pub async fn produce_until_committed(addr: &str, topic_id: WireUuid) -> i16 {
    let client = client_at(addr).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut code = produce_once(&client, topic_id, 5_000).await;
    while code != codes::NONE && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        code = produce_once(&client, topic_id, 5_000).await;
    }
    code
}
