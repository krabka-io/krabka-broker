//! Exact single-partition requests for replication and durability scenarios.

use assert2::assert;
use bytes::Bytes;
use krabka_broker::BrokerHandle;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::create_topics_request::CreateTopicsRequest,
    records::{Record, RecordBatch},
};

use super::topic_id_for;

pub fn record_batch_with_values(values: &[&str]) -> RecordBatch {
    let mut batch = RecordBatch {
        last_offset_delta: (i32::try_from(values.len()).unwrap() - 1).max(0),
        max_timestamp: i64::try_from(values.len()).unwrap(),
        ..RecordBatch::default()
    };
    for (i, v) in values.iter().enumerate() {
        batch.records.push(Record {
            offset_delta: i32::try_from(i).unwrap(),
            value: Some(Bytes::from(v.to_string())),
            ..Default::default()
        });
    }
    batch
}

pub async fn create_topic_on_replicas(broker: &BrokerHandle, bootstrap: &str, name: &str, rf: i16) {
    let client = Client::builder()
        .bootstrap(bootstrap.to_string())
        .build()
        .await
        .unwrap();
    let replicas: Vec<i32> = (1..=i32::from(rf)).collect();
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![super::topic_on(name, &[&replicas])],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics failed: {resp:?}"
    );
    // CreateTopics ack means the controller's quorum committed the
    // metadata record, but the supervisor's reconcile loop materializes
    // the partition locally asynchronously. Wait until it appears so
    // subsequent Produce/Fetch don't race the materialization.
    broker.wait_until_partition_present(name, 0).await;
}

pub async fn produce_batch(
    bootstrap: &str,
    topic: &str,
    batch: RecordBatch,
    acks: i16,
    timeout_ms: i32,
) -> Result<i64, i16> {
    let client = Client::builder()
        .bootstrap(bootstrap.to_string())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, topic).await;
    let pr = super::client::produce_batch(&client, topic, topic_id, batch, acks, timeout_ms).await;
    if pr.error_code == 0 {
        Ok(pr.base_offset)
    } else {
        Err(pr.error_code)
    }
}

pub async fn produce_acks(
    bootstrap: &str,
    topic: &str,
    values: &[&str],
    acks: i16,
    timeout_ms: i32,
) -> Result<i64, i16> {
    produce_batch(
        bootstrap,
        topic,
        record_batch_with_values(values),
        acks,
        timeout_ms,
    )
    .await
}
