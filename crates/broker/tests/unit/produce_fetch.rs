//! The record round trip over one broker.
//!
//! A produce assigns base offsets, a fetch reads the batches back, and
//! `ListOffsets` reports the ends of an empty partition. All three run against
//! the same in-process broker.

use assert2::{assert, check};
use krabka_protocol::{
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    records::{Record, RecordBatch},
};

use crate::{
    harness::{create_topic, topic_id_for},
    support,
};

/// Builds one `RecordBatch` that carries `n` empty records with sequential
/// offset deltas.
fn one_record_batch(n: i32) -> RecordBatch {
    let mut b = RecordBatch {
        last_offset_delta: (n - 1).max(0),
        ..RecordBatch::default()
    };
    for i in 0..n {
        b.records.push(Record {
            offset_delta: i,
            ..Default::default()
        });
    }
    b
}

#[tokio::test]
async fn produce_assigns_base_offsets() {
    let p = support::start().await;
    create_topic(&p, "prod", 1).await;
    let topic_id = topic_id_for(&p, "prod").await;

    // First produce: 3 records → base 0.
    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "prod".into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_record_batch(3).into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = p.client.send(req).await.expect("Produce 1");
    assert!(resp.responses.len() == 1);
    assert!(resp.responses[0].partition_responses.len() == 1);
    check!(resp.responses[0].partition_responses[0].error_code == 0);
    check!(resp.responses[0].partition_responses[0].base_offset == 0);

    // Second produce: 2 records → base 3.
    let req2 = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "prod".into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_record_batch(2).into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp2 = p.client.send(req2).await.expect("Produce 2");
    assert!(resp2.responses[0].partition_responses[0].error_code == 0);
    assert!(resp2.responses[0].partition_responses[0].base_offset == 3);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn produce_to_unknown_topic_returns_3() {
    let p = support::start().await;
    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "nope".into(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_record_batch(1).into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = p.client.send(req).await.expect("Produce unknown");
    assert!(resp.responses[0].partition_responses[0].error_code == 3);
    p.broker.shutdown().await;
}

#[tokio::test]
async fn produce_then_fetch_round_trip() {
    let p = support::start().await;
    create_topic(&p, "round", 1).await;
    let topic_id = topic_id_for(&p, "round").await;

    let prod = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "round".into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_record_batch(3).into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let presp = p.client.send(prod).await.expect("Produce");
    assert!(presp.responses[0].partition_responses[0].error_code == 0);

    let fetch = FetchRequest {
        max_wait_ms: 100,
        min_bytes: 1,
        topics: vec![FetchTopic {
            topic: "round".into(),
            topic_id,
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset: 0,
                partition_max_bytes: 1_048_576,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let fresp = p.client.send(fetch).await.expect("Fetch");
    assert!(fresp.responses.len() == 1);
    let part = &fresp.responses[0].partitions[0];
    assert!(part.error_code == 0);
    let batches = part
        .records
        .as_ref()
        .and_then(|p| p.as_v2())
        .expect("v2 records must be present after produce");
    let total: usize = batches.iter().map(|b| b.records.len()).sum();
    assert!(total == 3);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn list_offsets_earliest_and_latest() {
    let p = support::start().await;
    create_topic(&p, "empty", 1).await;

    let mk = |ts: i64| ListOffsetsRequest {
        replica_id: -1,
        topics: vec![ListOffsetsTopic {
            name: "empty".into(),
            partitions: vec![ListOffsetsPartition {
                partition_index: 0,
                timestamp: ts,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let earliest = p.client.send(mk(-2)).await.expect("ListOffsets earliest");
    let latest = p.client.send(mk(-1)).await.expect("ListOffsets latest");
    for (label, resp) in [("earliest", &earliest), ("latest", &latest)] {
        check!(resp.topics[0].partitions[0].error_code == 0, "{label}");
        check!(resp.topics[0].partitions[0].offset == 0, "{label}");
    }

    p.broker.shutdown().await;
}
