//! The idempotent-producer sequence checks on the produce path.
//!
//! A batch that carries a producer id and a base sequence is deduplicated when
//! it repeats, and it is rejected when its sequence skips ahead, so both cases
//! drive the produce path with a producer-stamped batch.

use assert2::assert;
use krabka_protocol::{
    owned::{
        init_producer_id_request::InitProducerIdRequest,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    records::{Record, RecordBatch},
};

use crate::{
    harness::{create_topic, topic_id_for},
    support,
};

fn one_batch_with_producer(pid: i64, epoch: i16, base_seq: i32, values: &[&str]) -> RecordBatch {
    let n = i32::try_from(values.len()).expect("values.len fits i32");
    let mut records = Vec::with_capacity(values.len());
    for (i, v) in values.iter().enumerate() {
        records.push(Record {
            offset_delta: i32::try_from(i).expect("index fits i32"),
            value: Some(bytes::Bytes::from(v.to_string())),
            ..Default::default()
        });
    }
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

#[tokio::test]
async fn idempotent_produce_dedups_duplicate_batch() {
    let p = support::start().await;

    create_topic(&p, "idem", 1).await;
    let idem_id = topic_id_for(&p, "idem").await;

    let init = p
        .client
        .send(InitProducerIdRequest::default())
        .await
        .expect("InitProducerId");
    let pid = init.producer_id;

    let req = ProduceRequest {
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "idem".into(),
            topic_id: idem_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_batch_with_producer(pid, 0, 0, &["a", "b", "c"]).into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let r1 = p.client.send(req.clone()).await.expect("Produce 1");
    assert!(r1.responses[0].partition_responses[0].error_code == 0);
    assert!(r1.responses[0].partition_responses[0].base_offset == 0);

    // Send the same batch again — must be deduped (error 0, base_offset 0).
    let r2 = p.client.send(req).await.expect("Produce 2 (dup)");
    assert!(r2.responses[0].partition_responses[0].error_code == 0);
    assert!(r2.responses[0].partition_responses[0].base_offset == 0);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn out_of_order_returns_45() {
    let p = support::start().await;

    create_topic(&p, "ooo", 1).await;
    let ooo_id = topic_id_for(&p, "ooo").await;

    let init = p
        .client
        .send(InitProducerIdRequest::default())
        .await
        .expect("InitProducerId");
    let pid = init.producer_id;

    let mk = |base_seq: i32| ProduceRequest {
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "ooo".into(),
            topic_id: ooo_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_batch_with_producer(pid, 0, base_seq, &["x", "y"]).into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    // First batch (base_seq=0, 2 records → last_seq=1). Must succeed.
    let r1 = p.client.send(mk(0)).await.expect("Produce seq=0");
    assert!(r1.responses[0].partition_responses[0].error_code == 0);

    // Skip to base_seq=10 — gap → OUT_OF_ORDER_SEQUENCE_NUMBER (45).
    let r2 = p.client.send(mk(10)).await.expect("Produce seq=10");
    assert!(r2.responses[0].partition_responses[0].error_code == 45);

    p.broker.shutdown().await;
}
