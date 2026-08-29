//! The byte format of `leader-epoch-checkpoint`, which Kafka's own tooling and
//! a restarting broker both parse: a version header, a row count, and one
//! `epoch offset` row per epoch.
//!
//! This is the only test in the suite that reads the file off disk rather than
//! going over the wire, which is why it stands on its own.

use assert2::check;
use krabka_client_core::Client;
use krabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};

use crate::epoch_harness::{boot_single, create_topic, record, topic_id_for};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn epoch_checkpoint_byte_compat() {
    let (broker, bootstrap, dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "ckpt").await;

    // Produce at epoch 0.
    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, "ckpt").await;
    client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "ckpt".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(record("v0").into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("produce");

    // Bump epoch to 1 + produce another.
    broker.test_set_leader_epoch("ckpt", 0, 1);
    client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "ckpt".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(record("v1").into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("produce");

    // Read the checkpoint file from disk.
    let path = dir.path().join("ckpt-0").join("leader-epoch-checkpoint");
    let s = std::fs::read_to_string(&path).expect("checkpoint file");
    // Format: header "0\n", count "2\n", rows "0 0\n1 1\n".
    check!(s.starts_with("0\n"), "header should be '0\\n', got: {s:?}");
    check!(s.contains("\n2\n"), "count should be 2, got: {s:?}");
    check!(s.contains("0 0\n"), "epoch 0 row missing: {s:?}");
    check!(s.contains("1 1\n"), "epoch 1 row missing: {s:?}");

    broker.shutdown().await;
}
