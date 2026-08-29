//! KIP-101 fencing on the leader: a `Fetch` whose `current_leader_epoch` is
//! behind the partition's epoch gets `FENCED_LEADER_EPOCH`, and one that is
//! ahead of it gets `UNKNOWN_LEADER_EPOCH`.
//!
//! The two error codes are the two sides of the same comparison, so they are
//! asserted next to each other.

use assert2::assert;
use krabka_client_core::Client;
use krabka_protocol::owned::{
    fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
};

use crate::epoch_harness::{boot_single, create_topic, record, topic_id_for};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fenced_leader_epoch_truncates_zombie_writes() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "fence").await;

    // Produce a record at epoch 0.
    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, "fence").await;
    client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "fence".into(),
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

    // Force the partition's epoch up to 5 (simulate "split brain").
    broker.test_set_leader_epoch("fence", 0, 5);

    // Fetch with current_leader_epoch=2 → FENCED_LEADER_EPOCH (code 74).
    let resp = client
        .send(FetchRequest {
            replica_id: 99,
            max_wait_ms: 100,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: "fence".into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    current_leader_epoch: 2,
                    partition_max_bytes: 1 << 20,
                    ..FetchPartition::default()
                }],
                ..FetchTopic::default()
            }],
            ..FetchRequest::default()
        })
        .await
        .expect("fetch");
    let pd = &resp.responses[0].partitions[0];
    // FENCED_LEADER_EPOCH = 74
    assert!(pd.error_code == 74, "expected FENCED_LEADER_EPOCH");

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_leader_epoch_on_metadata_lag() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "unknown").await;
    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, "unknown").await;

    // Fetch with current_leader_epoch=5 — broker has epoch=0; UNKNOWN_LEADER_EPOCH (code 75).
    let resp = client
        .send(FetchRequest {
            replica_id: 99,
            max_wait_ms: 100,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: "unknown".into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    current_leader_epoch: 5,
                    partition_max_bytes: 1 << 20,
                    ..FetchPartition::default()
                }],
                ..FetchTopic::default()
            }],
            ..FetchRequest::default()
        })
        .await
        .expect("fetch");
    let pd = &resp.responses[0].partitions[0];
    // UNKNOWN_LEADER_EPOCH = 75
    assert!(pd.error_code == 75, "expected UNKNOWN_LEADER_EPOCH");

    broker.shutdown().await;
}
