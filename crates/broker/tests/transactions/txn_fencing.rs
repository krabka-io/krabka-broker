//! Zombie-fencing tests (KIP-98 and KIP-447).
//!
//! A second producer that claims the same `transactional_id` bumps the epoch
//! and fences the first, and the broker rejects a `TxnOffsetCommit` whose group
//! metadata is stale — a classic-group generation that no longer matches, an
//! unknown member, or a next-gen member epoch that is behind or ahead of the
//! live one.

use assert2::assert;
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_client_producer::Producer;

use crate::txn_harness::{boot_single, create_topic, init_transaction, rec};

/// Producer B with the same `transactional_id` fences Producer A. Producer A's
/// `Transaction::commit` must return `ProducerError::FencedProducer`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fenced_producer_cannot_commit() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "tf").await;

    let producer_a = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("shared-tid")
        .build()
        .await
        .unwrap();
    producer_a.init_transactions().await.unwrap();
    let txn_a = producer_a.begin_transaction().await.unwrap();
    drop(producer_a.send(rec("tf", "first")).await);

    // Producer B initializes with the same transactional_id — bumps epoch,
    // fences A.
    let producer_b = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("shared-tid")
        .build()
        .await
        .unwrap();
    producer_b.init_transactions().await.unwrap();

    // Producer A's commit must fail with FencedProducer.
    let err = txn_a
        .commit()
        .await
        .expect_err("commit should fail after fencing");
    assert!(
        matches!(
            err.source,
            krabka_client_producer::ProducerError::FencedProducer
        ),
        "expected FencedProducer, got: {err:?}"
    );

    broker.shutdown().await;
}

/// The broker fences a classic-group `TxnOffsetCommit` when it carries a stale
/// generation (`ILLEGAL_GENERATION`) or an unknown member
/// (`UNKNOWN_MEMBER_ID`), and accepts it when the metadata matches the live
/// group. The test uses raw `TxnOffsetCommitRequest` values, which give it
/// precise control over the metadata.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn txn_offset_commit_fences_classic_generation_and_member() {
    use krabka_protocol::owned::txn_offset_commit_request::{
        TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    };

    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "fence-in").await;

    // A real classic consumer joins, establishing the group's member id +
    // generation.
    let consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .group_id("fence-g")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe(["fence-in".to_string()])
        .build()
        .await
        .unwrap();
    let meta = consumer.group_metadata();
    // A non-empty member id proves the join completed; the fencing assertions
    // below hold for whatever generation the group settled on (we send
    // `generation_id + 1` for the stale case, which always mismatches).
    assert!(
        !meta.member_id.is_empty(),
        "consumer should have a member id: {meta:?}"
    );

    let client = krabka_client_core::Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let (producer_id, producer_epoch) = init_transaction(&client, "fence-tid").await;

    let mk = |generation_id: i32, member_id: &str| TxnOffsetCommitRequest {
        transactional_id: "fence-tid".into(),
        group_id: "fence-g".into(),
        producer_id,
        producer_epoch,
        generation_id,
        member_id: member_id.into(),
        topics: vec![TxnOffsetCommitRequestTopic {
            name: "fence-in".into(),
            partitions: vec![TxnOffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: 1,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    // Stale generation → ILLEGAL_GENERATION (22).
    let stale = client
        .send(mk(meta.generation_id + 1, &meta.member_id))
        .await
        .unwrap();
    assert!(
        stale.topics[0].partitions[0].error_code == 22,
        "stale generation should be ILLEGAL_GENERATION: {stale:?}"
    );

    // Correct generation but unknown member → UNKNOWN_MEMBER_ID (25).
    let unknown = client
        .send(mk(meta.generation_id, "ghost-member"))
        .await
        .unwrap();
    assert!(
        unknown.topics[0].partitions[0].error_code == 25,
        "unknown member should be UNKNOWN_MEMBER_ID: {unknown:?}"
    );

    // Matching metadata → accepted (NONE = 0).
    let ok = client
        .send(mk(meta.generation_id, &meta.member_id))
        .await
        .unwrap();
    assert!(
        ok.topics[0].partitions[0].error_code == 0,
        "valid metadata should commit: {ok:?}"
    );

    consumer.close().await.unwrap();
    broker.shutdown().await;
}

/// The broker fences a KIP-848 next-gen "consumer"-protocol `TxnOffsetCommit`
/// when it carries a stale member epoch (`STALE_MEMBER_EPOCH`), and accepts it
/// at the current epoch. The member epoch travels in the `generation_id`
/// field.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn txn_offset_commit_fences_next_gen_member_epoch() {
    use krabka_protocol::owned::{
        consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
        txn_offset_commit_request::{
            TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
        },
    };

    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "ng-in").await;

    let client = krabka_client_core::Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let (producer_id, producer_epoch) = init_transaction(&client, "ng-tid").await;

    // Establish a next-gen group member; after the first heartbeat the member
    // is at epoch 1.
    let mut hb = ConsumerGroupHeartbeatRequest {
        group_id: "ng-g".into(),
        member_id: String::new(),
        member_epoch: 0,
        rebalance_timeout_ms: 60_000,
        ..Default::default()
    };
    hb.subscribed_topic_names = Some(vec!["ng-in".into()]);
    let hb_resp = client.send(hb).await.unwrap();
    assert!(hb_resp.error_code == 0, "heartbeat failed: {hb_resp:?}");
    let member_id = hb_resp.member_id.clone().unwrap();
    let epoch = hb_resp.member_epoch;
    assert!(
        epoch >= 1,
        "member should have a positive epoch: {hb_resp:?}"
    );

    let mk = |epoch_val: i32| TxnOffsetCommitRequest {
        transactional_id: "ng-tid".into(),
        group_id: "ng-g".into(),
        producer_id,
        producer_epoch,
        generation_id: epoch_val, // carries the member epoch for next-gen groups
        member_id: member_id.clone(),
        topics: vec![TxnOffsetCommitRequestTopic {
            name: "ng-in".into(),
            partitions: vec![TxnOffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: 1,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    // Stale epoch (< current) → STALE_MEMBER_EPOCH (113).
    let stale = client.send(mk(epoch - 1)).await.unwrap();
    assert!(
        stale.topics[0].partitions[0].error_code == 113,
        "stale epoch should be STALE_MEMBER_EPOCH: {stale:?}"
    );

    // Future epoch (> current) → FENCED_MEMBER_EPOCH (110).
    let fenced = client.send(mk(epoch + 1)).await.unwrap();
    assert!(
        fenced.topics[0].partitions[0].error_code == 110,
        "future epoch should be FENCED_MEMBER_EPOCH: {fenced:?}"
    );

    // Current epoch + known member → accepted (NONE = 0).
    let ok = client.send(mk(epoch)).await.unwrap();
    assert!(
        ok.topics[0].partitions[0].error_code == 0,
        "current epoch should commit: {ok:?}"
    );

    broker.shutdown().await;
}
