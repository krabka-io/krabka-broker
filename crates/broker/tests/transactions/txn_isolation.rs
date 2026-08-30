//! Consumer-isolation outcomes of a committed and an aborted transaction.
//!
//! A `read_committed` consumer must see every record of a committed
//! transaction and none of an aborted one, while a `read_uncommitted` consumer
//! sees both. The interleaved case reuses one `transactional_id` across three
//! back-to-back transactions.

use std::time::Duration;

use assert2::assert;
use krabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use krabka_client_core::Client;
use krabka_client_producer::Producer;
use krabka_protocol::owned::{
    fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    metadata_request::{MetadataRequest, MetadataRequestTopic},
};
use krabka_units::bytes;

use crate::txn_harness::{
    boot_single, create_topic, create_topic_with_segment_bytes, rec, send_ok,
};

/// Commits a transaction, after which a `read_committed` consumer sees all 3
/// records.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_then_read_committed_sees_records() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "t").await;

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("my-tid")
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();
    let txn = producer.begin_transaction().await.unwrap();
    for v in ["a", "b", "c"] {
        drop(producer.send(rec("t", v)).await);
    }
    txn.commit().await.unwrap();

    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("g1")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe(["t".to_string()])
        .build()
        .await
        .unwrap();

    let mut seen: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while seen.len() < 3 && std::time::Instant::now() < deadline {
        for r in consumer.poll(krabka_units::millis(200)).await.unwrap() {
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    assert!(seen == vec!["a", "b", "c"]);

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}

// ── test 2 ────────────────────────────────────────────────────────────────────

/// Aborts a transaction. `read_committed` then sees 0 records, and
/// `read_uncommitted` sees 3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_then_read_committed_skips_records() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic_with_segment_bytes(&bootstrap, "ta", 1).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !broker
        .partition_log_config_for_test("ta", 0)
        .is_some_and(|config| config.segment_size == bytes(1))
    {
        assert!(
            std::time::Instant::now() < deadline,
            "segment.bytes did not reach the transaction log"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("abort-tid")
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();
    let txn = producer.begin_transaction().await.unwrap();
    for v in ["x", "y", "z"] {
        drop(producer.send(rec("ta", v)).await);
    }
    txn.abort().await.unwrap();

    // The next append rolls the abort marker and its transaction index into a
    // sealed segment. A lagging fetch must still receive that abort entry.
    let later = Producer::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    send_ok(&later, rec("ta", "after")).await;
    broker.wait_until_high_watermark("ta", 0, 5).await;

    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let metadata = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some("ta".into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .unwrap();
    let topic_id = metadata.topics[0].topic_id;
    let fetched = client
        .send(FetchRequest {
            replica_id: -1,
            isolation_level: 1,
            max_wait_ms: 1_000,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: "ta".into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    let aborted = fetched.responses[0].partitions[0]
        .aborted_transactions
        .as_deref()
        .unwrap_or_default();
    assert!(
        aborted.len() == 1 && aborted[0].first_offset == 0,
        "read_committed fetch must describe the abort from the sealed segment: {aborted:?}"
    );

    // read_committed: must skip the three aborted records and see the later one.
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .group_id("g-abort")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe(["ta".to_string()])
        .build()
        .await
        .unwrap();
    let mut seen = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while seen.is_empty() && std::time::Instant::now() < deadline {
        for record in consumer.poll(krabka_units::millis(200)).await.unwrap() {
            seen.push(String::from_utf8_lossy(record.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    assert!(seen == ["after"], "read_committed exposed aborted records");
    consumer.close().await.unwrap();

    // read_uncommitted: sees all 4 data records (including aborted ones).
    let mut consumer_uc = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("g-abort-uc")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadUncommitted)
        .subscribe(["ta".to_string()])
        .build()
        .await
        .unwrap();
    let mut seen2: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while seen2.len() < 4 && std::time::Instant::now() < deadline {
        for r in consumer_uc.poll(krabka_units::millis(200)).await.unwrap() {
            seen2.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    assert!(
        seen2 == ["x", "y", "z", "after"],
        "read_uncommitted must see aborted records"
    );
    consumer_uc.close().await.unwrap();

    later.close().await.unwrap();
    producer.close().await.unwrap();
    broker.shutdown().await;
}

// ── test 3 ────────────────────────────────────────────────────────────────────

/// commit("a","b","c"), abort("X","Y"), commit("d","e","f","g"):
/// `read_committed` sees exactly \["a","b","c","d","e","f","g"\].
///
/// Exercises rapid reuse of one `transactional_id` across three back-to-back
/// transactions. This used to flake with `Server(48)` (`INVALID_TXN_STATE`)
/// because `flush` returned before an in-flight Produce had transitioned the
/// coordinator to `Ongoing`, so the following `EndTxn` arrived while the entry
/// was still `CompleteCommit`/`CompleteAbort`. `Producer::flush` now waits for
/// in-flight batches, so the partition-register Produce is always acked before
/// `EndTxn` is sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interleaved_commit_and_abort() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "ti").await;

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("interleave-tid")
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();

    // First txn: commit ["a", "b", "c"].
    let txn = producer.begin_transaction().await.unwrap();
    for v in ["a", "b", "c"] {
        drop(producer.send(rec("ti", v)).await);
    }
    txn.commit().await.unwrap();

    // Second txn: abort ["X", "Y"].
    let txn = producer.begin_transaction().await.unwrap();
    for v in ["X", "Y"] {
        drop(producer.send(rec("ti", v)).await);
    }
    txn.abort().await.unwrap();

    // Third txn: commit ["d", "e", "f", "g"].
    let txn = producer.begin_transaction().await.unwrap();
    for v in ["d", "e", "f", "g"] {
        drop(producer.send(rec("ti", v)).await);
    }
    txn.commit().await.unwrap();

    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("g-interleave")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe(["ti".to_string()])
        .build()
        .await
        .unwrap();

    let mut seen: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while seen.len() < 7 && std::time::Instant::now() < deadline {
        for r in consumer.poll(krabka_units::millis(200)).await.unwrap() {
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    assert!(seen == vec!["a", "b", "c", "d", "e", "f", "g"]);

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}
