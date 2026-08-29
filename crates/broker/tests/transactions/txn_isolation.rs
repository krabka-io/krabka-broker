//! Consumer-isolation outcomes of a committed and an aborted transaction.
//!
//! A `read_committed` consumer must see every record of a committed
//! transaction and none of an aborted one, while a `read_uncommitted` consumer
//! sees both. The interleaved case reuses one `transactional_id` across three
//! back-to-back transactions.

use std::time::Duration;

use assert2::assert;
use krabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use krabka_client_producer::Producer;

use crate::txn_harness::{boot_single, create_topic, rec};

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
    create_topic(&bootstrap, "ta").await;

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

    // read_committed: must see 0 records.
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .group_id("g-abort")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe(["ta".to_string()])
        .build()
        .await
        .unwrap();
    let mut seen = 0usize;
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        let records = consumer.poll(krabka_units::millis(200)).await.unwrap();
        seen += records.len();
        if !records.is_empty() {
            break;
        }
    }
    assert!(seen == 0, "read_committed must skip aborted records");
    consumer.close().await.unwrap();

    // read_uncommitted: sees all 3 records (including aborted ones).
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
    while seen2.len() < 3 && std::time::Instant::now() < deadline {
        for r in consumer_uc.poll(krabka_units::millis(200)).await.unwrap() {
            seen2.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    assert!(
        seen2.len() == 3,
        "read_uncommitted must see aborted records"
    );
    consumer_uc.close().await.unwrap();

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
