//! Writer-loop tests for the high-watermark recompute that follows an
//! append, including the replication factors that must leave it where it
//! was.

use assert2::assert;
use tempfile::tempdir;

use super::*;
use crate::partition_writer::test_support::sample_batch;

#[tokio::test]
async fn writer_fires_hw_notify_after_produce_when_rf_one() {
    let dir = tempdir().expect("tempdir");
    let log = open_default_log(dir.path());
    let (tx, rx) = mpsc::channel(1);
    let append_notify = Arc::new(Notify::new());
    let replica_state = replica_with_isr(&[1]).await;
    let hw_advance_notify = Arc::new(Notify::new());
    let writer = spawn_writer(
        dir.path(),
        log.clone(),
        rx,
        WriterOptions {
            append_notify: append_notify.clone(),
            replica_state: replica_state.clone(),
            hw_advance_notify: hw_advance_notify.clone(),
            ..Default::default()
        },
    );

    let waiter = hw_advance_notify.notified();
    tokio::pin!(waiter);

    let _ack_rx = queue_batch(&tx, sample_batch(2)).await;

    tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .expect("hw_advance_notify did not fire");

    assert!(replica_state.lock().await.hw == 2);

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn writer_does_not_notify_hw_when_append_leaves_hw_unchanged() {
    let dir = tempdir().expect("tempdir");
    let log = open_default_log(dir.path());
    let (tx, rx) = mpsc::channel(1);
    let append_notify = Arc::new(Notify::new());
    let replica_state = replica_with_isr(&[1, 2]).await;
    let hw_advance_notify = Arc::new(Notify::new());
    let writer = spawn_writer(
        dir.path(),
        log.clone(),
        rx,
        WriterOptions {
            append_notify,
            replica_state: replica_state.clone(),
            hw_advance_notify: hw_advance_notify.clone(),
            ..Default::default()
        },
    );

    let waiter = hw_advance_notify.notified();
    tokio::pin!(waiter);

    let ack_rx = queue_batch(&tx, sample_batch(1)).await;
    ack_rx.await.expect("ack").expect("append ok");

    assert!(replica_state.lock().await.hw == 0);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), waiter)
            .await
            .is_err()
    );

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn writer_does_not_advance_hw_when_followers_lagging() {
    let dir = tempdir().expect("tempdir");
    let log = open_default_log(dir.path());
    let (tx, rx) = mpsc::channel(1);
    let append_notify = Arc::new(Notify::new());
    let replica_state = replica_with_isr(&[1, 2, 3]).await;
    let hw_advance_notify = Arc::new(Notify::new());
    let writer = spawn_writer(
        dir.path(),
        log.clone(),
        rx,
        WriterOptions {
            append_notify: append_notify.clone(),
            replica_state: replica_state.clone(),
            hw_advance_notify: hw_advance_notify.clone(),
            ..Default::default()
        },
    );

    let ack_rx = queue_batch(&tx, sample_batch(3)).await;
    ack_rx.await.expect("ack").expect("append ok");

    assert!(replica_state.lock().await.hw == 0);

    drop(tx);
    writer.await.expect("writer join");
}
