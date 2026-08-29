//! Writer-loop tests for the high-watermark recompute that follows an
//! append, including the replication factors that must leave it where it
//! was.

use assert2::assert;
use krabka_log::LogConfig;
use tempfile::tempdir;
use tokio::sync::oneshot;

use super::*;
use crate::{
    partition::{ProduceData, ProduceJob},
    partition_writer::test_support::sample_batch,
};

#[tokio::test]
async fn writer_fires_hw_notify_after_produce_when_rf_one() {
    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), LogConfig::default()).expect("open log"),
    ));
    let (tx, rx) = mpsc::channel(1);
    let append_notify = Arc::new(Notify::new());
    let replica_state = Arc::new(tokio::sync::Mutex::new(
        crate::replica_state::ReplicaState::new(),
    ));
    {
        let mut st = replica_state.lock().await;
        st.install_isr(
            &[krabka_audit::NodeId(1)],
            &[krabka_audit::NodeId(1)],
            krabka_audit::NodeId(1),
            std::time::Instant::now(),
        );
    }
    let hw_advance_notify = Arc::new(Notify::new());
    let writer = tokio::spawn(run_writer!(
        "t".to_string(),
        PartitionIndex(0),
        log.clone(),
        Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        rx,
        append_notify.clone(),
        replica_state.clone(),
        hw_advance_notify.clone(),
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(ProducerState::new()),
        None,
    ));

    let waiter = hw_advance_notify.notified();
    tokio::pin!(waiter);

    let (ack, _ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Owned(sample_batch(2)),
        ack,
    }))
    .await
    .expect("send job");

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
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), LogConfig::default()).expect("open log"),
    ));
    let (tx, rx) = mpsc::channel(1);
    let append_notify = Arc::new(Notify::new());
    let replica_state = Arc::new(tokio::sync::Mutex::new(
        crate::replica_state::ReplicaState::new(),
    ));
    {
        let mut st = replica_state.lock().await;
        st.install_isr(
            &[krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            &[krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            krabka_audit::NodeId(1),
            std::time::Instant::now(),
        );
    }
    let hw_advance_notify = Arc::new(Notify::new());
    let writer = tokio::spawn(run_writer!(
        "t".to_string(),
        PartitionIndex(0),
        log.clone(),
        Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        rx,
        append_notify,
        replica_state.clone(),
        hw_advance_notify.clone(),
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(ProducerState::new()),
        None,
    ));

    let waiter = hw_advance_notify.notified();
    tokio::pin!(waiter);

    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Owned(sample_batch(1)),
        ack,
    }))
    .await
    .expect("send job");
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
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), LogConfig::default()).expect("open log"),
    ));
    let (tx, rx) = mpsc::channel(1);
    let append_notify = Arc::new(Notify::new());
    let replica_state = Arc::new(tokio::sync::Mutex::new(
        crate::replica_state::ReplicaState::new(),
    ));
    {
        let mut st = replica_state.lock().await;
        st.install_isr(
            &[
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(2),
                krabka_audit::NodeId(3),
            ],
            &[
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(2),
                krabka_audit::NodeId(3),
            ],
            krabka_audit::NodeId(1),
            std::time::Instant::now(),
        );
    }
    let hw_advance_notify = Arc::new(Notify::new());
    let writer = tokio::spawn(run_writer!(
        "t".to_string(),
        PartitionIndex(0),
        log.clone(),
        Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        rx,
        append_notify.clone(),
        replica_state.clone(),
        hw_advance_notify.clone(),
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(ProducerState::new()),
        None,
    ));

    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Owned(sample_batch(3)),
        ack,
    }))
    .await
    .expect("send job");
    ack_rx.await.expect("ack").expect("append ok");

    assert!(replica_state.lock().await.hw == 0);

    drop(tx);
    writer.await.expect("writer join");
}
