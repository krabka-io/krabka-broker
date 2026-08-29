//! Writer-loop tests for the follower-facing arms: an append at a caller
//! supplied offset and the truncation that undoes one.

use assert2::assert;
use krabka_log::{LogConfig, Offset};
use tempfile::tempdir;
use tokio::sync::oneshot;

use super::*;
use crate::{
    partition::{ProduceData, ProduceJob},
    partition_writer::test_support::sample_batch,
};

#[tokio::test]
async fn writer_handles_replicate_with_caller_offset() {
    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), LogConfig::default()).expect("open log"),
    ));
    let (tx, rx) = mpsc::channel(1);
    let notify = Arc::new(Notify::new());
    let writer = tokio::spawn(run_writer!(
        "t".to_string(),
        PartitionIndex(0),
        log.clone(),
        Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        rx,
        notify.clone(),
        Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        )),
        Arc::new(Notify::new()),
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(ProducerState::new()),
        None,
    ));

    // First replicate batch must start at offset 0 to match the
    // empty local log's `log_end_offset()`.
    let mut batch = sample_batch(3);
    batch.base_offset = 0;
    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Replicate { batch, ack })
        .await
        .expect("send replicate");
    ack_rx.await.expect("ack recv").expect("replicate ok");
    assert!(log.lock().unwrap().log_end_offset() == 3);

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn writer_replicate_offset_mismatch_surfaces_error() {
    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), LogConfig::default()).expect("open log"),
    ));
    let (tx, rx) = mpsc::channel(1);
    let notify = Arc::new(Notify::new());
    let writer = tokio::spawn(run_writer!(
        "t".to_string(),
        PartitionIndex(0),
        log.clone(),
        Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        rx,
        notify.clone(),
        Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        )),
        Arc::new(Notify::new()),
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(ProducerState::new()),
        None,
    ));

    // Wrong offset — log_end_offset is 0 but we claim 7.
    let mut batch = sample_batch(1);
    batch.base_offset = 7;
    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Replicate { batch, ack })
        .await
        .expect("send replicate");
    let err = ack_rx
        .await
        .expect("ack recv")
        .expect_err("expected offset mismatch");
    assert!(matches!(err, crate::error::BrokerError::Log(_)));
    // Local log must not have advanced.
    assert!(log.lock().unwrap().log_end_offset() == 0);

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn writer_truncate_drops_records() {
    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), LogConfig::default()).expect("open log"),
    ));
    let (tx, rx) = mpsc::channel(1);
    let notify = Arc::new(Notify::new());
    let writer = tokio::spawn(run_writer!(
        "t".to_string(),
        PartitionIndex(0),
        log.clone(),
        Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        rx,
        notify.clone(),
        Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        )),
        Arc::new(Notify::new()),
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(ProducerState::new()),
        None,
    ));

    // Produce two batches so the log has some data.
    for _ in 0..2 {
        let (ack, ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Produce(ProduceJob {
            data: ProduceData::Owned(sample_batch(2)),
            ack,
        }))
        .await
        .expect("send produce");
        ack_rx.await.expect("ack").expect("ok");
    }
    assert!(log.lock().unwrap().log_end_offset() == 4);

    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Truncate {
        offset: Offset(0),
        ack,
    })
    .await
    .expect("send truncate");
    ack_rx.await.expect("ack").expect("truncate ok");
    assert!(log.lock().unwrap().log_end_offset() == 0);

    drop(tx);
    writer.await.expect("writer join");
}
