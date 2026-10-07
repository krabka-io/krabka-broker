//! Writer-loop tests for the arms that maintain the log rather than write
//! to it: a config swap and a log-start trim.

use assert2::assert;
use krabka_log::{LogConfig, Offset};
use tempfile::tempdir;

use super::*;
use crate::partition_writer::test_support::sample_batch;

#[tokio::test]
async fn writer_set_log_config_swaps_config() {
    let dir = tempdir().expect("tempdir");
    let log = open_default_log(dir.path());
    let (tx, rx) = mpsc::channel(1);
    let writer = spawn_writer(dir.path(), log.clone(), rx, WriterOptions::default());

    let new_cfg = LogConfig {
        retention: Some(krabka_units::minutes(2)),
        ..LogConfig::default()
    };
    let (ack, ack_rx) = tokio::sync::oneshot::channel();
    tx.send(WriterMessage::SetLogConfig {
        config: new_cfg.clone(),
        ack,
    })
    .await
    .expect("send");
    ack_rx.await.expect("ack");

    let observed = log.lock().expect("lock").config_snapshot();
    assert!(observed.retention == new_cfg.retention);

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn writer_trim_to_offset_advances_log_start() {
    let dir = tempdir().expect("tempdir");
    let log = open_default_log(dir.path());
    // Pre-populate with two batches → LEO = 4.
    for _ in 0..2 {
        log.lock()
            .expect("lock")
            .append(&mut sample_batch(2))
            .expect("append");
    }

    let (tx, rx) = mpsc::channel(1);
    let writer = spawn_writer(dir.path(), log.clone(), rx, WriterOptions::default());

    let (ack, ack_rx) = tokio::sync::oneshot::channel();
    tx.send(WriterMessage::TrimToOffset {
        new_start: Offset(3),
        ack,
    })
    .await
    .expect("send");
    let new_start = ack_rx.await.expect("ack").expect("trim ok");
    assert!(new_start >= 3);
    assert!(log.lock().expect("lock").log_start_offset() == new_start);

    drop(tx);
    writer.await.expect("writer join");
}
