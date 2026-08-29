//! Writer-loop tests for the diskless path, where an external sequencer
//! assigns offsets and the WAL, not the local log, decides durability.

use std::sync::atomic::Ordering;

use assert2::{assert, check};
use krabka_log::{LogConfig, Offset};
use tempfile::tempdir;
use tokio::sync::oneshot;

use super::*;
use crate::{
    partition::{ProduceData, ProduceJob},
    partition_writer::test_support::{GatedWal, sample_batch, test_sequencer},
};

#[tokio::test]
async fn diskless_writer_acks_all_gates_on_durable_hw() {
    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), LogConfig::default()).expect("open log"),
    ));
    let (sync_started_tx, sync_started_rx) = oneshot::channel();
    let (release_sync_tx, release_sync_rx) = oneshot::channel();
    let wal: Option<crate::wal::SharedWal> =
        Some(Arc::new(GatedWal::new(sync_started_tx, release_sync_rx)));
    let (tx, rx) = mpsc::channel(1);
    let append_notify = Arc::new(Notify::new());
    let replica_state = Arc::new(tokio::sync::Mutex::new(ReplicaState::new()));
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
    let writer = tokio::spawn(run_with_sequencer(
        ("t".to_string(), PartitionIndex(0)),
        (
            log.clone(),
            Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        ),
        rx,
        (
            append_notify,
            replica_state.clone(),
            hw_advance_notify.clone(),
            DeliveryHandles::new(),
        ),
        (
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(ProducerState::new()),
            wal,
        ),
        (
            crate::config::BrokerConfig::default().producer_id_expiration,
            crate::config::BrokerConfig::default().max_produce_group,
        ),
        Some(test_sequencer()),
    ));

    let hw_waiter = hw_advance_notify.notified();
    tokio::pin!(hw_waiter);

    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Owned(sample_batch(3)),
        ack,
    }))
    .await
    .expect("send job");

    let assigned = ack_rx.await.expect("ack recv").expect("append ok");
    assert!(assigned == 0);
    tokio::time::timeout(std::time::Duration::from_secs(1), sync_started_rx)
        .await
        .expect("wal sync_durable did not start")
        .expect("sync start signal sent");

    assert!(replica_state.lock().await.hw == 0);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut hw_waiter)
            .await
            .is_err()
    );

    release_sync_tx.send(()).expect("release sync");
    tokio::time::timeout(std::time::Duration::from_secs(1), &mut hw_waiter)
        .await
        .expect("hw_advance_notify did not fire");
    assert!(replica_state.lock().await.hw == 3);

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn diskless_acked_record_survives_reopen() {
    let dir = tempdir().expect("tempdir");
    {
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).expect("open log"),
        ));
        let wal: Option<crate::wal::SharedWal> =
            Some(Arc::new(crate::wal::LocalFsyncWal::new(log.clone())));
        let (tx, rx) = mpsc::channel(1);
        let append_notify = Arc::new(Notify::new());
        let replica_state = Arc::new(tokio::sync::Mutex::new(ReplicaState::new()));
        {
            let mut st = replica_state.lock().await;
            st.install_isr(
                &[krabka_audit::NodeId(1)],
                &[krabka_audit::NodeId(1)],
                krabka_audit::NodeId(1),
                std::time::Instant::now(),
            );
        }
        let writer = tokio::spawn(run_with_sequencer(
            ("t".to_string(), PartitionIndex(0)),
            (
                log.clone(),
                Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            ),
            rx,
            (
                append_notify,
                replica_state.clone(),
                Arc::new(Notify::new()),
                DeliveryHandles::new(),
            ),
            (
                crate::log_dir_status::LogDirRegistry::default(),
                Arc::new(ProducerState::new()),
                wal,
            ),
            (
                crate::config::BrokerConfig::default().producer_id_expiration,
                crate::config::BrokerConfig::default().max_produce_group,
            ),
            Some(test_sequencer()),
        ));

        let (ack, ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Produce(ProduceJob {
            data: ProduceData::Owned(sample_batch(1)),
            ack,
        }))
        .await
        .expect("send job");

        let assigned = ack_rx.await.expect("ack recv").expect("append ok");
        assert_eq!(assigned, 0);

        drop(tx);
        tokio::time::timeout(std::time::Duration::from_secs(10), writer)
            .await
            .expect("writer did not drain after local fsync")
            .expect("writer join");
        assert_eq!(replica_state.lock().await.hw, 1);
    }

    let log = Log::open(dir.path(), LogConfig::default()).expect("reopen log");
    assert_eq!(log.log_end_offset(), Offset(1));
}

#[tokio::test]
async fn diskless_writer_delegates_trim_to_the_wal() {
    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), LogConfig::default()).expect("open log"),
    ));
    log.lock()
        .expect("lock")
        .append(&mut sample_batch(4))
        .expect("append");

    let (sync_started_tx, _sync_started_rx) = oneshot::channel();
    let (_release_sync_tx, release_sync_rx) = oneshot::channel();
    let gated_wal = Arc::new(GatedWal::new(sync_started_tx, release_sync_rx));
    let wal: crate::wal::SharedWal = gated_wal.clone();
    let (tx, rx) = mpsc::channel(1);
    let writer = tokio::spawn(run_writer!(
        "t".to_string(),
        PartitionIndex(0),
        log.clone(),
        Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        rx,
        Arc::new(Notify::new()),
        Arc::new(tokio::sync::Mutex::new(ReplicaState::new())),
        Arc::new(Notify::new()),
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(ProducerState::new()),
        Some(wal),
    ));

    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::TrimToOffset {
        new_start: Offset(3),
        ack,
    })
    .await
    .expect("send trim");

    check!(ack_rx.await.expect("trim ack").expect("trim succeeds") == Offset(3));
    check!(gated_wal.trimmed_to.load(Ordering::SeqCst) == 3);
    check!(log.lock().expect("lock").log_start_offset() == Offset(0));

    drop(tx);
    writer.await.expect("writer join");
}
