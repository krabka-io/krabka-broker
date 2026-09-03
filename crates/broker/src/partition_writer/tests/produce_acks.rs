//! Writer-loop tests for the Produce arm: offset assignment, the ack
//! ordering, verbatim byte-exactness, group draining, and the append
//! notification.

use assert2::assert;
use krabka_log::{LogConfig, Offset};
use tempfile::tempdir;
use tokio::sync::oneshot;

use super::*;
use crate::{
    partition::{ProduceData, ProduceJob},
    partition_writer::test_support::{GatedWal, sample_batch, test_sequencer},
};

#[tokio::test]
async fn writer_appends_and_acks() {
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

    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Owned(sample_batch(3)),
        ack,
    }))
    .await
    .expect("send job");

    let assigned = ack_rx.await.expect("ack recv").expect("append ok");
    assert!(assigned.base_offset == 0);

    // Second append assigns offset 3.
    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Owned(sample_batch(2)),
        ack,
    }))
    .await
    .expect("send job 2");
    assert!(
        ack_rx
            .await
            .expect("ack recv 2")
            .expect("append 2 ok")
            .base_offset
            == 3
    );

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn writer_groups_queued_produces_up_to_configured_cap() {
    const MAX_GROUP: usize = 2;

    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), LogConfig::default()).expect("open log"),
    ));
    let (sync_started_tx, sync_started_rx) = oneshot::channel();
    let (_release_sync_tx, release_sync_rx) = oneshot::channel();
    let wal: crate::wal::SharedWal = Arc::new(GatedWal::new(sync_started_tx, release_sync_rx));
    let (tx, rx) = mpsc::channel(3);

    for _ in 0..3 {
        let (ack, _ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Produce(ProduceJob {
            data: ProduceData::Owned(sample_batch(1)),
            ack,
        }))
        .await
        .expect("queue produce");
    }

    let writer = tokio::spawn(run_with_sequencer(
        ("t".to_string(), PartitionIndex(0)),
        (
            log.clone(),
            Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        ),
        rx,
        (
            Arc::new(Notify::new()),
            Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            Arc::new(Notify::new()),
            DeliveryHandles::new(),
        ),
        (
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(ProducerState::new()),
            Some(wal),
        ),
        (
            crate::config::BrokerConfig::default().producer_id_expiration,
            MAX_GROUP,
        ),
        Some(test_sequencer()),
    ));

    tokio::time::timeout(std::time::Duration::from_secs(10), sync_started_rx)
        .await
        .expect("first group did not reach WAL sync")
        .expect("first group reached WAL sync");
    assert!(log.lock().unwrap().log_end_offset() == Offset(2));

    writer.abort();
    let _ = writer.await;
    drop(tx);
}

#[tokio::test]
async fn durable_sync_ack_waits_for_diskless_wal() {
    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), LogConfig::default()).expect("open log"),
    ));
    let (sync_started_tx, sync_started_rx) = oneshot::channel();
    let (release_sync_tx, release_sync_rx) = oneshot::channel();
    let wal: crate::wal::SharedWal = Arc::new(GatedWal::new(sync_started_tx, release_sync_rx));
    let (tx, rx) = mpsc::channel(2);
    let writer = tokio::spawn(run_with_sequencer(
        ("t".to_string(), PartitionIndex(0)),
        (
            log,
            Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        ),
        rx,
        (
            Arc::new(Notify::new()),
            Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            Arc::new(Notify::new()),
            DeliveryHandles::new(),
        ),
        (
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(ProducerState::new()),
            Some(wal),
        ),
        (
            crate::config::BrokerConfig::default().producer_id_expiration,
            1,
        ),
        Some(test_sequencer()),
    ));

    let (append_ack, append_ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Owned(sample_batch(1)),
        ack: append_ack,
    }))
    .await
    .expect("send produce");
    assert!(
        append_ack_rx
            .await
            .expect("append ack")
            .expect("append")
            .base_offset
            == 0
    );

    let (durable_ack, mut durable_ack_rx) = oneshot::channel();
    tx.send(WriterMessage::SyncDurable {
        leo: Offset(1),
        ack: durable_ack,
    })
    .await
    .expect("send durable sync");
    sync_started_rx.await.expect("WAL sync started");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut durable_ack_rx)
            .await
            .is_err(),
        "durable ack must wait for WAL"
    );

    release_sync_tx.send(()).expect("release WAL sync");
    durable_ack_rx
        .await
        .expect("durable ack")
        .expect("durable sync");
    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writer_appends_and_acks_on_multi_thread_runtime() {
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

    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Owned(sample_batch(3)),
        ack,
    }))
    .await
    .expect("send job");

    let assigned = ack_rx.await.expect("ack recv").expect("append ok");
    assert!(assigned.base_offset == 0);

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn writer_appends_verbatim_byte_exact() {
    use krabka_log::VerbatimBatch;
    use krabka_protocol::records::RecordBatch as ProtoBatch;

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

    // "Producer" batch with a bogus base_offset + epoch the log overwrites.
    let mut producer = sample_batch(1);
    producer.base_offset = 555;
    producer.partition_leader_epoch = -1;
    producer.max_timestamp = 1_234;
    let mut wire = bytes::BytesMut::new();
    producer.encode(&mut wire).unwrap();
    let wire = wire.freeze();

    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Verbatim(VerbatimBatch {
            bytes: wire.clone(),
            last_offset_delta: 0,
            max_timestamp: 1_234,
            leader_epoch: krabka_log::LeaderEpoch(5),
            producer_id: krabka_log::ProducerId(-1),
            producer_epoch: -1,
            base_sequence: -1,
            is_transactional: false,
        }),
        ack,
    }))
    .await
    .expect("send verbatim job");
    let assigned = ack_rx.await.expect("ack").expect("append ok");
    assert!(assigned.base_offset == 0);

    // Read back: bytes 21.. must equal the producer's, only offset+epoch changed.
    let r = log
        .lock()
        .unwrap()
        .read_raw(Offset(0), Offset(1), krabka_units::mebibytes(10))
        .unwrap();
    assert!(&r.bytes[21..] == &wire[21..], "CRC-covered region verbatim");
    assert!(&r.bytes[17..21] == &wire[17..21], "CRC unchanged");
    // Decodes with the assigned offset + stamped epoch.
    let mut cur: &[u8] = &r.bytes;
    let decoded = ProtoBatch::decode(&mut cur).unwrap();
    assert!(decoded.base_offset == 0);
    assert!(decoded.partition_leader_epoch == 5);

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn writer_fires_notify_after_append() {
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

    // Subscribe BEFORE sending so we don't miss the notification.
    let waiter = notify.notified();
    tokio::pin!(waiter);

    let (ack, _ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Owned(sample_batch(1)),
        ack,
    }))
    .await
    .expect("send job");

    // Should wake within a short timeout.
    tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .expect("notify did not fire");

    drop(tx);
    writer.await.expect("writer join");
}
