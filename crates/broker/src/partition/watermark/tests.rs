//! Unit tests for the partition's high-watermark and ISR bookkeeping.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicI32, AtomicU64},
};

use arc_swap::ArcSwap;
use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig, Offset};
use tempfile::tempdir;
use tokio::sync::{Notify, mpsc};

use crate::{
    delivery::DeliveryHandles,
    partition::{
        Partition, WriterMessage, initial_replication_target,
        test_support::{append_records, test_partition},
    },
};

#[tokio::test]
async fn high_watermark_reads_cached_value() {
    let dir = tempdir().expect("tempdir");
    let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
    let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
    let writer = tokio::spawn(async {});
    let replica_state = Arc::new(tokio::sync::Mutex::new(
        crate::replica_state::ReplicaState::new(),
    ));
    {
        let mut st = replica_state.lock().await;
        st.hw = Offset(42);
    }
    let p = Partition {
        topic: "t".into(),
        index: PartitionIndex(0),
        log_dir: Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        log: Arc::new(Mutex::new(log)),
        writer_tx: tx,
        append_notify: Arc::new(Notify::new()),
        replica_state,
        hw_advance_notify: Arc::new(Notify::new()),
        current_leader: Arc::new(AtomicU64::new(0)),
        current_leader_epoch: Arc::new(AtomicI32::new(0)),
        delivery: DeliveryHandles::new(),
        replication_target: initial_replication_target(None),
        diskless: false,
        writer_handle: Arc::new(Mutex::new(Some(writer))),
    };
    assert!(p.high_watermark().await == 42);
}

#[tokio::test]
async fn install_isr_populates_replica_state() {
    let dir = tempdir().expect("tempdir");
    let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
    let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
    let writer = tokio::spawn(async {});
    let p = Partition {
        topic: "t".into(),
        index: PartitionIndex(0),
        log_dir: Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        log: Arc::new(Mutex::new(log)),
        writer_tx: tx,
        append_notify: Arc::new(Notify::new()),
        replica_state: Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        )),
        hw_advance_notify: Arc::new(Notify::new()),
        current_leader: Arc::new(AtomicU64::new(0)),
        current_leader_epoch: Arc::new(AtomicI32::new(0)),
        delivery: DeliveryHandles::new(),
        replication_target: initial_replication_target(None),
        diskless: false,
        writer_handle: Arc::new(Mutex::new(Some(writer))),
    };
    p.install_isr(
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
    )
    .await;
    let st = p.replica_state.lock().await;
    check!(
        st.isr
            == [
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(2),
                krabka_audit::NodeId(3)
            ]
            .into_iter()
            .collect()
    );
    check!(st.per_follower.get(&krabka_audit::NodeId(2)).map(|f| f.leo) == Some(Offset(0)));
}

#[tokio::test]
async fn install_isr_notifies_when_high_watermark_advances() {
    let hw_advance_notify = Arc::new(Notify::new());
    let (p, _td) = test_partition(hw_advance_notify.clone());
    append_records(&p, 3);
    assert!(p.high_watermark().await == 0);

    let waiter = hw_advance_notify.notified();
    tokio::pin!(waiter);
    assert!(
        futures_util::poll!(&mut waiter).is_pending(),
        "waiter registers on first poll"
    );

    p.install_isr(
        &[krabka_audit::NodeId(1)],
        &[krabka_audit::NodeId(1)],
        krabka_audit::NodeId(1),
    )
    .await;

    assert!(p.high_watermark().await == 3);
    assert!(
        futures_util::poll!(&mut waiter).is_ready(),
        "notify should fire when ISR install advances HW"
    );
}

#[tokio::test]
async fn install_isr_does_not_advance_diskless_hw_from_unsynced_leo() {
    let hw_advance_notify = Arc::new(Notify::new());
    let (mut p, _td) = test_partition(hw_advance_notify.clone());
    p.diskless = true;
    append_records(&p, 3);
    assert!(p.high_watermark().await == 0);

    let waiter = hw_advance_notify.notified();
    tokio::pin!(waiter);
    assert!(
        futures_util::poll!(&mut waiter).is_pending(),
        "waiter registers on first poll"
    );

    p.install_isr(
        &[krabka_audit::NodeId(1)],
        &[krabka_audit::NodeId(1)],
        krabka_audit::NodeId(1),
    )
    .await;

    assert!(p.high_watermark().await == 0);
    assert!(
        futures_util::poll!(&mut waiter).is_pending(),
        "diskless ISR install must not release HW before WAL sync"
    );
}

#[tokio::test]
async fn install_isr_same_high_watermark_does_not_notify() {
    let hw_advance_notify = Arc::new(Notify::new());
    let (p, _td) = test_partition(hw_advance_notify.clone());
    append_records(&p, 2);
    p.install_isr(
        &[krabka_audit::NodeId(1)],
        &[krabka_audit::NodeId(1)],
        krabka_audit::NodeId(1),
    )
    .await;
    assert!(p.high_watermark().await == 2);

    let waiter = hw_advance_notify.notified();
    tokio::pin!(waiter);
    assert!(
        futures_util::poll!(&mut waiter).is_pending(),
        "waiter registers on first poll"
    );

    p.install_isr(
        &[krabka_audit::NodeId(1)],
        &[krabka_audit::NodeId(1)],
        krabka_audit::NodeId(1),
    )
    .await;

    assert!(p.high_watermark().await == 2);
    assert!(
        futures_util::poll!(&mut waiter).is_pending(),
        "unchanged HW must not wake waiters"
    );
}

#[tokio::test]
async fn await_hw_returns_immediately_if_already_satisfied() {
    let dir = tempdir().expect("tempdir");
    let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
    let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
    let writer = tokio::spawn(async {});
    let replica_state = Arc::new(tokio::sync::Mutex::new(
        crate::replica_state::ReplicaState::new(),
    ));
    {
        let mut st = replica_state.lock().await;
        st.hw = Offset(100);
    }
    let p = Partition {
        topic: "t".into(),
        index: PartitionIndex(0),
        log_dir: Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        log: Arc::new(Mutex::new(log)),
        writer_tx: tx,
        append_notify: Arc::new(Notify::new()),
        replica_state,
        hw_advance_notify: Arc::new(Notify::new()),
        current_leader: Arc::new(AtomicU64::new(0)),
        current_leader_epoch: Arc::new(AtomicI32::new(0)),
        delivery: DeliveryHandles::new(),
        replication_target: initial_replication_target(None),
        diskless: false,
        writer_handle: Arc::new(Mutex::new(Some(writer))),
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    p.await_hw_at_least(Offset(50), deadline)
        .await
        .expect("immediate");
}

#[tokio::test]
async fn await_hw_returns_timeout_when_unreached() {
    let dir = tempdir().expect("tempdir");
    let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
    let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
    let writer = tokio::spawn(async {});
    let p = Partition {
        topic: "t".into(),
        index: PartitionIndex(0),
        log_dir: Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        log: Arc::new(Mutex::new(log)),
        writer_tx: tx,
        append_notify: Arc::new(Notify::new()),
        replica_state: Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        )),
        hw_advance_notify: Arc::new(Notify::new()),
        current_leader: Arc::new(AtomicU64::new(0)),
        current_leader_epoch: Arc::new(AtomicI32::new(0)),
        delivery: DeliveryHandles::new(),
        replication_target: initial_replication_target(None),
        diskless: false,
        writer_handle: Arc::new(Mutex::new(Some(writer))),
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(50);
    let result = p.await_hw_at_least(Offset(100), deadline).await;
    assert!(matches!(result, Err(crate::partition::HwTimeout)));
}

#[tokio::test]
async fn set_follower_hw_clamps_advances_and_notifies() {
    use krabka_protocol::records::{Attributes, Record, RecordBatch};

    let dir = tempdir().expect("tempdir");
    let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
    let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
    let writer = tokio::spawn(async {});
    let hw_advance_notify = Arc::new(Notify::new());
    let p = Partition {
        topic: "t".into(),
        index: PartitionIndex(0),
        log_dir: Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        log: Arc::new(Mutex::new(log)),
        writer_tx: tx,
        append_notify: Arc::new(Notify::new()),
        replica_state: Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        )),
        hw_advance_notify: hw_advance_notify.clone(),
        current_leader: Arc::new(AtomicU64::new(0)),
        current_leader_epoch: Arc::new(AtomicI32::new(0)),
        delivery: DeliveryHandles::new(),
        replication_target: initial_replication_target(None),
        diskless: false,
        writer_handle: Arc::new(Mutex::new(Some(writer))),
    };

    // Append a 3-record batch so log_end_offset() == 3.
    let mut batch = RecordBatch {
        base_offset: 0,
        partition_leader_epoch: -1,
        attributes: Attributes::default(),
        last_offset_delta: 2,
        base_timestamp: 1_700_000_000,
        max_timestamp: 1_700_000_000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: (0..3)
            .map(|i| Record {
                attributes: 0,
                offset_delta: i,
                timestamp_delta: 0,
                key: None,
                value: Some(bytes::Bytes::from_static(b"v")),
                headers: vec![],
            })
            .collect(),
    };
    p.log
        .lock()
        .expect("log mutex")
        .append(&mut batch)
        .expect("append");
    assert!(p.log_end_offset() == 3);

    // reported_hw below log_end: stored verbatim, notify fires.
    // A `Notified` future does not register with the `Notify` until it is
    // first polled, and `notify_waiters()` only wakes already-registered
    // waiters — so poll once (Pending) to register BEFORE advancing HW.
    let waiter = hw_advance_notify.notified();
    tokio::pin!(waiter);
    assert!(
        futures_util::poll!(&mut waiter).is_pending(),
        "waiter registers on first poll"
    );
    p.set_follower_hw(Offset(2)).await;
    assert!(p.high_watermark().await == 2);
    assert!(
        futures_util::poll!(&mut waiter).is_ready(),
        "notify should fire when HW advances"
    );

    // reported_hw above log_end: clamped to log_end (3).
    p.set_follower_hw(Offset(100)).await;
    assert!(p.high_watermark().await == 3);

    // reported_hw below current HW: no regression.
    p.set_follower_hw(Offset(1)).await;
    assert!(p.high_watermark().await == 3);
}

#[tokio::test]
async fn set_follower_hw_same_high_watermark_does_not_notify() {
    let hw_advance_notify = Arc::new(Notify::new());
    let (p, _td) = test_partition(hw_advance_notify.clone());
    assert!(p.high_watermark().await == 0);

    let waiter = hw_advance_notify.notified();
    tokio::pin!(waiter);
    assert!(
        futures_util::poll!(&mut waiter).is_pending(),
        "waiter registers on first poll"
    );

    p.set_follower_hw(Offset(0)).await;

    assert!(p.high_watermark().await == 0);
    assert!(
        futures_util::poll!(&mut waiter).is_pending(),
        "unchanged HW must not wake waiters"
    );
}

#[tokio::test]
async fn await_hw_wakes_on_advance() {
    let dir = tempdir().expect("tempdir");
    let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
    let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
    let writer = tokio::spawn(async {});
    let replica_state = Arc::new(tokio::sync::Mutex::new(
        crate::replica_state::ReplicaState::new(),
    ));
    let hw_advance_notify = Arc::new(Notify::new());
    let p = Partition {
        topic: "t".into(),
        index: PartitionIndex(0),
        log_dir: Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        log: Arc::new(Mutex::new(log)),
        writer_tx: tx,
        append_notify: Arc::new(Notify::new()),
        replica_state: replica_state.clone(),
        hw_advance_notify: hw_advance_notify.clone(),
        current_leader: Arc::new(AtomicU64::new(0)),
        current_leader_epoch: Arc::new(AtomicI32::new(0)),
        delivery: DeliveryHandles::new(),
        replication_target: initial_replication_target(None),
        diskless: false,
        writer_handle: Arc::new(Mutex::new(Some(writer))),
    };
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        replica_state.lock().await.hw = Offset(100);
        hw_advance_notify.notify_waiters();
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    p.await_hw_at_least(Offset(50), deadline)
        .await
        .expect("woke on advance");
}
