//! Fixture builders that the partition module's unit tests share: a partition
//! over a temporary log directory, one wired to a live writer task, and a
//! helper that appends records straight to the log.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicI32, AtomicU64},
};

use arc_swap::ArcSwap;
use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig};
use tempfile::tempdir;
use tokio::sync::{Notify, mpsc};

use super::*;
use crate::{
    delivery::DeliveryHandles,
    partition::{Partition, WriterMessage, initial_replication_target},
};

pub(super) fn test_partition(hw_advance_notify: Arc<Notify>) -> (Partition, tempfile::TempDir) {
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
        hw_advance_notify,
        current_leader: Arc::new(AtomicU64::new(0)),
        current_leader_epoch: Arc::new(AtomicI32::new(0)),
        delivery: DeliveryHandles::new(),
        replication_target: initial_replication_target(None),
        diskless: false,
        writer_handle: Arc::new(Mutex::new(Some(writer))),
    };
    (p, dir)
}

pub(super) fn test_partition_with_writer() -> (Partition, tempfile::TempDir) {
    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), LogConfig::default()).expect("open log"),
    ));
    let log_dir = Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf()));
    let (tx, rx) = mpsc::channel::<WriterMessage>(8);
    let append_notify = Arc::new(Notify::new());
    let replica_state = Arc::new(tokio::sync::Mutex::new(
        crate::replica_state::ReplicaState::new(),
    ));
    let hw_advance_notify = Arc::new(Notify::new());
    // The writer and the partition share one set of delivery handles, as
    // they do in production: the writer refreshes the mirror the partition
    // reads.
    let delivery = DeliveryHandles::new();
    let writer = tokio::spawn(crate::partition_writer::run(
        ("t".to_string(), PartitionIndex(0)),
        (log.clone(), log_dir.clone()),
        rx,
        (
            append_notify.clone(),
            replica_state.clone(),
            hw_advance_notify.clone(),
            delivery.clone(),
        ),
        (
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            None,
        ),
    ));
    let p = Partition {
        topic: "t".into(),
        index: PartitionIndex(0),
        log_dir,
        log,
        writer_tx: tx,
        append_notify,
        replica_state,
        hw_advance_notify,
        current_leader: Arc::new(AtomicU64::new(0)),
        current_leader_epoch: Arc::new(AtomicI32::new(0)),
        delivery,
        replication_target: initial_replication_target(None),
        diskless: false,
        writer_handle: Arc::new(Mutex::new(Some(writer))),
    };
    (p, dir)
}

pub(super) fn append_records(p: &Partition, count: i32) {
    use krabka_protocol::records::{Attributes, Record, RecordBatch};

    let mut batch = RecordBatch {
        base_offset: 0,
        partition_leader_epoch: -1,
        attributes: Attributes::default(),
        last_offset_delta: count - 1,
        base_timestamp: 1_700_000_000,
        max_timestamp: 1_700_000_000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: (0..count)
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
}
