//! Fixtures the future-log unit tests share: a deterministic `StampSource`, a
//! default `MovePolicy`, and builders for a source `Partition` and the record
//! batches it holds.

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_protocol::records::{Attributes, Record, RecordBatch};
use krabka_units::{mebibytes, millis};

use super::MovePolicy;
use crate::partition::Partition;

#[derive(Debug)]
pub(super) struct TestStampSource(pub(super) AtomicU64);

impl krabka_log::StampSource for TestStampSource {
    fn next_stamp(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

pub(super) fn test_policy() -> MovePolicy {
    MovePolicy {
        retry_backoff: millis(5),
        read_chunk: mebibytes(1),
        throttle: Arc::new(crate::throttle::TokenBucket::new()),
    }
}

/// Build a `Partition` rooted at `<log_dir>/<topic>-<partition>`
/// and do not use `Broker::start`. Returns the parent dir
/// and the `Arc<Partition>`.
pub(super) fn fixture_partition(
    log_dir: &Path,
    topic: &str,
    partition: PartitionIndex,
) -> Arc<Partition> {
    crate::test_support::open_partition(
        log_dir,
        crate::test_support::StandalonePartitionSetup {
            topic,
            partition: krabka_ids::PartitionIndex(partition.get()),
            ..Default::default()
        },
    )
}

pub(super) fn append_records(
    part: &Arc<Partition>,
    setup: crate::test_support::PartitionRecordsSetup,
) {
    let mut batch = crate::test_support::partition_records_batch(setup);
    part.log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .append(&mut batch)
        .expect("append source records");
}

/// Append the source data before making the partition visible in the registry.
pub(super) fn append_and_register_source(
    part: &Arc<Partition>,
    partitions: &crate::partition_registry::PartitionRegistry,
    setup: crate::test_support::PartitionRecordsSetup,
) {
    append_records(part, setup);
    partitions.insert("t".into(), PartitionIndex(0), part.clone());
}

#[derive(Clone, Copy)]
pub(super) struct RecordValueBytes(pub usize);

impl Default for RecordValueBytes {
    fn default() -> Self {
        Self(20)
    }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct FutureRecordSetup {
    pub value_size: RecordValueBytes,
    #[default(krabka_ids::LeaderEpoch(1))]
    pub leader_epoch: krabka_ids::LeaderEpoch,
}

pub(super) use append_epoch_batch as append_value_batch;

/// Appends one repeated-value record at the selected leader epoch.
pub(super) fn append_epoch_batch(part: &Arc<Partition>, setup: FutureRecordSetup) {
    let mut batch = RecordBatch {
        base_offset: 0,
        partition_leader_epoch: setup.leader_epoch.0,
        attributes: Attributes::default(),
        last_offset_delta: 0,
        base_timestamp: 1_700_000_000,
        max_timestamp: 1_700_000_000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: vec![Record {
            attributes: 0,
            offset_delta: 0,
            timestamp_delta: 0,
            key: None,
            value: Some(Bytes::from(vec![b'x'; setup.value_size.0])),
            headers: vec![],
        }],
    };
    part.log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .append(&mut batch)
        .expect("append source batch");
}

/// Open a staged future replica at the same path a real move uses.
pub(super) fn open_future_log(target: &Path) -> (PathBuf, Arc<Mutex<krabka_log::Log>>) {
    let path = crate::log_dir::future_partition_dir(target, "t", 0);
    std::fs::create_dir_all(&path).unwrap();
    let log = krabka_log::Log::open(&path, krabka_log::LogConfig::default()).unwrap();
    (path, Arc::new(Mutex::new(log)))
}
