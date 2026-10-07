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
    crate::test_support::open_partition(log_dir, topic, partition.get())
}

pub(super) fn append_records(part: &Arc<Partition>, count: i32) {
    let mut batch = crate::test_support::repeated_records_batch(count, 1_700_000_000);
    part.log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .append(&mut batch)
        .expect("append source records");
}

/// Appends a one-record batch stamped with leader epoch 1.
pub(super) fn append_value_batch(part: &Arc<Partition>, value_size: usize) {
    append_epoch_batch(part, value_size, 1);
}

/// Appends a one-record batch stamped with `leader_epoch`, as a replicated
/// partition holds them.
pub(super) fn append_epoch_batch(part: &Arc<Partition>, value_size: usize, leader_epoch: i32) {
    let mut batch = RecordBatch {
        base_offset: 0,
        partition_leader_epoch: leader_epoch,
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
            value: Some(Bytes::from(vec![b'x'; value_size])),
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
