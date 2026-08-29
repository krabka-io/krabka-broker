//! Fixtures the future-log unit tests share: a deterministic `StampSource`, a
//! default `MovePolicy`, and builders for a source `Partition` and the record
//! batches it holds.

use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig};
use krabka_protocol::records::{Attributes, Record, RecordBatch};
use krabka_units::{mebibytes, millis};

use super::MovePolicy;
use crate::{log_dir, partition::Partition};

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
    let part_dir = log_dir::partition_dir(log_dir, topic, partition.get());
    std::fs::create_dir_all(&part_dir).unwrap();
    let log = Log::open(&part_dir, LogConfig::default()).unwrap();
    crate::broker::spawn_partition(
        topic.to_string(),
        partition,
        log_dir.to_path_buf(),
        log,
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    )
}

pub(super) fn append_records(part: &Arc<Partition>, count: i32) {
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
                value: Some(Bytes::from_static(b"v")),
                headers: vec![],
            })
            .collect(),
    };
    part.log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .append(&mut batch)
        .expect("append source records");
}

pub(super) fn append_value_batch(part: &Arc<Partition>, value_size: usize) {
    let mut batch = RecordBatch {
        base_offset: 0,
        partition_leader_epoch: -1,
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
