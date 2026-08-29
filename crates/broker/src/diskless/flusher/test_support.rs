//! Fixtures that more than one of this module's unit-test modules needs: a
//! three-record batch to seed a log with, and a spawned `Partition` backed by
//! that log on disk.

use std::{
    path::Path,
    sync::{Arc, atomic::Ordering},
};

use krabka_log::{Log, LogConfig};
use krabka_metadata::NodeId;
use krabka_protocol::records::{Attributes, Record, RecordBatch};

use crate::partition::Partition;

fn batch(count: i32) -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        partition_leader_epoch: 0,
        attributes: Attributes::default(),
        last_offset_delta: count - 1,
        base_timestamp: 0,
        max_timestamp: 0,
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
    }
}

pub(super) fn test_partition(
    root: &Path,
    topic: &str,
    partition: i32,
    diskless: bool,
    leader: NodeId,
) -> Arc<Partition> {
    let partition_dir = root.join(format!("{topic}-{partition}"));
    std::fs::create_dir_all(&partition_dir).unwrap();
    let mut log = Log::open(&partition_dir, LogConfig::default()).unwrap();
    log.append(&mut batch(3)).unwrap();
    let handle = crate::broker::spawn_partition(
        topic.to_owned(),
        krabka_ids::PartitionIndex(partition),
        root.to_path_buf(),
        log,
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        diskless,
    );
    handle.current_leader.store(leader.0, Ordering::Relaxed);
    handle
}
