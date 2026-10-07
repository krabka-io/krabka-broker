//! Fixtures that more than one of this module's unit-test modules needs: a
//! three-record batch to seed a log with, and a spawned `Partition` backed by
//! that log on disk.

use std::{
    path::Path,
    sync::{Arc, atomic::Ordering},
};

use krabka_log::{Log, LogConfig};
use krabka_metadata::NodeId;
use krabka_protocol::records::RecordBatch;

use crate::partition::Partition;

/// A batch of `count` records, stamped with the wall clock the way a real
/// producer stamps one. The timestamp matters: `retention.ms` reads the
/// batch's `max_timestamp` off the index the flusher builds from this, and a
/// batch left at the epoch would be older than any retention window.
fn batch(count: i32) -> RecordBatch {
    RecordBatch {
        partition_leader_epoch: 0,
        ..crate::test_support::repeated_records_batch(count, crate::time_util::now_ms())
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
