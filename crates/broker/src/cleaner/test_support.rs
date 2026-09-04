//! Fixtures the cleaner's unit tests share: a keyed record batch, a partition
//! whose log holds compactable duplicates, and the record count that shows
//! whether a sweep compacted one.

use std::sync::{Arc, atomic::Ordering};

use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_metadata::NodeId;
use krabka_protocol::records::{Record, RecordBatch};
use tempfile::TempDir;

use crate::partition::Partition;

fn keyed_batch(base: i64, key: &[u8], value: &[u8]) -> RecordBatch {
    RecordBatch {
        base_offset: base,
        records: vec![Record {
            offset_delta: 0,
            key: Some(Bytes::copy_from_slice(key)),
            value: Some(Bytes::copy_from_slice(value)),
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub(super) fn compactable_partition(
    root: &TempDir,
    topic: &str,
    partition_id: i32,
    leader: NodeId,
    cleanup_policy: krabka_log::CleanupPolicy,
) -> Arc<Partition> {
    compactable_partition_with_config(
        root,
        topic,
        partition_id,
        leader,
        krabka_log::LogConfig {
            cleanup_policy,
            segment_size: krabka_units::bytes(256),
            ..Default::default()
        },
    )
}

/// The same fixture against a caller-supplied log-dir registry, for the tests
/// that watch a compaction failure reach it. The default registry every other
/// fixture builds has nowhere to report a flip to.
pub(super) fn compactable_partition_in_registry(
    root: &TempDir,
    topic: &str,
    leader: NodeId,
    log_dir_status: crate::log_dir_status::LogDirRegistry,
) -> Arc<Partition> {
    open_compactable_partition(
        root,
        topic,
        0,
        leader,
        krabka_log::LogConfig {
            cleanup_policy: krabka_log::CleanupPolicy::Compact,
            segment_size: krabka_units::bytes(256),
            ..Default::default()
        },
        log_dir_status,
    )
}

/// Make every compaction pass on `partition`'s log fail with a real
/// `io::Error`, by putting a directory where the rewrite has to create its
/// `.swap` file. Opening a directory for writing fails with `EISDIR` for
/// every user including root, so this is a storage failure the filesystem
/// raises rather than one a test hook fabricates.
///
/// Returns the paths it blocked so a test can unblock them and watch the
/// cleaner recover.
pub(super) fn block_compaction_swap(root: &TempDir, topic: &str) -> Vec<std::path::PathBuf> {
    let part_dir = crate::log_dir::partition_dir(root.path(), topic, 0);
    let mut blocked = Vec::new();
    for entry in std::fs::read_dir(&part_dir).expect("read partition dir") {
        let path = entry.expect("partition dir entry").path();
        if path.extension().is_some_and(|ext| ext == "log") {
            let swap = path.with_extension("log.swap");
            std::fs::create_dir(&swap).expect("block the swap path");
            blocked.push(swap);
        }
    }
    assert2::assert!(!blocked.is_empty(), "the fixture sealed no segment to block");
    blocked
}

/// The same fixture over a caller-chosen `LogConfig`, for the cleaner's
/// dirty-ratio and compaction-lag tests.
pub(super) fn compactable_partition_with_config(
    root: &TempDir,
    topic: &str,
    partition_id: i32,
    leader: NodeId,
    cfg: krabka_log::LogConfig,
) -> Arc<Partition> {
    open_compactable_partition(
        root,
        topic,
        partition_id,
        leader,
        cfg,
        crate::log_dir_status::LogDirRegistry::default(),
    )
}

fn open_compactable_partition(
    root: &TempDir,
    topic: &str,
    partition_id: i32,
    leader: NodeId,
    cfg: krabka_log::LogConfig,
    log_dir_status: crate::log_dir_status::LogDirRegistry,
) -> Arc<Partition> {
    let part_dir = crate::log_dir::partition_dir(root.path(), topic, partition_id);
    std::fs::create_dir_all(&part_dir).expect("create partition dir");
    let mut log = krabka_log::Log::open(&part_dir, cfg).expect("open compactable log");
    for idx in 0..12 {
        let mut batch = keyed_batch(idx, b"duplicate-key", format!("v{idx}").as_bytes());
        log.append(&mut batch).expect("append duplicate-key batch");
    }
    let mut active = keyed_batch(12, b"active-key", b"active");
    log.append(&mut active).expect("append active batch");

    let part = crate::broker::spawn_partition(
        topic.to_string(),
        PartitionIndex(partition_id),
        root.path().to_path_buf(),
        log,
        log_dir_status,
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    );
    part.current_leader.store(leader.0, Ordering::Relaxed);
    part
}

pub(super) fn record_count(partition: &Partition) -> usize {
    let read = partition
        .log
        .lock()
        .expect("partition log lock")
        .read(krabka_log::Offset(0), krabka_units::mebibytes(1))
        .expect("read partition log");
    read.batches.iter().map(|batch| batch.records.len()).sum()
}
