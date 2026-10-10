//! Fixtures the cleaner's unit tests share: a keyed record batch, a partition
//! whose log holds compactable duplicates, and the record count that shows
//! whether a sweep compacted one.

use std::sync::Arc;

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

pub(super) async fn compactable_partition(
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
    .await
}

/// The same fixture against a caller-supplied log-dir registry, for the tests
/// that watch a compaction failure reach it. The default registry every other
/// fixture builds has nowhere to report a flip to.
pub(super) async fn compactable_partition_in_registry(
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
    .await
}

/// Make every compaction pass on `partition`'s log fail with a real
/// `io::Error`, by putting a directory where the rewrite has to create its
/// `.cleaned` file. Opening a directory for writing fails with `EISDIR` for
/// every user including root, so this is a storage failure the filesystem
/// raises rather than one a test hook fabricates.
///
/// The rewrite streams into `.cleaned` files, which `atomic_swap` promotes
/// through `.swap`. Returns the blocked paths so a test can unblock them and
/// watch the cleaner recover.
pub(super) fn block_compaction_swap(root: &TempDir, topic: &str) -> Vec<std::path::PathBuf> {
    crate::test_support::block_log_artifact_paths(
        root.path(),
        topic,
        "log.cleaned",
        "block the rewrite path",
    )
}

/// The same fixture over a caller-chosen `LogConfig`, for the cleaner's
/// dirty-ratio and compaction-lag tests.
pub(super) async fn compactable_partition_with_config(
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
    .await
}

async fn open_compactable_partition(
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

    crate::test_support::committed_partition(
        root.path(),
        topic,
        PartitionIndex(partition_id),
        leader,
        log,
        log_dir_status,
    )
    .await
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

/// Register a compactable partition and capture its original record count.
pub(super) async fn register_compactable(
    root: &TempDir,
    registry: &crate::partition_registry::PartitionRegistry,
    topic: &str,
) -> (Arc<Partition>, usize) {
    let partition = compactable_partition(
        root,
        topic,
        0,
        NodeId(7),
        krabka_log::CleanupPolicy::Compact,
    )
    .await;
    let before = record_count(&partition);
    registry.insert(topic.into(), PartitionIndex(0), Arc::clone(&partition));
    (partition, before)
}
