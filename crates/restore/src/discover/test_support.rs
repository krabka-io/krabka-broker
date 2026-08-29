//! Fixtures shared by the discovery unit tests: an archive written key by key
//! at the exact KIP-405 layout, the [`SegmentInventory`] a complete copy of one
//! segment should produce, and the `--rlmm-snapshot` file the reconciliation
//! tests hand back to the scan.

use std::collections::BTreeMap;

use clap::Parser as _;
use krabka_ids::{LeaderEpoch, Offset};
use krabka_remote_storage::{
    RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentState,
    RlmmCacheDump, TopicIdPartition, kafka_uuid,
};
use krabka_remote_storage_topic::Snapshot;
use uuid::Uuid;

use crate::{
    args::RestoreArgs,
    backend::ArchiveStore,
    discover::{ArchiveObject, SegmentInventory},
};

/// transaction index is excluded on purpose: it is optional even for a
/// copy discovery has no reason to call torn.
pub(super) const FULL_SEGMENT_SUFFIXES: [&str; 5] = [
    ".log",
    ".index",
    ".timeindex",
    ".snapshot",
    ".leader_epoch_checkpoint",
];

/// Bytes every fixture artifact is written with, so every
/// [`ArchiveObject::size`] in an expected structure is this length.
pub(super) const STUB_BYTES: &[u8] = b"stub";

pub(super) fn args_from(archive_dir: &std::path::Path, extra: &[&str]) -> RestoreArgs {
    let mut argv: Vec<String> = vec![
        "krabka-restore".to_owned(),
        "--log-dir".to_owned(),
        "/target".to_owned(),
        "--archive-local".to_owned(),
        archive_dir.display().to_string(),
    ];
    argv.extend(extra.iter().map(|s| (*s).to_owned()));
    crate::Cli::parse_from(argv).args
}

/// The directory-naming identity of one partition, factored out of
/// [`write_artifact`]'s arguments so the helper stays under Clippy's
/// argument-count limit.
#[derive(Clone, Copy)]
pub(super) struct PartitionKey<'a> {
    pub(super) topic: &'a str,
    pub(super) partition: i32,
    pub(super) topic_id: Uuid,
}

/// Write one artifact at the exact KIP-405 archive key layout, by hand.
pub(super) fn write_artifact(
    root: &std::path::Path,
    prefix: Option<&str>,
    partition: PartitionKey<'_>,
    base_offset: i64,
    segment_id: Uuid,
    suffix: &str,
) {
    let dir_name = format!(
        "{}-{}-{}",
        partition.topic,
        partition.partition,
        kafka_uuid(partition.topic_id)
    );
    let file_name = format!("{base_offset:020}-{}{suffix}", kafka_uuid(segment_id));
    let mut dir = root.to_path_buf();
    if let Some(prefix) = prefix {
        dir.push(prefix);
    }
    dir.push(dir_name);
    std::fs::create_dir_all(&dir).expect("create partition dir");
    std::fs::write(dir.join(file_name), STUB_BYTES).expect("write artifact");
}

/// Write every artifact of one complete segment copy.
pub(super) fn write_full_segment(
    root: &std::path::Path,
    topic: &str,
    partition: i32,
    topic_id: Uuid,
    base_offset: i64,
    segment_id: Uuid,
) {
    let key = PartitionKey {
        topic,
        partition,
        topic_id,
    };
    for suffix in FULL_SEGMENT_SUFFIXES {
        write_artifact(root, None, key, base_offset, segment_id, suffix);
    }
}

/// The [`SegmentInventory`] a call to [`write_full_segment`] with the same
/// arguments produces.
pub(super) fn expected_full_segment(
    store: &ArchiveStore,
    topic: &str,
    partition: i32,
    topic_id: Uuid,
    base_offset: i64,
    segment_id: Uuid,
) -> SegmentInventory {
    let object = |suffix: &str| {
        Some(ArchiveObject {
            key: store.key(&format!(
                "{topic}-{partition}-{}/{base_offset:020}-{}{suffix}",
                kafka_uuid(topic_id),
                kafka_uuid(segment_id),
            )),
            size: STUB_BYTES.len() as u64,
        })
    };
    SegmentInventory {
        segment_id,
        base_offset: Offset(base_offset),
        log: object(".log"),
        offset_index: object(".index"),
        time_index: object(".timeindex"),
        producer_snapshot: object(".snapshot"),
        leader_epoch: object(".leader_epoch_checkpoint"),
        transaction_index: None,
    }
}

/// One RLMM-tracked segment, for a `--rlmm-snapshot` fixture.
pub(super) fn snapshot_segment(
    topic: &str,
    partition: i32,
    topic_id: Uuid,
    segment_id: Uuid,
    base_offset: i64,
    state: RemoteLogSegmentState,
) -> RemoteLogSegmentMetadata {
    RemoteLogSegmentMetadata::new(
        RemoteLogSegmentId::new(
            TopicIdPartition::new(topic_id, topic, partition),
            segment_id,
        ),
        base_offset,
        base_offset,
        0,
        1,
        0,
        RemoteLogSegmentDetails::new(
            i32::try_from(STUB_BYTES.len()).expect("stub length fits in i32"),
            state,
            maplit::btreemap! {LeaderEpoch(0) => base_offset},
        ),
    )
    .expect("valid segment metadata")
}

pub(super) fn write_snapshot(path: &std::path::Path, dump: RlmmCacheDump) {
    Snapshot {
        committed_offsets: Vec::new(),
        dump,
    }
    .write_atomic(path)
    .expect("write snapshot");
}
