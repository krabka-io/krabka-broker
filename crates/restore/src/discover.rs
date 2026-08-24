//! The archive inventory: what the archive holds, before anything is read.
//!
//! This module owns the scan. It lists the archive under the operator's
//! prefix, decodes every key with the KIP-405 archive key codec
//! (`parse_partition_dir_name` and `parse_segment_file_name`), and groups
//! the result into one [`SegmentInventory`] per archived segment, with the
//! object key and size of each artifact that is present. It keeps every key it
//! cannot attribute rather than discarding it, because an unrecognized key is
//! the first sign that `--archive-prefix` points at the wrong tree. When
//! `--rlmm-snapshot` is given, this module reconciles the scan against it and
//! reports a disagreement instead of choosing a winner; without the snapshot a
//! segment the old cluster had marked for deletion looks exactly like a live
//! one. This module decides nothing about completeness or integrity. It
//! reports presence, and the verify stage judges it.

use crabka_ids::Offset;
use crabka_remote_storage::TopicIdPartition;
use object_store::path::Path;
use uuid::Uuid;

use crate::{args::RestoreArgs, backend::ArchiveStore, error::RestoreError};

/// One artifact of one archived segment, as the scan found it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveObject {
    /// The full object key, prefix included.
    pub key: Path,
    /// The object size the store reports. It stays the store's own `u64`,
    /// because it is compared against byte positions inside the object.
    pub size: u64,
}

/// One archived segment and the artifacts the archive holds for it.
///
/// Kafka copies the log, the offset index, the time index, the producer
/// snapshot, and the leader-epoch checkpoint together. The transaction index
/// is absent for a segment with no aborted transaction, so its absence is not
/// by itself a torn copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentInventory {
    /// The per-copy segment id the archive names the artifacts with.
    pub segment_id: Uuid,
    /// First offset the segment holds, as the key encodes it.
    pub base_offset: Offset,
    /// The segment data.
    pub log: Option<ArchiveObject>,
    /// The sparse offset index.
    pub offset_index: Option<ArchiveObject>,
    /// The sparse time index.
    pub time_index: Option<ArchiveObject>,
    /// The producer id snapshot.
    pub producer_snapshot: Option<ArchiveObject>,
    /// The leader-epoch checkpoint.
    pub leader_epoch: Option<ArchiveObject>,
    /// The aborted-transaction index. It is optional.
    pub transaction_index: Option<ArchiveObject>,
}

/// One partition of the archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionInventory {
    /// The partition the archive directory names.
    pub partition: TopicIdPartition,
    /// Every segment found for the partition, in base-offset order.
    pub segments: Vec<SegmentInventory>,
}

/// Everything the scan found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArchiveInventory {
    /// The selected partitions, ordered by topic then partition index.
    pub partitions: Vec<PartitionInventory>,
    /// Keys the codec could not attribute to a segment.
    pub unrecognized: Vec<Path>,
}

/// Scan `store` and build the inventory of the partitions `args` selects.
///
/// # Errors
///
/// Returns [`RestoreError::ObjectStore`] when the listing fails,
/// [`RestoreError::EmptyArchive`] when no selected topic has a segment, and
/// [`RestoreError::MetadataDisagreement`] when `--rlmm-snapshot` contradicts
/// the scan.
pub async fn inventory(
    _store: &ArchiveStore,
    _args: &RestoreArgs,
) -> Result<ArchiveInventory, RestoreError> {
    todo!("list the archive, decode the keys, and reconcile with the RLMM snapshot")
}
