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
//!
//! This file holds the scan itself and the shapes it produces. The
//! snapshot comparison, which is the only part with a second source of truth to
//! weigh, lives beside it in [`self::reconcile`].

use std::collections::HashMap;

use krabka_ids::Offset;
use krabka_remote_storage::{
    IndexType, LOG_FILE_SUFFIX, TopicIdPartition, parse_partition_dir_name, parse_segment_file_name,
};
use object_store::path::Path;
use uuid::Uuid;

mod reconcile;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::reconcile::reconcile_with_snapshot;
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

impl ArchiveInventory {
    /// Whether the scan found `topic`-`partition`.
    ///
    /// Compared by topic name and partition index, the pair a bound is
    /// written against, not by the archive's internal topic id.
    #[must_use]
    pub fn holds(&self, topic: &str, partition: i32) -> bool {
        holds_partition(&self.partitions, topic, partition)
    }
}

/// Whether `partitions` names `topic`-`partition`, by topic name and
/// partition index rather than by the archive's internal topic id.
///
/// Shared by [`ArchiveInventory::holds`] and by [`inventory`]'s own bound
/// check, which runs against the partitions built so far before they are
/// wrapped in an [`ArchiveInventory`].
fn holds_partition(partitions: &[PartitionInventory], topic: &str, partition: i32) -> bool {
    partitions
        .iter()
        .any(|entry| entry.partition.topic == topic && entry.partition.partition == partition)
}

/// One artifact of a segment, as [`inventory`] is still accumulating it.
///
/// Distinguishes the segment's `.log` data from its indexes so the artifact a
/// key names can be assigned to the right [`SegmentInventory`] field without
/// an intermediate `unrecognized` entry for a key whose suffix is simply not
/// yet known at the point the accumulator slot is chosen.
#[derive(Debug, Clone, Copy)]
enum Artifact {
    /// The segment's `.log` data.
    Log,
    /// One of the segment's indexes.
    Index(IndexType),
}

/// Segments accumulated so far for one partition, keyed by segment id so an
/// artifact found later for a segment already seen joins the same entry.
#[derive(Debug, Default)]
struct PartitionAccum {
    /// The topic name, as the partition directory names it.
    topic: String,
    /// Segments seen so far, keyed by segment id.
    segments: HashMap<Uuid, SegmentInventory>,
}

/// A [`SegmentInventory`] with every artifact absent, the accumulator's
/// starting point for a segment id seen for the first time.
fn empty_segment(segment_id: Uuid, base_offset: Offset) -> SegmentInventory {
    SegmentInventory {
        segment_id,
        base_offset,
        log: None,
        offset_index: None,
        time_index: None,
        producer_snapshot: None,
        leader_epoch: None,
        transaction_index: None,
    }
}

/// Scan `store` and build the inventory of the partitions `args` selects.
///
/// # Errors
///
/// Returns [`RestoreError::ObjectStore`] when the listing fails,
/// [`RestoreError::UnknownPartition`] when a `--to-offset` or
/// `--exclude-offset` bound names a topic partition the scan did not find,
/// [`RestoreError::EmptyArchive`] when no selected topic has a segment, and
/// [`RestoreError::MetadataDisagreement`] when `--rlmm-snapshot` contradicts
/// the scan.
pub async fn inventory(
    store: &ArchiveStore,
    args: &RestoreArgs,
) -> Result<ArchiveInventory, RestoreError> {
    let listed = store.ops().list(store.root()).await?;
    let root = store.root().unwrap_or(Path::ROOT);

    let mut unrecognized = Vec::new();
    let mut accum: HashMap<(Uuid, i32), PartitionAccum> = HashMap::new();

    for object in listed {
        // Collected to owned `String`s immediately, so the parsed pieces
        // below never borrow `object.location` and it stays free to move
        // into `unrecognized` or clone into an `ArchiveObject` right after.
        let Some(relative) = object.location.prefix_match(&root) else {
            unrecognized.push(object.location);
            continue;
        };
        let relative: Vec<String> = relative.map(|part| part.as_ref().to_owned()).collect();
        let [partition_dir, segment_file] = relative.as_slice() else {
            unrecognized.push(object.location);
            continue;
        };

        let Some(dir) = parse_partition_dir_name(partition_dir) else {
            unrecognized.push(object.location);
            continue;
        };
        let Some(file) = parse_segment_file_name(segment_file) else {
            unrecognized.push(object.location);
            continue;
        };

        if !args.selects_topic(&dir.topic) {
            unrecognized.push(object.location);
            continue;
        }

        let artifact = if file.suffix == LOG_FILE_SUFFIX {
            Artifact::Log
        } else if let Some(index_type) = IndexType::from_suffix(file.suffix) {
            Artifact::Index(index_type)
        } else {
            unrecognized.push(object.location);
            continue;
        };

        let archive_object = ArchiveObject {
            key: object.location.clone(),
            size: object.size,
        };

        let partition_accum = accum
            .entry((dir.topic_id, dir.partition))
            .or_insert_with(|| PartitionAccum {
                topic: dir.topic.clone(),
                segments: HashMap::new(),
            });
        let segment_accum = partition_accum
            .segments
            .entry(file.segment_id)
            .or_insert_with(|| empty_segment(file.segment_id, Offset(file.base_offset)));

        match artifact {
            Artifact::Log => segment_accum.log = Some(archive_object),
            Artifact::Index(IndexType::Offset) => segment_accum.offset_index = Some(archive_object),
            Artifact::Index(IndexType::Timestamp) => {
                segment_accum.time_index = Some(archive_object);
            }
            Artifact::Index(IndexType::ProducerSnapshot) => {
                segment_accum.producer_snapshot = Some(archive_object);
            }
            Artifact::Index(IndexType::LeaderEpoch) => {
                segment_accum.leader_epoch = Some(archive_object);
            }
            Artifact::Index(IndexType::Transaction) => {
                segment_accum.transaction_index = Some(archive_object);
            }
        }
    }

    let mut partitions: Vec<PartitionInventory> = accum
        .into_iter()
        .map(|((topic_id, partition), partition_accum)| {
            let mut segments: Vec<SegmentInventory> =
                partition_accum.segments.into_values().collect();
            segments.sort_by_key(|segment| segment.base_offset);
            PartitionInventory {
                partition: TopicIdPartition::new(topic_id, partition_accum.topic, partition),
                segments,
            }
        })
        .collect();
    partitions.sort_by(|a, b| {
        (a.partition.topic.as_str(), a.partition.partition)
            .cmp(&(b.partition.topic.as_str(), b.partition.partition))
    });

    // Checked against the raw scan, before the RLMM reconciliation and the
    // empty-archive check below: a bound that names a partition the scan
    // never saw is a typo the operator needs to hear about specifically,
    // not the more general "nothing selected" of `EmptyArchive`, and not
    // masked by a `--rlmm-snapshot` disagreement on some other partition.
    for partition in args
        .to_offset
        .iter()
        .map(|bound| &bound.partition)
        .chain(args.exclude_offset.iter().map(|range| &range.partition))
    {
        if !holds_partition(&partitions, &partition.topic, partition.partition) {
            return Err(RestoreError::UnknownPartition {
                topic: partition.topic.clone(),
                partition: partition.partition,
            });
        }
    }

    if let Some(snapshot_path) = &args.archive.rlmm_snapshot {
        reconcile_with_snapshot(&mut partitions, args, snapshot_path)?;
    }

    if partitions.is_empty() {
        return Err(RestoreError::EmptyArchive {
            prefix: store.prefix().unwrap_or("").to_owned(),
        });
    }

    Ok(ArchiveInventory {
        partitions,
        unrecognized,
    })
}
