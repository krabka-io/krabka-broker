//! Integrity of one archived segment, and the facts it yields.
//!
//! This module owns the decision that a segment is safe to rehydrate. It
//! fetches the segment's artifacts, walks the `.log` batch by batch with
//! `krabka_protocol::records::validate_one_v2_batch`, and checks framing and
//! CRC without decoding records. A batch whose declared length overruns the
//! object is a truncated segment; a batch whose CRC disagrees with its body is
//! a checksum mismatch, reported with the object key and the byte position. It
//! also checks that the copy is not torn: a segment that carries a log but no
//! time index was archived only in part. From the batch headers it derives the
//! two facts the rest of the pipeline needs and the archive metadata cannot be
//! trusted for, the segment's true `end_offset` and `max_timestamp_ms`, and it
//! returns the verified log bytes so the segment is fetched exactly once.
//!
//! This file holds the orchestration and the size caps every fetch is held to.
//! The per-artifact checks live beside it: [`self::log_walk`] for the `.log`
//! pass, [`self::index`] for the sparse sidecar indexes, [`self::snapshot`] for
//! the producer-state snapshot, and [`self::leader_epoch`] for the
//! leader-epoch checkpoint.

use bytes::Bytes;
use krabka_ids::{LeaderEpoch, Offset};
use krabka_object_store::{ObjectOps, ObjectStoreError};
use krabka_remote_storage::TopicIdPartition;
use object_store::path::Path;
use uuid::Uuid;

mod index;
mod leader_epoch;
mod log_walk;
mod snapshot;

#[cfg(test)]
mod tests;

use self::{
    index::{validate_offset_index, validate_time_index, validate_txn_index},
    leader_epoch::parse_leader_epoch_checkpoint,
    log_walk::walk_log,
    snapshot::validate_producer_snapshot,
};
use crate::{
    backend::ArchiveStore,
    discover::{ArchiveObject, SegmentInventory},
    error::RestoreError,
};

/// What verification established about one segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFacts {
    /// The per-copy segment id.
    pub segment_id: Uuid,
    /// First offset the segment holds.
    pub base_offset: Offset,
    /// Last offset the segment holds, derived from the batch headers.
    pub end_offset: Offset,
    /// Highest record timestamp in the segment, derived from the batch
    /// headers. It is `-1` for a segment with no timestamped record.
    pub max_timestamp_ms: i64,
    /// Batches the segment holds.
    pub batches: u64,
    /// Records the batch headers account for.
    pub records: u64,
    /// Size of the verified `.log`, in bytes.
    pub log_bytes: u64,
    /// The offset each leader epoch starts at, from the leader-epoch
    /// checkpoint. The target partition needs it to answer `OffsetForLeaderEpoch`.
    pub leader_epochs: Vec<(LeaderEpoch, Offset)>,
}

/// A segment that passed verification, with the bytes that passed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedSegment {
    /// What the verification established.
    pub facts: SegmentFacts,
    /// The verified `.log` bytes.
    pub log: Bytes,
}

/// Guard against an unbounded read of a corrupt or hostile `.log` object.
/// Kafka's default `segment.bytes` is 1 GiB, and an operator can raise it, so
/// this cap is a generous multiple of that default rather than the exact
/// configured value, which this offline tool never sees.
const MAX_LOG_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Guard for the sparse `.index` and `.timeindex` sidecars. An entry lands
/// only every `index.interval.bytes` (4 KiB by default), so even a segment at
/// the [`MAX_LOG_BYTES`] cap produces a sidecar many orders of magnitude
/// smaller than this.
const MAX_INDEX_BYTES: u64 = 256 * 1024 * 1024;

/// Guard for the `.txnindex` sidecar: one entry per aborted transaction.
const MAX_TXN_INDEX_BYTES: u64 = MAX_INDEX_BYTES;

/// Guard for the `.snapshot` producer-state sidecar: 46 bytes per producer.
const MAX_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024;

/// Guard for the `.leader_epoch_checkpoint` text sidecar: one short line per
/// leader change over the segment's lifetime.
const MAX_LEADER_EPOCH_BYTES: u64 = 16 * 1024 * 1024;

/// Fetch and verify one segment.
///
/// # Errors
///
/// Returns [`RestoreError::TornCopy`] when a required artifact is absent,
/// [`RestoreError::TruncatedSegment`] when the log ends inside a batch,
/// [`RestoreError::ChecksumMismatch`] when a batch CRC disagrees with its
/// body, and [`RestoreError::ObjectStore`] when a fetch fails.
pub async fn verify_segment(
    store: &ArchiveStore,
    partition: &TopicIdPartition,
    segment: &SegmentInventory,
) -> Result<VerifiedSegment, RestoreError> {
    // Checked in the order the broker's copy path writes the artifacts, so a
    // torn copy is reported by the first one actually missing.
    let log = require_artifact(segment.log.as_ref(), ".log", partition, segment)?;
    let offset_index =
        require_artifact(segment.offset_index.as_ref(), ".index", partition, segment)?;
    let time_index = require_artifact(
        segment.time_index.as_ref(),
        ".timeindex",
        partition,
        segment,
    )?;
    let producer_snapshot = require_artifact(
        segment.producer_snapshot.as_ref(),
        ".snapshot",
        partition,
        segment,
    )?;
    let leader_epoch = require_artifact(
        segment.leader_epoch.as_ref(),
        ".leader_epoch_checkpoint",
        partition,
        segment,
    )?;

    let ops = store.ops();
    // The `.log` is fetched exactly once, here; every later stage reads it
    // from the `Bytes` this function returns, not from the archive again.
    let log_bytes = fetch_capped(ops, &log.key, MAX_LOG_BYTES).await?;
    let offset_index_bytes = fetch_capped(ops, &offset_index.key, MAX_INDEX_BYTES).await?;
    let time_index_bytes = fetch_capped(ops, &time_index.key, MAX_INDEX_BYTES).await?;
    let producer_snapshot_bytes =
        fetch_capped(ops, &producer_snapshot.key, MAX_SNAPSHOT_BYTES).await?;
    let leader_epoch_bytes = fetch_capped(ops, &leader_epoch.key, MAX_LEADER_EPOCH_BYTES).await?;
    let transaction_index = match &segment.transaction_index {
        Some(artifact) => Some((
            artifact.key.clone(),
            fetch_capped(ops, &artifact.key, MAX_TXN_INDEX_BYTES).await?,
        )),
        None => None,
    };

    let walked = walk_log(&log.key, segment, &log_bytes)?;
    let log_bytes_len = u64::try_from(log_bytes.len()).unwrap_or(u64::MAX);

    validate_offset_index(
        &offset_index.key,
        &offset_index_bytes,
        segment.base_offset,
        walked.end_offset,
        log_bytes_len,
    )?;
    validate_time_index(
        &time_index.key,
        &time_index_bytes,
        segment.base_offset,
        walked.end_offset,
    )?;
    if let Some((key, bytes)) = &transaction_index {
        validate_txn_index(key, bytes, segment.base_offset, walked.end_offset)?;
    }
    validate_producer_snapshot(&producer_snapshot.key, &producer_snapshot_bytes)?;
    let leader_epochs = parse_leader_epoch_checkpoint(&leader_epoch.key, &leader_epoch_bytes)?;

    Ok(VerifiedSegment {
        facts: SegmentFacts {
            segment_id: segment.segment_id,
            base_offset: segment.base_offset,
            end_offset: walked.end_offset,
            max_timestamp_ms: walked.max_timestamp_ms,
            batches: walked.batches,
            records: walked.records,
            log_bytes: log_bytes_len,
            leader_epochs,
        },
        log: log_bytes,
    })
}

/// Look up one mandatory artifact, or report the torn copy it reveals.
fn require_artifact<'a>(
    artifact: Option<&'a ArchiveObject>,
    name: &str,
    partition: &TopicIdPartition,
    segment: &SegmentInventory,
) -> Result<&'a ArchiveObject, RestoreError> {
    artifact.ok_or_else(|| RestoreError::TornCopy {
        topic: partition.topic.clone(),
        partition: partition.partition,
        segment_id: segment.segment_id,
        artifact: name.to_owned(),
    })
}

/// Fetch a whole object, refusing it before buffering any bytes if it exceeds
/// `max_bytes`. This mirrors `krabka_object_store::read_capped`'s head-then-get
/// guard against OOM on a corrupt or oversized archive object; it is
/// reimplemented here because that helper takes the concrete
/// `Arc<dyn object_store::ObjectStore>` rather than the [`ObjectOps`] surface
/// [`ArchiveStore`] exposes.
async fn fetch_capped(
    ops: &dyn ObjectOps,
    key: &Path,
    max_bytes: u64,
) -> Result<Bytes, RestoreError> {
    let meta = ops.head(key).await?;
    if meta.size > max_bytes {
        return Err(ObjectStoreError::TooLarge {
            key: key.clone(),
            size: meta.size,
            max_bytes,
        }
        .into());
    }
    Ok(ops.get(key).await?)
}

/// Best-effort `i64` → `u64` for a diagnostic [`RestoreError`] field. A Kafka
/// offset or timestamp is never negative in a well-formed segment; this only
/// has to render something sensible for corrupt input, never panic, and never
/// allocate.
fn offset_as_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}
