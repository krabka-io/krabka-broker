//! Integrity of one archived segment, and the facts it yields.
//!
//! This module owns the decision that a segment is safe to rehydrate. It
//! fetches the segment's artifacts, walks the `.log` batch by batch with
//! `crabka_protocol::records::validate_one_v2_batch`, and checks framing and
//! CRC without decoding records. A batch whose declared length overruns the
//! object is a truncated segment; a batch whose CRC disagrees with its body is
//! a checksum mismatch, reported with the object key and the byte position. It
//! also checks that the copy is not torn: a segment that carries a log but no
//! time index was archived only in part. From the batch headers it derives the
//! two facts the rest of the pipeline needs and the archive metadata cannot be
//! trusted for, the segment's true `end_offset` and `max_timestamp_ms`, and it
//! returns the verified log bytes so the segment is fetched exactly once.

use bytes::Bytes;
use crabka_ids::{LeaderEpoch, Offset};
use crabka_remote_storage::TopicIdPartition;
use uuid::Uuid;

use crate::{backend::ArchiveStore, discover::SegmentInventory, error::RestoreError};

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

/// Fetch and verify one segment.
///
/// # Errors
///
/// Returns [`RestoreError::TornCopy`] when a required artifact is absent,
/// [`RestoreError::TruncatedSegment`] when the log ends inside a batch,
/// [`RestoreError::ChecksumMismatch`] when a batch CRC disagrees with its
/// body, and [`RestoreError::ObjectStore`] when a fetch fails.
pub async fn verify_segment(
    _store: &ArchiveStore,
    _partition: &TopicIdPartition,
    _segment: &SegmentInventory,
) -> Result<VerifiedSegment, RestoreError> {
    todo!("fetch the segment's artifacts, check framing and CRC, and derive its facts")
}
