//! Writing the restored cluster: the format, then the partition data.
//!
//! This module owns everything that touches the target log directory. It
//! formats the target through `crabka_format::run_from_args_with_records`,
//! forwarding the `--cluster-id`, `--node-id`, `--standalone`,
//! `--initial-controllers`, `--no-initial-controllers`, and
//! `--controller-listener` flags and seeding the `TopicRecord` and one
//! `PartitionRecord` per partition that the inventory recovered, so the
//! restored cluster boots with its topics already present. It then writes each
//! verified segment into the target partition, applying [`Predicates`] as it
//! walks the batches: a batch the bound keeps is written verbatim through
//! [`Log::append_verbatim_at`], with its base offset and leader epoch
//! restamped and its producer CRC untouched; a batch the bound filters is
//! rewritten through [`crabka_log::filter_batch`] and written through
//! [`Log::append_at`]; and a batch every one of whose records the bound
//! excludes is still written through [`Log::append_at`], as a bare header
//! with zero records and the archived `base_offset` and `last_offset_delta`
//! preserved. That third case is not optional: [`Log::append_at`] and
//! [`Log::append_verbatim_at`] both require `offset == log_end_offset()`, so
//! skipping a batch's write entirely leaves the target log's end offset
//! behind the archive's and makes every later batch in the partition
//! unappendable. Under `--dry-run` it does the same work and writes nothing.

use crabka_ids::Offset;
use crabka_remote_storage::TopicIdPartition;
use serde::Serialize;
use uuid::Uuid;

use crate::{
    args::RestoreArgs, bound::Predicates, discover::ArchiveInventory, error::RestoreError,
    verify::VerifiedSegment,
};

/// What writing one segment into the target produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SegmentOutcome {
    /// The per-copy segment id in the archive.
    pub segment_id: Uuid,
    /// First offset the written segment holds.
    pub base_offset: Offset,
    /// Last offset the written segment holds.
    pub end_offset: Offset,
    /// Batches written unchanged.
    pub batches_kept: u64,
    /// Batches re-encoded because the bound dropped some of their records.
    pub batches_rewritten: u64,
    /// Batches written as a bare header because the bound excluded every
    /// record. The offset range stays claimed; see
    /// [`crate::bound::BatchDecision::Empty`].
    pub batches_emptied: u64,
    /// Records written.
    pub records_kept: u64,
    /// Records the bound dropped.
    pub records_dropped: u64,
    /// Bytes written into the target segment.
    pub bytes_written: u64,
}

/// Format the target log directory, seed it with the recovered topics, and
/// return the cluster id it was formatted with.
///
/// The formatter generates a cluster id when none is given and does not report
/// it back, so this passes an explicit `--cluster-id` and keeps it for the
/// report. An operator who restores a cluster has to know its identity.
///
/// # Errors
///
/// Returns [`RestoreError::Format`] when the formatter rejects the target-side
/// flags, and [`RestoreError::Io`] when the target cannot be written.
pub async fn format_target(
    _args: &RestoreArgs,
    _inventory: &ArchiveInventory,
) -> Result<Uuid, RestoreError> {
    todo!("build the topic and partition records and run the formatter over --log-dir")
}

/// Write one verified segment into its target partition, under the bound.
///
/// # Errors
///
/// Returns [`RestoreError::Records`] when a batch will not re-encode, and
/// [`RestoreError::Log`] or [`RestoreError::Io`] when the target rejects the
/// write.
pub async fn write_segment(
    _args: &RestoreArgs,
    _partition: &TopicIdPartition,
    _segment: &VerifiedSegment,
    _predicates: &Predicates,
) -> Result<SegmentOutcome, RestoreError> {
    todo!("walk the verified batches, apply the bound, and append to the target partition")
}
