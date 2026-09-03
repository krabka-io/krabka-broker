//! Remote-retention eviction: which segments the remote tier has held past
//! the topic's total retention window, and the delete lifecycle that removes
//! them.
//!
//! A write-once archive evicts nothing, so the pass ends before it lists.

use std::sync::Arc;

use krabka_log::{LogConfig, Offset};
use krabka_remote_storage::{
    RemoteLogMetadataManager, RemoteLogSegmentMetadata, RemoteLogSegmentState,
    RemoteStorageManager, TopicIdPartition,
};
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
};
use tracing::warn;

use super::{NO_BYTES, archive::ArchiveMode, delete::delete_one_segment};

/// KIP-405: compute the set of finished remote segments the topic no longer
/// keeps, in oldest-first order. Mirrors
/// [`local_retention_target`](super::local_retention::local_retention_target)'s
/// walk. It **stops at
/// the first non-deletable segment**, so the remaining remote prefix stays
/// contiguous. This matches Kafka.
///
/// A segment is deletable when any of:
/// - `md.end_offset() < log_start_offset`, the log-start breach, or
/// - `now_ms - md.max_timestamp_ms > retention`, or
/// - the running sum of sizes from the oldest forward must exceed
///   `total - retention_size` (greedy size eviction).
///
/// A `None` setting disables its axis; the log-start breach has no setting to
/// disable and evicts whatever falls below the floor even when both retention
/// settings are `None`. That is what makes a `DeleteRecords` on a tiered topic
/// free the remote bytes it deleted, rather than leaving them listed,
/// fetchable and billed until time or size retention happens to reach them.
/// Kafka's `RemoteLogRetentionHandler.deleteLogStartOffsetBreachedSegments`
/// is the same rule.
///
/// The caller must already have filtered to `CopySegmentFinished` and sorted
/// by `start_offset`.
///
/// [`ArchiveMode::WriteOnce`] evicts nothing, whatever the topic's retention
/// settings and log start say: remote retention is a delete, and a write-once
/// archive has none to give.
pub(crate) fn remote_retention_eviction_set(
    archive: ArchiveMode,
    finished: &[RemoteLogSegmentMetadata],
    retention: Option<Time>,
    retention_size: Option<ByteSize>,
    log_start_offset: Offset,
    now_ms: i64,
) -> Vec<RemoteLogSegmentMetadata> {
    let total: ByteSize = finished
        .iter()
        .map(segment_size)
        .fold(NO_BYTES, |acc, size| acc + size);
    let size_to_reclaim = retention_size.map_or(NO_BYTES, |budget| (total - budget).max(NO_BYTES));
    // The verified walk knows two axes: one flag per segment that makes it
    // deletable on its own, and a running size budget. A segment wholly below
    // the log start is deletable on its own, so it joins the flag the time
    // window sets. The walk's contiguous-prefix rule then holds over the
    // union, which is what Kafka's expiration task produces too.
    let expired: Vec<bool> = finished
        .iter()
        .map(|md| {
            let max_timestamp_ms = md.max_timestamp_ms();
            let age = Time::from_millis(now_ms.saturating_sub(max_timestamp_ms));
            let time_expired =
                max_timestamp_ms != -1 && matches!(retention, Some(window) if age > window);
            time_expired || md.end_offset() < log_start_offset.0
        })
        .collect();
    let sizes: Vec<u64> = finished
        .iter()
        .map(|md| segment_size(md).bytes_u64())
        .collect();
    let finished_flags = vec![true; finished.len()];
    let prefix = krabka_verified::retention::retention_prefix(
        archive != ArchiveMode::WriteOnce,
        &finished_flags,
        &expired,
        &sizes,
        size_to_reclaim.bytes_u64(),
    );
    finished.iter().take(prefix.len).cloned().collect()
}

/// The remote metadata's `segment_size_in_bytes` (a wire `int32`) as a
/// quantity. Negative sizes are impossible but cheap to clamp.
fn segment_size(md: &RemoteLogSegmentMetadata) -> ByteSize {
    ByteSize::from_bytes_i64(i64::from(md.segment_size_in_bytes().max(0)))
}

/// The partition facts one [`remote_retention_pass`] measures its segments
/// against: the topic's total-retention settings, whether the archive accepts
/// a delete at all, the global log start a `DeleteRecords` may have moved, and
/// the clock reading segments are aged against.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RemoteRetentionBounds<'a> {
    pub log_config: &'a LogConfig,
    pub archive: ArchiveMode,
    pub log_start_offset: Offset,
    pub now_ms: i64,
}

/// What one [`remote_retention_pass`] did to a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct RemoteRetentionOutcome {
    /// Segments that reached `DeleteSegmentFinished`.
    pub deleted: usize,
    /// The floor the caller must raise the partition's `log_start_offset` to,
    /// or `None` when the pass deleted nothing. It is the last deleted
    /// segment's `end_offset + 1`: those records are now in no tier, so
    /// `ListOffsets(earliest)` and the fetch path must both follow. Kafka's
    /// `cleanupExpiredRemoteLogSegments` hands the same value to
    /// `handleLogStartOffsetUpdate`.
    pub log_start: Option<Offset>,
}

/// KIP-405: evict remote segments the topic no longer keeps -- past its total
/// retention window (`retention.ms` and `retention.bytes`), or wholly below
/// its `log_start_offset`. For each deletable segment, it runs the lifecycle
/// `CopySegmentFinished` → `DeleteSegmentStarted` → RSM delete →
/// `DeleteSegmentFinished`. A failure logs at WARN and ends the
/// partition's pass early. Leftover `DeleteSegmentStarted` metadata is
/// invisible to the read path's finished-only filter, and the next tick
/// retries it.
///
/// The pass runs even when neither retention setting is set, because the
/// log-start breach is an axis of its own: a `DeleteRecords` that moved the
/// floor has to free the remote bytes below it.
///
/// Under [`ArchiveMode::WriteOnce`] the pass returns before it lists
/// anything, so a partition on a write-once archive costs a 30-second tick
/// nothing at all.
pub(crate) async fn remote_retention_pass(
    tp: &TopicIdPartition,
    broker_id: i32,
    bounds: RemoteRetentionBounds<'_>,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
) -> RemoteRetentionOutcome {
    let RemoteRetentionBounds {
        log_config,
        archive,
        log_start_offset,
        now_ms,
    } = bounds;
    if archive == ArchiveMode::WriteOnce {
        return RemoteRetentionOutcome::default();
    }
    let retention = log_config.retention;
    let retention_size = log_config.retention_size;

    let mut finished: Vec<RemoteLogSegmentMetadata> = match rlmm.list_remote_log_segments(tp) {
        Ok(list) => list
            .into_iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .collect(),
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to list remote segments for retention");
            return RemoteRetentionOutcome::default();
        }
    };
    finished.sort_by_key(RemoteLogSegmentMetadata::start_offset);

    let evict = remote_retention_eviction_set(
        archive,
        &finished,
        retention,
        retention_size,
        log_start_offset,
        now_ms,
    );
    let mut outcome = RemoteRetentionOutcome::default();
    // The floor may only cross offsets this pass actually removed, in one
    // unbroken run up from where it stands. The finished list is not always a
    // contiguous offset prefix: `copy_eligible` skips a segment whose copy
    // failed and carries on with the next one, so a gap can sit between two
    // finished segments. Publishing the last delete's end over such a gap
    // would put the floor above a segment that is still on local disk and
    // still readable, and make it unreadable.
    let mut floor = log_start_offset;
    let mut contiguous = true;
    for md in evict {
        if !delete_one_segment(tp, broker_id, &md, archive, rsm, rlmm).await {
            // Stop at the first failure to preserve the contiguous-prefix
            // invariant — the next tick re-tries from the same base.
            break;
        }
        outcome.deleted += 1;
        if contiguous && md.start_offset() <= floor.0 {
            floor = floor.max(Offset(md.end_offset() + 1));
        } else {
            contiguous = false;
        }
    }
    if floor > log_start_offset {
        outcome.log_start = Some(floor);
    }
    outcome
}

#[cfg(test)]
mod tests;
