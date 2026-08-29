//! Remote-retention eviction: which segments the remote tier has held past
//! the topic's total retention window, and the delete lifecycle that removes
//! them.
//!
//! A write-once archive evicts nothing, so the pass ends before it lists.

use std::sync::Arc;

use krabka_log::LogConfig;
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

/// KIP-405: compute the set of finished remote segments whose
/// total-retention window has expired, by time or by size budget, in
/// oldest-first order. Mirrors
/// [`local_retention_target`](super::local_retention::local_retention_target)'s
/// walk. It **stops at
/// the first non-deletable segment**, so the remaining remote prefix stays
/// contiguous. This matches Kafka.
///
/// A segment is deletable when either:
/// - `now_ms - md.max_timestamp_ms > retention`, or
/// - the running sum of sizes from the oldest forward must exceed
///   `total - retention_size` (greedy size eviction).
///
/// A `None` setting disables that axis. The caller must already have filtered
/// to `CopySegmentFinished` and sorted by `start_offset`.
///
/// [`ArchiveMode::WriteOnce`] evicts nothing, whatever the topic's retention
/// settings say: remote retention is a delete, and a write-once archive has
/// none to give.
pub(crate) fn remote_retention_eviction_set(
    archive: ArchiveMode,
    finished: &[RemoteLogSegmentMetadata],
    retention: Option<Time>,
    retention_size: Option<ByteSize>,
    now_ms: i64,
) -> Vec<RemoteLogSegmentMetadata> {
    if archive == ArchiveMode::WriteOnce {
        return Vec::new();
    }
    let total: ByteSize = finished
        .iter()
        .map(segment_size)
        .fold(NO_BYTES, |acc, size| acc + size);
    let mut size_to_reclaim =
        retention_size.map_or(NO_BYTES, |budget| (total - budget).max(NO_BYTES));
    let mut out = Vec::new();
    for md in finished {
        let age = Time::from_millis(now_ms.saturating_sub(md.max_timestamp_ms()));
        let by_time = matches!(retention, Some(window) if age > window);
        let by_size = size_to_reclaim > NO_BYTES;
        if !(by_time || by_size) {
            break;
        }
        if by_size {
            size_to_reclaim = (size_to_reclaim - segment_size(md)).max(NO_BYTES);
        }
        out.push(md.clone());
    }
    out
}

/// The remote metadata's `segment_size_in_bytes` (a wire `int32`) as a
/// quantity. Negative sizes are impossible but cheap to clamp.
fn segment_size(md: &RemoteLogSegmentMetadata) -> ByteSize {
    ByteSize::from_bytes_i64(i64::from(md.segment_size_in_bytes().max(0)))
}

/// KIP-405: evict remote segments past the topic's total
/// retention window (`retention.ms` and `retention.bytes`). For each
/// deletable segment, it runs the lifecycle
/// `CopySegmentFinished` → `DeleteSegmentStarted` → RSM delete →
/// `DeleteSegmentFinished`. A failure logs at WARN and ends the
/// partition's pass early. Leftover `DeleteSegmentStarted` metadata is
/// invisible to the read path's finished-only filter, and the next tick
/// retries it. Returns the count of segments that reached
/// `DeleteSegmentFinished`, that is, the successfully evicted ones.
///
/// Under [`ArchiveMode::WriteOnce`] the pass returns `0` before it lists
/// anything, so a partition on a write-once archive costs a 30-second tick
/// nothing at all.
pub(crate) async fn remote_retention_pass(
    tp: &TopicIdPartition,
    broker_id: i32,
    log_config: &LogConfig,
    archive: ArchiveMode,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    now_ms: i64,
) -> usize {
    if archive == ArchiveMode::WriteOnce {
        return 0;
    }
    let retention = log_config.retention;
    let retention_size = log_config.retention_size;
    if retention.is_none() && retention_size.is_none() {
        return 0;
    }

    let mut finished: Vec<RemoteLogSegmentMetadata> = match rlmm.list_remote_log_segments(tp) {
        Ok(list) => list
            .into_iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .collect(),
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to list remote segments for retention");
            return 0;
        }
    };
    finished.sort_by_key(RemoteLogSegmentMetadata::start_offset);

    let evict =
        remote_retention_eviction_set(archive, &finished, retention, retention_size, now_ms);
    let mut deleted = 0;
    for md in evict {
        if delete_one_segment(tp, broker_id, &md, archive, rsm, rlmm).await {
            deleted += 1;
        } else {
            // Stop at the first failure to preserve the contiguous-prefix
            // invariant — the next tick re-tries from the same base.
            break;
        }
    }
    deleted
}

#[cfg(test)]
mod tests;
