//! Tiered storage (KIP-405): describing sealed segments for offload and
//! dropping the local copies once they are safely remote.
//!
//! `Log` enforces no tiered-storage invariant of its own. It reports what
//! a `RemoteLogManager` needs and deletes what that manager tells it to
//! delete.

use std::{collections::HashSet, path::PathBuf};

use krabka_ids::{LeaderEpoch, Offset};
use krabka_units::prelude::ByteSize;
use tracing::instrument;

use super::Log;
use crate::{error::LogError, name, producer_snapshot, retention, segment::Segment};

/// A sealed segment described for tiered-storage offload (KIP-405).
///
/// It carries the on-disk file paths, the offset, timestamp, and size
/// metadata, and the leader-epoch ranges that a `RemoteLogManager` needs to
/// build remote-segment metadata. [`Log::tierable_segments`] produces these
/// values.
// No `Eq`: `size` is a `ByteSize`, which stores `f64`. The derive was unused —
// `SegmentExport` is never hashed nor used as a map key.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentExport {
    /// First absolute offset in the segment.
    pub base_offset: Offset,
    /// Last absolute offset (inclusive) in the segment.
    pub last_offset: Offset,
    /// Highest record timestamp in the segment, or `-1` when unknown
    /// (a sealed segment loaded from disk without a tail scan).
    pub max_timestamp: i64,
    /// `.log` file size.
    pub size: ByteSize,
    /// Path to the `.log` data file.
    pub log_path: PathBuf,
    /// Path to the `.index` (offset index) file.
    pub offset_index_path: PathBuf,
    /// Path to the `.timeindex` file.
    pub time_index_path: PathBuf,
    /// Path to the `.txnindex` file, present only when it exists on disk.
    pub transaction_index_path: Option<PathBuf>,
    /// Producer-state snapshot at `last_offset + 1`.
    pub producer_snapshot_path: PathBuf,
    /// Leader epochs whose coverage overlaps `[base_offset, last_offset]`,
    /// as `(epoch, start_offset)` clamped to `base_offset`, ordered by
    /// offset. May be empty when no epochs were recorded for this log.
    pub leader_epochs: Vec<(LeaderEpoch, Offset)>,
}

/// Leader epochs whose coverage `[start_e, start_{e+1})` overlaps the
/// segment range `[base, last]`, returned as `(epoch, start_offset)` with
/// the start clamped up to `base` and ordered by offset. An epoch with no
/// recorded entries yields an empty result.
///
/// `sorted` must be ordered by `start_offset` ascending (the caller sorts
/// once and reuses the slice across segments).
fn epochs_for_range(
    sorted: &[crate::leader_epoch_checkpoint::EpochEntry],
    base: Offset,
    last: Offset,
) -> Vec<(LeaderEpoch, Offset)> {
    let mut out = Vec::new();
    for (i, e) in sorted.iter().enumerate() {
        // Coverage of this epoch is [start_offset, next.start_offset).
        let end = sorted
            .get(i + 1)
            .map_or(Offset(i64::MAX), |n| n.start_offset);
        if e.start_offset <= last && end > base {
            out.push((e.epoch, e.start_offset.max(base)));
        }
    }
    out
}

impl Log {
    /// First absolute offset still present on this broker's local disk
    /// (KIP-405).
    ///
    /// This method delegates to [`Log::log_start_offset`]. The two pointers
    /// co-advance.
    #[must_use]
    pub fn local_log_start_offset(&self) -> Offset {
        self.log_start_offset()
    }

    /// Delete every sealed segment whose `last_offset < target` from disk,
    /// then advance `log_start_offset` to `target` (KIP-405).
    ///
    /// This method never touches the active segment. It returns the number of
    /// segments removed. It does nothing and returns `Ok(0)` when
    /// `target <= local_log_start_offset()`.
    ///
    /// The caller must confirm that these segments are safely in the remote
    /// tier (`CopySegmentFinished`) before it calls this method. `Log`
    /// enforces no tiered-storage invariants. See
    /// `crates/broker/src/remote_log_manager.rs` for the production caller.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::InvalidArgument`] if `target` is negative.
    #[instrument(
        level = "info",
        skip(self),
        fields(removed = tracing::field::Empty),
        err,
    )]
    pub fn delete_local_segments_through(&mut self, target: Offset) -> Result<usize, LogError> {
        if target < 0 {
            return Err(LogError::InvalidArgument(
                "delete_local_segments_through: target must be >= 0".into(),
            ));
        }
        if target <= self.local_log_start_offset() {
            return Ok(0);
        }

        // Mirror `tierable_segments`: each sealed segment's last offset is
        // `next.base_offset - 1`, where `next` is the next sealed segment
        // or — for the most-recent sealed segment — the active segment.
        let active_base = self
            .active
            .as_ref()
            .map_or_else(|| self.log_end_offset(), Segment::base_offset);
        let next_bases: Vec<Offset> = self
            .segments
            .iter()
            .map(Segment::base_offset)
            .skip(1)
            .chain(std::iter::once(active_base))
            .collect();

        let to_drop: Vec<Offset> = self
            .segments
            .iter()
            .zip(next_bases.iter())
            .filter_map(|(seg, next_base)| {
                let last = *next_base - 1;
                (last < target).then(|| seg.base_offset())
            })
            .collect();

        let removed = to_drop.len();
        tracing::Span::current().record("removed", removed);
        let drop_set: HashSet<Offset> = to_drop.iter().copied().collect();
        self.segments
            .retain(|s| !drop_set.contains(&s.base_offset()));
        self.stamp_indexes
            .retain(|base, _| !drop_set.contains(base));
        for base in &to_drop {
            let _ = retention::delete_segment_files(&self.dir, *base);
        }

        // Advance the (single) log-start pointer. `local_log_start_offset`
        // delegates here, so the local floor moves in lockstep.
        self.start_offset_override = Some(target);

        Ok(removed)
    }

    /// Describe every sealed segment for tiered-storage offload (KIP-405).
    ///
    /// The result never includes the active segment. Only sealed segments are
    /// immutable and safe to copy.
    ///
    /// `last_offset` comes from the next segment's `base_offset`. For the
    /// most-recent sealed segment it comes from the active segment's base.
    /// The value is therefore correct even for segments loaded from disk
    /// without a tail scan. `max_timestamp` falls back to `-1`, which means
    /// unknown, when the in-memory value is not set.
    #[must_use]
    pub fn tierable_segments(&self) -> Vec<SegmentExport> {
        // Sort the epoch entries once here rather than per-segment inside
        // `epochs_for_range`.
        let mut epoch_entries = self.epoch_checkpoint.entries().to_vec();
        epoch_entries.sort_by_key(|e| e.start_offset);
        let active_base = self
            .active
            .as_ref()
            .map_or_else(|| self.log_end_offset(), Segment::base_offset);
        let next_bases: Vec<Offset> = self
            .segments
            .iter()
            .map(Segment::base_offset)
            .skip(1)
            .chain(std::iter::once(active_base))
            .collect();

        self.segments
            .iter()
            .zip(next_bases)
            .map(|(seg, next_base)| {
                let base = seg.base_offset();
                let last = next_base - 1;
                let max_ts = seg.max_timestamp();
                let txn = name::txnindex_path(&self.dir, base.0);
                SegmentExport {
                    base_offset: base,
                    last_offset: last,
                    max_timestamp: if max_ts == i64::MIN { -1 } else { max_ts },
                    size: seg.size(),
                    log_path: name::log_path(&self.dir, base.0),
                    offset_index_path: name::index_path(&self.dir, base.0),
                    time_index_path: name::timeindex_path(&self.dir, base.0),
                    transaction_index_path: txn.exists().then_some(txn),
                    producer_snapshot_path: producer_snapshot::path(&self.dir, next_base),
                    leader_epochs: epochs_for_range(&epoch_entries, base, last),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests;
