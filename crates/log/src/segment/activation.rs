//! Scheduled-delivery activation walks over one segment.
//!
//! A batch becomes readable only once its activation time has passed, so a
//! fetch limit needs the end of the active prefix and a share reader needs the
//! gaps behind it. Both answers come from the same header-only walk, which is
//! why they sit together.

use std::ops::ControlFlow;

use krabka_ids::Offset;

use super::{ActivationScan, Segment};
use crate::error::LogError;

impl Segment {
    /// Walk forward from `from` and find where the active prefix ends.
    ///
    /// A batch is active when its `max_timestamp` is at or below
    /// `active_through_ms`. The caller has already subtracted its
    /// clock-uncertainty bound from the current time, so this method compares
    /// two plain instants.
    ///
    /// The walk stops at the first batch that is still waiting and reports
    /// that batch's activation time. Batches are not ordered by timestamp, so
    /// a later batch may well be active; the *prefix* is what a fetch limit
    /// can use, and [`Segment::pending_activation_ranges_into`] answers the
    /// other question.
    pub(crate) fn scan_activation(
        &self,
        from: Offset,
        active_through_ms: i64,
    ) -> Result<ActivationScan, LogError> {
        let segment_end = (self.last_offset + 1).max(from);
        if self.is_wholly_active(active_through_ms) {
            return Ok(ActivationScan {
                active_end: segment_end,
                pending_at: None,
            });
        }

        let mut active_end = from;
        let mut pending_at = None;
        self.walk_batch_headers(self.position_for(from)?, |view| {
            if view.last_offset < from {
                return ControlFlow::Continue(());
            }
            if view.max_timestamp > active_through_ms {
                pending_at = Some(view.max_timestamp);
                return ControlFlow::Break(());
            }
            active_end = view.last_offset + 1;
            ControlFlow::Continue(())
        })?;
        if pending_at.is_none() {
            // Nothing in this segment is waiting, so the prefix runs to its
            // end even where a torn trailing batch cut the walk short: those
            // bytes are not readable through `read_raw` either.
            active_end = active_end.max(segment_end);
        }
        Ok(ActivationScan {
            active_end,
            pending_at,
        })
    }

    /// Append every not-yet-active batch that overlaps `[start, end]` to
    /// `out`, as inclusive, batch-aligned offset ranges.
    ///
    /// Share consumers may skip a waiting batch and come back to it, so they
    /// need the gaps and not only the leading prefix that
    /// [`Segment::scan_activation`] reports. A range covers a whole batch even
    /// where the window cuts through it, because the share reader fetches with
    /// `read_raw` and that is batch-granular.
    pub(crate) fn pending_activation_ranges_into(
        &self,
        start: Offset,
        end: Offset,
        active_through_ms: i64,
        out: &mut Vec<(Offset, Offset)>,
    ) -> Result<(), LogError> {
        if self.is_wholly_active(active_through_ms) {
            return Ok(());
        }
        self.walk_batch_headers(self.position_for(start)?, |view| {
            if view.last_offset < start {
                return ControlFlow::Continue(());
            }
            if view.base_offset > end {
                return ControlFlow::Break(());
            }
            if view.max_timestamp > active_through_ms {
                out.push((view.base_offset, view.last_offset));
            }
            ControlFlow::Continue(())
        })
    }

    /// `true` when the segment's own maximum proves every batch in it is
    /// active, so an activation walk has nothing to find.
    ///
    /// This is what lets a scheduled topic skip whole segments of records
    /// that came due long ago.
    fn is_wholly_active(&self, active_through_ms: i64) -> bool {
        // Emptiness is decided by the file, not by `last_offset`. The same
        // unvalidated open that leaves the maximum unknown also leaves
        // `last_offset` at `base_offset - 1` over a segment full of records,
        // so that comparison would call a full segment empty.
        if self.log_size == 0 {
            return true;
        }
        // The sentinel means "not known yet", not "very old". An active
        // segment opened with `validate_on_open` off keeps it while holding
        // real batches, so a shortcut here would serve a batch before its
        // activation time.
        self.max_timestamp != i64::MIN && self.max_timestamp <= active_through_ms
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::segment::test_support::{DENSE_INDEX, sample_batch};

    /// A segment whose maximum timestamp is unknown must be walked, never
    /// skipped.
    ///
    /// `Segment::open` is the no-scan load, and it leaves `max_timestamp` at
    /// its sentinel. `Log::open` follows it with `restore_max_timestamp`, but
    /// an active segment opened with `validate_on_open` off does not, and it
    /// keeps the sentinel over real batches. Reading that sentinel as an old
    /// timestamp would take the whole-segment shortcut and report a batch as
    /// visible before its activation time, which scheduled delivery has to
    /// rule out.
    #[test]
    fn a_segment_with_an_unknown_maximum_is_never_skipped_as_active() {
        let dir = tempdir().unwrap();
        {
            let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
            seg.append(&sample_batch(0, 1, 9_000), DENSE_INDEX).unwrap();
        }

        // The no-scan load: real bytes on disk, maximum still unknown.
        let seg = Segment::open(dir.path(), Offset(0)).unwrap();
        assert2::assert!(seg.max_timestamp == i64::MIN);

        // 9_000 is in the future against this clock, so the batch is waiting.
        let scan = seg.scan_activation(Offset(0), 1_000).unwrap();
        assert2::check!(scan.active_end == Offset(0));
        assert2::check!(scan.pending_at == Some(9_000));

        let mut pending = Vec::new();
        seg.pending_activation_ranges_into(Offset(0), Offset(0), 1_000, &mut pending)
            .unwrap();
        assert2::check!(pending == vec![(Offset(0), Offset(0))]);
        drop(dir);
    }

    /// A segment with no bytes takes the shortcut: there is nothing to walk.
    #[test]
    fn an_empty_segment_is_wholly_active() {
        let dir = tempdir().unwrap();
        let seg = Segment::create(dir.path(), Offset(0)).unwrap();

        let scan = seg.scan_activation(Offset(0), 1_000).unwrap();
        assert2::check!(scan.pending_at == None);

        let mut pending = Vec::new();
        seg.pending_activation_ranges_into(Offset(0), Offset(0), 1_000, &mut pending)
            .unwrap();
        assert2::check!(pending == Vec::new());
        drop(dir);
    }
}
