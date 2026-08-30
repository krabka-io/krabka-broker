//! Truncation of the log tail and trimming of the log head.
//!
//! `truncate_to` discards every record at or past an offset, which
//! replication and leader election need after a divergence, and
//! `trim_to_offset` moves the log start forward without touching the
//! active segment.

use std::{collections::HashSet, fs};

use krabka_ids::Offset;
use tracing::instrument;

use super::Log;
use crate::{
    error::LogError, name, producer_snapshot, retention, segment::Segment, txn_index::TxnIndex,
};

impl Log {
    /// Truncate the log so that no record at offset `>= offset` remains.
    /// Replication and leader election use this method.
    #[instrument(level = "info", skip(self), err)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn truncate_to(&mut self, offset: Offset) -> Result<(), LogError> {
        let log_start = self.log_start_offset();
        let log_end = self.log_end_offset();
        if offset >= log_end {
            return Ok(()); // nothing to truncate
        }
        // The discarded tail may hold the batch that stopped the last
        // activation walk, so what that walk learned about this offset and
        // above no longer describes the log.
        self.invalidate_delivery_schedule(offset);
        if offset < log_start {
            return Err(LogError::OffsetTooLow {
                requested: offset,
                log_start,
            });
        }

        if !self.segments.is_empty() {
            producer_snapshot::remove_after(&self.dir, offset)?;
        }

        // Drop sealed segments whose base_offset >= offset.
        while let Some(last_sealed) = self.segments.last() {
            if last_sealed.base_offset() >= offset {
                let popped = self.segments.pop().expect("non-empty by while-let");
                let base = popped.base_offset();
                drop(popped);
                let _ = fs::remove_file(name::log_path(&self.dir, base.0));
                let _ = fs::remove_file(name::index_path(&self.dir, base.0));
                let _ = fs::remove_file(name::timeindex_path(&self.dir, base.0));
                let _ = fs::remove_file(name::txnindex_path(&self.dir, base.0));
                let _ = fs::remove_file(name::stampindex_path(&self.dir, base.0));
                self.stamp_indexes.remove(&base);
            } else {
                break;
            }
        }

        // Drop the active segment if its base_offset >= offset.
        if let Some(active) = &self.active
            && active.base_offset() >= offset
        {
            let base = active.base_offset();
            self.active = None;
            let _ = fs::remove_file(name::log_path(&self.dir, base.0));
            let _ = fs::remove_file(name::index_path(&self.dir, base.0));
            let _ = fs::remove_file(name::timeindex_path(&self.dir, base.0));
            let _ = fs::remove_file(name::txnindex_path(&self.dir, base.0));
            let _ = fs::remove_file(name::stampindex_path(&self.dir, base.0));
            self.stamp_indexes.remove(&base);
        }

        // If no active segment, promote the last sealed one (if any) and
        // truncate it in place. Otherwise, create a fresh one at `offset`.
        if self.active.is_none() {
            if let Some(mut seg) = self.segments.pop() {
                let rel = u32::try_from(offset.0 - seg.base_offset().0)
                    .map_err(|_| LogError::BadSegmentName("offset overflow".into()))?;
                seg.truncate_to_relative(rel)?;
                self.active_txn_index = TxnIndex::open(seg.txn_index_path())?;
                let base = seg.base_offset();
                let stamp_index_path = seg.stamp_index_path();
                self.active = Some(seg);
                self.stamp_indexes
                    .retain(|segment_base, _| *segment_base <= base);
                self.reopen_active_stamp_index(base, stamp_index_path)?;
                if let Some(index) = self.stamp_indexes.get_mut(&base) {
                    index.truncate_from(offset)?;
                }
            } else {
                let mut new_seg = Segment::create(&self.dir, offset)?;
                new_seg.set_io(self.io.clone());
                self.active_txn_index = TxnIndex::open(new_seg.txn_index_path())?;
                let stamp_index_path = new_seg.stamp_index_path();
                self.active = Some(new_seg);
                self.dir_sync_needed = true;
                self.stamp_indexes.clear();
                self.reopen_active_stamp_index(offset, stamp_index_path)?;
            }
        } else if let Some(active) = self.active.as_mut()
            && active.last_offset() >= offset
        {
            // The surviving active segment contains records at or past
            // `offset`; truncate them in place.
            let rel = u32::try_from(offset.0 - active.base_offset().0)
                .map_err(|_| LogError::BadSegmentName("offset overflow".into()))?;
            active.truncate_to_relative(rel)?;
            self.active_txn_index = TxnIndex::open(active.txn_index_path())?;
            let base = active.base_offset();
            let stamp_index_path = active.stamp_index_path();
            self.reopen_active_stamp_index(base, stamp_index_path)?;
            if let Some(index) = self.stamp_indexes.get_mut(&base) {
                index.truncate_from(offset)?;
            }
        }
        if !self.producer_state.is_empty() {
            self.rebuild_producer_and_transaction_state()?;
        }
        // After truncation, LSO can't exceed log_end_offset.
        self.lso = self.lso.min(self.log_end_offset());
        // Drop leader-epoch checkpoint entries for the truncated-away tail so
        // latest_epoch()/end_offset_for_epoch() don't report epochs that no
        // longer have records (mirrors Kafka's truncateFromEnd).
        self.epoch_checkpoint
            .truncate_from_end(self.log_end_offset())?;
        Ok(())
    }

    /// Trim from the start of the log and return the resulting
    /// `log_start_offset`.
    ///
    /// This method drops every sealed segment whose last offset is
    /// `< target`. It advances `log_start_offset` when `target` falls inside
    /// the active segment. It never deletes the active segment.
    ///
    /// `target` is clamped to `[0, log_end_offset()]`. A caller that asks for
    /// a trim past the LEO gets a trim to the LEO.
    ///
    /// # Errors
    ///
    /// Returns `LogError::InvalidArgument` if `target < 0`.
    #[instrument(
        level = "info",
        skip(self),
        fields(new_log_start = tracing::field::Empty),
        err,
    )]
    pub fn trim_to_offset(&mut self, target: Offset) -> Result<Offset, LogError> {
        if target < 0 {
            return Err(LogError::InvalidArgument(
                "trim_to_offset: target must be >= 0".into(),
            ));
        }
        let leo = self.log_end_offset();
        let target = target.min(leo);
        let log_start = self.log_start_offset();
        if target <= log_start {
            tracing::Span::current().record("new_log_start", log_start.0);
            return Ok(log_start);
        }

        // Drop sealed segments whose last record is < target. A sealed
        // segment covers [base_offset, next_segment_base_offset). The
        // "last offset" of a sealed segment equals `next_base - 1`
        // where `next_base` is the next segment's `base_offset`
        // (or, for the most-recent sealed segment, the active segment's
        // `base_offset`).
        let active_base = self.active.as_ref().map_or(leo, Segment::base_offset);
        let next_bases: Vec<Offset> = self
            .segments
            .iter()
            .map(Segment::base_offset)
            .skip(1)
            .chain(std::iter::once(active_base))
            .collect();

        let mut to_drop: Vec<Offset> = Vec::new();
        for (seg, next_base) in self.segments.iter().zip(next_bases.iter()) {
            if *next_base <= target {
                to_drop.push(seg.base_offset());
            } else {
                break;
            }
        }

        let drop_set: HashSet<Offset> = to_drop.iter().copied().collect();
        self.segments
            .retain(|s| !drop_set.contains(&s.base_offset()));
        self.stamp_indexes
            .retain(|base, _| !drop_set.contains(base));
        for base in &to_drop {
            let _ = retention::delete_segment_files(&self.dir, *base);
        }

        // If target falls inside the active segment (or between the first
        // remaining sealed segment's base and `target`), advance the
        // start override.
        let new_log_start = self
            .segments
            .first()
            .map_or(active_base, Segment::base_offset);
        if target > new_log_start {
            self.set_log_start_offset(target)?;
        }
        let result = self.log_start_offset();
        tracing::Span::current().record("new_log_start", result.0);
        Ok(result)
    }
}

#[cfg(test)]
mod tests;
