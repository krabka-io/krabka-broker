//! The owned append paths, which decode-and-re-encode a `RecordBatch`, and
//! the segment roll they share with every other writer.
//!
//! `append` assigns the next offset and `append_at` keeps a
//! caller-supplied one; both funnel into one private helper so the LSO,
//! the producer state, and the leader-epoch checkpoint move identically.

use krabka_ids::{LeaderEpoch, Offset, ProducerId};
use krabka_protocol::records::RecordBatch;
use tracing::instrument;

use super::{
    Log,
    control::{ControlBatchKind, control_batch_kind},
};
use crate::{error::LogError, producer_snapshot, segment::Segment, txn_index::TxnIndex};

impl Log {
    /// Append a `RecordBatch` and return the assigned `base_offset`.
    ///
    /// The log overwrites the batch's `base_offset` with the next assigned
    /// offset. `last_offset_delta` sets how many absolute offsets this batch
    /// consumes.
    #[instrument(
        level = "debug",
        skip_all,
        fields(assigned_base = tracing::field::Empty, leader_epoch = batch.partition_leader_epoch),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn append(&mut self, batch: &mut RecordBatch) -> Result<Offset, LogError> {
        // `partition_leader_epoch` is the raw KIP-320 wire `int32`; wrap it into
        // the domain newtype at this boundary.
        let leader_epoch = LeaderEpoch(batch.partition_leader_epoch);
        let assigned_base = self.log_end_offset();
        tracing::Span::current().record("assigned_base", assigned_base.0);
        batch.base_offset = assigned_base.0;
        self.append_preserving_offset(batch, None)?;
        // Record epoch transition when the epoch is valid and exceeds the
        // previously recorded epoch (or no epoch has been recorded yet).
        if leader_epoch.is_known()
            && self
                .epoch_checkpoint
                .latest_epoch()
                .is_none_or(|e| leader_epoch > e)
        {
            self.epoch_checkpoint.append(leader_epoch, assigned_base)?;
        }
        Ok(assigned_base)
    }

    /// Append a commit-marker batch with a coordinator-supplied transaction
    /// stamp and return its assigned base offset.
    ///
    /// The stamp is internal metadata. It is not encoded into the marker or
    /// any client-facing bytes. The marker must be a COMMIT control batch and
    /// a [`StampSource`] must already be installed.
    ///
    /// # Errors
    /// Returns an error for a non-commit batch, a missing stamp source, or any
    /// log/index I/O failure.
    pub fn append_with_commit_stamp(
        &mut self,
        batch: &mut RecordBatch,
        stamp: u64,
    ) -> Result<Offset, LogError> {
        self.validate_commit_stamp_batch(batch)?;
        let assigned_base = self.log_end_offset();
        batch.base_offset = assigned_base.0;
        self.observe_stamp(stamp);
        self.append_preserving_offset(batch, Some(stamp))?;
        Ok(assigned_base)
    }

    pub(super) fn append_at_expected_offset(&self) -> Offset {
        self.log_end_offset().max(self.reconciled_frontier)
    }

    /// Append a `RecordBatch` whose `base_offset` the caller sets.
    ///
    /// Unlike [`Log::append`], this method does NOT overwrite
    /// `batch.base_offset`. The broker's replicator uses it to keep the
    /// leader-assigned offset on the follower's local log.
    ///
    /// `offset` must equal the log's current [`Log::log_end_offset`]. If it
    /// does not, this method returns [`LogError::OffsetMismatch`]. On success
    /// the log sets `batch.base_offset` to `offset`, which should already
    /// match, before it writes the batch.
    #[instrument(
        level = "debug",
        skip(self, batch),
        fields(leader_epoch = batch.partition_leader_epoch),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn append_at(&mut self, batch: &mut RecordBatch, offset: Offset) -> Result<(), LogError> {
        let expected = self.append_at_expected_offset();
        if offset != expected {
            return Err(LogError::OffsetMismatch {
                expected,
                actual: offset,
            });
        }
        // `partition_leader_epoch` is the raw KIP-320 wire `int32`; wrap it here.
        let leader_epoch = LeaderEpoch(batch.partition_leader_epoch);
        batch.base_offset = offset.0;
        self.append_preserving_offset(batch, None)?;
        // Mirror the leader-side epoch bookkeeping in [`Log::append`]: record the
        // batch's leader epoch when it advances past the latest recorded epoch,
        // so a follower's leader-epoch checkpoint tracks replicated epochs.
        if leader_epoch.is_known()
            && self
                .epoch_checkpoint
                .latest_epoch()
                .is_none_or(|e| leader_epoch > e)
        {
            self.epoch_checkpoint.append(leader_epoch, offset)?;
        }
        Ok(())
    }

    /// Append a replicated commit marker at `offset` with its internal commit
    /// stamp. This is the follower counterpart of
    /// [`Log::append_with_commit_stamp`].
    ///
    /// # Errors
    /// Returns an error for an offset mismatch, a non-commit batch, a missing
    /// stamp source, or any log/index I/O failure.
    pub fn append_at_with_commit_stamp(
        &mut self,
        batch: &mut RecordBatch,
        offset: Offset,
        stamp: u64,
    ) -> Result<(), LogError> {
        let expected = self.append_at_expected_offset();
        if offset != expected {
            return Err(LogError::OffsetMismatch {
                expected,
                actual: offset,
            });
        }
        self.validate_commit_stamp_batch(batch)?;
        batch.base_offset = offset.0;
        self.observe_stamp(stamp);
        self.append_preserving_offset(batch, Some(stamp))
    }

    /// Internal helper shared by [`Log::append`] and [`Log::append_at`].
    ///
    /// This function rolls the segment if necessary, appends to the active
    /// segment, and honors `config.flush_on_append`. It does NOT reassign
    /// `batch.base_offset`. Callers must set that field first. It also
    /// updates the LSO and the active `.txnindex` from the batch attributes.
    fn append_preserving_offset(
        &mut self,
        batch: &mut RecordBatch,
        transaction_stamp: Option<u64>,
    ) -> Result<(), LogError> {
        Self::data_producer_tail(
            ProducerId(batch.producer_id),
            batch.base_sequence,
            batch.last_offset_delta,
            Offset(batch.base_offset),
        )?;
        let (segment_size, index_interval, flush_on_append) = {
            let cfg = self.config.read().unwrap();
            (cfg.segment_size, cfg.index_interval, cfg.flush_on_append)
        };

        let should_roll = match &self.active {
            Some(seg) => seg.size() >= segment_size,
            None => false,
        };
        if should_roll {
            self.roll_active_segment()?;
        }

        let active = self
            .active
            .as_mut()
            .expect("active segment must exist after Log::open");
        active.append(batch, index_interval)?;

        if flush_on_append && let Err(error) = self.active_segment_flush() {
            let active = self
                .active
                .as_mut()
                .expect("active segment must exist after Log::open");
            let relative = u32::try_from(batch.base_offset - active.base_offset().0)
                .map_err(|_| LogError::BadSegmentName("offset overflow".into()))?;
            active.truncate_to_relative(relative)?;
            return Err(error);
        }

        // --- .stampindex write (internal sidecar) ---
        // Ordinary data is stamped after the batch is durable. Transactional
        // data stays unstamped until its COMMIT marker lands; an ABORT leaves
        // it unstamped. Control records themselves never receive a stamp.
        let pid = ProducerId(batch.producer_id);
        let is_transactional = batch.attributes.is_transactional() && pid.get() >= 0;
        if !batch.attributes.is_control_batch() && !is_transactional {
            let last = Offset(batch.base_offset + i64::from(batch.last_offset_delta));
            self.record_stamp(Offset(batch.base_offset), last)?;
        }

        // --- LSO tracking + .txnindex writes ---
        match control_batch_kind(batch) {
            Some(ControlBatchKind::Barrier) => {
                // A barrier marker holds no producer id and it clears the
                // transactional bit. It opens no transaction and it closes
                // none, so `pending`, `pending_stamp_ranges`,
                // `coordinator_epochs`, the active `.txnindex`, and the
                // producer state all stay as they are. The offset that the
                // marker takes is its only effect on the log, so the LSO
                // moves exactly as it moves for an ordinary
                // non-transactional batch. The early return keeps it that
                // way. No statement after this match can reach a barrier.
                self.advance_lso_when_no_open_transaction();
                return Ok(());
            }
            Some(ControlBatchKind::Transaction) => {
                self.apply_transaction_marker(batch, pid, transaction_stamp)?;
                // LSO can advance only when no pending txns remain.
                self.advance_lso_when_no_open_transaction();
            }
            None if is_transactional => {
                // Record the first offset of this txn on this partition.
                let base = Offset(batch.base_offset);
                let last = Offset(batch.base_offset + i64::from(batch.last_offset_delta));
                self.pending.entry(pid).or_insert(base);
                self.pending_stamp_ranges
                    .entry(pid)
                    .or_default()
                    .push((base, last));
                // LSO stays where it is until commit/abort.
            }
            None => {
                // Non-transactional batch. LSO advances only when no
                // in-flight txns.
                self.advance_lso_when_no_open_transaction();
            }
        }

        self.update_owned_producer_entry(batch)?;

        Ok(())
    }

    #[instrument(
        level = "info",
        skip_all,
        fields(new_base = tracing::field::Empty),
        err,
    )]
    pub(super) fn roll_active_segment(&mut self) -> Result<(), LogError> {
        let new_base = self.log_end_offset();
        tracing::Span::current().record("new_base", new_base.0);
        // The snapshot must never become durable ahead of the records it
        // describes. Flush the segment first, then fsync and publish the
        // boundary snapshot.
        self.active_segment_flush()?;
        producer_snapshot::write(&self.dir, new_base, &self.producer_state)?;
        let mut old = self
            .active
            .take()
            .expect("active segment must exist before rolling");
        old.seal();
        self.segments.push(old);
        let mut new_seg = Segment::create(&self.dir, new_base)?;
        new_seg.set_io(self.io.clone());
        self.active_txn_index = TxnIndex::open(new_seg.txn_index_path())?;
        let stamp_index_path = new_seg.stamp_index_path();
        self.active = Some(new_seg);
        self.dir_sync_needed = true;
        self.reopen_active_stamp_index(new_base, stamp_index_path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
