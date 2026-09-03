//! The owned append paths, which decode-and-re-encode a `RecordBatch`, and
//! the segment roll they share with every other writer.
//!
//! `append` assigns the next offset and `append_at` keeps a
//! caller-supplied one; both funnel into one private helper so the LSO,
//! the producer state, and the leader-epoch checkpoint move identically.

use krabka_ids::{LeaderEpoch, Offset, ProducerId};
use krabka_protocol::records::{RecordBatch, TimestampType};
use tracing::instrument;

use super::{
    Log,
    control::{ControlBatchKind, control_batch_kind},
};
use crate::{
    config::{DeliveryPolicy, ScheduleOrder},
    error::LogError,
    producer_snapshot, retention,
    segment::Segment,
    txn_index::TxnIndex,
};

impl Log {
    /// Append a `RecordBatch` and return the assigned `base_offset` together
    /// with the log-append stamp the batch carries away from this call.
    ///
    /// The log overwrites the batch's `base_offset` with the next assigned
    /// offset. `last_offset_delta` sets how many absolute offsets this batch
    /// consumes.
    ///
    /// The second element is `Some(ms)` exactly when the partition's
    /// `message.timestamp.type` is `LogAppendTime`, and it is the broker clock
    /// reading this append stamped into the batch. It is Kafka's
    /// `LogAppendInfo.logAppendTime`, which `ProduceResponse.logAppendTimeMs`
    /// reports; `None` is a `CreateTime` partition, which Kafka answers with
    /// `-1`.
    #[instrument(
        level = "debug",
        skip_all,
        fields(assigned_base = tracing::field::Empty, leader_epoch = batch.partition_leader_epoch),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn append(&mut self, batch: &mut RecordBatch) -> Result<(Offset, Option<i64>), LogError> {
        // KFC-1, before an offset is assigned and before anything is written,
        // so a refused batch leaves the log exactly as it found it.
        self.reject_backwards_schedule(batch.max_timestamp)?;
        // `partition_leader_epoch` is the raw KIP-320 wire `int32`; wrap it into
        // the domain newtype at this boundary.
        let leader_epoch = LeaderEpoch(batch.partition_leader_epoch);
        let assigned_base = self.append_at_expected_offset();
        tracing::Span::current().record("assigned_base", assigned_base.0);
        batch.base_offset = assigned_base.0;
        let log_append_time_ms = self.stamp_owned_log_append_time(batch);
        self.append_preserving_offset(batch, None)?;
        // Record epoch transition when the epoch is valid and exceeds the
        // previously recorded epoch (or no epoch has been recorded yet).
        if leader_epoch.is_known()
            && self
                .epoch_checkpoint
                .latest_epoch()
                .is_none_or(|e| leader_epoch > e)
            && let Err(error) = self.epoch_checkpoint.append(leader_epoch, assigned_base)
        {
            self.rollback_failed_append(assigned_base)?;
            return Err(error);
        }
        Ok((assigned_base, log_append_time_ms))
    }

    /// Refuse a batch that would make a scheduled partition's schedule run
    /// backwards, which is KFC-1's `delivery.schedule.monotonic`.
    ///
    /// `delivery_ms` is the batch's `max_timestamp`, which on a
    /// [`DeliveryPolicy::Scheduled`] partition is the time its records become
    /// visible. KFC-1 defines the rejection against "the largest delivery time
    /// already in the partition", and the log answers that as an existence
    /// query: one record scheduled strictly after this batch is one record
    /// this batch would hold up.
    ///
    /// Visibility is offset-ordered for a classic group, because a group's
    /// position is one offset and a record it reads past is unreachable for it
    /// forever. A batch that comes due before an earlier one therefore stalls
    /// everything behind it rather than overtaking it, and the setting turns
    /// that silent stall into an error at the producer that caused it.
    ///
    /// The two offset-assigning leader appends call this, and they call it
    /// under the same lock acquisition that writes the batch. That is the
    /// whole point of the check living here: a test taken before the append
    /// admits two producers, or two jobs of one writer group, that then land
    /// out of order.
    ///
    /// [`Log::offset_for_timestamp`] skips a segment whose own cached maximum
    /// sits below the target, so a schedule that runs forward — the accepted
    /// case — costs one integer comparison per segment and no disk read. Only
    /// a rejected batch pays for an index lookup and a bounded scan. An
    /// immediate partition, and a scheduled one that did not ask for the
    /// setting, read two config fields and stop.
    ///
    /// # Errors
    /// Returns [`LogError::ScheduleRunsBackwards`] when the partition already
    /// holds a record whose delivery time is strictly after `delivery_ms`.
    pub(super) fn reject_backwards_schedule(&self, delivery_ms: i64) -> Result<(), LogError> {
        let monotonic = {
            let config = self.config.read().unwrap();
            config.delivery_policy == DeliveryPolicy::Scheduled
                && config.schedule_order == ScheduleOrder::Monotonic
        };
        if !monotonic {
            return Ok(());
        }
        // Nothing can be scheduled after `i64::MAX`, so a batch that names it
        // is never the one that runs backwards.
        let Some(later) = delivery_ms.checked_add(1) else {
            return Ok(());
        };
        if self.offset_for_timestamp(later).is_some() {
            return Err(LogError::ScheduleRunsBackwards { delivery_ms });
        }
        Ok(())
    }

    /// Stamp Kafka's log-append time onto an owned batch, when the partition
    /// asks for it, and report the stamp.
    ///
    /// This is `LogValidator`'s `batch.setMaxTimestamp(LOG_APPEND_TIME, now)`:
    /// on a `message.timestamp.type=LogAppendTime` partition every batch
    /// carries the broker's clock at append instead of the producer's own
    /// timestamps. Exactly two header fields move, the timestamp-type
    /// attribute bit and `max_timestamp`; `base_timestamp` and the per-record
    /// deltas stay as the producer wrote them, and a reader substitutes
    /// `max_timestamp` for every record while the bit is set. The owned path
    /// re-encodes the batch on the way to the segment, so its CRC follows on
    /// its own.
    ///
    /// Only the offset-assigning append paths call this. Kafka runs
    /// `LogValidator` under `validateAndAssignOffsets`, which is exactly the
    /// leader append, and never on `appendAsFollower`: a follower stores the
    /// leader's bytes as they arrived, already stamped, and re-stamping them
    /// with the follower's own clock would give the two replicas different
    /// bytes for one offset.
    fn stamp_owned_log_append_time(&self, batch: &mut RecordBatch) -> Option<i64> {
        let now = self.log_append_time_stamp()?;
        batch.attributes = batch
            .attributes
            .with_timestamp_type(TimestampType::LogAppendTime);
        batch.max_timestamp = now;
        Some(now)
    }

    /// The broker clock reading this append stamps, or `None` on a
    /// `CreateTime` partition, which stamps nothing.
    pub(super) fn log_append_time_stamp(&self) -> Option<i64> {
        let log_append_time =
            self.config.read().unwrap().message_timestamp_type == TimestampType::LogAppendTime;
        log_append_time.then(|| retention::now_ms(std::time::SystemTime::now()))
    }

    /// Append a commit-marker batch with a coordinator-supplied transaction
    /// stamp and return its assigned base offset.
    ///
    /// The stamp is internal metadata. It is not encoded into the marker or
    /// any client-facing bytes. The marker must be a COMMIT control batch and
    /// a [`crate::StampSource`] must already be installed.
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
        let assigned_base = self.append_at_expected_offset();
        batch.base_offset = assigned_base.0;
        // A marker is an offset-assigning append too. Kafka runs the same
        // `LogValidator` over an `AppendOrigin.COORDINATOR` append as over a
        // client one, so a `LogAppendTime` partition stamps its markers as
        // well as its data batches.
        self.stamp_owned_log_append_time(batch);
        self.append_preserving_offset(batch, Some(stamp))?;
        self.observe_stamp(stamp);
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
            && let Err(error) = self.epoch_checkpoint.append(leader_epoch, offset)
        {
            self.rollback_failed_append(offset)?;
            return Err(error);
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
        self.append_preserving_offset(batch, Some(stamp))?;
        self.observe_stamp(stamp);
        Ok(())
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
        let base_offset = Offset(batch.base_offset);
        let Some((last_offset, _)) = krabka_verified::local_append_coordinates(
            self.append_at_expected_offset().0,
            batch.base_offset,
            batch.last_offset_delta,
        ) else {
            return Err(LogError::InvalidArgument(
                "batch does not form a valid interval at the append frontier".into(),
            ));
        };
        let last_offset = Offset(last_offset);
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

        let result = (|| {
            let active = self
                .active
                .as_mut()
                .expect("active segment must exist after Log::open");
            active.append(batch, index_interval)?;

            let pid = ProducerId(batch.producer_id);
            let is_transactional = batch.attributes.is_transactional() && pid.get() >= 0;
            let control_kind = control_batch_kind(batch);
            let writes_durable_sidecar =
                matches!(control_kind, Some(ControlBatchKind::Transaction))
                    || (self.stamp_source.is_some() && control_kind.is_none() && !is_transactional);
            if flush_on_append || writes_durable_sidecar {
                self.active_segment_flush()?;
            }

            // Sidecars are written only after the batch bytes. Any failure in
            // this block takes the full-log rollback path below.
            if !batch.attributes.is_control_batch() && !is_transactional {
                self.record_stamp(base_offset, last_offset)?;
            }

            let is_barrier = match control_kind {
                Some(ControlBatchKind::Barrier) => {
                    self.refresh_lso()?;
                    true
                }
                Some(ControlBatchKind::Transaction) => {
                    self.apply_transaction_marker(batch, pid, last_offset, transaction_stamp)?;
                    self.refresh_lso()?;
                    false
                }
                None if is_transactional => {
                    self.pending.entry(pid).or_insert(base_offset);
                    self.pending_stamp_ranges
                        .entry(pid)
                        .or_default()
                        .push((base_offset, last_offset));
                    self.refresh_lso()?;
                    false
                }
                None => {
                    self.refresh_lso()?;
                    false
                }
            };

            if !is_barrier {
                self.update_owned_producer_entry(batch)?;
            }
            Ok(())
        })();

        if let Err(error) = result {
            self.rollback_failed_append(base_offset)?;
            return Err(error);
        }
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
        let old_base = old.base_offset();
        self.segments.push(old);
        let mut new_seg = Segment::create(&self.dir, new_base)?;
        new_seg.set_io(self.io.clone());
        let new_txn_index = TxnIndex::open(new_seg.txn_index_path())?;
        let old_txn_index = std::mem::replace(&mut self.active_txn_index, new_txn_index);
        self.sealed_txn_indexes.insert(old_base, old_txn_index);
        let stamp_index_path = new_seg.stamp_index_path();
        self.active = Some(new_seg);
        self.dir_sync_needed = true;
        self.reopen_active_stamp_index(new_base, stamp_index_path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
