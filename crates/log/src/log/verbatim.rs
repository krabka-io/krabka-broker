//! The zero-copy passthrough append: a producer batch written to the log
//! byte-for-byte, with no decode and no re-encode.
//!
//! Only `base_offset` and `partition_leader_epoch` are patched into the
//! bytes, and both sit outside the CRC region, so the producer's body and
//! checksum reach disk exactly as they arrived.

use bytes::Bytes;
use krabka_ids::{LeaderEpoch, Offset, ProducerId};
use tracing::instrument;

use super::Log;
use crate::error::LogError;

/// A producer batch that the log appends **verbatim**, with no decode and
/// no re-encode.
///
/// The produce zero-copy passthrough path uses this type. It carries the
/// producer's exact wire bytes plus the header fields the log needs for
/// offset assignment, LSO and transaction tracking, the leader-epoch
/// checkpoint, and the time index. The caller has already read all of those
/// fields from the batch header with a borrowed header-only decode.
///
/// The append patches only `base_offset` and `partition_leader_epoch` into a
/// writable copy of [`Self::bytes`]. Both fields sit outside the CRC region.
/// The log writes the body and the CRC byte-for-byte as the producer sent
/// them.
///
/// This type deliberately **cannot** hold a control batch, that is, a
/// transaction marker. The LSO bookkeeping for a control batch needs the
/// inner marker record, which the header-only path does not read. Such
/// batches take the owned [`Log::append`] path instead.
#[derive(Debug, Clone)]
pub struct VerbatimBatch {
    /// The producer's verbatim v2 batch bytes (CRC-validated by the caller).
    pub bytes: Bytes,
    /// `last_offset_delta` from the header: how many offsets the batch spans.
    pub last_offset_delta: i32,
    /// `max_timestamp` from the header (for `max_timestamp` + time index).
    pub max_timestamp: i64,
    /// Leader epoch to stamp into the batch (`partition_leader_epoch`).
    pub leader_epoch: LeaderEpoch,
    /// `producer_id` from the header (for LSO/transaction tracking).
    pub producer_id: ProducerId,
    /// `producer_epoch` from the header.
    pub producer_epoch: i16,
    /// `base_sequence` from the header.
    pub base_sequence: i32,
    /// `true` when the batch's attributes mark it transactional.
    pub is_transactional: bool,
}

impl Log {
    /// Append a producer batch **verbatim** and return the assigned
    /// `base_offset`.
    ///
    /// The log does not decode or re-encode the batch, and it takes
    /// `base_offset` from the log's current end. This is the produce
    /// zero-copy passthrough path. The caller has already CRC-validated the
    /// bytes and read the header fields into [`VerbatimBatch`]. The log
    /// patches `base_offset` and `partition_leader_epoch`, which both sit
    /// outside the CRC region, then writes the bytes as they are. Offset
    /// assignment, segment roll, flush, LSO and transaction tracking, and the
    /// leader-epoch checkpoint behave exactly as in [`Log::append`]. The
    /// verbatim path and the owned path differ only in how the log gets the
    /// batch bytes, not in any log-level invariant.
    #[instrument(
        level = "debug",
        skip_all,
        fields(
            assigned_base = tracing::field::Empty,
            leader_epoch = batch.leader_epoch.0,
            bytes = batch.bytes.len(),
        ),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn append_verbatim(&mut self, batch: &VerbatimBatch) -> Result<Offset, LogError> {
        let leader_epoch = batch.leader_epoch;
        let assigned_base = self.append_at_expected_offset();
        tracing::Span::current().record("assigned_base", assigned_base.0);
        self.append_verbatim_preserving_offset(batch, assigned_base)?;
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
        Ok(assigned_base)
    }

    /// Append a producer batch **verbatim** at a caller-supplied base offset.
    ///
    /// `base_offset` must equal the log's current [`Log::log_end_offset`]. If
    /// it does not, this method returns [`LogError::OffsetMismatch`] and
    /// appends nothing. On success the log stamps the stored batch with
    /// `base_offset` and the batch's leader epoch. It does not decode or
    /// re-encode the CRC-covered bytes.
    ///
    /// # Errors
    /// Returns [`LogError::OffsetMismatch`] when `base_offset` is not the log
    /// end offset. It also propagates segment and checkpoint I/O errors and
    /// validation errors.
    #[instrument(
        level = "debug",
        skip_all,
        fields(
            supplied_base = base_offset.0,
            leader_epoch = batch.leader_epoch.0,
            bytes = batch.bytes.len(),
        ),
        err,
    )]
    pub fn append_verbatim_at(
        &mut self,
        batch: &VerbatimBatch,
        base_offset: Offset,
    ) -> Result<Offset, LogError> {
        let expected = self.append_at_expected_offset();
        if base_offset != expected {
            return Err(LogError::OffsetMismatch {
                expected,
                actual: base_offset,
            });
        }

        let leader_epoch = batch.leader_epoch;
        self.append_verbatim_preserving_offset(batch, base_offset)?;
        if leader_epoch.is_known()
            && self
                .epoch_checkpoint
                .latest_epoch()
                .is_none_or(|e| leader_epoch > e)
            && let Err(error) = self.epoch_checkpoint.append(leader_epoch, base_offset)
        {
            self.rollback_failed_append(base_offset)?;
            return Err(error);
        }
        Ok(base_offset)
    }

    /// Verbatim counterpart of [`Log::append_preserving_offset`].
    ///
    /// This function rolls the segment if necessary, appends the verbatim
    /// bytes to the active segment, honors `flush_on_append`, and updates the
    /// LSO from the batch's transactional and producer metadata. It mirrors
    /// the non-control branches of the owned path. Control batches never
    /// reach here, because they take the owned path.
    fn append_verbatim_preserving_offset(
        &mut self,
        batch: &VerbatimBatch,
        base_offset: Offset,
    ) -> Result<(), LogError> {
        let Some((last_offset, _)) = krabka_verified::local_append_coordinates(
            self.append_at_expected_offset().0,
            base_offset.0,
            batch.last_offset_delta,
        ) else {
            return Err(LogError::InvalidArgument(
                "batch does not form a valid interval at the append frontier".into(),
            ));
        };
        let last_offset = Offset(last_offset);
        Self::data_producer_tail(
            batch.producer_id,
            batch.base_sequence,
            batch.last_offset_delta,
            base_offset,
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
            active.append_verbatim(
                &batch.bytes,
                base_offset,
                batch.last_offset_delta,
                batch.max_timestamp,
                batch.leader_epoch,
                index_interval,
            )?;

            let is_transactional = batch.is_transactional && batch.producer_id.get() >= 0;
            if flush_on_append || (self.stamp_source.is_some() && !is_transactional) {
                self.active_segment_flush()?;
            }

            if !is_transactional {
                self.record_stamp(base_offset, last_offset)?;
            }

            let pid = batch.producer_id;
            if is_transactional {
                self.pending.entry(pid).or_insert(base_offset);
                self.pending_stamp_ranges
                    .entry(pid)
                    .or_default()
                    .push((base_offset, last_offset));
            }
            self.refresh_lso()?;

            self.update_data_producer_entry(
                (batch.producer_id, batch.producer_epoch),
                (batch.base_sequence, batch.last_offset_delta),
                (base_offset, batch.max_timestamp, is_transactional),
            )
        })();

        if let Err(error) = result {
            self.rollback_failed_append(base_offset)?;
            return Err(error);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
