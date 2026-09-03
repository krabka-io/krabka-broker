//! The zero-copy passthrough append: a producer batch written to the log
//! byte-for-byte, with no decode and no re-encode.
//!
//! Only `base_offset` and `partition_leader_epoch` are patched into the
//! bytes, and both sit outside the CRC region, so the producer's body and
//! checksum reach disk exactly as they arrived.
//!
//! A `message.timestamp.type=LogAppendTime` partition is the one exception:
//! it also rewrites the timestamp-type attribute bit, `max_timestamp`, and
//! the CRC over them, which is exactly the three fields Kafka's
//! `LogValidator` rewrites. The producer's records are still stored as they
//! arrived.

use bytes::{Bytes, BytesMut};
use krabka_ids::{LeaderEpoch, Offset, ProducerId};
use krabka_protocol::records::{Attributes, CRC_COVERAGE_START, HEADER_LEN, TimestampType};
use tracing::instrument;

use super::Log;
use crate::error::LogError;

/// Byte range of the `crc` field in the v2 batch header: the four bytes that
/// sit immediately before the region the CRC covers.
const CRC_RANGE: std::ops::Range<usize> = CRC_COVERAGE_START - 4..CRC_COVERAGE_START;

/// Byte range of `attributes` in the v2 batch header. It is the first field
/// the CRC covers, so it starts at [`CRC_COVERAGE_START`] and is two bytes
/// wide.
const ATTRIBUTES_RANGE: std::ops::Range<usize> = CRC_COVERAGE_START..CRC_COVERAGE_START + 2;

/// Byte range of `max_timestamp` in the v2 batch header: `attributes` (2) plus
/// `last_offset_delta` (4) plus `base_timestamp` (8) after
/// [`CRC_COVERAGE_START`], and eight bytes wide.
const MAX_TIMESTAMP_RANGE: std::ops::Range<usize> =
    CRC_COVERAGE_START + 14..CRC_COVERAGE_START + 22;

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

impl VerbatimBatch {
    /// This batch with Kafka's log-append time stamped into it.
    ///
    /// This is `LogValidator`'s `batch.setMaxTimestamp(LOG_APPEND_TIME, now)`
    /// applied to wire bytes rather than to a decoded batch: it moves the
    /// timestamp-type attribute bit, `max_timestamp`, and the CRC that covers
    /// them, and nothing else. `base_timestamp` and the per-record deltas stay
    /// as the producer wrote them, exactly as in Kafka, because a reader
    /// substitutes `max_timestamp` for every record while the bit is set.
    ///
    /// The three fields sit at fixed offsets in the header, so the patch needs
    /// no decode. Two of them are inside the CRC region, which is why this is
    /// the one place the passthrough path recomputes a CRC: over the patched
    /// header tail and the producer's own body, which never changes. The batch
    /// bytes are copied once, so only a `LogAppendTime` partition pays for the
    /// copy and a `CreateTime` one keeps the zero-copy append whole.
    #[must_use]
    fn stamped_with_log_append_time(&self, now: i64) -> Self {
        let mut bytes = BytesMut::from(&self.bytes[..]);
        let attributes = Attributes(i16::from_be_bytes([
            bytes[ATTRIBUTES_RANGE.start],
            bytes[ATTRIBUTES_RANGE.start + 1],
        ]))
        .with_timestamp_type(TimestampType::LogAppendTime);
        bytes[ATTRIBUTES_RANGE].copy_from_slice(&attributes.0.to_be_bytes());
        bytes[MAX_TIMESTAMP_RANGE].copy_from_slice(&now.to_be_bytes());
        // The CRC covers the header from `attributes` to the end of the batch,
        // and the body follows the header without a gap, so one pass over the
        // tail of the buffer is the whole covered region.
        let crc = crc32c::crc32c(&bytes[CRC_COVERAGE_START..]);
        bytes[CRC_RANGE].copy_from_slice(&crc.to_be_bytes());
        Self {
            bytes: bytes.freeze(),
            max_timestamp: now,
            ..self.clone()
        }
    }
}

impl Log {
    /// Append a producer batch **verbatim** and return the assigned
    /// `base_offset` plus the log-append stamp, as [`Log::append`] does.
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
    pub fn append_verbatim(
        &mut self,
        batch: &VerbatimBatch,
    ) -> Result<(Offset, Option<i64>), LogError> {
        // KFC-1, before an offset is assigned and before anything is written,
        // so a refused batch leaves the log exactly as it found it.
        self.reject_backwards_schedule(batch.max_timestamp)?;
        let leader_epoch = batch.leader_epoch;
        let assigned_base = self.append_at_expected_offset();
        tracing::Span::current().record("assigned_base", assigned_base.0);
        // A batch too short to hold a header cannot be patched. Passing it on
        // unchanged lets the segment append report the truncation it already
        // reports, rather than turning it into a panic here.
        let stamp = self
            .log_append_time_stamp()
            .filter(|_| batch.bytes.len() >= HEADER_LEN);
        let stamped = stamp.map(|now| batch.stamped_with_log_append_time(now));
        let batch = stamped.as_ref().unwrap_or(batch);
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
        Ok((assigned_base, stamp))
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
