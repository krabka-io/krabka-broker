//! The two append paths: the encoding one and the verbatim passthrough.
//!
//! Both write to the active `.log` file and update the same segment state and
//! sparse indexes, so the shared side effects stay in one file. The verbatim
//! path additionally has to leave the producer's CRC-covered bytes untouched.

use std::io::IoSlice;

use krabka_ids::{LeaderEpoch, Offset};
use krabka_protocol::records::{HEADER_LEN, RecordBatch, patch_base_offset_and_leader_epoch};
use krabka_units::prelude::{ByteSize, ByteSizeExt};
use tracing::instrument;

use super::{
    Segment,
    io::{write_all, write_all_vectored},
};
use crate::error::LogError;

impl Segment {
    /// Append a record batch and return the byte position where the batch
    /// starts.
    ///
    /// Side effects:
    /// - Updates `log_size`, `max_timestamp`, and `last_offset`.
    /// - Adds sparse index entries when the byte count since the last entry
    ///   exceeds `index_interval`, and for the first batch.
    #[instrument(
        level = "debug",
        skip(self, batch),
        fields(
            base_offset = self.base_offset.0,
            batch_base = batch.base_offset,
            bytes = batch.encoded_len(),
            position = tracing::field::Empty,
        ),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn append(
        &mut self,
        batch: &RecordBatch,
        index_interval: ByteSize,
    ) -> Result<u64, LogError> {
        if self.sealed {
            return Err(LogError::Io(std::io::Error::other("segment is sealed")));
        }

        let mut buf = bytes::BytesMut::with_capacity(batch.encoded_len());
        batch.encode(&mut buf)?;
        let bytes = buf.freeze();

        let (last_offset, _) = krabka_verified::local_append_coordinates(
            batch.base_offset,
            batch.base_offset,
            batch.last_offset_delta,
        )
        .ok_or_else(|| LogError::InvalidArgument("invalid batch offset interval".into()))?;
        let position = self.log_size;
        let appended_len = u64::try_from(bytes.len())
            .map_err(|_| LogError::InvalidArgument("encoded batch length overflow".into()))?;
        let new_log_size = position
            .checked_add(appended_len)
            .ok_or_else(|| LogError::InvalidArgument("segment byte length overflow".into()))?;
        let previous_last_offset = self.last_offset;
        let previous_max_timestamp = self.max_timestamp;
        let should_index = match self.offset_index.last_entry() {
            None => true,
            Some((_, last_pos)) => {
                position.saturating_sub(u64::from(last_pos)) >= index_interval.bytes_u64()
            }
        };
        let index_entry = if should_index {
            let rel = u32::try_from(batch.base_offset - self.base_offset.0)
                .map_err(|_| LogError::BadSegmentName("offset overflow in segment".into()))?;
            let pos = u32::try_from(position)
                .map_err(|_| LogError::BadSegmentName("position overflow in segment".into()))?;
            Some((rel, pos))
        } else {
            None
        };
        // The active file cursor is kept at log_size by open/recovery/truncate,
        // so the hot append path does not need an lseek before every write.
        if let Err(error) = write_all(&*self.io, &self.log_file, &bytes) {
            self.rollback_failed_write(position, previous_last_offset, previous_max_timestamp)?;
            return Err(error.into());
        }
        self.log_size = new_log_size;

        self.last_offset = Offset(last_offset);
        if batch.max_timestamp > self.max_timestamp {
            self.max_timestamp = batch.max_timestamp;
        }

        if let Some((rel, pos)) = index_entry
            && let Err(error) = self
                .offset_index
                .append(rel, pos)
                .and_then(|()| self.time_index.append(self.max_timestamp, rel))
        {
            self.rollback_failed_write(position, previous_last_offset, previous_max_timestamp)?;
            return Err(error);
        }

        tracing::Span::current().record("position", position);
        Ok(position)
    }

    /// Append a batch **verbatim** and write the producer's exact wire bytes.
    ///
    /// This method does not decode, re-encode, recompress, or recompute the
    /// CRC. `bytes` is the producer's verbatim v2 batch, which the caller has
    /// already CRC-validated through the borrowed header-only path. This
    /// method patches only `base_offset`, bytes 0..8, and
    /// `partition_leader_epoch`, bytes 12..16, into a writable copy, then
    /// writes those bytes. Both fields sit outside the CRC-covered region. The
    /// stored CRC stays byte-identical to the producer's CRC, because no
    /// CRC-covered byte changes.
    ///
    /// `base_offset`, `last_offset_delta`, `max_timestamp`, and
    /// `leader_epoch` come from the caller's borrowed header read. This method
    /// updates the segment side effects, `log_size`, `last_offset`,
    /// `max_timestamp`, and the sparse index, exactly as [`Segment::append`]
    /// does.
    ///
    /// It returns the byte position where the batch starts.
    #[instrument(
        level = "debug",
        skip(self, bytes),
        fields(
            seg_base_offset = self.base_offset.0,
            base_offset = base_offset.0,
            bytes_len = bytes.len(),
            position = tracing::field::Empty,
        ),
        err,
    )]
    // cargo-mutants: the only mutant here flips the sparse-index `rel = batch_base - seg_base`
    // to `+`, corrupting an OFFSET-INDEX hint only. Every read/truncate path
    // treats the index as a lower-bound hint and re-scans + filters, so an
    // inflated `rel` (seg_base > 0) resolves to the from-start fallback and
    // yields identical output; the last_offset/index-presence effects are
    // pinned by `append_verbatim_updates_index_and_last_offset`.
    #[cfg_attr(test, mutants::skip)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn append_verbatim(
        &mut self,
        bytes: &[u8],
        base_offset: Offset,
        last_offset_delta: i32,
        max_timestamp: i64,
        leader_epoch: LeaderEpoch,
        index_interval: ByteSize,
    ) -> Result<u64, LogError> {
        if self.sealed {
            return Err(LogError::Io(std::io::Error::other("segment is sealed")));
        }
        if bytes.len() < HEADER_LEN {
            return Err(LogError::Corrupt(
                "verbatim batch shorter than v2 header".into(),
            ));
        }

        // Patch base_offset + partition_leader_epoch in a copy of *just* the
        // fixed-size header — both fields live below byte 16, well under the
        // CRC-covered region (byte 21), so the producer's CRC stays valid (no
        // recompute). The batch BODY is written straight from the input slice
        // with no copy: the previous `bytes.to_vec()` was a full-payload memcpy
        // on the produce hot path (100 KiB+ per batch for large messages), the
        // dominant remaining produce-side cost. The active file cursor is kept
        // at log_size, so one writev appends the patched header plus original
        // body without an lseek or full-payload copy.
        let mut header = [0u8; HEADER_LEN];
        header.copy_from_slice(&bytes[..HEADER_LEN]);
        // The protocol patcher writes the raw KIP-320 wire `int32`; unwrap here.
        patch_base_offset_and_leader_epoch(&mut header, base_offset.0, leader_epoch.0);

        let (last_offset, _) = krabka_verified::local_append_coordinates(
            base_offset.0,
            base_offset.0,
            last_offset_delta,
        )
        .ok_or_else(|| LogError::InvalidArgument("invalid batch offset interval".into()))?;
        let position = self.log_size;
        let appended_len = u64::try_from(bytes.len())
            .map_err(|_| LogError::InvalidArgument("encoded batch length overflow".into()))?;
        let new_log_size = position
            .checked_add(appended_len)
            .ok_or_else(|| LogError::InvalidArgument("segment byte length overflow".into()))?;
        let previous_last_offset = self.last_offset;
        let previous_max_timestamp = self.max_timestamp;
        let should_index = match self.offset_index.last_entry() {
            None => true,
            Some((_, last_pos)) => {
                position.saturating_sub(u64::from(last_pos)) >= index_interval.bytes_u64()
            }
        };
        let index_entry = if should_index {
            let rel = u32::try_from(base_offset.0 - self.base_offset.0)
                .map_err(|_| LogError::BadSegmentName("offset overflow in segment".into()))?;
            let pos = u32::try_from(position)
                .map_err(|_| LogError::BadSegmentName("position overflow in segment".into()))?;
            Some((rel, pos))
        } else {
            None
        };
        let mut bufs = [IoSlice::new(&header), IoSlice::new(&bytes[HEADER_LEN..])];
        if let Err(error) = write_all_vectored(&*self.io, &self.log_file, &mut bufs) {
            self.rollback_failed_write(position, previous_last_offset, previous_max_timestamp)?;
            return Err(error.into());
        }
        self.log_size = new_log_size;

        self.last_offset = Offset(last_offset);
        if max_timestamp > self.max_timestamp {
            self.max_timestamp = max_timestamp;
        }

        if let Some((rel, pos)) = index_entry
            && let Err(error) = self
                .offset_index
                .append(rel, pos)
                .and_then(|()| self.time_index.append(self.max_timestamp, rel))
        {
            self.rollback_failed_write(position, previous_last_offset, previous_max_timestamp)?;
            return Err(error);
        }

        tracing::Span::current().record("position", position);
        Ok(position)
    }
}

#[cfg(test)]
mod tests {
    use krabka_units::prelude::kibibytes;
    use tempfile::tempdir;

    use super::*;
    use crate::segment::test_support::{
        DENSE_INDEX, NO_LIMIT, sample_batch, test_batch_at, test_segment,
    };

    #[test]
    fn append_then_read_back() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        let b1 = sample_batch(0, 3, 1_000_000);
        let b2 = sample_batch(3, 2, 2_000_000);
        seg.append(&b1, kibibytes(4)).unwrap();
        seg.append(&b2, kibibytes(4)).unwrap();
        let read = seg.read(Offset(0), NO_LIMIT).unwrap();
        assert2::assert!(seg.last_offset() == Offset(4));
        assert2::assert!(read == vec![b1, b2]);
    }

    #[test]
    fn append_rejects_offset_successor_overflow_before_writing() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(i64::MAX)).unwrap();
        let batch = sample_batch(i64::MAX, 1, 100);

        let error = seg.append(&batch, DENSE_INDEX).unwrap_err();

        assert2::assert!(matches!(error, LogError::InvalidArgument(_)));
        assert2::assert!(seg.size().bytes_u64() == 0);
        assert2::assert!(
            std::fs::metadata(crate::name::log_path(dir.path(), i64::MAX))
                .unwrap()
                .len()
                == 0
        );
    }

    #[test]
    fn append_after_truncate_writes_at_new_eof() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        seg.append(&sample_batch(0, 1, 100), DENSE_INDEX).unwrap();
        let expected_position = seg.size().bytes_u64();
        seg.append(&sample_batch(1, 1, 200), DENSE_INDEX).unwrap();

        seg.truncate_to_relative(1).unwrap();
        let position = seg.append(&sample_batch(1, 1, 300), DENSE_INDEX).unwrap();

        let read = seg.read(Offset(0), NO_LIMIT).unwrap();
        assert2::assert!(position == expected_position);
        assert2::assert!(seg.last_offset() == Offset(1));
        assert2::assert!(read == vec![sample_batch(0, 1, 100), sample_batch(1, 1, 300)]);
    }

    #[test]
    fn append_to_sealed_segment_errors() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        seg.seal();
        assert2::assert!(seg.is_sealed());
        let err = seg
            .append(&sample_batch(0, 1, 0), kibibytes(4))
            .unwrap_err();
        assert2::assert!(matches!(err, LogError::Io(_)));
    }

    #[test]
    fn append_verbatim_is_byte_exact_except_offset_and_epoch() {
        let (dir, mut seg) = test_segment();
        // Build a batch as a "producer" would, with its own base_offset and
        // leader epoch, then encode to verbatim wire bytes.
        let mut producer = test_batch_at(0);
        producer.base_offset = 999; // producer-supplied (to be overwritten)
        producer.partition_leader_epoch = -1; // producer-supplied
        producer.last_offset_delta = 0;
        producer.max_timestamp = 1_000;
        let mut wire = bytes::BytesMut::new();
        producer.encode(&mut wire).unwrap();
        let wire = wire.freeze();

        // Append verbatim with an assigned base_offset and a stamped epoch.
        let assigned_base = Offset(0);
        let stamped_epoch = 7i32;
        seg.append_verbatim(
            &wire,
            assigned_base,
            0,
            1_000,
            LeaderEpoch(stamped_epoch),
            DENSE_INDEX,
        )
        .unwrap();
        assert2::assert!(seg.last_offset() == 0);

        // Read back the raw .log bytes.
        let mut on_disk = Vec::new();
        seg.read_log_range(0, &mut on_disk, usize::MAX).unwrap();
        let mut expected_wire = wire.to_vec();
        expected_wire[0..8].copy_from_slice(&assigned_base.0.to_be_bytes());
        expected_wire[12..16].copy_from_slice(&stamped_epoch.to_be_bytes());
        assert2::assert!(on_disk == expected_wire);

        // And it decodes (CRC still valid).
        let mut cur: &[u8] = &on_disk;
        let decoded = krabka_protocol::records::RecordBatch::decode(&mut cur).unwrap();
        assert2::assert!(decoded.base_offset == assigned_base.0);
        assert2::assert!(decoded.partition_leader_epoch == stamped_epoch);
        drop(dir);
    }

    #[test]
    fn append_verbatim_updates_index_and_last_offset() {
        let (dir, mut seg) = test_segment();
        let mut producer = test_batch_at(0);
        producer.last_offset_delta = 2; // spans 3 offsets
        producer.max_timestamp = 5_000;
        let mut wire = bytes::BytesMut::new();
        producer.encode(&mut wire).unwrap();
        let wire = wire.freeze();

        seg.append_verbatim(&wire, Offset(0), 2, 5_000, LeaderEpoch(0), DENSE_INDEX)
            .unwrap();
        // Reading at offset 2 (inside the batch) returns the batch.
        let read = seg.read(Offset(2), NO_LIMIT).unwrap();
        assert2::assert!(seg.last_offset() == Offset(2));
        assert2::assert!(seg.max_timestamp() == 5_000);
        assert2::assert!(read == vec![producer]);
        drop(dir);
    }

    #[test]
    fn append_verbatim_to_sealed_segment_errors() {
        let (dir, mut seg) = test_segment();
        seg.seal();
        let mut wire = bytes::BytesMut::new();
        test_batch_at(0).encode(&mut wire).unwrap();
        let err = seg
            .append_verbatim(&wire.freeze(), Offset(0), 0, 0, LeaderEpoch(0), DENSE_INDEX)
            .unwrap_err();
        assert2::assert!(matches!(err, LogError::Io(_)));
        drop(dir);
    }
}
