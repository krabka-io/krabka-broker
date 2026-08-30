//! State transitions of a segment after it holds data: sealing, flushing, and
//! truncation.
//!
//! Each one rewrites the segment's own view of what it holds -- the sealed
//! flag, the byte length, the last offset, the maximum timestamp, and the
//! sparse indexes -- so they belong together rather than beside a read path.

use krabka_ids::Offset;
use krabka_protocol::records::RecordBatch;
use tracing::instrument;

use super::{Segment, io::seek_to_log_size};
use crate::error::LogError;

impl Segment {
    /// Mark this segment as sealed. No more appends.
    pub fn seal(&mut self) {
        self.sealed = true;
    }

    /// Seal a segment loaded through the no-scan [`Segment::open`] path and
    /// set its `last_offset` to `last`.
    ///
    /// Callers pass `next_segment.base_offset - 1`, the highest offset this
    /// sealed segment can hold. `Segment::open` leaves
    /// `last_offset = base_offset - 1` because it does not scan the `.log`.
    /// Without this fix, a sealed segment recovered on
    /// [`Log::open`](crate::Log) reports that stale `last_offset`.
    /// `Log::read_raw` skips any segment whose
    /// `last_offset() < fetch_offset`, so it would skip the first sealed
    /// segment after a restart and serve a later segment's base offset. That
    /// creates an offset gap, and a follower that fetches at 0 then loops on
    /// the resulting append mismatch.
    pub fn seal_at(&mut self, last: Offset) {
        self.sealed = true;
        self.last_offset = last;
    }

    /// Force-sync everything to disk.
    #[instrument(
        level = "debug",
        skip_all,
        fields(base_offset = self.base_offset.0, log_size = self.log_size),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn flush(&mut self) -> Result<(), LogError> {
        self.io.sync_data(&self.log_file)?;
        self.offset_index.flush()?;
        self.time_index.flush()?;
        Ok(())
    }

    pub(super) fn rollback_failed_write(
        &mut self,
        position: u64,
        last_offset: Offset,
        max_timestamp: i64,
    ) -> Result<(), LogError> {
        self.log_file.set_len(position)?;
        seek_to_log_size(&self.log_file, position)?;
        self.log_size = position;
        self.last_offset = last_offset;
        self.max_timestamp = max_timestamp;
        let position = u32::try_from(position)
            .map_err(|_| LogError::BadSegmentName("position overflow".into()))?;
        self.offset_index.truncate_by_position(position)?;
        let next_relative = u32::try_from(last_offset.0 + 1 - self.base_offset.0)
            .map_err(|_| LogError::BadSegmentName("offset overflow".into()))?;
        self.time_index.truncate_by_relative_offset(next_relative)?;
        Ok(())
    }

    pub(crate) fn set_io(&mut self, io: std::sync::Arc<dyn crate::io::LogIo>) {
        self.io = io;
    }

    /// Truncate the `.log` file and the indexes so that no batch at
    /// `relative_offset` `>= rel` remains. `Log::truncate_to` uses this
    /// method. The segment stays unsealed.
    #[instrument(
        level = "info",
        skip(self),
        fields(base_offset = self.base_offset.0, new_last_offset = tracing::field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn truncate_to_relative(&mut self, rel: u32) -> Result<(), LogError> {
        // Read only as far as the cut can be: every kept batch lives below
        // the first index entry at or after `rel`. When `rel` is past the
        // last index entry, fall back to the whole file. This avoids
        // slurping the discarded tail on each truncate.
        let read_limit = self
            .offset_index
            .position_at_or_after(rel)
            .map_or(self.log_size, u64::from);
        let mut buf = Vec::new();
        let to_read = usize::try_from(read_limit).unwrap_or(usize::MAX);
        self.read_log_range(0, &mut buf, to_read)?;

        let target_abs = self.base_offset + i64::from(rel);
        let mut cur: &[u8] = &buf;
        let mut pos: u64 = 0;
        let mut last_kept_offset = self.base_offset - 1;
        let mut last_kept_ts = i64::MIN;
        while !cur.is_empty() {
            let before = cur.len();
            let Ok(batch) = RecordBatch::decode(&mut cur) else {
                break;
            };
            let batch_last_offset = Offset(batch.base_offset + i64::from(batch.last_offset_delta));
            if batch_last_offset >= target_abs {
                break;
            }
            pos += (before - cur.len()) as u64;
            last_kept_offset = batch_last_offset;
            if batch.max_timestamp > last_kept_ts {
                last_kept_ts = batch.max_timestamp;
            }
        }

        self.log_file.set_len(pos)?;
        seek_to_log_size(&self.log_file, pos)?;
        self.log_size = pos;
        self.last_offset = last_kept_offset;
        self.max_timestamp = last_kept_ts;

        let pos_u32 =
            u32::try_from(pos).map_err(|_| LogError::BadSegmentName("position overflow".into()))?;
        self.offset_index.truncate_by_position(pos_u32)?;
        self.time_index.truncate_by_relative_offset(rel)?;
        self.sealed = false;
        tracing::Span::current().record("new_last_offset", self.last_offset.0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::prelude::{ByteSize, ByteSizeExt, kibibytes};
    use tempfile::tempdir;

    use super::*;
    use crate::segment::test_support::{DENSE_INDEX, NO_LIMIT, sample_batch};

    /// Truncating to a relative offset keeps every batch that ends before it,
    /// and leaves the segment describing exactly what it kept.
    ///
    /// Three things are rewritten from the walk and each is read back later:
    /// the byte length, which decides where the next append lands; the last
    /// offset, which decides what the next batch is numbered; and the maximum
    /// timestamp, which time-based retention and `MAX_TIMESTAMP` both read.
    /// Truncating everything away is the case that pins the last offset down --
    /// it has to fall back to one before the base.
    #[test]
    fn truncating_a_segment_leaves_it_describing_what_it_kept() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(100)).unwrap();
        // Three batches: offsets 100..=101, 102..=103, 104..=105, with
        // timestamps 500, 600, 700.
        for i in 0..3i64 {
            seg.append(&sample_batch(100 + i * 2, 2, 500 + i * 100), DENSE_INDEX)
                .unwrap();
        }
        let full_size = seg.size();
        check!(seg.last_offset() == Offset(105));
        check!(
            seg.max_timestamp() == 701,
            "batch 3 carries the newest record"
        );

        // Keep only the first batch: relative offset 2 is the start of the
        // second, and the bound is exclusive of the batch containing it.
        seg.truncate_to_relative(2).unwrap();
        check!(
            seg.last_offset() == Offset(101),
            "last kept batch ends at 101"
        );
        check!(seg.max_timestamp() == 501, "the newest surviving record");
        let kept = seg.size();
        check!(
            kept > ByteSize::ZERO && kept < full_size,
            "shorter, not empty"
        );

        // Truncating everything away: nothing is kept, so the segment reports
        // one before its base and no timestamp at all.
        seg.truncate_to_relative(0).unwrap();
        check!(seg.size() == ByteSize::ZERO, "no bytes survive");
        check!(
            seg.last_offset() == Offset(99),
            "one before the base, got {:?}",
            seg.last_offset()
        );
        check!(seg.max_timestamp() == i64::MIN, "no records, no timestamp");
    }

    /// Sealing is what `is_sealed` reports.
    #[test]
    fn is_sealed_follows_seal() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        seg.append(&sample_batch(0, 2, 100), DENSE_INDEX).unwrap();
        check!(!seg.is_sealed(), "a fresh segment is open");
        seg.seal();
        check!(seg.is_sealed(), "a sealed segment reports it");
    }

    /// `truncate_to_relative` decides which batches to drop by each batch's
    /// last offset, `batch.base_offset + last_offset_delta`, compared against
    /// `target_abs`. MULTI-record batches make the `+` load-bearing. Batch A
    /// spans 0..=2 and batch B spans 3..=5, so a truncate to rel 3, where
    /// `target_abs = 3`, must keep A, whose last offset 2 is < 3, and drop B,
    /// whose last offset 5 is >= 3. A mutation of `+` to `-` computes A's last
    /// offset as -2 and B's as 1, so it wrongly keeps B and the read still
    /// returns batch B.
    #[test]
    fn truncate_to_relative_uses_batch_last_offset() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        seg.append(&sample_batch(0, 3, 100), DENSE_INDEX).unwrap(); // offsets 0..=2
        seg.append(&sample_batch(3, 3, 200), DENSE_INDEX).unwrap(); // offsets 3..=5
        assert2::assert!(seg.last_offset() == 5);

        // target_abs = base(0) + rel(3) = 3. Drop batches with last >= 3.
        seg.truncate_to_relative(3).unwrap();
        let read = seg.read(Offset(0), NO_LIMIT).unwrap();
        assert2::assert!(seg.last_offset() == Offset(2));
        assert2::assert!(read == vec![sample_batch(0, 3, 100)]);
    }

    #[test]
    fn flush_succeeds() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        seg.append(&sample_batch(0, 1, 42), kibibytes(4)).unwrap();
        seg.flush().unwrap();
    }
}
