//! The zero-copy descriptor form of the verbatim read, for the `sendfile`
//! fetch path.
//!
//! The walk mirrors `read_raw` byte for byte but returns a file-region
//! descriptor instead of an owned buffer, so the broker sends the run straight
//! out of the page cache. The whole module is gated on the SENDFILE alias by
//! its declaration in the parent, which is why nothing here carries a `cfg`.

use std::sync::Arc;

use krabka_ids::Offset;
use krabka_protocol::records::{HEADER_LEN, RecordBatchHeader};
use krabka_units::prelude::{ByteSize, ByteSizeExt};
use tracing::instrument;
use zerocopy::FromBytes;

use super::{RawSegmentDesc, Segment, io::read_full_at};
use crate::error::LogError;

impl Segment {
    /// Descriptor variant of [`Segment::read_raw`] for the zero-copy
    /// `sendfile` fetch path.
    ///
    /// This method runs the **same** boundary walk and selects the identical
    /// `[start_pos+range_start, start_pos+range_end)` byte range that
    /// `read_raw` would have sliced. It returns a [`krabka_protocol::records::FileRegion`] descriptor
    /// instead of a `pread` of the payload into an owned `Bytes`.
    ///
    /// The walk is header-only. It `pread`s only the fixed v2 batch headers to
    /// find batch boundaries, and it uses the header's `batch_length`. It
    /// never reads the record payloads. The region is byte-identical to the
    /// `bytes` of `read_raw` for the same
    /// `(fetch_offset, limit_offset, max_bytes)`.
    #[instrument(
        level = "debug",
        skip(self),
        fields(base_offset = self.base_offset.0),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn read_raw_desc(
        &self,
        fetch_offset: Offset,
        limit_offset: Offset,
        max_size: ByteSize,
    ) -> Result<RawSegmentDesc, LogError> {
        if fetch_offset > self.last_offset || fetch_offset >= limit_offset {
            return Ok(RawSegmentDesc::empty());
        }
        let target_rel = u32::try_from((fetch_offset.0 - self.base_offset.0).max(0))
            .map_err(|_| LogError::Corrupt("read_raw_desc target offset out of range".into()))?;
        let start_pos = u64::from(self.offset_index.lookup(target_rel));

        // Mirror `read_raw`'s windowing **exactly** so the chosen byte range is
        // byte-identical. `read_raw` first reads `first_read = max_bytes.max(
        // HEADER_LEN)` bytes (capped by the bytes available after `start_pos`)
        // into a buffer, then only includes a batch whose end lands within that
        // buffer. A batch that straddles the buffer end is included **only** as
        // the single anti-stall batch when nothing has been included yet (it is
        // then re-read in full if it's complete on disk). We reproduce that with
        // a `window` instead of an actual payload read — the scan stays
        // header-only.
        //
        // The budget crosses back to a raw byte count here: everything below
        // is a file position or a region length.
        let max_bytes = max_size.bytes_usize();
        let first_read = max_bytes.max(HEADER_LEN) as u64;
        let available = self.log_size.saturating_sub(start_pos);
        let window = first_read.min(available); // == read_raw's buf.len()

        let mut pos: u64 = 0;
        let mut range_start: Option<u64> = None;
        let mut range_end: u64 = 0;
        let mut start_offset = fetch_offset;
        let mut last_offset = fetch_offset - 1;
        let mut hdr_buf = [0u8; HEADER_LEN];

        loop {
            // `read_raw` breaks when the next header can't fit in the window.
            if pos + HEADER_LEN as u64 > window {
                break;
            }
            let n = read_full_at(&self.log_file, start_pos + pos, &mut hdr_buf)?;
            if n < HEADER_LEN {
                break;
            }
            let hdr = RecordBatchHeader::ref_from_bytes(&hdr_buf)
                .map_err(|_| LogError::Corrupt("record batch header".into()))?;
            // Wire values from the fixed v2 header stay raw `i64`.
            let base = hdr.base_offset.get();
            let batch_len = usize::try_from(hdr.batch_length.get().max(0)).unwrap_or(0);
            let total = 12 + batch_len as u64;
            let batch_last = base + i64::from(hdr.last_offset_delta.get());

            if batch_last < fetch_offset {
                pos += total;
                continue;
            }
            if base >= limit_offset {
                break;
            }
            // Batch straddles the window end. `read_raw` re-reads exactly one
            // such batch when nothing is buffered yet (anti-stall: always return
            // at least one complete batch), provided it's complete on disk.
            if pos + total > window {
                if range_start.is_none() {
                    if start_pos + pos + total > self.log_size {
                        // Not a complete batch on disk — `read_raw` breaks.
                        break;
                    }
                    let len = usize::try_from(total)
                        .map_err(|_| LogError::Corrupt("read_raw_desc batch too large".into()))?;
                    return Ok(RawSegmentDesc {
                        start_offset: Offset(base),
                        last_offset: Offset(batch_last),
                        region: Some(krabka_protocol::records::FileRegion {
                            file: Arc::clone(&self.log_file),
                            offset: start_pos + pos,
                            len,
                        }),
                    });
                }
                break;
            }

            if range_start.is_none() {
                range_start = Some(pos);
                start_offset = Offset(base);
            }
            range_end = pos + total;
            last_offset = Offset(batch_last);
            pos += total;

            if range_end - range_start.expect("set above") >= max_bytes as u64 {
                break;
            }
        }

        match range_start {
            Some(s) => {
                let len = usize::try_from(range_end - s)
                    .map_err(|_| LogError::Corrupt("read_raw_desc region too large".into()))?;
                Ok(RawSegmentDesc {
                    start_offset,
                    last_offset,
                    region: Some(krabka_protocol::records::FileRegion {
                        file: Arc::clone(&self.log_file),
                        offset: start_pos + s,
                        len,
                    }),
                })
            }
            None => Ok(RawSegmentDesc::empty()),
        }
    }
}

#[cfg(test)]
mod tests {
    use krabka_units::prelude::{bytes, mebibytes};

    use super::*;
    use crate::segment::test_support::{DENSE_INDEX, test_batch_at, test_segment};

    /// `pread` a `FileRegion` into a fresh `Vec`. These are the bytes that the
    /// broker's sendfile would transmit, and that its TLS pread-fallback would
    /// copy.
    fn region_bytes(region: &krabka_protocol::records::FileRegion) -> Vec<u8> {
        use std::os::unix::fs::FileExt;
        let mut buf = vec![0u8; region.len];
        let mut filled = 0;
        let mut off = region.offset;
        while filled < buf.len() {
            let n = region.file.read_at(&mut buf[filled..], off).unwrap();
            assert2::assert!(n > 0);
            filled += n;
            off += n as u64;
        }
        buf
    }

    /// The load-bearing Increment-D/E invariant: the `read_raw_desc` region
    /// maps to exactly the bytes that `read_raw` would have returned, for the
    /// same `(fetch_offset, limit_offset, max_bytes)`. This test covers
    /// single-batch reads, multi-batch reads, mid-stream start offsets, the
    /// limit clamp, and the one-batch-over-budget anti-stall rule.
    #[test]
    fn read_raw_desc_region_equals_read_raw_bytes() {
        let (dir, mut seg) = test_segment();
        for off in 0..5i64 {
            seg.append(&test_batch_at(off), DENSE_INDEX).unwrap();
        }
        let cases = [
            ("all batches", 0i64, 5i64, mebibytes(10)),
            ("limit clamp", 0, 3, mebibytes(10)),
            ("mid-stream start", 2, 5, mebibytes(10)),
            ("one-batch anti-stall", 0, 5, bytes(1)),
            ("last batch", 4, 5, mebibytes(10)),
        ];
        for (_name, fo, lo, mb) in cases {
            let raw = seg.read_raw(Offset(fo), Offset(lo), mb).unwrap();
            let desc = seg.read_raw_desc(Offset(fo), Offset(lo), mb).unwrap();
            assert2::assert!(desc.start_offset == raw.start_offset);
            assert2::assert!(desc.last_offset == raw.last_offset);
            match &desc.region {
                Some(region) => {
                    assert2::assert!(region.len == raw.bytes.len());
                    assert2::assert!(region_bytes(region) == raw.bytes.to_vec());
                }
                None => assert2::assert!(raw.bytes.is_empty()),
            }
        }
        drop(dir);
    }

    /// A truncated trailing batch, where the byte budget cuts mid-batch, must
    /// produce a region whose bytes equal the clipped output of `read_raw`. A
    /// sendfile of a clipped range is wire-valid, because the consumer drops
    /// the partial batch.
    #[test]
    fn read_raw_desc_matches_read_raw_when_budget_clips_run() {
        let (dir, mut seg) = test_segment();
        // Several batches; a mid-size budget will include some but not all.
        for off in 0..6i64 {
            seg.append(&test_batch_at(off), DENSE_INDEX).unwrap();
        }
        // Budget that admits ~2-3 batches (each batch is small but > a few bytes).
        let raw = seg.read_raw(Offset(0), Offset(6), bytes(80)).unwrap();
        let desc = seg.read_raw_desc(Offset(0), Offset(6), bytes(80)).unwrap();
        let region = desc.region.expect("non-empty");
        assert2::assert!(desc.start_offset == raw.start_offset);
        assert2::assert!(desc.last_offset == raw.last_offset);
        assert2::assert!(region.len == raw.bytes.len());
        assert2::assert!(region_bytes(&region) == raw.bytes.to_vec());
        drop(dir);
    }
}
