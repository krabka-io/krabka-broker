//! The verbatim, decode-free read path.
//!
//! A fetch serves the producer's own batch bytes, so this walk reads only the
//! fixed v2 batch headers to find run boundaries and never touches a record
//! body. The zero-copy descriptor form of the same walk is the sibling
//! `read_raw_desc` module.

use bytes::Bytes;
use krabka_ids::Offset;
use krabka_protocol::records::{HEADER_LEN, RecordBatchHeader};
use krabka_units::prelude::{ByteSize, ByteSizeExt};
use tracing::instrument;
use zerocopy::FromBytes;

use super::{RawSegmentRead, Segment};
use crate::{config::DEFAULT_READ_BUFFER_CAP, error::LogError};

impl Segment {
    /// Read a contiguous run of **complete, verbatim** record-batch bytes.
    ///
    /// The run starts at the batch that contains `fetch_offset`. It includes
    /// only batches whose `base_offset < limit_offset`, up to about
    /// `max_bytes`, and always at least one batch. That last rule is Kafka's
    /// anti-stall rule. This method decodes no records. It reads only the
    /// fixed batch headers to find the boundaries.
    #[instrument(
        level = "debug",
        skip(self),
        fields(base_offset = self.base_offset.0, bytes = tracing::field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn read_raw(
        &self,
        fetch_offset: Offset,
        limit_offset: Offset,
        max_size: ByteSize,
    ) -> Result<RawSegmentRead, LogError> {
        self.read_raw_with_buffer_cap(
            fetch_offset,
            limit_offset,
            max_size,
            DEFAULT_READ_BUFFER_CAP,
        )
    }

    pub(crate) fn read_raw_with_buffer_cap(
        &self,
        fetch_offset: Offset,
        limit_offset: Offset,
        max_size: ByteSize,
        read_buffer_cap: ByteSize,
    ) -> Result<RawSegmentRead, LogError> {
        if fetch_offset > self.last_offset || fetch_offset >= limit_offset {
            return Ok(RawSegmentRead::empty());
        }
        let target_rel = u32::try_from((fetch_offset.0 - self.base_offset.0).max(0))
            .map_err(|_| LogError::Corrupt("read_raw target offset out of range".into()))?;
        let start_pos = u64::from(self.offset_index.lookup(target_rel));

        // Below this line the budget indexes into a byte buffer, so it
        // crosses back to `usize` once, here.
        let max_bytes = max_size.bytes_usize();
        let first_read = max_bytes.max(HEADER_LEN);
        let mut buf: Vec<u8> = Vec::with_capacity(first_read.min(read_buffer_cap.bytes_usize()));
        self.read_log_range(start_pos, &mut buf, first_read)?;

        let mut pos = 0usize;
        let mut range_start: Option<usize> = None;
        let mut range_end = 0usize;
        let mut start_offset = fetch_offset;
        let mut last_offset = fetch_offset - 1;

        loop {
            if pos + HEADER_LEN > buf.len() {
                break;
            }
            let hdr = RecordBatchHeader::ref_from_bytes(&buf[pos..pos + HEADER_LEN])
                .map_err(|_| LogError::Corrupt("record batch header".into()))?;
            // Wire values from the fixed v2 header stay raw `i64`.
            let base = hdr.base_offset.get();
            let batch_len = usize::try_from(hdr.batch_length.get().max(0)).unwrap_or(0);
            let total = 12 + batch_len;
            let batch_last = base + i64::from(hdr.last_offset_delta.get());

            if batch_last < fetch_offset {
                pos += total;
                continue;
            }
            if base >= limit_offset {
                break;
            }
            if pos + total > buf.len() {
                if range_start.is_none() {
                    let mut one: Vec<u8> = Vec::with_capacity(total);
                    self.read_log_range(start_pos + pos as u64, &mut one, total)?;
                    if one.len() < total {
                        break;
                    }
                    return Ok(RawSegmentRead {
                        start_offset: Offset(base),
                        last_offset: Offset(batch_last),
                        bytes: Bytes::from(one),
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

            if range_end - range_start.expect("set above") >= max_bytes {
                break;
            }
        }

        match range_start {
            Some(s) => {
                let bytes = Bytes::from(buf).slice(s..range_end);
                tracing::Span::current().record("bytes", bytes.len());
                Ok(RawSegmentRead {
                    start_offset,
                    last_offset,
                    bytes,
                })
            }
            None => Ok(RawSegmentRead::empty()),
        }
    }
}

#[cfg(test)]
mod tests {
    use krabka_units::prelude::{bytes, mebibytes};
    use tempfile::tempdir;

    use super::*;
    use crate::segment::test_support::{
        DENSE_INDEX, NO_LIMIT, sample_batch, test_batch_at, test_segment,
    };

    /// A fetch reads nothing when it starts past the segment, and nothing when
    /// it starts at or past the limit. Either condition alone is enough --
    /// joined with `&&` a fetch would have to be both before it read nothing.
    #[test]
    fn a_fetch_past_the_segment_or_at_the_limit_reads_nothing() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        seg.append(&sample_batch(0, 3, 100), DENSE_INDEX).unwrap(); // offsets 0..=2

        let past = seg.read_raw(Offset(3), Offset(99), NO_LIMIT).unwrap();
        check!(past.is_empty(), "a fetch past the last offset");

        let at_limit = seg.read_raw(Offset(0), Offset(0), NO_LIMIT).unwrap();
        check!(at_limit.is_empty(), "a fetch at the limit");

        let inside = seg.read_raw(Offset(0), Offset(3), NO_LIMIT).unwrap();
        check!(
            !inside.is_empty(),
            "a fetch inside the segment and below the limit"
        );
    }

    /// Batches before the fetch offset are stepped over by their own length.
    /// Advancing by anything else lands mid-batch and the walk reads a header
    /// out of the middle of a record.
    #[test]
    fn a_fetch_mid_segment_steps_over_the_batches_before_it() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        // A sparse index, so only the first batch gets an entry and the lookup
        // lands at the segment head. The walk then has to step over the two
        // batches before the one asked for -- with a dense index it would jump
        // straight there and never step at all.
        let sparse = mebibytes(1);
        seg.append(&sample_batch(0, 2, 100), sparse).unwrap(); // 0..=1
        seg.append(&sample_batch(2, 2, 200), sparse).unwrap(); // 2..=3
        seg.append(&sample_batch(4, 2, 300), sparse).unwrap(); // 4..=5

        let read = seg
            .read_raw_with_buffer_cap(Offset(4), Offset(99), NO_LIMIT, mebibytes(1))
            .unwrap();
        check!(
            read.start_offset == Offset(4),
            "start {:?}",
            read.start_offset
        );
        check!(read.last_offset == Offset(5), "last {:?}", read.last_offset);
    }

    /// A batch that will not fit the first read is fetched on its own, from
    /// its own position in the file.
    ///
    /// That position is `start_pos + pos`, and with a dense index and a fetch
    /// past the first batch `start_pos` is well away from the file head -- so
    /// reading from anywhere else returns a different batch, or nothing.
    #[test]
    fn a_batch_too_large_for_the_first_read_is_fetched_from_its_own_position() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        for i in 0..4i64 {
            seg.append(&sample_batch(i * 2, 2, 100 + i), DENSE_INDEX)
                .unwrap();
        }

        // A one-byte budget makes the first read header-sized, so the batch
        // cannot fit it and takes the read-one-batch path.
        let read = seg.read_raw(Offset(4), Offset(99), bytes(1)).unwrap();
        check!(
            read.start_offset == Offset(4),
            "start {:?}",
            read.start_offset
        );
        check!(read.last_offset == Offset(5), "last {:?}", read.last_offset);
        check!(!read.is_empty());
    }

    /// The byte budget stops the walk once the selected range reaches it, so a
    /// small budget returns fewer batches than an unlimited one.
    #[test]
    fn the_byte_budget_bounds_how_much_a_fetch_returns() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        for i in 0..6i64 {
            seg.append(&sample_batch(i * 2, 2, 100 + i), DENSE_INDEX)
                .unwrap();
        }

        let everything = seg.read_raw(Offset(0), Offset(99), NO_LIMIT).unwrap();
        let clipped = seg.read_raw(Offset(0), Offset(99), bytes(1)).unwrap();
        check!(
            !clipped.is_empty(),
            "a budget of one byte still returns a batch"
        );
        check!(
            clipped.last_offset < everything.last_offset,
            "clipped to {:?}, unlimited reached {:?}",
            clipped.last_offset,
            everything.last_offset
        );
    }

    // `Segment::read_raw` maps the fetch offset to the relative index key the
    // same way. base_offset 100, dense index, `read_raw(103)` must begin at
    // the offset-103 batch (`start_offset == 103`). Mutating `-`→`+` computes
    // `203`, whose lookup skips past the offset-103 batch → `start_offset`
    // becomes 105.
    #[test]
    fn read_raw_uses_relative_offset_for_index_lookup() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(100)).unwrap();
        seg.append(&sample_batch(100, 3, 100), DENSE_INDEX).unwrap(); // offsets 100..=102
        seg.append(&sample_batch(103, 2, 200), DENSE_INDEX).unwrap(); // offsets 103..=104
        seg.append(&sample_batch(105, 1, 300), DENSE_INDEX).unwrap(); // offset 105

        let r = seg.read_raw(Offset(103), Offset(1000), NO_LIMIT).unwrap();
        assert2::assert!(!r.is_empty());
        assert2::assert!(r.start_offset == Offset(103));
    }

    #[test]
    fn read_raw_is_byte_exact_and_multi_batch() {
        let (dir, mut seg) = test_segment();
        let mut wire = bytes::BytesMut::new();
        for off in 0..3i64 {
            let b = test_batch_at(off);
            seg.append(&b, DENSE_INDEX).unwrap();
            b.encode(&mut wire).unwrap();
        }
        let wire = wire.freeze();
        let r = seg.read_raw(Offset(0), Offset(3), mebibytes(10)).unwrap();
        assert2::assert!(r.start_offset == Offset(0));
        assert2::assert!(r.last_offset == Offset(2));
        assert2::assert!(&r.bytes[..] == &wire[..]);
        drop(dir);
    }

    #[test]
    fn read_raw_clamps_at_limit_offset() {
        let (dir, mut seg) = test_segment();
        let mut expected = bytes::BytesMut::new();
        for off in 0..3i64 {
            let batch = test_batch_at(off);
            seg.append(&batch, DENSE_INDEX).unwrap();
            if off < 2 {
                batch.encode(&mut expected).unwrap();
            }
        }
        let r = seg.read_raw(Offset(0), Offset(2), mebibytes(10)).unwrap();
        assert2::assert!(r.start_offset == Offset(0));
        assert2::assert!(r.last_offset == Offset(1));
        assert2::assert!(&r.bytes[..] == &expected[..]);
        drop(dir);
    }

    #[test]
    fn read_raw_returns_at_least_one_batch_over_budget() {
        let (dir, mut seg) = test_segment();
        let batch = test_batch_at(0);
        let mut expected = bytes::BytesMut::new();
        batch.encode(&mut expected).unwrap();
        seg.append(&batch, DENSE_INDEX).unwrap();
        let r = seg.read_raw(Offset(0), Offset(1), bytes(1)).unwrap();
        assert2::assert!(r.start_offset == Offset(0));
        assert2::assert!(r.last_offset == Offset(0));
        assert2::assert!(&r.bytes[..] == &expected[..]);
        drop(dir);
    }
}
