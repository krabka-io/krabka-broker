//! The header-only walk over a segment's `.log` file, and the index lookup
//! that gives it a starting position.
//!
//! Both the activation scan and the maximum-timestamp restore step over batch
//! boundaries without decoding a single record, so the walk lives in one place
//! and takes a visitor.

use std::ops::ControlFlow;

use krabka_ids::Offset;
use krabka_protocol::records::{HEADER_LEN, RecordBatchHeader};
use krabka_units::prelude::ByteSizeExt;
use zerocopy::FromBytes;

use super::Segment;
use crate::{config::DEFAULT_TIMESTAMP_SCAN_WINDOW, error::LogError};

/// Bytes of a v2 batch header that hold the base offset.
const BASE_OFFSET_LEN: usize = 8;

/// Bytes of a v2 batch up to and including the `batch_length` field.
const BATCH_PREFIX_LEN: usize = 12;

/// The fields of one v2 batch header that an activation walk reads.
#[derive(Debug, Clone, Copy)]
pub(super) struct BatchHeaderView {
    pub(super) base_offset: Offset,
    /// `base_offset + last_offset_delta`: the batch's highest offset.
    pub(super) last_offset: Offset,
    /// The batch's activation time. A scheduled topic makes the batch visible
    /// once this instant has passed.
    pub(super) max_timestamp: i64,
}

impl Segment {
    /// Byte position the sparse offset index gives as a floor for `offset`.
    pub(super) fn position_for(&self, offset: Offset) -> Result<u64, LogError> {
        let rel = u32::try_from((offset.0 - self.base_offset.0).max(0))
            .map_err(|_| LogError::Corrupt("activation scan offset out of range".into()))?;
        Ok(u64::from(self.offset_index.lookup(rel)))
    }

    /// Byte position a read for `target_rel` starts at.
    ///
    /// The sparse offset index holds the **last** offset of each indexed batch,
    /// as Kafka's does, so the entry a lookup lands on is a batch that ends at
    /// or below the target. Unless it ends exactly on the target, it holds
    /// nothing the read wants, and this method returns the position of the
    /// batch after it, found from the length in the indexed batch's header.
    /// Starting the read there keeps a read with a small byte budget from
    /// spending it stepping over that batch, which would return nothing.
    pub(super) fn read_start_position(&self, target_rel: u32) -> Result<u64, LogError> {
        let Some((indexed_last, position)) = self.offset_index.floor_entry(target_rel) else {
            return Ok(0);
        };
        let position = u64::from(position);
        if indexed_last == target_rel {
            return Ok(position);
        }
        // `base_offset` (8 bytes), then `batch_length` (4 bytes), which counts
        // everything after itself.
        let mut prefix = Vec::with_capacity(BATCH_PREFIX_LEN);
        self.read_log_range(position, &mut prefix, BATCH_PREFIX_LEN)?;
        let Some(batch_length) = prefix
            .get(BASE_OFFSET_LEN..BATCH_PREFIX_LEN)
            .and_then(|field| <[u8; 4]>::try_from(field).ok())
            .map(i32::from_be_bytes)
            .filter(|length| *length > 0)
        else {
            return Ok(position);
        };
        let next = position + BATCH_PREFIX_LEN as u64 + u64::from(batch_length.unsigned_abs());
        Ok(if next <= self.log_size {
            next
        } else {
            position
        })
    }

    /// Walk the fixed v2 batch headers from `start_pos` forward and hand each
    /// one to `visit`.
    ///
    /// The walk reads headers only. It steps by the header's `batch_length`,
    /// so it never touches a record body and nothing decompresses. It reads a
    /// window at a time rather than one header per system call, because a
    /// segment full of small batches would otherwise cost one `pread` per
    /// batch. It ends at the end of the file, at a torn trailing batch, or
    /// when `visit` breaks.
    pub(super) fn walk_batch_headers(
        &self,
        start_pos: u64,
        mut visit: impl FnMut(&BatchHeaderView) -> ControlFlow<()>,
    ) -> Result<(), LogError> {
        let window = DEFAULT_TIMESTAMP_SCAN_WINDOW.bytes_usize().max(HEADER_LEN);
        let mut buf: Vec<u8> = Vec::new();
        let mut pos = start_pos;
        while pos < self.log_size {
            buf.clear();
            self.read_log_range(pos, &mut buf, window)?;
            let mut at = 0usize;
            while at + HEADER_LEN <= buf.len() {
                let header = RecordBatchHeader::ref_from_bytes(&buf[at..at + HEADER_LEN])
                    .map_err(|_| LogError::Corrupt("record batch header".into()))?;
                // Wire values from the fixed v2 header stay raw `i64`.
                let batch_len = usize::try_from(header.batch_length.get().max(0)).unwrap_or(0);
                let total = 12 + batch_len;
                if total < HEADER_LEN {
                    // A batch cannot be shorter than its own header. The tail
                    // is torn, and a step of `total` would not terminate.
                    return Ok(());
                }
                let base = header.base_offset.get();
                let view = BatchHeaderView {
                    base_offset: Offset(base),
                    last_offset: Offset(base + i64::from(header.last_offset_delta.get())),
                    max_timestamp: header.max_timestamp.get(),
                };
                if visit(&view).is_break() {
                    return Ok(());
                }
                at += total;
            }
            if at == 0 {
                // Less than one header left: a torn trailing batch.
                return Ok(());
            }
            pos += at as u64;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::ControlFlow;

    use tempfile::tempdir;

    use super::*;
    use crate::segment::test_support::{DENSE_INDEX, sample_batch};

    #[test]
    fn position_for_and_walk_batch_headers_semantics() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(10)).unwrap();
        seg.append(&sample_batch(10, 5, 1_000), DENSE_INDEX)
            .unwrap();
        let pos2 = seg.log_size;
        seg.append(&sample_batch(15, 5, 2_000), DENSE_INDEX)
            .unwrap();

        // position_for uses the sparse offset index, whose entries hold each
        // indexed batch's last offset. The first batch takes none (Kafka's
        // `LogSegment.append` does not index it), so the second batch's, 19, is
        // the only entry: offsets below it floor to the segment start, and 19
        // lands on the entry itself.
        let p1 = seg.position_for(Offset(10)).unwrap();
        let p2 = seg.position_for(Offset(15)).unwrap();
        let p3 = seg.position_for(Offset(19)).unwrap();
        assert2::check!(p1 == 0);
        assert2::check!(p2 == 0);
        assert2::check!(p3 == pos2);
        assert2::check!(p3 > 0);

        // A read below the only entry starts at the segment start, and one for
        // the entry's own offset starts at its batch.
        assert2::check!(seg.read_start_position(5).unwrap() == 0);
        assert2::check!(seg.read_start_position(9).unwrap() == pos2);
        assert2::check!(seg.read_start_position(4).unwrap() == 0);

        // walk from 0 visits both batches:
        let mut views = Vec::new();
        seg.walk_batch_headers(0, |v| {
            views.push((v.base_offset, v.last_offset, v.max_timestamp));
            ControlFlow::Continue(())
        })
        .unwrap();
        assert2::check!(
            views
                == vec![
                    (Offset(10), Offset(14), 1_004),
                    (Offset(15), Offset(19), 2_004),
                ]
        );

        // walk from pos2 visits only the second batch:
        let mut views2 = Vec::new();
        seg.walk_batch_headers(pos2, |v| {
            views2.push((v.base_offset, v.last_offset, v.max_timestamp));
            ControlFlow::Continue(())
        })
        .unwrap();
        assert2::check!(views2 == vec![(Offset(15), Offset(19), 2_004)]);

        // break early stops walk:
        let mut count = 0;
        seg.walk_batch_headers(0, |_| {
            count += 1;
            ControlFlow::Break(())
        })
        .unwrap();
        assert2::check!(count == 1);
    }

    /// A read for an offset past an indexed batch starts at the batch after
    /// it, so a small byte budget is not spent stepping over the indexed one.
    #[test]
    fn a_read_past_an_indexed_batch_starts_at_the_batch_after_it() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(10)).unwrap();
        seg.append(&sample_batch(10, 5, 1_000), DENSE_INDEX)
            .unwrap();
        seg.append(&sample_batch(15, 5, 2_000), DENSE_INDEX)
            .unwrap();
        let third = seg.log_size;
        seg.append(&sample_batch(20, 5, 3_000), DENSE_INDEX)
            .unwrap();

        // The second batch (ending at relative offset 9) is indexed, and the
        // read for relative offset 12 is inside the third.
        assert2::check!(seg.read_start_position(12).unwrap() == third);
    }
}
