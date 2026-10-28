//! The header-only walk over a segment's `.log` file, and the index lookup
//! that gives it a starting position.
//!
//! Both the activation scan and the maximum-timestamp restore step over batch
//! boundaries without decoding a single record, so the walk lives in one place
//! and takes a visitor.

use {
    super::{
        Segment,
        io::SourceIo,
        read_source::{IndexHint, ReadSource},
    },
    crate::{
        config::{DEFAULT_INDEX_INTERVAL, DEFAULT_TIMESTAMP_SCAN_WINDOW},
        error::LogError,
        index::OffsetIndexEntry,
    },
    krabka_ids::Offset,
    krabka_protocol::records::{HEADER_LEN, RecordBatchHeader},
    krabka_units::prelude::ByteSizeExt,
    std::ops::ControlFlow,
    zerocopy::FromBytes,
};

/// The first window a header walk reads when nothing bounds the distance to
/// the batch it looks for.
///
/// A walk starts at an offset-index floor, and the index takes an entry about
/// every index interval, so the batch it looks for usually starts within one
/// interval of the floor. One interval and one header read it in one `pread`.
pub(super) fn initial_header_walk_window() -> usize {
    DEFAULT_INDEX_INTERVAL.bytes_usize() + HEADER_LEN
}

/// The largest window a header walk grows to.
fn max_header_walk_window() -> usize { DEFAULT_TIMESTAMP_SCAN_WINDOW.bytes_usize().max(HEADER_LEN) }

/// The window a header walk from `position` reads first, given `bound`, the
/// position of a batch the walk is known to stop at or before.
///
/// The window reaches the header at `bound`, so a consistent index finds the
/// batch in one read. Without a bound it is [`initial_header_walk_window`].
/// Either way it lies between one header and [`max_header_walk_window`]; the
/// walk grows it if the batch lies further on.
fn first_header_walk_window(position: u64, bound: Option<u64>) -> usize {
    bound
        .and_then(|bound| usize::try_from(bound.saturating_sub(position)).ok())
        .map_or_else(initial_header_walk_window, |gap| {
            gap.saturating_add(HEADER_LEN)
        })
        .clamp(HEADER_LEN, max_header_walk_window())
}

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

    /// Byte position of the first batch whose last offset reaches `target_rel`.
    ///
    /// A sparse index is only a floor: unindexed batches before the target
    /// must be skipped before the caller spends its payload byte budget.
    /// Otherwise a small read can return empty and the log can advance to the
    /// next segment while records in this one remain unread.
    pub(super) fn read_start_position(&self, target_rel: u32) -> Result<u64, LogError> {
        self.read_start_position_with(target_rel, |pos, buf, len| {
            self.read_log_range(pos, buf, len)
        })
    }
}

/// The header walks of a [`ReadSource`]: the lookup of a read's start
/// position, and the step over batch boundaries behind it. Provided for every
/// source, so they run the same over a live segment and over a view of one.
pub trait HeaderWalk: ReadSource {
    /// [`Segment::read_start_position`] over the positional reads of
    /// `read_range`, so a caller can count the I/O of the lookup.
    fn read_start_position_with(
        &self,
        target_rel: u32,
        mut read_range: impl FnMut(u64, &mut Vec<u8>, usize) -> Result<(), LogError>,
    ) -> Result<u64, LogError> {
        let target = self.base().0 + i64::from(target_rel);
        let IndexHint { floor, bound } = self.index_hint(target_rel);
        let position = match floor {
            Some(OffsetIndexEntry {
                relative_offset,
                position,
            }) if relative_offset == target_rel => {
                return Ok(u64::from(position));
            }
            Some(OffsetIndexEntry {
                relative_offset,
                position,
            }) => {
                let position = u64::from(position);
                // The index records each batch's last offset. A follower
                // normally asks for the next offset, so its index floor is
                // the preceding batch. Its fixed header gives the next byte
                // position without copying a whole header-scan window.
                if relative_offset.checked_add(1) == Some(target_rel)
                    && let Some(next) =
                        self.indexed_successor_with(position, target - 1, &mut read_range)?
                {
                    return Ok(next);
                }
                position
            }
            None => 0,
        };
        // The first index entry at or past the target holds a batch whose
        // last offset reaches it, so the walk stops at or before that entry.
        let window = first_header_walk_window(position, bound);
        self.walk_batch_headers_with(position, window, read_range, |view| {
            if view.last_offset.0 >= target {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
    }

    /// The byte after an indexed batch whose last offset immediately
    /// precedes the fetch. An inconsistent index or torn header leaves the
    /// ordinary boundary walk in charge.
    fn indexed_successor_with(
        &self,
        position: u64,
        previous_last: i64,
        read_range: &mut impl FnMut(u64, &mut Vec<u8>, usize) -> Result<(), LogError>,
    ) -> Result<Option<u64>, LogError> {
        let mut buf = Vec::with_capacity(HEADER_LEN);
        read_range(position, &mut buf, HEADER_LEN)?;
        let Ok(header) = RecordBatchHeader::ref_from_bytes(&buf) else {
            return Ok(None);
        };
        if header.magic != 2
            || header
                .base_offset
                .get()
                .checked_add(i64::from(header.last_offset_delta.get()))
                != Some(previous_last)
        {
            return Ok(None);
        }
        let Some(total) = usize::try_from(header.batch_length.get())
            .ok()
            .and_then(|length| length.checked_add(12))
            .filter(|&length| length >= HEADER_LEN)
        else {
            return Ok(None);
        };
        let next = u64::try_from(total)
            .ok()
            .and_then(|length| position.checked_add(length));
        Ok(next.filter(|&next| next <= self.log_size()))
    }

    /// Walk the fixed v2 batch headers from `start_pos` forward and hand each
    /// one to `visit`.
    ///
    /// The walk reads headers only. It steps by the header's `batch_length`,
    /// so it never touches a record body and nothing decompresses. It reads a
    /// window at a time rather than one header per system call, because a
    /// segment full of small batches would otherwise cost one `pread` per
    /// batch. The first window is [`initial_header_walk_window`]. It ends at the end of the file, at a torn trailing batch, or
    /// when `visit` breaks, and returns the byte position where it stopped.
    fn walk_batch_headers(
        &self,
        start_pos: u64,
        visit: impl FnMut(&BatchHeaderView) -> ControlFlow<()>,
    ) -> Result<u64, LogError> {
        self.walk_batch_headers_with(
            start_pos,
            initial_header_walk_window(),
            |pos, buf, len| self.read_log_range(pos, buf, len),
            visit,
        )
    }

    /// [`Self::walk_batch_headers`] over the positional reads of
    /// `read_range`, starting with a `first_window`-byte read.
    ///
    /// Each later window doubles the one before, up to
    /// [`max_header_walk_window`], so a walk that starts near its batch reads
    /// little and a long walk still takes few system calls. Where a window
    /// ends inside a header, the next window starts at that header.
    fn walk_batch_headers_with(
        &self,
        start_pos: u64,
        first_window: usize,
        mut read_range: impl FnMut(u64, &mut Vec<u8>, usize) -> Result<(), LogError>,
        mut visit: impl FnMut(&BatchHeaderView) -> ControlFlow<()>,
    ) -> Result<u64, LogError> {
        let max_window = max_header_walk_window();
        let mut window = first_window.clamp(HEADER_LEN, max_window);
        let mut buf: Vec<u8> = Vec::with_capacity(window);
        let mut pos = start_pos;
        while pos < self.log_size() {
            buf.clear();
            read_range(pos, &mut buf, window)?;
            let mut at = 0usize;
            while let Some(header_bytes) = buf.get(at..at + HEADER_LEN) {
                let header = RecordBatchHeader::ref_from_bytes(header_bytes)
                    .map_err(|_| LogError::Corrupt("record batch header".into()))?;
                // Wire values from the fixed v2 header stay raw `i64`.
                let batch_len = usize::try_from(header.batch_length.get().max(0)).unwrap_or(0);
                let total = 12 + batch_len;
                if total < HEADER_LEN {
                    // A batch cannot be shorter than its own header. The tail
                    // is torn, and a step of `total` would not terminate.
                    return Ok(pos + at as u64);
                }
                let base = header.base_offset.get();
                let view = BatchHeaderView {
                    base_offset: Offset(base),
                    last_offset: Offset(base + i64::from(header.last_offset_delta.get())),
                    max_timestamp: header.max_timestamp.get(),
                };
                if visit(&view).is_break() {
                    return Ok(pos + at as u64);
                }
                at += total;
            }
            if at == 0 {
                // Less than one header left: a torn trailing batch.
                return Ok(pos);
            }
            pos += at as u64;
            window = window.saturating_mul(2).min(max_window);
        }
        Ok(pos)
    }
}

impl<S: ReadSource + ?Sized> HeaderWalk for S {}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            config::DEFAULT_INDEX_INTERVAL,
            segment::test_support::{
                DENSE_INDEX, FIRST_HEADER_WALK_BATCH, SECOND_HEADER_WALK_BATCH, TempSegment,
                sample_batch, seeded_fixture,
            },
        },
        krabka_units::prelude::ByteSize,
        std::{iter, ops::ControlFlow},
        tempfile::tempdir,
    };

    #[test]
    fn position_for_and_walk_batch_headers_semantics() {
        let TempSegment {
            dir: _dir,
            segment: mut seg,
        } = seeded_fixture()
            .offset(10)
            .batches(&[FIRST_HEADER_WALK_BATCH])
            .call();
        let pos2 = seg.log_size;
        seg.append(
            &sample_batch()
                .offset(SECOND_HEADER_WALK_BATCH.offset.0)
                .records(SECOND_HEADER_WALK_BATCH.records.0)
                .timestamp(SECOND_HEADER_WALK_BATCH.timestamp.0)
                .call(),
            DENSE_INDEX,
        )
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

        // Unlike an index floor, a read starts at the first eligible batch,
        // including when the requested offset is below the only index entry.
        assert2::check!(seg.read_start_position(5).unwrap() == pos2);
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
        let TempSegment {
            dir: _dir,
            segment: mut seg,
        } = seeded_fixture()
            .offset(10)
            .batches(&[FIRST_HEADER_WALK_BATCH, SECOND_HEADER_WALK_BATCH])
            .call();
        let third = seg.log_size;
        seg.append(
            &sample_batch().offset(20).records(5).timestamp(3_000).call(),
            DENSE_INDEX,
        )
        .unwrap();

        // The second batch (ending at relative offset 9) is indexed, and the
        // read for relative offset 12 is inside the third.
        assert2::check!(seg.read_start_position(12).unwrap() == third);
    }

    #[test]
    fn sparse_reads_find_batches_across_multiple_header_windows() {
        use krabka_units::prelude::bytes;

        let window = DEFAULT_TIMESTAMP_SCAN_WINDOW.bytes_usize();
        let batch_size = sample_batch()
            .offset(10)
            .records(3)
            .timestamp(1_000)
            .call()
            .encoded_len();
        let count = 3 * window / batch_size + 2;
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for interval in [
            DEFAULT_INDEX_INTERVAL,
            bytes(u32::try_from(4 * window).unwrap()),
        ] {
            let (
                TempSegment {
                    dir: _dir,
                    segment: seg,
                },
                positions,
            ) = three_record_segment(count, interval, false);
            for batch in [
                0,
                1,
                window / batch_size - 1,
                window / batch_size,
                window / batch_size + 1,
                count - 1,
            ] {
                // The requested offset is inside the batch, before its last
                // offset (the key a sparse index would store).
                let relative = u32::try_from(3 * batch + 1).unwrap();
                actual.push(seg.read_start_position(relative).unwrap());
                expected.push(positions[batch]);
            }
            let beyond_end = u32::try_from(3 * count).unwrap();
            actual.push(seg.read_start_position(beyond_end).unwrap());
            expected.push(seg.log_size);
        }
        assert2::assert!(actual == expected);
    }

    #[test]
    fn sparse_reads_handle_split_headers_large_batches_and_torn_tails() {
        use {bytes::Bytes, krabka_units::prelude::bytes};

        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(10)).unwrap();
        let window = DEFAULT_TIMESTAMP_SCAN_WINDOW.bytes_usize();
        let interval = bytes(u32::try_from(4 * window).unwrap());
        let mut first = sample_batch().offset(10).timestamp(1_000).call();
        first.records[0].value = Some(Bytes::from(vec![b'x'; window]));
        let overhead = first.encoded_len() - window;
        let first_size = window - HEADER_LEN / 2;
        first.records[0].value = Some(Bytes::from(vec![b'x'; first_size - overhead]));
        assert2::assert!(first.encoded_len() == first_size);
        seg.append(&first, interval).unwrap();
        let split_header = seg.log_size;
        seg.append(&sample_batch().offset(11).timestamp(1_000).call(), interval)
            .unwrap();
        let large_position = seg.log_size;
        let mut large = sample_batch().offset(12).timestamp(1_000).call();
        large.records[0].value = Some(Bytes::from(vec![b'x'; 2 * window]));
        seg.append(&large, interval).unwrap();
        let tail = seg.log_size;
        seg.append(&sample_batch().offset(13).timestamp(1_000).call(), interval)
            .unwrap();

        let actual: Vec<_> = (0..=4)
            .map(|relative| seg.read_start_position(relative).unwrap())
            .collect();
        assert2::assert!(actual == vec![0, split_header, large_position, tail, seg.log_size]);

        // A partial final header stops at that header's position, rather than
        // advancing past it or looping when the next window reaches EOF.
        seg.log_file
            .set_len(tail + (HEADER_LEN / 2) as u64)
            .unwrap();
        assert2::assert!(seg.read_start_position(3).unwrap() == tail);
    }

    /// What one walk saw: every header it visited, where it stopped, and the
    /// length of every read it asked for.
    #[derive(Debug, PartialEq, Eq)]
    struct Walk {
        views: Vec<(Offset, Offset, i64)>,
        end: u64,
    }

    /// Walk `seg` from `start` with a `first_window`-byte first read,
    /// stopping at the first batch whose last offset reaches `stop_at`.
    fn walk(
        seg: &Segment,
        start: u64,
        first_window: usize,
        stop_at: Option<i64>,
    ) -> (Walk, Vec<usize>) {
        let mut views = Vec::new();
        let mut reads = Vec::new();
        let end = seg
            .walk_batch_headers_with(
                start,
                first_window,
                |pos, buf, len| {
                    reads.push(len);
                    seg.read_log_range(pos, buf, len)
                },
                |view| {
                    views.push((view.base_offset, view.last_offset, view.max_timestamp));
                    if stop_at.is_some_and(|stop| view.last_offset.0 >= stop) {
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                },
            )
            .unwrap();
        (Walk { views, end }, reads)
    }

    /// A segment of `count` three-record batches from offset 10, whose
    /// payloads grow so that batch boundaries fall at varied places in a
    /// window, and the position of each batch.
    fn varied_segment(count: usize, interval: ByteSize) -> (TempSegment, Vec<u64>) {
        three_record_segment(count, interval, true)
    }

    /// A segment of `count` three-record batches from offset 10, and the
    /// position of each batch. Uniform batches share one timestamp and the
    /// sample payload; varied ones step the timestamp and grow the first
    /// record's value, so batch boundaries fall at varied places in a window.
    fn three_record_segment(
        count: usize,
        interval: ByteSize,
        varied: bool,
    ) -> (TempSegment, Vec<u64>) {
        use bytes::Bytes;

        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(10)).unwrap();
        let mut positions = Vec::new();
        for batch in 0..count {
            positions.push(seg.log_size);
            let step = if varied {
                i64::try_from(batch).unwrap()
            } else {
                0
            };
            let mut sample = sample_batch()
                .offset(10 + 3 * i64::try_from(batch).unwrap())
                .records(3)
                .timestamp(1_000 + step)
                .call();
            if varied {
                sample.records[0].value = Some(Bytes::from(vec![b'x'; batch % 97]));
            }
            seg.append(&sample, interval).unwrap();
        }
        (TempSegment { dir, segment: seg }, positions)
    }

    /// The first window only decides where reads fall. Headers that straddle
    /// a window's end, windows smaller than a batch, and a torn trailing
    /// header leave the visited batches and the stopping position unchanged.
    #[test]
    fn a_walk_sees_the_same_batches_from_any_first_window() {
        let (
            TempSegment {
                dir: _dir,
                segment: seg,
            },
            positions,
        ) = varied_segment(1_500, DEFAULT_INDEX_INTERVAL);
        let batch_len = usize::try_from(positions[1]).unwrap();
        let first_windows = [
            HEADER_LEN,
            HEADER_LEN + 1,
            2 * HEADER_LEN - 1,
            batch_len + HEADER_LEN / 2,
            initial_header_walk_window(),
            max_header_walk_window(),
        ];
        let starts = [0, positions[1], positions[700], positions[1_499]];
        let stops = [None, Some(11), Some(2_000), Some(4_000)];
        for start in starts {
            for stop in stops {
                let (reference, _) = walk(&seg, start, max_header_walk_window(), stop);
                for first_window in first_windows {
                    let (actual, _) = walk(&seg, start, first_window, stop);
                    assert2::check!(actual == reference, "{start} {stop:?} {first_window}");
                }
            }
        }

        // A torn trailing header stops every walk at that header.
        let torn = positions[1_499];
        seg.log_file
            .set_len(torn + (HEADER_LEN / 2) as u64)
            .unwrap();
        let (reference, _) = walk(&seg, 0, max_header_walk_window(), None);
        assert2::check!(reference.end == torn);
        for first_window in first_windows {
            let (actual, _) = walk(&seg, 0, first_window, None);
            assert2::check!(actual == reference, "{first_window}");
        }
    }

    /// Each window doubles the one before until it reaches the cap, so a long
    /// walk from a small first window still takes few reads.
    #[test]
    fn a_long_walk_grows_its_window_geometrically_to_the_cap() {
        let (
            TempSegment {
                dir: _dir,
                segment: seg,
            },
            _,
        ) = varied_segment(4_000, DEFAULT_INDEX_INTERVAL);
        let first = HEADER_LEN + 7;

        let (_, reads) = walk(&seg, 0, first, None);

        let max = max_header_walk_window();
        let expected: Vec<usize> = iter::successors(Some(first), |window| {
            Some(window.saturating_mul(2).min(max))
        })
        .take(reads.len())
        .collect();
        assert2::check!(reads == expected);
        assert2::check!(reads.last() == Some(&max));
    }

    /// A read lookup starts with a window that reaches the next index entry,
    /// and finds the same batch as a walk with the largest window, for every
    /// offset under dense, default and sparse indexes.
    #[test]
    fn a_read_lookup_finds_the_batch_holding_every_offset() {
        use krabka_units::prelude::bytes;

        for interval in [DENSE_INDEX, DEFAULT_INDEX_INTERVAL, bytes(200_000)] {
            let (
                TempSegment {
                    dir: _dir,
                    segment: seg,
                },
                positions,
            ) = varied_segment(1_200, interval);
            let mut actual = Vec::new();
            let mut expected = Vec::new();
            for relative in 0..=u32::try_from(3 * positions.len()).unwrap() {
                actual.push(seg.read_start_position(relative).unwrap());
                let target = 10 + i64::from(relative);
                expected.push(walk(&seg, 0, max_header_walk_window(), Some(target)).0.end);
            }
            assert2::check!(actual == expected, "{interval:?}");
        }
    }

    #[yare::parameterized(
        no_bound = { 100, None, DEFAULT_INDEX_INTERVAL.bytes_usize() + HEADER_LEN },
        bound_one_interval_on = { 100, Some(4_196), 4_096 + HEADER_LEN },
        bound_at_position = { 100, Some(100), HEADER_LEN },
        bound_below_position = { 100, Some(50), HEADER_LEN },
        bound_past_the_cap = { 0, Some(1 << 20), DEFAULT_TIMESTAMP_SCAN_WINDOW.bytes_usize() },
    )]
    fn the_first_window_reaches_the_bounding_header(
        position: u64,
        bound: Option<u64>,
        expected: usize,
    ) {
        assert2::check!(first_header_walk_window(position, bound) == expected);
    }
}
