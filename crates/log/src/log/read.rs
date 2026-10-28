//! The read paths: decoded batches, verbatim wire bytes, and file-region
//! descriptors for the zero-copy `sendfile` fetch.
//!
//! All three find the first segment that can hold the fetch offset by a
//! binary search over the sealed segments, then walk on from it through the
//! later sealed segments and the active one, so a read can span a segment
//! boundary. All three ask each segment for at least one whole batch header
//! to satisfy Kafka's anti-stall rule. A read goes on into the next segment
//! only after it reads the segment before it to the end. Only the first batch
//! of a read can be larger than its budget.

#[cfg(not(target_os = "wasi"))]
use super::page_cache;
use {
    super::Log,
    crate::{
        error::LogError,
        segment::{RawReadSource as _, ReadSource, Segment, SegmentReadView},
    },
    bytes::{Bytes, BytesMut},
    krabka_ids::Offset,
    krabka_protocol::records::{HEADER_LEN, RecordBatch},
    krabka_units::prelude::{ByteSize, ByteSizeExt, bytes},
    std::{
        ops::Range,
        sync::{
            Arc, PoisonError,
            atomic::{AtomicU64, Ordering},
        },
    },
    tracing::{field, instrument},
};
#[krabka_macros::sendfile_cfg]
use {crate::segment::RawDescSource as _, krabka_protocol::records::FileRegion, std::cell::Cell};

/// A `usize` byte length as a quantity.
///
/// The read paths track their budget as a [`ByteSize`], but the lengths they
/// subtract come from `Bytes::len` and `FileRegion::len`, which are `usize`.
fn size_from_len(len: usize) -> ByteSize {
    ByteSize::from_bytes(u64::try_from(len).unwrap_or(u64::MAX))
}

/// The fixed v2 record-batch header, as a quantity.
///
/// The log asks every per-segment read for at least this much, so the header
/// walk that finds batch boundaries always has a header to read. This is
/// Kafka's anti-stall rule. The value comes from [`HEADER_LEN`] rather than a
/// restated constant, so the floor cannot drift from the protocol.
fn batch_header() -> ByteSize { size_from_len(HEADER_LEN) }

/// Whether a read that took a run out of `segment` can go on into the next
/// segment.
///
/// `next` is the offset after the run. The read can go on only when the run
/// reached the end of the segment. A run that stopped before the end stopped
/// on the budget or on the limit. The next segment starts after batches that
/// this segment still holds. If the read went on, the response would put the
/// first batch of the next segment after the run, and skip every offset
/// between them. Kafka's `LocalLog.read` never leaves the segment that it
/// found data in.
fn emptied(segment: &impl ReadSource, next: Offset) -> bool { next > segment.last() }

/// How far behind a cold fetch of up to `max_size` bytes the pages its
/// reader's previous fetch brought in can start: twice the size, since only
/// a fetch's first batch may be larger than the size it asked for. See
/// `Log::drop_cold`.
fn cold_lookback(max_size: ByteSize) -> u64 { max_size.bytes_u64().saturating_mul(2) }

/// Whether a run of `len` bytes fits the `remaining` budget of a read that
/// already has its first batch.
///
/// Each segment read gives its first batch whole, however small its budget.
/// That is Kafka's anti-stall rule, and KIP-74 applies it only to the first
/// batch of a read. A later run that is larger than the remaining budget
/// waits for the next read.
fn fits_after_first(len: ByteSize, remaining: ByteSize) -> bool { len <= remaining }

/// The progress of a raw read and the chunks it collected.
struct RawChunks<T> {
    cursor: RawReadCursor,
    chunks: Vec<T>,
}

/// Shared progress for verbatim bytes and file regions, including the first-batch exception.
struct RawReadCursor {
    start: Offset,
    next: Offset,
    remaining: ByteSize,
    last: Option<Offset>,
}

impl RawReadCursor {
    fn new(offset: Offset, budget: ByteSize) -> Self {
        Self {
            start: offset,
            next: offset,
            remaining: budget,
            last: None,
        }
    }

    fn budget(&self) -> ByteSize { self.remaining.max(batch_header()) }

    fn accept(&mut self, start: Offset, last: Offset, bytes: usize) -> bool {
        let len = size_from_len(bytes);
        if self.last.is_some() && !fits_after_first(len, self.remaining) {
            return false;
        }
        if self.last.is_none() {
            self.start = start;
        }
        self.remaining = (self.remaining - len).max(ByteSize::ZERO);
        self.next = last + 1;
        self.last = Some(last);
        true
    }

    fn finished(&self, segment: &impl ReadSource, limit: Offset) -> bool {
        self.remaining == ByteSize::ZERO || self.next >= limit || !emptied(segment, self.next)
    }
}

/// A complete segment result with its offset span and byte cost.
struct RawChunk<T> {
    data: T,
    start: Offset,
    last: Offset,
    len: usize,
}

/// Result of [`Log::read`]: the absolute offset of the first batch
/// returned and the batches themselves.
///
/// `start_offset` falls back to the requested offset when the read returns
/// no batches, for example a read at the log end. Callers can then resume
/// from the value and need no special case for an empty result.
#[derive(Debug)]
pub struct ReadOutput {
    /// Absolute offset of the first record in [`Self::batches`], or the
    /// requested offset when no batches were returned.
    pub start_offset: Offset,
    /// Decoded batches in offset order. May be empty if the log has no
    /// data at or after the requested offset.
    pub batches: Vec<RecordBatch>,
}

/// Verbatim, decode-free output of [`Log::read_raw`].
#[derive(Debug, Clone)]
pub struct RawRead {
    /// Absolute offset of the first batch in [`Self::bytes`], or the
    /// requested offset when no bytes were returned.
    pub start_offset: Offset,
    /// Verbatim `.log` bytes: zero or more complete v2 batches. The bytes
    /// can span segment boundaries.
    pub bytes: Bytes,
    /// Length of [`Self::bytes`] in bytes.
    pub total: usize,
    /// Last offset included in [`Self::bytes`], or `None` when empty.
    pub last_offset: Option<Offset>,
}

#[krabka_macros::sendfile_cfg]
/// Descriptor form of [`Log::read_raw`] for the zero-copy `sendfile` fetch
/// path, Increments D + E.
///
/// One [`krabka_protocol::records::FileRegion`] describes the records run
/// for each contributing segment. A multi-segment fetch therefore goes
/// out as several `sendfile` regions with **no** coalescing copy.
/// `read_raw` instead concatenates cross-segment chunks into a fresh
/// `BytesMut`. This type is compiled on the SENDFILE alias: Linux, Apple,
/// and FreeBSD/DragonFly.
#[derive(Debug, Clone)]
pub struct RawReadDesc {
    /// Absolute offset of the first batch in the regions, or the requested
    /// offset when no bytes were returned.
    pub start_offset: Offset,
    /// One file-backed region per contributing segment, in wire order.
    pub regions: Vec<FileRegion>,
    /// Total byte length across all regions.
    pub total: usize,
}

impl Log {
    /// The index in `self.segments` of the first sealed segment that can hold
    /// `offset`, or `self.segments.len()` when none can and a read starts in
    /// the active segment.
    ///
    /// That is the first sealed segment whose last offset is at or past
    /// `offset`: the segment a walk from the oldest one would stop skipping
    /// at. The search is a binary search, so a tail fetch does not cost a
    /// step for every retained segment.
    ///
    /// It is exact because sealed last offsets never decrease. Bases strictly
    /// increase, and a segment's last offset lies between its own base less
    /// one, for an empty segment, and the next base less one. A compacted gap
    /// lowers a last offset only within that range, and a reopened segment
    /// takes the next base less one.
    fn first_sealed_from(&self, offset: Offset) -> usize {
        self.segments
            .partition_point(|segment| segment.last_offset() < offset)
    }

    /// The segments a read from `offset` walks, in offset order: the sealed
    /// ones from [`Self::first_sealed_from`] on, then the active one.
    fn segments_from(&self, offset: Offset) -> impl Iterator<Item = &Segment> {
        self.segments
            .iter()
            .skip(self.first_sealed_from(offset))
            .chain(self.active.as_ref())
    }

    /// Plan a raw read from `fetch_offset`, to run once the lock on this log
    /// is released.
    ///
    /// A fetch of cold bytes walks batch headers, asks the kernel to read
    /// ahead and copies or describes megabytes, and all of it waits on the
    /// disk or costs system calls: milliseconds per fetch of a consumer that
    /// is catching up. Under the log lock those milliseconds hold up every
    /// append of the partition, which a producer sees as its publish p99
    /// while a backlog drains. The plan takes what the read needs of the
    /// log in a few `Arc` clones and index lookups, and the read then runs
    /// on it without the log: [`RawReadPlan::read_bytes`] and
    /// [`RawReadPlan::read_regions`] give what [`Log::read_raw`] and
    /// [`Log::read_raw_desc`] would, as of the moment of the plan.
    ///
    /// The plan views each segment the read may walk, from the one that
    /// holds `fetch_offset` on, as [`RawReadPlan`] walks them: a later
    /// segment is read from the offset after the one before it, once that one
    /// is read to its end, so every view's offset is known now. Planning stops
    /// once the views can fill `max_size`, counting each from its index floor,
    /// which is at or before where its read starts.
    ///
    /// # Errors
    /// Returns [`LogError::OffsetTooLow`] below the log start and
    /// [`LogError::OffsetBelowLocalStart`] below the local one, as the reads
    /// do, and an error for an offset too far past a segment for its index.
    pub fn plan_raw_read(
        &self,
        fetch_offset: Offset,
        limit_offset: Offset,
        max_size: ByteSize,
    ) -> Result<RawReadPlan, LogError> {
        let (read_buffer_cap, read_ahead_max, budget) = {
            let config = self.config.read().unwrap_or_else(PoisonError::into_inner);
            (
                config.read_buffer_cap,
                config.read_ahead_max,
                config.page_cache_budget,
            )
        };
        self.check_locally_readable(fetch_offset)?;
        let mut plan = RawReadPlan {
            fetch_offset,
            limit_offset,
            max_size,
            read_buffer_cap,
            read_ahead_max,
            views: Vec::new(),
            may_block: self.read_may_block(fetch_offset),
            truncations: Arc::clone(&self.truncations),
            truncations_seen: self.truncations.load(Ordering::SeqCst),
        };
        if fetch_offset >= limit_offset {
            return Ok(plan);
        }
        let mut next = fetch_offset;
        let mut planned = 0_u64;
        for segment in self.segments_from(fetch_offset) {
            if segment.last_offset() < next {
                continue;
            }
            #[cfg(not(target_os = "wasi"))]
            let cold_before = self.cold_before(segment, budget);
            #[cfg(target_os = "wasi")]
            let cold_before = {
                let _ = budget;
                None
            };
            let view = segment.read_view(next, cold_before)?;
            planned = planned.saturating_add(view.bytes_from_floor());
            plan.views.push(view);
            if planned >= max_size.bytes_u64() {
                break;
            }
            next = segment.last_offset() + 1;
            if next >= limit_offset {
                break;
            }
        }
        // The segment's own verdict knows the page cache and its direct-write
        // tail, not the budget: pages of the active segment the budget has
        // dropped are on the disk, and a read that starts in them waits.
        plan.may_block |= plan.views.first().is_some_and(SegmentReadView::starts_cold);
        Ok(plan)
    }

    #[krabka_macros::sendfile_cfg]
    /// Descriptor variant of [`Log::read_raw`] for the zero-copy `sendfile`
    /// fetch path.
    ///
    /// This method walks sealed segments and then the active segment exactly
    /// as `read_raw` does. It collects one [`krabka_protocol::records::FileRegion`] for each contributing segment
    /// through `Segment::read_raw_desc`, instead of owned `Bytes`.
    /// Multi-segment fetches are **not** coalesced. Each region goes out in
    /// its own `sendfile` call, so the cross-segment copy disappears.
    ///
    /// The selected byte ranges are byte-identical to what `read_raw` would
    /// have returned for the same `(fetch_offset, limit_offset, max_size)`.
    /// It plans and runs under the caller's lock: see [`Log::plan_raw_read`]
    /// for the fetch path's split.
    #[instrument(
    level = "debug",
    skip(self),
    fields(regions = field::Empty, total = field::Empty),
    err,
)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn read_raw_desc(
        &self,
        fetch_offset: Offset,
        limit_offset: Offset,
        max_size: ByteSize,
    ) -> Result<RawReadDesc, LogError> {
        self.plan_raw_read(fetch_offset, limit_offset, max_size)?
            .read_regions()
    }

    /// The position in `segment`'s `.log` file before which a fetch is cold:
    /// the page-cache budget no longer holds those bytes, so the pages a fetch
    /// of them reads in would stay in the cgroup until its limit evicted
    /// them. `None` when no fetch of the segment is cold: there is no budget.
    ///
    /// The budget's ledger drops a file's oldest pages first, so the pages of
    /// a file it holds are the newest of those handed over, and everything
    /// before the oldest of them is cold. A sealed segment with no page in the
    /// ledger is cold throughout: its pages were handed over and dropped, or
    /// it was written before this process opened the log. (The partial
    /// write-back window at its end was never handed over and may still be
    /// cached; a fetch of it counts as cold all the same, and a second reader
    /// of those few pages reads the disk.) The active segment is cold up to
    /// where its background syncs have handed pages over, once the ledger
    /// holds none of them, and hot from there: those pages are its newest.
    #[cfg(not(target_os = "wasi"))]
    fn cold_before(&self, segment: &Segment, budget: Option<ByteSize>) -> Option<u64> {
        budget?;
        let retained_from = self
            .rollover_flusher
            .page_cache_retained_from(segment.log_file());
        Some(match retained_from {
            Some(from) => from,
            None if segment.is_sealed() => u64::MAX,
            None => self.writeback_handed,
        })
    }

    /// Reject an offset no local segment can answer.
    ///
    /// Below the global log start there is nothing anywhere, and the error
    /// says so. Between the global floor and the local one (KIP-405) the
    /// records are in the remote tier: the read fails all the same, so the
    /// broker's fetch path falls through to the remote reader rather than
    /// serving the first batch a surviving segment happens to start with.
    fn check_locally_readable(&self, offset: Offset) -> Result<(), LogError> {
        let log_start = self.log_start_offset();
        if offset < log_start {
            return Err(LogError::OffsetTooLow {
                requested: offset,
                log_start,
            });
        }
        let local_log_start = self.local_log_start_offset();
        if offset < local_log_start {
            return Err(LogError::OffsetBelowLocalStart {
                requested: offset,
                local_log_start,
            });
        }
        Ok(())
    }

    /// Read batches from `offset` and return up to `max_size` of `.log`
    /// data.
    ///
    /// The read walks sealed segments first, then the active segment, so a
    /// read can span segment boundaries. It goes on into a segment only after
    /// it reads the segment before it to the end.
    #[instrument(
        level = "debug",
        skip(self),
        fields(batches = field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn read(&self, offset: Offset, max_size: ByteSize) -> Result<ReadOutput, LogError> {
        let log_end = self.log_end_offset();
        self.check_locally_readable(offset)?;
        if offset >= log_end {
            return Ok(ReadOutput {
                start_offset: log_end,
                batches: Vec::new(),
            });
        }

        let read_buffer_cap = self
            .config
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .read_buffer_cap;
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut current_offset = offset;
        let mut remaining = max_size;

        for seg in self.segments_from(offset) {
            if seg.last_offset() < current_offset {
                continue;
            }
            let bs =
                seg.read_with_buffer_cap(current_offset, remaining.max(bytes(1)), read_buffer_cap)?;
            let Some(last) = bs.last() else {
                continue;
            };
            current_offset = Offset(last.base_offset + i64::from(last.last_offset_delta) + 1);
            let consumed: usize = bs.iter().map(RecordBatch::encoded_len).sum();
            remaining = (remaining - size_from_len(consumed)).max(ByteSize::ZERO);
            batches.extend(bs);
            if remaining == ByteSize::ZERO || !emptied(seg, current_offset) {
                break;
            }
        }

        let start_offset = batches.first().map_or(offset, |b| Offset(b.base_offset));
        tracing::Span::current().record("batches", batches.len());
        Ok(ReadOutput {
            start_offset,
            batches,
        })
    }

    /// Like [`Log::read`], but returns verbatim wire bytes with no decode.
    ///
    /// The read walks sealed segments and then the active segment. It goes on
    /// into a segment only after it reads the segment before it to the end.
    /// It includes only batches with `base_offset < limit_offset`, up to
    /// `max_size`, and always at least one batch. The first batch is included
    /// whole, even when it is larger than `max_size`. It plans and runs under
    /// the caller's lock: see [`Log::plan_raw_read`] for the fetch path's
    /// split.
    #[instrument(
        level = "debug",
        skip(self),
        fields(total = field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn read_raw(
        &self,
        fetch_offset: Offset,
        limit_offset: Offset,
        max_size: ByteSize,
    ) -> Result<RawRead, LogError> {
        self.plan_raw_read(fetch_offset, limit_offset, max_size)?
            .read_bytes()
    }
}

/// A raw read planned under the log lock, to run without it.
///
/// The plan holds a view of each segment the read may walk, taken by
/// [`Log::plan_raw_read`], and the read's bounds and policy. Running it is
/// the header walks, the readahead hints, the copies or descriptions and the
/// drops of cold pages that [`Log::read_raw`] and [`Log::read_raw_desc`] do,
/// and it needs nothing of the log: the views hold the file handles, and
/// read up to the sizes they saw.
#[derive(Debug, Clone)]
pub struct RawReadPlan {
    fetch_offset: Offset,
    limit_offset: Offset,
    max_size: ByteSize,
    read_buffer_cap: ByteSize,
    read_ahead_max: ByteSize,
    views: Vec<SegmentReadView>,
    may_block: bool,
    /// The log's truncation count, and its value when the plan was taken:
    /// see `Log::truncations`.
    truncations: Arc<AtomicU64>,
    truncations_seen: u64,
}

impl RawReadPlan {
    /// Whether running the plan may wait on more than the page cache:
    /// [`Log::read_may_block`] for the read's offset, as of the plan. A
    /// caller that must not block a runtime thread runs such a plan away
    /// from it.
    #[must_use]
    pub fn may_block(&self) -> bool { self.may_block }

    /// Whether the log has cut a segment file since the plan was taken. The
    /// bytes a read copied by then may mix what stood before the cut with
    /// what was written after it, so the read serves nothing, and the next
    /// fetch plans again against the log as it is now.
    fn truncated_since(&self) -> bool {
        self.truncations.load(Ordering::SeqCst) != self.truncations_seen
    }

    /// Run the plan as [`Log::read_raw`] would have read.
    ///
    /// # Errors
    /// Returns an error when log I/O fails or a record or index is corrupt.
    pub fn read_bytes(&self) -> Result<RawRead, LogError> {
        let lookback = cold_lookback(self.max_size);
        let RawChunks { cursor, mut chunks } = self.chunks(|view, next, remaining| {
            let read = view.read_raw_with_policy(
                next,
                self.limit_offset,
                remaining,
                self.read_buffer_cap,
                self.read_ahead_max,
            )?;
            if read.is_empty() {
                return Ok(None);
            }
            if let Some(file_range) = &read.file_range {
                drop_cold(view, file_range.clone(), lookback, true);
            }
            Ok(Some(RawChunk {
                len: read.bytes.len(),
                data: read.bytes,
                start: read.start_offset,
                last: read.last_offset,
            }))
        })?;

        if self.truncated_since() {
            chunks.clear();
        }
        let bytes = match chunks.len() {
            // No chunk is empty bytes, and one chunk is itself.
            0 | 1 => chunks.pop().unwrap_or_default(),
            _ => {
                let total: usize = chunks.iter().map(Bytes::len).sum();
                let mut b = BytesMut::with_capacity(total);
                for c in &chunks {
                    b.extend_from_slice(c);
                }
                b.freeze()
            }
        };
        let total = bytes.len();
        tracing::Span::current().record("total", total);
        let served = total > 0;
        Ok(RawRead {
            start_offset: if served {
                cursor.start
            } else {
                self.fetch_offset
            },
            bytes,
            total,
            last_offset: cursor.last.filter(|_| served),
        })
    }

    /// Run the plan as [`Log::read_raw_desc`] would have read.
    ///
    /// # Errors
    /// Returns an error when log I/O fails or a record or index is corrupt.
    #[krabka_macros::sendfile_cfg]
    pub fn read_regions(&self) -> Result<RawReadDesc, LogError> {
        let lookback = cold_lookback(self.max_size);
        // A segment whose run would be walked on bytes that are not in the
        // page cache is not described (`read_raw_desc_if_cached`). The walk
        // stops there, so the fetch sends the runs before it, or, when it
        // starts in that segment, gets nothing and falls back to the
        // byte-copy path, which reads the run in one large read. Passing over
        // it would send a later segment's runs and skip its offsets.
        let stopped = Cell::new(false);
        let RawChunks {
            cursor,
            chunks: mut regions,
        } = self.chunks(|view, next, remaining| {
            if stopped.get() {
                return Ok(None);
            }
            let Some(read) = view.read_raw_desc_if_cached(
                next,
                self.limit_offset,
                remaining,
                self.read_ahead_max,
            )?
            else {
                stopped.set(true);
                return Ok(None);
            };
            if let Some(region) = &read.region {
                let end = region.offset.saturating_add(region.len as u64);
                drop_cold(view, region.offset..end, lookback, false);
            }
            Ok(read.region.map(|region| RawChunk {
                len: region.len,
                data: region,
                start: read.start_offset,
                last: read.last_offset,
            }))
        })?;

        if self.truncated_since() {
            regions.clear();
        }
        let total: usize = regions.iter().map(|r| r.len).sum();
        let span = tracing::Span::current();
        span.record("regions", regions.len());
        span.record("total", total);
        Ok(RawReadDesc {
            start_offset: if regions.is_empty() {
                self.fetch_offset
            } else {
                cursor.start
            },
            regions,
            total,
        })
    }

    /// The walk both reads share: each view in turn, from the offset after
    /// the one before it, while the budget and the limit allow and the one
    /// before was read to its end.
    fn chunks<T>(
        &self,
        mut read: impl FnMut(
            &SegmentReadView,
            Offset,
            ByteSize,
        ) -> Result<Option<RawChunk<T>>, LogError>,
    ) -> Result<RawChunks<T>, LogError> {
        let mut cursor = RawReadCursor::new(self.fetch_offset, self.max_size);
        let mut chunks = Vec::new();
        for view in &self.views {
            if view.last() < cursor.next {
                continue;
            }
            let Some(chunk) = read(view, cursor.next, cursor.budget())? else {
                continue;
            };
            if !cursor.accept(chunk.start, chunk.last, chunk.len) {
                break;
            }
            chunks.push(chunk.data);
            if cursor.finished(view, self.limit_offset) {
                break;
            }
        }
        Ok(RawChunks { cursor, chunks })
    }
}

/// Drop from the page cache what a fetch of `range` of `view`'s `.log` file
/// leaves there, when the range is cold: see [`Log::cold_before`]. A
/// `sendfile` fetch sends its bytes after this returns, so it drops the pages
/// behind it, where its reader's previous fetch was, back to `lookback` bytes
/// before the range. A fetch that already `held` its bytes, having copied
/// them, drops its own pages as well. Whole pages only, so the page a
/// `sendfile` range starts in stays for the send.
///
/// A consumer catching up on a backlog thus leaves at most a fetch or two of
/// pages behind, instead of filling the cgroup to its limit and making every
/// append reclaim: see [`page_cache`].
#[cfg(not(target_os = "wasi"))]
fn drop_cold(view: &SegmentReadView, range: Range<u64>, lookback: u64, held: bool) {
    let Some(cold_before) = view.cold_before() else {
        return;
    };
    if range.end > cold_before {
        return;
    }
    let from = range.start.saturating_sub(lookback) & !(page_cache::PAGE - 1);
    let to = if held {
        range.end
    } else {
        range.start & !(page_cache::PAGE - 1)
    };
    if to > from {
        view.drop_cached(from, to - from);
    }
}

/// WASI has no flush pool, no page-cache ledger and no fetch that is cold.
#[cfg(target_os = "wasi")]
fn drop_cold(view: &SegmentReadView, range: Range<u64>, lookback: u64, held: bool) {
    let _ = (view, range, lookback, held);
}

#[cfg(test)]
mod tests;
