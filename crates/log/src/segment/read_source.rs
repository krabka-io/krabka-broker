//! What a verbatim read needs of a segment, and a snapshot of it that a read
//! can use once the log lock is released.
//!
//! A consumer that is catching up reads cold bytes: its fetch walks batch
//! headers one small read at a time, asks the kernel to read a window ahead
//! in 128 KiB hints, and copies or describes megabytes. Each of those is a
//! system call, and some of them wait for the disk. Run under the log lock,
//! as the log's own `read_raw` does, they hold up every append of the
//! partition for as long as they take: milliseconds per fetch, which is what
//! a producer sees as its publish p99 while a backlog drains.
//!
//! [`ReadSource`] is the view of a segment the verbatim read code runs over.
//! The live [`Segment`] implements it on its fields, so a read under the lock
//! works as before. [`SegmentReadView`] implements it on a snapshot taken
//! under the lock: the file and I/O handles, the sizes and offsets, the
//! direct-mode tail cache, and the sparse-index lookups the read's start
//! position needs. The snapshot costs a few `Arc` clones and two binary
//! searches, and the read then runs on it with the lock released.
//!
//! A view is of one moment. A sealed segment never changes under it. The
//! active segment grows, and a view reads up to the size it saw: bytes
//! appended after the plan belong to the next fetch.

#[krabka_macros::sendfile_cfg]
use std::sync::atomic::AtomicBool;
use {
    super::{Segment, direct::TailCache},
    crate::{error::LogError, index::OffsetIndexEntry, io::LogIo},
    bytes::Bytes,
    krabka_ids::Offset,
    std::{fs::File, sync::Arc},
};

/// The sparse-index lookups a read's start position needs, for one target
/// offset of one segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexHint {
    /// The index entry at or before the target.
    pub(super) floor: Option<OffsetIndexEntry>,
    /// The `.log` position of the first index entry at or past the target,
    /// which bounds the header walk that finds the target's batch.
    pub(super) bound: Option<u64>,
}

/// The relative offset of `target` in a segment based at `base`, clamped at
/// zero for a target before the segment. A target too far past the segment
/// to fit the index's `u32` is an error only once a read needs it: a fetch
/// that starts past the segment's last offset reads nothing from it.
pub(super) fn relative_target(
    base: Offset,
    target: Offset,
    operation: &str,
) -> Result<u32, LogError> {
    u32::try_from((target.0 - base.0).max(0))
        .map_err(|_| LogError::Corrupt(format!("{operation} target offset out of range")))
}

/// What the verbatim read code reads a segment through.
///
/// Every method is a plain accessor, so the read code written against this
/// trait is the same whether it runs over a live segment or a snapshot of
/// one. The index lookups are the exception: a segment answers for any
/// target, a view only for the target it was taken for.
pub trait ReadSource {
    /// The `.log` file.
    fn file(&self) -> &Arc<File>;
    /// The I/O the reads, hints and drops go through.
    fn io(&self) -> &Arc<dyn LogIo>;
    /// The bytes of the `.log` file a read may use.
    fn log_size(&self) -> u64;
    /// The segment's base offset.
    fn base(&self) -> Offset;
    /// The last offset of any batch in the segment.
    fn last(&self) -> Offset;
    /// Whether the segment has written through `O_DIRECT`, so its bytes are
    /// mostly not in the page cache. Only the descriptor read asks, so it
    /// exists where `sendfile` does.
    #[krabka_macros::sendfile_cfg]
    fn wrote_direct(&self) -> bool;
    /// The newest bytes of a segment that writes directly, held in memory.
    fn tail_cache(&self) -> Option<&TailCache>;
    /// Whether the page-cache probe is known not to work on this segment's
    /// filesystem; see the descriptor read's `byte_is_cached`, its only user.
    #[krabka_macros::sendfile_cfg]
    fn cache_probe_unsupported(&self) -> &AtomicBool;
    /// The index lookups for a read whose target is `target_rel`.
    fn index_hint(&self, target_rel: u32) -> IndexHint;

    /// The bytes from `pos` the segment holds in memory, when it writes
    /// directly and the tail cache reaches `pos`.
    fn cached_bytes(&self, pos: u64, max: usize) -> Option<Bytes> {
        self.tail_cache()?.bytes_from(pos, max)
    }
}

/// The accessors a segment and its view answer from fields of the same
/// name: the view copies them from the segment.
macro_rules! field_accessors {
    () => {
        fn file(&self) -> &Arc<File> { &self.log_file }

        fn io(&self) -> &Arc<dyn LogIo> { &self.io }

        fn log_size(&self) -> u64 { self.log_size }

        fn base(&self) -> Offset { self.base_offset }

        fn last(&self) -> Offset { self.last_offset }

        #[krabka_macros::sendfile_cfg]
        fn wrote_direct(&self) -> bool { self.wrote_direct }
    };
}

impl ReadSource for Segment {
    field_accessors!();

    fn tail_cache(&self) -> Option<&TailCache> { self.direct.as_ref().map(|mode| &mode.cache) }

    #[krabka_macros::sendfile_cfg]
    fn cache_probe_unsupported(&self) -> &AtomicBool { &self.cache_probe_unsupported }

    fn index_hint(&self, target_rel: u32) -> IndexHint {
        IndexHint {
            floor: self.offset_index.floor_entry(target_rel),
            bound: self
                .offset_index
                .position_at_or_after(target_rel)
                .map(u64::from),
        }
    }
}

/// A segment as a read from one offset sees it, taken under the log lock so
/// the read can run without it. See the module documentation.
#[derive(Debug, Clone)]
pub struct SegmentReadView {
    log_file: Arc<File>,
    io: Arc<dyn LogIo>,
    log_size: u64,
    base_offset: Offset,
    last_offset: Offset,
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    ))]
    wrote_direct: bool,
    tail: Option<TailCache>,
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    ))]
    cache_probe_unsupported: Arc<AtomicBool>,
    /// The target the index hint is for: the read's offset, relative.
    target_rel: u32,
    hint: IndexHint,
    /// The `.log` position before which a fetch of this segment is cold, so
    /// the pages it reads in are dropped behind it; `None` when no fetch of
    /// it is. See `Log::cold_before`.
    cold_before: Option<u64>,
}

impl SegmentReadView {
    /// The `.log` position before which a fetch of the segment is cold, or
    /// `None` when no fetch of it is.
    pub fn cold_before(&self) -> Option<u64> { self.cold_before }

    /// The bytes from the index floor of the view's target to the end of
    /// the segment: at least what a read from the target can take from it,
    /// since the read starts at or after the floor.
    pub fn bytes_from_floor(&self) -> u64 {
        let floor = self.hint.floor.map_or(0, |entry| u64::from(entry.position));
        self.log_size.saturating_sub(floor)
    }

    /// Drop the `len` bytes of the `.log` file from `offset` from the page
    /// cache, in whole pages: [`LogIo::drop_cached`]. A fetch of bytes the
    /// page-cache budget no longer holds calls this on the pages behind it,
    /// so a consumer reading an old backlog leaves none in the cgroup.
    #[cfg(not(target_os = "wasi"))]
    pub fn drop_cached(&self, offset: u64, len: u64) {
        self.io.drop_cached(&self.log_file, offset, len);
    }
}

impl ReadSource for SegmentReadView {
    field_accessors!();

    fn tail_cache(&self) -> Option<&TailCache> { self.tail.as_ref() }

    #[krabka_macros::sendfile_cfg]
    fn cache_probe_unsupported(&self) -> &AtomicBool { &self.cache_probe_unsupported }

    fn index_hint(&self, target_rel: u32) -> IndexHint {
        assert2::debug_assert!(
            target_rel == self.target_rel,
            "a read view answers for the offset it was taken for"
        );
        self.hint
    }
}

impl Segment {
    /// A view of this segment for a read from `fetch_offset`, to run after
    /// the log lock is released. `cold_before` is the log's verdict on where
    /// a fetch of this segment turns cold, taken now because it needs the
    /// log.
    ///
    /// # Errors
    /// Returns an error when `fetch_offset` is too far past the segment for
    /// its index, which no read of the segment could use either.
    pub fn read_view(
        &self,
        fetch_offset: Offset,
        cold_before: Option<u64>,
    ) -> Result<SegmentReadView, LogError> {
        let target_rel = relative_target(self.base_offset, fetch_offset, "read_view")?;
        Ok(SegmentReadView {
            log_file: Arc::clone(&self.log_file),
            io: Arc::clone(&self.io),
            log_size: self.log_size,
            base_offset: self.base_offset,
            last_offset: self.last_offset,
            #[cfg(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "ios",
                target_os = "tvos",
                target_os = "watchos",
                target_os = "freebsd",
                target_os = "dragonfly",
            ))]
            wrote_direct: self.wrote_direct,
            tail: self.direct.as_ref().map(|mode| mode.cache.clone()),
            #[cfg(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "ios",
                target_os = "tvos",
                target_os = "watchos",
                target_os = "freebsd",
                target_os = "dragonfly",
            ))]
            cache_probe_unsupported: Arc::clone(&self.cache_probe_unsupported),
            target_rel,
            hint: self.index_hint(target_rel),
            cold_before,
        })
    }
}

#[cfg(test)]
mod tests {
    #[krabka_macros::sendfile_cfg]
    use crate::segment::RawDescSource as _;
    use {
        super::*,
        crate::segment::{
            RawReadSource as _,
            test_support::{DENSE_INDEX, TempSegment, test_batch_at, test_segment},
        },
        krabka_units::prelude::{bytes, mebibytes},
    };

    /// A view taken under the lock reads what the segment reads, through
    /// both verbatim paths, from every offset, and keeps reading it after
    /// the segment has grown: it is of the moment it was taken.
    #[krabka_macros::sendfile_cfg]
    #[test]
    fn a_view_reads_what_its_segment_read_when_it_was_taken() {
        let TempSegment {
            dir: _dir,
            segment: mut seg,
        } = test_segment();
        for off in 0..12i64 {
            seg.append(&test_batch_at(Offset(off)), DENSE_INDEX)
                .unwrap();
        }
        let limit = Offset(12);
        for fetch in 0..=12i64 {
            for budget in [bytes(1), bytes(200), mebibytes(1)] {
                let fetch = Offset(fetch);
                let view = seg.read_view(fetch, None).unwrap();
                let from_segment = seg.read_raw(fetch, limit, budget).unwrap();
                let from_view = view
                    .read_raw_with_policy(fetch, limit, budget, mebibytes(4), mebibytes(4))
                    .unwrap();
                assert2::check!(
                    from_view.bytes == from_segment.bytes,
                    "{fetch:?} {budget:?}"
                );
                assert2::check!(from_view.start_offset == from_segment.start_offset);
                assert2::check!(from_view.last_offset == from_segment.last_offset);
                let described = view
                    .read_raw_desc_if_cached(fetch, limit, budget, mebibytes(4))
                    .unwrap()
                    .expect("a freshly written segment is cached");
                assert2::check!(
                    described.region.as_ref().map(|region| region.len)
                        == Some(from_segment.bytes.len()).filter(|len| *len > 0)
                );
            }
        }

        // Appends after the view do not reach it: it reads to the size it saw.
        let view = seg.read_view(Offset(0), None).unwrap();
        let before = seg.read_raw(Offset(0), Offset(99), mebibytes(1)).unwrap();
        seg.append(&test_batch_at(Offset(12)), DENSE_INDEX).unwrap();
        let after = view
            .read_raw_with_policy(
                Offset(0),
                Offset(99),
                mebibytes(1),
                mebibytes(4),
                mebibytes(4),
            )
            .unwrap();
        assert2::check!(after.bytes == before.bytes);
        assert2::check!(after.last_offset == Offset(11));
    }

    /// The relative target clamps at the segment start and refuses a target
    /// the index cannot hold.
    #[test]
    fn the_relative_target_clamps_below_and_refuses_overflow() {
        assert2::check!(relative_target(Offset(10), Offset(3), "test").unwrap() == 0);
        assert2::check!(relative_target(Offset(10), Offset(17), "test").unwrap() == 7);
        assert2::check!(
            relative_target(Offset(0), Offset(i64::from(u32::MAX) + 1), "test").is_err()
        );
    }
}
