//! The read-side window of a segment that writes directly: its newest bytes,
//! kept in memory.

use {
    super::to_u64,
    crate::segment::tail_arena::{FIRST_SLAB, Headroom, SLAB, TailArena},
    bytes::{Bytes, BytesMut},
    std::{collections::VecDeque, mem, sync::Arc},
};

/// The newest bytes of a segment that writes directly, `[start, end)`, as
/// shared slices: what a read takes from memory. A read view clones the part
/// it needs with [`Self::suffix`], which copies slice handles, not bytes.
#[derive(Debug, Clone, Default)]
pub struct TailWindow {
    start: u64,
    len: usize,
    chunks: VecDeque<Bytes>,
}

impl TailWindow {
    pub(crate) fn start(&self) -> u64 { self.start }

    pub(crate) fn end(&self) -> u64 { self.start + to_u64(self.len) }

    /// The window from the chunk that holds `pos` on: what a read that starts
    /// at or after `pos` can use. Chunks wholly before `pos` are left out, and
    /// the walk goes back from the end, so a reader near the end touches a few
    /// handles whatever the window's size: the plan that takes this view holds
    /// the log lock.
    pub(crate) fn suffix(&self, pos: u64) -> Self {
        let end = self.end();
        let mut start = end;
        let mut kept = 0;
        for chunk in self.chunks.iter().rev() {
            if start <= pos {
                break;
            }
            start -= to_u64(chunk.len());
            kept += 1;
        }
        Self {
            start,
            len: usize::try_from(end - start).unwrap_or(self.len),
            chunks: self
                .chunks
                .range(self.chunks.len() - kept..)
                .cloned()
                .collect(),
        }
    }

    /// The bytes from `pos` on: the rest of the chunk `pos` falls in, then
    /// whole chunks while the total stays within `max`. The first chunk comes
    /// whatever its size. One chunk is a shared slice, with no copy; several
    /// are copied into one buffer. `None` when `pos` is outside the window.
    pub(crate) fn bytes_from(&self, pos: u64, max: usize) -> Option<Bytes> {
        if pos < self.start || pos >= self.end() {
            return None;
        }
        let mut skip = usize::try_from(pos - self.start).ok()?;
        let mut taken: Vec<Bytes> = Vec::new();
        let mut total = 0;
        for chunk in &self.chunks {
            if skip >= chunk.len() {
                skip -= chunk.len();
                continue;
            }
            let part = chunk.slice(skip..);
            skip = 0;
            if !taken.is_empty() && total + part.len() > max {
                break;
            }
            total += part.len();
            taken.push(part);
        }
        if taken.len() == 1 {
            return taken.pop();
        }
        let mut joined = bytes::BytesMut::with_capacity(total);
        for part in &taken {
            joined.extend_from_slice(part);
        }
        Some(joined.freeze())
    }

    /// Copy `[pos, pos + out.len())` into `out` and return true when the whole
    /// range is in the window. Return false and leave `out` as it is when it
    /// is not.
    pub(crate) fn read(&self, pos: u64, out: &mut [u8]) -> bool {
        let Some(read_end) = pos.checked_add(to_u64(out.len())) else {
            return false;
        };
        if pos < self.start || read_end > self.end() {
            return false;
        }
        let Ok(mut skip) = usize::try_from(pos - self.start) else {
            return false;
        };
        let mut filled = 0;
        for chunk in &self.chunks {
            if filled == out.len() {
                break;
            }
            if skip >= chunk.len() {
                skip -= chunk.len();
                continue;
            }
            let take = (chunk.len() - skip).min(out.len() - filled);
            let (Some(dst), Some(src)) = (
                out.get_mut(filled..filled + take),
                chunk.get(skip..skip + take),
            ) else {
                return false;
            };
            dst.copy_from_slice(src);
            filled += take;
            skip = 0;
        }
        true
    }

    /// Forget every byte at or past `end`. When `end` is at or before the
    /// window's start, the window becomes empty and begins at `end`.
    fn truncate(&mut self, end: u64) {
        if end >= self.end() {
            return;
        }
        if end <= self.start {
            self.chunks.clear();
            self.len = 0;
            self.start = end;
            return;
        }
        // `end` is below the window's end, so `keep` is below `self.len`.
        let keep = usize::try_from(end - self.start).unwrap_or(self.len);
        let mut kept = 0;
        let mut chunks = 0;
        for chunk in &mut self.chunks {
            if kept == keep {
                break;
            }
            let take = chunk.len().min(keep - kept);
            chunk.truncate(take);
            kept += take;
            chunks += 1;
        }
        self.chunks.truncate(chunks);
        self.len = keep;
    }
}

/// A slab a cache copies its batches into, and the segment position its
/// last batch ends at.
#[derive(Debug)]
struct Slab {
    /// The slab's free space; its filled part was split off into chunks.
    free: BytesMut,
    /// What the arena counted for the slab.
    capacity: usize,
    /// The segment positions of its first byte and of the end of its last
    /// batch.
    start: u64,
    end: u64,
}

/// The most recently appended bytes of an active segment, kept in memory to
/// serve reads, and every byte at or past `written` whatever the size.
///
/// The window holds at least `floor` bytes when the segment has written that
/// many. It grows towards `max` while the process's [`TailArena`] has a
/// budget and the caches together are under it; at the budget a cache keeps
/// the size it has, and with no budget it holds `floor`, as a fixed window
/// did. Eviction drops the oldest chunk. A chunk that is not on the disk yet
/// is never evicted, so a read below the window's start can always go to the
/// file.
///
/// Each batch is copied into a slab from the arena, and the window holds
/// shared slices of the slabs: see [`tail_arena`](crate::segment::tail_arena).
#[derive(Debug)]
pub struct TailCache {
    floor: usize,
    max: usize,
    written: u64,
    window: TailWindow,
    slabs: VecDeque<Slab>,
    arena: Arc<TailArena>,
}

impl Drop for TailCache {
    fn drop(&mut self) {
        for slab in self.slabs.drain(..) {
            self.arena.give_back(slab.free, slab.capacity);
        }
    }
}

impl TailCache {
    /// An empty window of `cap` bytes that begins at `start`, the segment's
    /// logical end when the cache is made, in a test's own arena.
    #[cfg(test)]
    pub(crate) fn new(cap: usize, start: u64) -> Self {
        Self::in_arena(cap, cap, start, Arc::new(TailArena::default()))
    }

    /// An empty window that begins at `start`, the segment's logical end when
    /// the cache is made, holding `floor` to `max` bytes in `arena`.
    pub(crate) fn in_arena(floor: usize, max: usize, start: u64, arena: Arc<TailArena>) -> Self {
        Self {
            floor,
            max: max.max(floor),
            written: start,
            window: TailWindow {
                start,
                len: 0,
                chunks: VecDeque::new(),
            },
            slabs: VecDeque::new(),
            arena,
        }
    }

    /// Learn that every byte below `written` is on the disk, so the chunks
    /// below it can be evicted.
    pub(crate) fn set_written(&mut self, written: u64) { self.written = self.written.max(written); }

    pub(crate) fn start(&self) -> u64 { self.window.start }

    pub(crate) fn end(&self) -> u64 { self.window.end() }

    /// The bytes the cache holds, for reads.
    pub(crate) fn window(&self) -> &TailWindow { &self.window }

    /// Take the window and its slabs, leaving an empty cache that begins at
    /// [`Self::end`] in the same arena.
    pub(crate) fn detach(&mut self) -> Self {
        let empty = Self::in_arena(self.floor, self.max, self.end(), Arc::clone(&self.arena));
        mem::replace(self, empty)
    }

    /// Record the concatenation of `parts`, appended at [`Self::end`], as one
    /// chunk.
    ///
    /// Then drop the oldest chunks while the window is larger than it may be
    /// and the oldest chunk is on the disk. The newest chunk stays even when
    /// it alone is larger, so a reader right behind the producer always finds
    /// the last append.
    pub(crate) fn push(&mut self, parts: &[&[u8]]) {
        let len = parts.iter().map(|part| part.len()).sum();
        if len == 0 {
            return;
        }
        let before = self.window.len;
        let chunk = self.copy_in(parts, len);
        self.window.len += len;
        self.window.chunks.push_back(chunk);
        self.evict(before);
    }

    /// Record the batches of one coalesced write, `group[..ends[0]]`,
    /// `group[ends[0]..ends[1]]` and so on, appended at [`Self::end`], one
    /// chunk each.
    pub(crate) fn push_group(&mut self, group: &[u8], ends: &[usize]) {
        let mut start = 0;
        for &end in ends {
            if let Some(batch) = group.get(start..end) {
                self.push(&[batch]);
            }
            start = end;
        }
    }

    /// Copy `len` bytes, the concatenation of `parts`, into the newest slab,
    /// or a new one when it has no room, and return them as a shared slice.
    fn copy_in(&mut self, parts: &[&[u8]], len: usize) -> Bytes {
        let start = self.window.end();
        let end = start + to_u64(len);
        let room = self
            .slabs
            .back()
            .is_some_and(|slab| slab.free.capacity() - slab.free.len() >= len);
        if !room {
            let mut wanted = self
                .slabs
                .back()
                .map_or(FIRST_SLAB, |slab| slab.capacity.saturating_mul(2))
                .clamp(FIRST_SLAB, SLAB);
            while wanted < len && wanted < SLAB {
                wanted *= 2;
            }
            let free = self.arena.take(wanted.min(SLAB), len);
            let capacity = free.capacity();
            self.slabs.push_back(Slab {
                free,
                capacity,
                start,
                end,
            });
        }
        let Some(slab) = self.slabs.back_mut() else {
            return Bytes::new();
        };
        for part in parts {
            slab.free.extend_from_slice(part);
        }
        slab.end = end;
        slab.free.split().freeze()
    }

    /// The most bytes the window may hold after a push that found it holding
    /// `before`: `max` while the arena's budget has room, the larger of
    /// `floor` and `before` once the caches reach it, and `floor` with no
    /// budget.
    fn limit(&self, before: usize) -> usize {
        match self.arena.headroom() {
            Headroom::Unbudgeted => self.floor,
            Headroom::Room => self.max,
            Headroom::Full => self.floor.max(before),
        }
    }

    fn evict(&mut self, before: usize) {
        let limit = self.limit(before);
        while self.window.len > limit && self.window.chunks.len() > 1 {
            let Some(oldest) = self.window.chunks.front().map(Bytes::len) else {
                break;
            };
            if self.window.start + to_u64(oldest) > self.written {
                break;
            }
            self.window.chunks.pop_front();
            self.window.len -= oldest;
            self.window.start += to_u64(oldest);
        }
        self.release_slabs();
    }

    /// Give the arena back every slab, but the newest, that the window no
    /// longer reaches.
    fn release_slabs(&mut self) {
        while self.slabs.len() > 1
            && self
                .slabs
                .front()
                .is_some_and(|slab| slab.end <= self.window.start)
        {
            if let Some(slab) = self.slabs.pop_front() {
                self.arena.give_back(slab.free, slab.capacity);
            }
        }
    }

    /// Forget every byte at or past `end`, as a truncate of the segment does.
    ///
    /// When `end` is at or before the window's start, the window becomes
    /// empty and begins at `end`.
    pub(crate) fn truncate(&mut self, end: u64) {
        self.written = self.written.min(end);
        self.window.truncate(end);
        // A slab that starts at or past the cut holds only bytes the cut
        // dropped; the next push starts a slab of its own there.
        while let Some(slab) = self.slabs.pop_back_if(|slab| slab.start >= end) {
            self.arena.give_back(slab.free, slab.capacity);
        }
        for slab in &mut self.slabs {
            slab.end = slab.end.min(end);
        }
        self.written = self.written.min(self.window.end());
        self.release_slabs();
    }

    /// The bytes the cache's slabs hold, counted by the arena.
    #[cfg(test)]
    pub(crate) fn slab_bytes(&self) -> usize { self.slabs.iter().map(|slab| slab.capacity).sum() }
}

#[cfg(test)]
#[cfg(unix)]
mod tests;
