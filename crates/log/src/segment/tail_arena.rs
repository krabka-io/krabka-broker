//! The memory the direct-write tail caches keep their bytes in.
//!
//! A tail cache copies each batch it keeps into a slab: a buffer of up to
//! [`SLAB`] bytes that it fills front to back and hands out shared slices
//! of. Eviction gives a slab back once every batch in it has left the
//! window, and the arena reuses it once no fetch response still holds a
//! slice of it. Slabs have one size, so a cache that churns through
//! gigabytes reuses the same few buffers. Before slabs each kept batch was
//! an allocation of its own size, and under the image's malloc tunables
//! (`trim_threshold=128MiB`, `mmap_threshold=64MiB`) the freed ones stayed
//! in whichever thread's arena allocated them: a single-partition consumer
//! at 1.17M records/s with a 1 GiB cache held 2.25 GB of heap with 766 MB
//! in use, and the broker peaked at 5.6–6.1 GiB resident.
//!
//! The arena also counts the slab bytes every cache holds, against an
//! optional broker-wide budget. A cache grows its window past its floor only
//! while the caches together are under the budget; see `TailCache::push`.

use {
    bytes::BytesMut,
    std::sync::{
        Arc, Mutex, OnceLock, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
};

/// The size of a full slab. A batch larger than this gets a slab of its
/// own size, which is not reused.
pub const SLAB: usize = 4 * 1024 * 1024;

/// The smallest slab a cache starts with. A cache's slabs double from here
/// to [`SLAB`], so a partition that takes little traffic holds little.
pub const FIRST_SLAB: usize = 64 * 1024;

/// How many full slabs the arena always keeps for reuse. Under a budget it
/// keeps more, as many as the budget has room for beside what the caches
/// hold: a segment's cache is dropped when the segment rolls, every second or
/// so for a busy partition, and the next segment's cache takes its slabs.
/// The floor covers a cache at the budget, which takes a slab for a push
/// before it evicts and gives one back, so the budget has no room just when
/// the slab comes back.
const POOL_SLABS: usize = 16;

/// A `usize` widened to a `u64`; no supported target has a wider `usize`.
fn to_u64(n: usize) -> u64 { n as u64 }

/// Where the caches stand against the arena's budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Headroom {
    /// No budget: each cache holds its floor, as a fixed window did.
    Unbudgeted,
    /// Under the budget: a cache may grow to its maximum.
    Room,
    /// At or over the budget: a cache keeps the size it has.
    Full,
}

/// The slab store shared by every tail cache in the process, or by the
/// caches of one test.
#[derive(Debug)]
pub struct TailArena {
    /// The slab bytes the caches may hold before a cache stops growing past
    /// its floor; `u64::MAX` for no budget.
    budget: AtomicU64,
    /// The slab bytes the caches hold now.
    held: AtomicU64,
    /// Full slabs given back, for reuse once nothing else holds them.
    pool: Mutex<Vec<BytesMut>>,
    /// Full slabs made new rather than reused.
    #[cfg(test)]
    fresh: AtomicU64,
}

impl Default for TailArena {
    fn default() -> Self { Self::new(None) }
}

impl TailArena {
    /// An arena with `budget` bytes for its caches, or none.
    #[must_use]
    pub fn new(budget: Option<u64>) -> Self {
        Self {
            budget: AtomicU64::new(budget.unwrap_or(u64::MAX)),
            held: AtomicU64::new(0),
            pool: Mutex::new(Vec::new()),
            #[cfg(test)]
            fresh: AtomicU64::new(0),
        }
    }

    /// The arena of every tail cache in this process.
    #[must_use]
    pub fn global() -> &'static Arc<Self> {
        static GLOBAL: OnceLock<Arc<TailArena>> = OnceLock::new();
        GLOBAL.get_or_init(|| Arc::new(Self::default()))
    }

    /// Set the bytes the caches may hold together before a cache stops
    /// growing past its floor, or remove the budget.
    pub fn set_budget(&self, budget: Option<u64>) {
        self.budget
            .store(budget.unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    /// The slab bytes the caches hold now.
    #[must_use]
    pub fn held(&self) -> u64 { self.held.load(Ordering::Relaxed) }

    /// Whether a cache may grow past its floor: see [`Headroom`].
    pub(crate) fn headroom(&self) -> Headroom {
        let budget = self.budget.load(Ordering::Relaxed);
        if budget == u64::MAX {
            Headroom::Unbudgeted
        } else if self.held.load(Ordering::Relaxed) < budget {
            Headroom::Room
        } else {
            Headroom::Full
        }
    }

    /// An empty slab with room for at least `len` bytes, of `wanted` bytes
    /// when that is enough. A full-size slab comes from the pool when one
    /// there is free.
    pub(crate) fn take(&self, wanted: usize, len: usize) -> BytesMut {
        let capacity = wanted.max(len);
        let slab = if capacity == SLAB {
            self.reuse().unwrap_or_else(|| {
                #[cfg(test)]
                self.fresh.fetch_add(1, Ordering::Relaxed);
                BytesMut::with_capacity(SLAB)
            })
        } else {
            BytesMut::with_capacity(capacity)
        };
        self.held
            .fetch_add(to_u64(slab.capacity()), Ordering::Relaxed);
        slab
    }

    /// Give back a slab whose bytes no cache window holds any more.
    /// `capacity` is what [`Self::take`] counted for it. A full-size slab
    /// waits in the pool until the last fetch response that holds a slice of
    /// it is gone.
    pub(crate) fn give_back(&self, slab: BytesMut, capacity: usize) {
        self.held.fetch_sub(to_u64(capacity), Ordering::Relaxed);
        if capacity != SLAB {
            return;
        }
        let budget = self.budget.load(Ordering::Relaxed);
        let mut pool = self.pool.lock().unwrap_or_else(PoisonError::into_inner);
        let room = if budget == u64::MAX {
            POOL_SLABS
        } else {
            usize::try_from(budget.saturating_sub(self.held()) / to_u64(SLAB))
                .unwrap_or(usize::MAX)
                .max(POOL_SLABS)
        };
        if pool.len() < room {
            pool.push(slab);
        }
    }

    /// A pooled slab that nothing else holds a slice of, emptied.
    fn reuse(&self) -> Option<BytesMut> {
        let mut pool = self.pool.lock().unwrap_or_else(PoisonError::into_inner);
        let free = pool.iter_mut().position(|slab| {
            slab.clear();
            slab.try_reclaim(SLAB)
        })?;
        Some(pool.swap_remove(free))
    }

    /// How many full slabs were made new rather than reused.
    #[cfg(test)]
    pub(crate) fn fresh(&self) -> u64 { self.fresh.load(Ordering::Relaxed) }

    /// How many slabs wait in the pool.
    #[cfg(test)]
    pub(crate) fn pooled(&self) -> usize {
        self.pool
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_slab_is_reused_once_no_slice_of_it_is_held() {
        let arena = TailArena::new(None);
        let mut slab = arena.take(SLAB, 10);
        assert2::assert!(arena.held() == to_u64(SLAB));
        slab.extend_from_slice(b"0123456789");
        let slice = slab.split().freeze();
        arena.give_back(slab, SLAB);
        assert2::assert!(arena.held() == 0);
        assert2::assert!(arena.pooled() == 1);
        // A response still holds a slice, so the slab is not free yet.
        let fresh = arena.take(SLAB, 10);
        assert2::assert!(arena.pooled() == 1);
        arena.give_back(fresh, SLAB);
        drop(slice);
        let reused = arena.take(SLAB, 10);
        assert2::assert!(reused.capacity() >= SLAB && reused.is_empty());
        assert2::assert!(arena.pooled() == 1, "the fresh slab is still pooled");
    }

    #[test]
    fn a_slab_of_another_size_is_freed_not_pooled() {
        let arena = TailArena::new(None);
        let big = arena.take(SLAB, SLAB + 1);
        assert2::assert!(big.capacity() > SLAB);
        let capacity = big.capacity();
        arena.give_back(big, capacity);
        let small = arena.take(FIRST_SLAB, 1);
        let capacity = small.capacity();
        arena.give_back(small, capacity);
        assert2::assert!(arena.pooled() == 0 && arena.held() == 0);
    }

    #[test]
    fn the_budget_bounds_what_the_caches_hold() {
        let arena = TailArena::new(Some(to_u64(SLAB)));
        assert2::assert!(arena.headroom() == Headroom::Room);
        let first = arena.take(SLAB, 1);
        assert2::assert!(arena.headroom() == Headroom::Full);
        let second = arena.take(FIRST_SLAB, 1);
        assert2::assert!(arena.headroom() == Headroom::Full);
        arena.set_budget(None);
        assert2::assert!(arena.headroom() == Headroom::Unbudgeted);
        let capacity = second.capacity();
        arena.give_back(second, capacity);
        arena.give_back(first, SLAB);
    }
}
