//! Session allocation and the LRU eviction that makes room for it.
//!
//! `try_allocate` picks the victim when the cache is full, where a
//! non-privileged caller may displace only a non-privileged session, then
//! draws a fresh wire-legal session id and inserts the new session. It refuses
//! the allocation when no session may be displaced, and the caller then falls
//! back to a sessionless response.

use std::{collections::HashMap, sync::atomic::Ordering};

use qubit_clock::MonotonicClock as _;

use super::{
    cache::{FetchSession, FetchSessionCache},
    epoch::{
        FIRST_SESSION_ID, FetchSessionId, INITIAL_EPOCH, INVALID_SESSION_ID, next_epoch,
        session_id_is_reserved,
    },
    state::{CachedPartitionState, FetchSessionKey},
};

impl FetchSessionCache {
    /// Allocates a fresh session for a `NewSession` decision. `partitions`
    /// must carry both the desired state (`fetch_offset`, `max_bytes`, and the
    /// rest) and the response-side `last_*` values for what the broker just
    /// sent. The next incremental fetch compares the new response state to
    /// those values.
    ///
    /// Returns the assigned id, or `INVALID_SESSION_ID` (0) when the cache is
    /// full and it can evict no eligible victim. On a refused allocation the
    /// caller emits `response.session_id = 0`, and the client falls back to
    /// sessionless full fetches without further signalling.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn try_allocate(
        &self,
        privileged: bool,
        creator_principal: String,
        partitions: Vec<(FetchSessionKey, CachedPartitionState)>,
    ) -> FetchSessionId {
        if self.max_slots == 0 {
            return INVALID_SESSION_ID;
        }
        let mut guard = self.inner.lock().expect("poisoned");

        if guard.sessions.len() >= self.max_slots {
            // Pick a victim: LRU non-privileged session if one exists,
            // otherwise (only when the caller is itself privileged) the
            // LRU session of any kind. Non-privileged callers cannot
            // evict privileged sessions — they fall back to sessionless.
            let victim: Option<FetchSessionId> = guard
                .sessions
                .iter()
                .filter(|(_, s)| if privileged { true } else { !s.privileged })
                .min_by_key(|(_, s)| s.last_used)
                .map(|(id, _)| *id);
            match victim {
                Some(id) => {
                    let evicted = guard.sessions.remove(&id).expect("victim present");
                    self.num_sessions.fetch_sub(1, Ordering::Relaxed);
                    self.num_partitions
                        .fetch_sub(evicted.partitions.len(), Ordering::Relaxed);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                }
                None => return INVALID_SESSION_ID,
            }
        }

        // Allocate a fresh id. AtomicI32::fetch_add wraps, so we skip
        // 0 (sentinel) and any negative (would round-trip on the wire
        // as a "negative session id" the client rejects) and any
        // id that's already taken (extremely rare — happens only after
        // 2^31 allocations of overlap). The loop is bounded by the number of
        // live ids it could collide with plus the reserved wrap value and reset.
        let mut id = None;
        for _ in 0..guard.sessions.len().saturating_add(3) {
            let candidate = self.next_id.fetch_add(1, Ordering::Relaxed);
            if session_id_is_reserved(candidate) {
                // Wrapped past i32::MAX or hit zero. Reset to the first
                // allocatable id and try again; the next iteration will
                // fetch_add to 2 and store 3.
                self.next_id.store(FIRST_SESSION_ID, Ordering::Relaxed);
                continue;
            }
            if !guard.sessions.contains_key(&candidate) {
                id = Some(candidate);
                break;
            }
        }
        let Some(id) = id else {
            return INVALID_SESSION_ID;
        };

        let partitions: HashMap<FetchSessionKey, CachedPartitionState> =
            partitions.into_iter().collect();
        let session = FetchSession {
            id,
            // Client's first incremental request after a new-session
            // allocation must carry the epoch after INITIAL (i.e. 1).
            next_epoch: next_epoch(INITIAL_EPOCH),
            privileged,
            creator_principal,
            partitions,
            last_used: self.clock.now().elapsed_since_origin(),
        };
        let added_partitions = session.partitions.len();
        guard.sessions.insert(id, session);
        self.num_sessions.fetch_add(1, Ordering::Relaxed);
        self.num_partitions
            .fetch_add(added_partitions, Ordering::Relaxed);
        id
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_protocol::primitives::uuid::Uuid as WireUuid;

    use super::*;
    use crate::fetch_session::test_support::{TICK, manual_cache};

    #[test]
    fn allocate_returns_nonzero_monotonic_ids() {
        let cache = FetchSessionCache::new(10);
        let a = cache.try_allocate(false, "alice".into(), vec![]);
        let b = cache.try_allocate(false, "alice".into(), vec![]);
        // Id allocation starts at 1 and increments monotonically.
        check!(a == 1);
        check!(b == 2);
        check!(cache.len() == 2);
    }

    #[test]
    fn allocate_skips_zero_on_wrap() {
        let cache = FetchSessionCache::new(10);
        // Force the next id to be 0 — the loop should skip and start from 1.
        cache.next_id.store(0, Ordering::Relaxed);
        let id = cache.try_allocate(false, "alice".into(), vec![]);
        assert!(id > 0);
    }

    #[test]
    fn allocate_skips_existing_session_id_collision() {
        let cache = FetchSessionCache::new(10);
        let first = cache.try_allocate(false, "alice".into(), vec![]);

        cache.next_id.store(first, Ordering::Relaxed);
        let second = cache.try_allocate(false, "bob".into(), vec![]);

        assert!(second == first + 1);
        assert!(cache.len() == 2);
    }

    #[test]
    fn allocate_returns_zero_when_max_slots_zero() {
        let cache = FetchSessionCache::new(0);
        let id = cache.try_allocate(false, "alice".into(), vec![]);
        assert!(id == INVALID_SESSION_ID);
    }

    #[test]
    fn lru_eviction_drops_oldest_non_privileged() {
        let (cache, clock) = manual_cache(2);
        let a = cache.try_allocate(false, "a".into(), vec![]);
        // Advance logical time so each session gets a strictly increasing
        // `last_used`, making `a` the unambiguous LRU victim — no sleep.
        clock.advance(TICK).expect("manual time moves forward");
        let b = cache.try_allocate(false, "b".into(), vec![]);
        clock.advance(TICK).expect("manual time moves forward");
        let c = cache.try_allocate(false, "c".into(), vec![]);
        assert!(cache.len() == 2);
        assert!(cache.evictions_total() == 1);
        // `a` (oldest) was evicted; `b` and `c` remain.
        let g = cache.inner.lock().unwrap();
        let mut ids: Vec<i32> = g.sessions.keys().copied().collect();
        ids.sort_unstable();
        assert!(!ids.contains(&a) && ids == vec![b, c]);
    }

    #[test]
    fn non_privileged_cannot_evict_privileged() {
        let cache = FetchSessionCache::new(1);
        let p = cache.try_allocate(true, "follower".into(), vec![]);
        assert!(p > 0);
        // Cache full, only session is privileged. Consumer alloc refused.
        let c = cache.try_allocate(false, "consumer".into(), vec![]);
        check!(c == INVALID_SESSION_ID);
        check!(cache.evictions_total() == 0);
        check!(cache.len() == 1);
    }

    #[test]
    fn privileged_can_evict_privileged() {
        let (cache, clock) = manual_cache(1);
        let p1 = cache.try_allocate(true, "f1".into(), vec![]);
        // Advance so `f2` is strictly newer than `f1`; `f1` is the LRU victim.
        clock.advance(TICK).expect("manual time moves forward");
        let p2 = cache.try_allocate(true, "f2".into(), vec![]);
        // p2 gets the next monotonic id (p1 + 1) after evicting p1.
        check!(p2 == p1 + 1);
        check!(cache.len() == 1);
        check!(cache.evictions_total() == 1);
        let g = cache.inner.lock().unwrap();
        assert!(!g.sessions.contains_key(&p1));
        assert!(g.sessions.contains_key(&p2));
    }

    #[test]
    fn counters_track_eviction() {
        let cache = FetchSessionCache::new(1);
        let mk = |p| {
            (
                FetchSessionKey {
                    topic_name: "t".into(),
                    topic_id: WireUuid::ZERO,
                    partition: p,
                },
                CachedPartitionState::default(),
            )
        };
        cache.try_allocate(false, "a".into(), vec![mk(0), mk(1)]);
        assert!(cache.total_partitions_cached() == 2);
        // Allocating into the full cache evicts the lone session (2 parts)
        // and inserts a fresh one (1 part).
        cache.try_allocate(false, "b".into(), vec![mk(0)]);
        assert!(cache.len() == 1);
        assert!(cache.total_partitions_cached() == 1);
    }
}
