//! Session allocation and the LRU eviction that makes room for it.
//!
//! `try_allocate` picks the victim when the cache is full, where a
//! non-privileged caller may displace only a non-privileged session, then
//! draws a fresh wire-legal session id and inserts the new session. It refuses
//! the allocation when no session may be displaced, and the caller then falls
//! back to a sessionless response.
//!
//! The victim comes from the recency order in `super::order`, so choosing it
//! costs the same whether the cache holds one session or all of them.

use std::{collections::HashMap, sync::atomic::Ordering};

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
            // The order index answers this in O(1); it does not scan.
            let Some(id) = guard.order.victim(privileged) else {
                return INVALID_SESSION_ID;
            };
            let evicted = guard.sessions.remove(&id).expect("victim present");
            guard.order.remove(id, evicted.privileged);
            self.num_sessions.fetch_sub(1, Ordering::Relaxed);
            self.num_partitions
                .fetch_sub(evicted.partitions.len(), Ordering::Relaxed);
            self.evictions.fetch_add(1, Ordering::Relaxed);
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
        };
        let added_partitions = session.partitions.len();
        guard.sessions.insert(id, session);
        guard.order.touch(id, privileged, self.clock.nanos());
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
    use crate::fetch_session::{
        SessionDecision,
        test_support::{TICK, mock_cache, req},
    };

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
        let (cache, mock) = mock_cache(2);
        let a = cache.try_allocate(false, "a".into(), vec![]);
        // Advance logical time so each session gets a strictly increasing
        // recency stamp, making `a` the unambiguous LRU victim — no sleep.
        mock.advance(TICK);
        let b = cache.try_allocate(false, "b".into(), vec![]);
        mock.advance(TICK);
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
        let (cache, mock) = mock_cache(1);
        let p1 = cache.try_allocate(true, "f1".into(), vec![]);
        // Advance so `f2` is strictly newer than `f1`; `f1` is the LRU victim.
        mock.advance(TICK);
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
    fn incremental_fetch_moves_a_session_off_the_victim_slot() {
        // The recency order is only useful if a live session's own traffic
        // updates it. `refetched` is allocated first, so it starts as the
        // victim; one incremental fetch on it must hand that role to `idle`.
        let (cache, mock) = mock_cache(2);
        let refetched = cache.try_allocate(false, "refetched".into(), vec![]);
        mock.advance(TICK);
        let idle = cache.try_allocate(false, "idle".into(), vec![]);

        mock.advance(TICK);
        let incremental = req(refetched, 1, vec![], vec![]);
        assert!(matches!(
            cache.classify(&incremental),
            SessionDecision::Incremental { .. }
        ));

        mock.advance(TICK);
        let newcomer = cache.try_allocate(false, "newcomer".into(), vec![]);
        check!(cache.evictions_total() == 1);
        let guard = cache.inner.lock().unwrap();
        let mut ids: Vec<i32> = guard.sessions.keys().copied().collect();
        ids.sort_unstable();
        assert!(!ids.contains(&idle) && ids == vec![refetched, newcomer]);
    }

    #[test]
    fn privileged_caller_takes_the_older_of_the_two_classes() {
        // A follower may displace either class, so the victim is simply the
        // oldest session, whichever class it belongs to. Run it both ways
        // round so neither answer can come from a standing class preference.
        let cases = [("follower is older", true), ("consumer is older", false)];
        for (label, follower_first) in cases {
            let (cache, mock) = mock_cache(2);
            let first = cache.try_allocate(follower_first, "first".into(), vec![]);
            mock.advance(TICK);
            let second = cache.try_allocate(!follower_first, "second".into(), vec![]);
            mock.advance(TICK);
            let third = cache.try_allocate(true, "follower".into(), vec![]);

            check!(cache.evictions_total() == 1, "{label}");
            let g = cache.inner.lock().unwrap();
            let mut ids: Vec<i32> = g.sessions.keys().copied().collect();
            ids.sort_unstable();
            check!(!ids.contains(&first), "{label}");
            check!(ids == vec![second, third], "{label}");
        }
    }

    #[test]
    fn a_closed_session_is_never_chosen_as_a_victim() {
        // Close has to drop the session from the recency order as well as from
        // the map. If it did not, the order would still name the closed
        // session as the oldest and the next allocation into a full cache
        // would go looking for a session that is no longer there.
        let (cache, mock) = mock_cache(2);
        let closed = cache.try_allocate(false, "closed".into(), vec![]);
        mock.advance(TICK);
        let oldest_live = cache.try_allocate(false, "oldest-live".into(), vec![]);
        cache.close(closed);

        mock.advance(TICK);
        let refill = cache.try_allocate(false, "refill".into(), vec![]);
        mock.advance(TICK);
        let newcomer = cache.try_allocate(false, "newcomer".into(), vec![]);

        // The cache refilled to {oldest_live, refill}; `newcomer` displaced
        // `oldest_live`, the oldest session that is still there.
        check!(cache.len() == 2);
        let guard = cache.inner.lock().unwrap();
        let mut ids: Vec<i32> = guard.sessions.keys().copied().collect();
        ids.sort_unstable();
        assert!(!ids.contains(&oldest_live) && ids == vec![refill, newcomer]);
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
