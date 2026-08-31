//! The recency index that makes eviction O(1).
//!
//! The cache used to find its victim with a `min_by_key` over the whole
//! session map. On a full broker that is a 1000-entry scan run with the global
//! fetch mutex held, so every concurrent `classify` waits behind it. This index
//! keeps the same ordering as an explicit list instead, and the victim is
//! whichever list head is older.
//!
//! ## Why two lists
//!
//! KIP-227 privilege is not a tie-breaker inside one order, it is a filter on
//! who may be displaced. A non-privileged caller may evict only a
//! non-privileged session; a privileged one (a follower fetch, `replica_id >=
//! 0`) may evict either. A single list would therefore have to be walked past
//! the privileged entries at its head to answer a non-privileged caller, which
//! is the O(n) scan again in the case that matters — a cache full of follower
//! sessions. Splitting the order by privilege gives each caller class a head it
//! can read directly.
//!
//! Each list is an [`LruCache`] used purely for its ordering: unbounded, so it
//! never evicts on its own, keyed by session id and valued by the session's
//! `last_used_nanos`. Those stamps are what let a privileged caller compare the
//! two heads. They come from the cache's injected
//! [`NanoClock`](qubit_clock::NanoClock), so a test can drive the order with a
//! mock timeline instead of a sleep.

use lru::LruCache;

use super::epoch::FetchSessionId;

/// LRU order over the live sessions, split into the two privilege classes.
///
/// The union of the two lists is exactly the key set of `Inner::sessions`.
/// Every insert, touch, evict and close updates both under the same lock, so
/// the two never drift.
///
/// Within one list, position *is* the recency order; the stamps are along for
/// the ride so that [`SessionOrder::victim`] can compare the two lists' heads
/// against each other. That is sound because the stamps come from a monotonic
/// clock read at touch time, so a later touch never carries an earlier stamp
/// and the two orderings agree.
pub(super) struct SessionOrder {
    /// Consumer sessions, least recently used first.
    ordinary: LruCache<FetchSessionId, i128>,
    /// Follower sessions (`replica_id >= 0`), least recently used first. Only
    /// a privileged caller may take a victim from here.
    privileged: LruCache<FetchSessionId, i128>,
}

impl SessionOrder {
    pub(super) fn new() -> Self {
        Self {
            ordinary: LruCache::unbounded(),
            privileged: LruCache::unbounded(),
        }
    }

    /// Records `id` as the most recently used session of its class, stamped
    /// `nanos`. Inserts it when it is not yet present, which is what an
    /// allocation does.
    ///
    /// O(1): [`LruCache::put`] moves an existing key to the most-recent end
    /// rather than re-sorting.
    pub(super) fn touch(&mut self, id: FetchSessionId, privileged: bool, nanos: i128) {
        self.class_mut(privileged).put(id, nanos);
    }

    /// Drops `id` from its class's order. Called for both an eviction and a
    /// client-requested close.
    pub(super) fn remove(&mut self, id: FetchSessionId, privileged: bool) {
        self.class_mut(privileged).pop(&id);
    }

    /// The session that a caller of this privilege may displace, or `None`
    /// when it may displace nothing and the allocation has to be refused.
    ///
    /// O(1): each candidate is a list head, read by [`LruCache::peek_lru`],
    /// which does not itself reorder.
    ///
    /// A privileged caller compares the two heads on `last_used_nanos` and
    /// takes the older. On equal stamps it takes the ordinary session. The
    /// `min_by_key` scan this replaced broke such a tie by `HashMap` iteration
    /// order, so there is no prior choice to reproduce; preferring the
    /// ordinary session makes the tie deterministic and keeps a follower
    /// session — the more expensive one to lose — for one more round.
    pub(super) fn victim(&self, privileged_caller: bool) -> Option<FetchSessionId> {
        let ordinary = self.ordinary.peek_lru();
        if !privileged_caller {
            return ordinary.map(|(id, _)| *id);
        }
        let privileged = self.privileged.peek_lru();
        match (ordinary, privileged) {
            (Some((ordinary_id, ordinary_nanos)), Some((privileged_id, privileged_nanos))) => {
                if privileged_nanos < ordinary_nanos {
                    Some(*privileged_id)
                } else {
                    Some(*ordinary_id)
                }
            }
            (Some((id, _)), None) | (None, Some((id, _))) => Some(*id),
            (None, None) => None,
        }
    }

    fn class_mut(&mut self, privileged: bool) -> &mut LruCache<FetchSessionId, i128> {
        if privileged {
            &mut self.privileged
        } else {
            &mut self.ordinary
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    /// One `victim` scenario: a label, the `(id, privileged, nanos)` triples
    /// to seed in touch order, the calling session's privilege, and the
    /// session the cache should offer up.
    type VictimCase = (
        &'static str,
        &'static [(FetchSessionId, bool, i128)],
        bool,
        Option<FetchSessionId>,
    );

    /// `(id, privileged, nanos)` triples to `touch`, in touch order.
    ///
    /// The stamps must not go backwards, because the production clock is
    /// monotonic and `victim` compares the two lists' heads on the assumption
    /// that list position and stamp order agree. Seeding a state that could
    /// not arise would test something the cache cannot do.
    fn seeded(entries: &[(FetchSessionId, bool, i128)]) -> SessionOrder {
        let mut order = SessionOrder::new();
        let mut previous = i128::MIN;
        for &(id, privileged, nanos) in entries {
            assert!(nanos >= previous, "stamps must not go backwards");
            previous = nanos;
            order.touch(id, privileged, nanos);
        }
        order
    }

    #[test]
    fn victim_selection_respects_privilege_and_recency() {
        let cases: [VictimCase; 8] = [
            ("empty order has no victim", &[], false, None),
            ("empty order has no victim for follower", &[], true, None),
            (
                "consumer takes the oldest consumer session",
                &[(3, false, 5), (1, false, 10), (2, false, 20)],
                false,
                Some(3),
            ),
            (
                "consumer may not take a follower session",
                &[(1, true, 1), (2, true, 2)],
                false,
                None,
            ),
            (
                "consumer skips an older follower session",
                &[(1, true, 1), (2, false, 99)],
                false,
                Some(2),
            ),
            (
                "follower takes the older of the two heads",
                &[(1, true, 1), (2, false, 99)],
                true,
                Some(1),
            ),
            (
                "follower takes a consumer session when it is older",
                &[(2, false, 1), (1, true, 99)],
                true,
                Some(2),
            ),
            (
                "a tie goes to the consumer session",
                &[(1, true, 7), (2, false, 7)],
                true,
                Some(2),
            ),
        ];
        for (label, entries, privileged_caller, want) in cases {
            let order = seeded(entries);
            check!(order.victim(privileged_caller) == want, "{label}");
        }
    }

    #[test]
    fn touching_an_existing_session_moves_it_off_the_head() {
        let mut order = seeded(&[(1, false, 10), (2, false, 20)]);
        assert!(order.victim(false) == Some(1));
        order.touch(1, false, 30);
        assert!(order.victim(false) == Some(2));
    }

    #[test]
    fn removing_the_head_promotes_the_next_session() {
        let mut order = seeded(&[(1, false, 10), (2, false, 20)]);
        order.remove(1, false);
        check!(order.victim(false) == Some(2));
        order.remove(2, false);
        check!(order.victim(false) == None);
    }

    #[test]
    fn remove_ignores_a_session_of_the_other_class() {
        let mut order = seeded(&[(1, false, 10)]);
        // The caller passes the session's own privilege; a mismatched call
        // must not silently drop the entry from the other list.
        order.remove(1, true);
        assert!(order.victim(false) == Some(1));
    }
}
