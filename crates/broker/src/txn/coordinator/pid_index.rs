//! The `producer_id` → `transactional_id` reverse index.
//!
//! The Produce handler reads the index to verify a transactional batch
//! (KIP-1319 v2), so a superseded producer id must not survive in it: a fenced
//! id that outlived its transaction would bypass coordinator validation. The
//! eviction that keeps only the live identities lives here, with the tests that
//! pin it.

use dashmap::DashMap;
use krabka_log::ProducerId;

use super::TxnCoordinator;
use crate::txn::state::TxnEntry;

impl TxnCoordinator {
    /// Keep only the current transaction and staged recovery producer IDs for
    /// this transactional ID. Repeated KIP-939 recovery calls can rotate the
    /// staged ID before the transaction completes; retaining the superseded
    /// mapping would let that fenced ID bypass coordinator validation.
    pub(super) fn evict_superseded_pids(
        pid_to_tid: &DashMap<ProducerId, String>,
        entry: &TxnEntry,
    ) {
        pid_to_tid.retain(|pid, tid| {
            tid != &entry.transactional_id
                || *pid == entry.producer_id
                || *pid == entry.next_producer_id
        });
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::txn::coordinator::test_support::entry;

    #[test]
    fn evict_rolled_pid_drops_only_the_prior_id_on_a_roll() {
        let map: DashMap<ProducerId, String> = DashMap::new();
        map.insert(ProducerId(1000), "tid-a".into()); // the pre-roll mapping

        // A roll: new pid 2000, prev = 1000. The stale 1000 mapping is evicted;
        // put then inserts 2000 (mirrored here).
        TxnCoordinator::evict_superseded_pids(&map, &entry(2000, 1000));
        map.insert(ProducerId(2000), "tid-a".into());

        assert!(
            map.get(&ProducerId(1000)).is_none(),
            "stale pre-roll pid must be evicted"
        );
        check!(map.get(&ProducerId(2000)).map(|e| e.value().clone()) == Some("tid-a".into()));
    }

    #[test]
    fn evict_rolled_pid_is_noop_without_a_roll() {
        let map: DashMap<ProducerId, String> = DashMap::new();
        map.insert(ProducerId(1000), "tid-a".into());
        // Never rolled: prev == -1 → nothing evicted.
        TxnCoordinator::evict_superseded_pids(&map, &entry(1000, -1));
        assert!(map.get(&ProducerId(1000)).is_some());
        // prev == current (defensive): nothing evicted.
        TxnCoordinator::evict_superseded_pids(&map, &entry(1000, 1000));
        assert!(map.get(&ProducerId(1000)).is_some());
    }

    #[test]
    fn evict_rolled_pid_is_idempotent_after_the_id_is_gone() {
        let map: DashMap<ProducerId, String> = DashMap::new();
        map.insert(ProducerId(2000), "tid-a".into());
        // prev=1000 already absent → repeated evictions are harmless no-ops.
        TxnCoordinator::evict_superseded_pids(&map, &entry(2000, 1000));
        TxnCoordinator::evict_superseded_pids(&map, &entry(2000, 1000));
        assert!(map.get(&ProducerId(1000)).is_none());
        assert!(map.get(&ProducerId(2000)).is_some());
    }

    #[test]
    fn evict_superseded_pids_removes_a_rotated_recovery_identity() {
        let map: DashMap<ProducerId, String> = DashMap::new();
        map.insert(ProducerId(1000), "tid-a".into());
        map.insert(ProducerId(2000), "tid-a".into());

        let mut current = entry(1000, -1);
        current.next_producer_id = ProducerId(3000);
        current.next_producer_epoch = 0;
        TxnCoordinator::evict_superseded_pids(&map, &current);

        assert!(map.get(&ProducerId(1000)).is_some());
        assert!(map.get(&ProducerId(2000)).is_none());
    }
}
