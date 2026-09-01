//! KIP-98 transactional-id expiry: the sweep that bounds `__transaction_state`.
//!
//! Kafka's `TransactionCoordinator` expires a transactional id whose state is
//! terminal or idle once `transactional.id.expiration.ms` has passed since its
//! last transition, and writes a tombstone so compaction reclaims it. Without
//! that sweep the coordinator keeps one live entry per transactional id ever
//! used: a Streams application with a per-task `transactional.id`, an
//! autoscaled EOS producer fleet or a CI run grows the in-memory map, and the
//! replay at broker start, without bound.
//!
//! The module holds the pure decision core, [`should_expire_transactional_id`],
//! and the orchestration that runs it over the tids this broker coordinates.
//! [`crate::txn::id_expiration`] is the background task that ticks it.
//!
//! **KIP-939 invariant:** the sweep never expires a prepared two-phase-commit
//! transaction. A 2PC transaction that an external transaction manager has
//! prepared sits in `PrepareCommit` or `PrepareAbort`, and
//! [`should_expire_transactional_id`] refuses every `Prepare*` state outright,
//! however long ago the prepare happened. That matches
//! [`crate::txn::two_pc::should_abort_idle_txn`], which refuses to abort the
//! same transaction on a timeout: neither reaper may take the commit-or-abort
//! decision away from the transaction manager that owns it.

use tracing::{info, warn};

use super::TxnCoordinator;
use crate::txn::state::TxnState;

/// Reports whether `state` is one the coordinator may expire at all.
///
/// The four expirable states are Kafka's: `Empty` (a tid that initialized and
/// never began a transaction, or one reset after a completed one), `Dead`, and
/// the two `Complete*` terminals. `Ongoing` is an open transaction, and each
/// `Prepare*` is a commit or abort that someone is still driving, the external
/// transaction manager of a 2PC transaction included.
///
/// The match is exhaustive on purpose: a new `TxnState` variant must state
/// which side it falls on rather than inherit a wildcard.
#[must_use]
fn state_allows_expiration(state: TxnState) -> bool {
    match state {
        TxnState::Empty | TxnState::Dead | TxnState::CompleteCommit | TxnState::CompleteAbort => {
            true
        }
        TxnState::Ongoing | TxnState::PrepareCommit | TxnState::PrepareAbort => false,
    }
}

/// THE decision: may the coordinator expire a transactional id that is in
/// `state`, last transitioned at `last_update_ms`, as of `now_ms`, under an
/// expiry of `expiration_ms`?
///
/// Returns `true` iff both of the following hold:
///  - [`state_allows_expiration`] accepts the state. This is the KIP-939
///    guarantee: a prepared 2PC transaction sits in `PrepareCommit` or
///    `PrepareAbort` and is refused here, before any arithmetic, so no clock
///    can expire it.
///  - at least `expiration_ms` has elapsed since the last transition
///    (`now_ms - last_update_ms >= expiration_ms`), which is Kafka's
///    `txnLastUpdateTimestamp <= now - transactionalIdExpirationMs`.
///
/// Pure and total: a backwards clock (`now_ms < last_update_ms`) gives a
/// negative elapsed time through a saturating subtraction and so yields
/// `false`, never a spurious expiry.
#[must_use]
pub(crate) fn should_expire_transactional_id(
    state: TxnState,
    last_update_ms: i64,
    now_ms: i64,
    expiration_ms: i64,
) -> bool {
    if !state_allows_expiration(state) {
        return false;
    }
    now_ms.saturating_sub(last_update_ms) >= expiration_ms
}

impl TxnCoordinator {
    /// KIP-98 transactional-id expiry: tombstones every locally-coordinated
    /// transactional id that [`should_expire_transactional_id`] accepts at
    /// `now_ms`, and drops it from the in-memory map. Returns the ids it
    /// expired, in iteration order.
    ///
    /// `now_ms` is the caller's clock, so a test drives the sweep at any
    /// instant without waiting. [`crate::txn::id_expiration`] passes the wall
    /// clock.
    ///
    /// The decision is taken against the live entry under that tid's own
    /// lock, not against the snapshot the scan started from, and the lock is
    /// **held across the tombstone append**. Every path that revives a known
    /// tid -- `InitProducerId` above all -- mutates the entry under the same
    /// lock, so no revival can slip between the decision and the append and
    /// leave a tombstone sitting after the reviving record in the log. A tid
    /// whose `__transaction_state` partition moved away is skipped: the broker
    /// that leads it now owns the decision. An append failure leaves the entry
    /// in place for the next tick.
    // cargo-mutants: I/O orchestration over live DashMap / partition state
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(
        name = "txn_coordinator_expire_transactional_ids",
        level = "debug",
        skip_all,
        fields(now_ms, expiration_ms)
    )]
    pub(crate) async fn expire_transactional_ids(
        &self,
        now_ms: i64,
        expiration_ms: i64,
    ) -> Vec<String> {
        // Snapshot the candidate tids first so no DashMap shard lock is held
        // across the append; each tid's own lock is re-acquired below.
        let candidates: Vec<String> = self.state.iter().map(|e| e.key().clone()).collect();
        let mut expired = Vec::new();
        for tid in candidates {
            // The same ownership check every other coordinator path makes:
            // only the broker that leads a tid's `__transaction_state`
            // partition decides its fate.
            if !self.is_coordinator_for(&tid).await {
                continue;
            }
            let Some(handle) = self.get(&tid) else {
                continue;
            };
            // The lock stays held across the append: see the method doc.
            let entry = handle.lock().await;
            if !should_expire_transactional_id(
                entry.state,
                entry.last_update_ms,
                now_ms,
                expiration_ms,
            ) {
                continue;
            }
            match self.tombstone(&entry).await {
                Ok(()) => {
                    info!(tid, "txn id expiry: tombstoned expired transactional id");
                    expired.push(tid);
                }
                Err(error) => {
                    warn!(tid, %error, "txn id expiry: tombstone append failed; will retry");
                }
            }
        }
        expired
    }
}

#[cfg(test)]
mod tests;
