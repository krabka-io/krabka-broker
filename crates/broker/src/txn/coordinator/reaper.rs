//! KIP-939 idle-transaction reaper: the timeout-driven abort path.
//!
//! The module holds the [`ReaperBackend`] seam, the pure transition and guard
//! helpers that decide each step of an idle abort, the orchestration loop that
//! runs the three phases over that seam, and the live adapter that runs those
//! phases against the in-memory state map, the `__transaction_state` log, the
//! partition leaders, and the producer-id allocator.

use async_trait::async_trait;
use krabka_log::ProducerId;
use tracing::{info, warn};

use super::TxnCoordinator;
use crate::txn::{
    handlers::end_txn::{completion_producer_identity, prepare_completion_identities},
    marker::MarkerType,
    state::{TxnEntry, TxnState},
    two_pc::should_abort_idle_txn,
    version::TxnVersion,
};

#[cfg(test)]
mod tests;

/// Live-dependency seam for the KIP-939 idle-transaction reaper.
///
/// `sweep_expired` orchestrates a three-phase abort for each tid. That
/// orchestration is pure decision logic around four irreducible side effects:
/// a coordinator-ownership check, two compare-and-swap-style persisted
/// transitions (`Ongoing → PrepareAbort`, then
/// `PrepareAbort → CompleteAbort`), the abort-marker fan-out, and
/// producer-identity allocation. Each one touches a live
/// `__transaction_state` partition, partition leaders, or the producer-id
/// allocator.
///
/// This trait puts those effects behind a seam, so a unit test can drive the
/// orchestration against a [`mockall`] mock. Every method returns
/// already-extracted plain data, that is, snapshots, so a mock can kill the
/// decisions that read them. The live adapter is [`TxnCoordinator`] itself.
///
/// The two `*_transition` methods mutate the entry **atomically under the
/// per-tid lock**, so a concurrent `EndTxn` or `InitProducerId` is not
/// overwritten. They return the resulting persisted snapshot, or `None` when
/// the guard failed because the caller lost a race or the entry is no longer
/// present. The pure helpers [`apply_prepare_abort`], [`apply_complete_abort`],
/// and [`complete_abort_guard_ok`] compute the transitions. The backend owns
/// only the compare-and-swap and the persistence, which is the irreducible
/// part.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub(crate) trait ReaperBackend: Send + Sync {
    /// Is this broker the transaction coordinator for `tid` right now?
    async fn is_coordinator_for(&self, tid: &str) -> bool;

    /// Moves `Ongoing → PrepareAbort` under `tid`'s entry lock, atomically.
    ///
    /// If the entry should abort as an idle transaction at `now_ms`, this
    /// method makes the transition, persists it, and returns the persisted
    /// snapshot. It returns `None` when the entry is absent, when the entry
    /// must not be reaped, or when the persistence failed.
    async fn prepare_abort(&self, tid: &str, now_ms: i64, txnv: TxnVersion) -> Option<TxnEntry>;

    /// Fan out abort markers for `entry`. Returns `false` if any marker could
    /// not be written, leaving the transaction in `PrepareAbort` for retry.
    async fn dispatch_abort_markers(&self, entry: &TxnEntry) -> bool;

    /// Moves `PrepareAbort → CompleteAbort` under `tid`'s entry lock,
    /// atomically.
    ///
    /// The method first checks that the current entry still matches the
    /// `prepared` snapshot this reaper wrote: the same identity, and still
    /// `PrepareAbort`. If it matches, the method moves the entry to
    /// `CompleteAbort`, bumps the producer identity at `now_ms` as KIP-890
    /// requires, persists the entry, and returns the persisted snapshot. It
    /// returns `None` when another caller advanced the entry or when the
    /// persistence failed.
    async fn complete_abort(
        &self,
        prepared: &TxnEntry,
        now_ms: i64,
        txnv: TxnVersion,
    ) -> Option<TxnEntry>;
}

/// Applies the `Ongoing → PrepareAbort` mutation for an idle-reaped entry. It
/// changes the state and stamps `last_update_ms`. The function is pure, so a
/// unit test can kill the transition without any persistence.
fn apply_prepare_abort(entry: &mut TxnEntry, now_ms: i64) {
    entry.state = TxnState::PrepareAbort;
    entry.last_update_ms = now_ms;
}

/// Applies the `PrepareAbort → CompleteAbort` mutation from the newly
/// allocated `(producer_id, producer_epoch)` of the KIP-890 identity bump. It
/// records the prior id as `prev_producer_id` only when a roll happened, that
/// is, when the allocator gave out a fresh pid. The function is pure, so a
/// unit test can kill the transition.
fn apply_complete_abort(entry: &mut TxnEntry, new_pid: ProducerId, new_epoch: i16, now_ms: i64) {
    if new_pid != entry.producer_id {
        entry.prev_producer_id = entry.producer_id;
    }
    entry.state = TxnState::CompleteAbort;
    entry.producer_id = new_pid;
    entry.producer_epoch = new_epoch;
    entry.next_producer_id = ProducerId(-1);
    entry.next_producer_epoch = -1;
    entry.partitions.clear();
    entry.last_update_ms = now_ms;
}

/// Reports whether the re-acquired `entry` still matches the `prepared`
/// snapshot this reaper wrote in phase 1, so that it is safe to finalise to
/// `CompleteAbort`. The guard protects against a concurrent `EndTxn` or
/// `InitProducerId` that advanced the entry. The function is pure, so a unit
/// test can kill the guard.
fn complete_abort_guard_ok(entry: &TxnEntry, prepared: &TxnEntry) -> bool {
    entry.producer_id == prepared.producer_id
        && entry.producer_epoch == prepared.producer_epoch
        && entry.state == TxnState::PrepareAbort
}

/// Runs the reaper orchestration loop.
///
/// The loop is generic over the [`ReaperBackend`] seam, so a unit test can
/// drive it against a mock. For each candidate tid it runs the three-phase
/// abort: ownership check, `prepare_abort` (compare-and-swap), marker fan-out,
/// then `complete_abort` (compare-and-swap). Returns the tids it finalised, in
/// iteration order.
async fn sweep_with_backend<B: ReaperBackend + ?Sized>(
    backend: &B,
    candidates: Vec<String>,
    now_ms: i64,
    txnv: TxnVersion,
) -> Vec<String> {
    let mut aborted = Vec::new();
    for tid in candidates {
        // Only reap transactions this broker currently coordinates: a
        // partition we used to lead may have moved, leaving stale state.
        if !backend.is_coordinator_for(&tid).await {
            continue;
        }

        // Phase 1: decide + Ongoing → PrepareAbort, persisted under the lock.
        let Some(prepared) = backend.prepare_abort(&tid, now_ms, txnv).await else {
            continue;
        };

        // Phase 2: fan out abort markers to local partition leaders.
        if !backend.dispatch_abort_markers(&prepared).await {
            continue;
        }

        // Phase 3: PrepareAbort → CompleteAbort, re-validating identity + state
        // under the lock so a concurrent EndTxn / InitProducerId is not
        // clobbered.
        if backend
            .complete_abort(&prepared, now_ms, txnv)
            .await
            .is_some()
        {
            info!(tid, "txn reaper: aborted timed-out transaction");
            aborted.push(tid);
        }
    }
    aborted
}

impl TxnCoordinator {
    /// KIP-939 idle-transaction reaper: aborts every locally-coordinated,
    /// non-2PC, `Ongoing` transaction whose timeout has elapsed at `now_ms`.
    ///
    /// The reaper skips 2PC transactions, where
    /// `txn_timeout_ms == NO_TIMEOUT_MS`. Their external transaction manager
    /// owns the commit or abort decision, and Kafka must never abort a
    /// prepared 2PC transaction on its own. [`should_abort_idle_txn`] makes
    /// the decision; it is the exhaustively model-checked core. See
    /// [`crate::txn::two_pc_model`].
    ///
    /// Each abort runs the same two-step transition + marker fan-out as an
    /// `EndTxn(committed=false)` and bumps the producer epoch on completion (at
    /// `TV >= 2`) so the timed-out producer is fenced. A marker failure leaves the
    /// entry in `PrepareAbort`; the next sweep retries the fan-out. A concurrent
    /// caller that changed the entry out from under us aborts this reap of that
    /// tid (re-validated before the Complete write). Returns the tids it
    /// finalized.
    // cargo-mutants: I/O orchestration over live DashMap/partition state
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(
        name = "txn_coordinator_sweep_expired",
        level = "debug",
        skip_all,
        fields(now_ms)
    )]
    pub(crate) async fn sweep_expired(&self, now_ms: i64, txnv: TxnVersion) -> Vec<String> {
        // Snapshot the candidate tids first so we don't hold DashMap shard locks
        // across the async abort work; the orchestration then drives the live
        // `ReaperBackend` (this coordinator), which re-acquires each entry's lock
        // per phase.
        let candidates: Vec<String> = self.state.iter().map(|e| e.key().clone()).collect();
        sweep_with_backend(self, candidates, now_ms, txnv).await
    }
}

/// Live adapter that runs the real reaper side effects against the in-memory
/// state map, the `__transaction_state` partition log, partition leaders, and
/// the producer-id allocator. Only the irreducible IO lives here. The pure
/// helpers and the orchestration logic above hold every decision.
#[async_trait]
impl ReaperBackend for TxnCoordinator {
    // cargo-mutants: thin adapter over inherent method / live lock state
    #[cfg_attr(test, mutants::skip)]
    async fn is_coordinator_for(&self, tid: &str) -> bool {
        let p = self.partition_for(tid);
        self.leader_partitions.read().await.contains(&p)
    }

    // cargo-mutants: I/O over live entry locks + raft persistence
    #[cfg_attr(test, mutants::skip)]
    async fn prepare_abort(&self, tid: &str, now_ms: i64, txnv: TxnVersion) -> Option<TxnEntry> {
        let handle = self.get(tid)?;
        let prepared = {
            let mut entry = handle.lock().await;
            if entry.state == TxnState::PrepareAbort {
                return Some(entry.clone());
            }
            if !should_abort_idle_txn(entry.state, entry.txn_timeout_ms, entry.start_ms, now_ms) {
                return None;
            }
            apply_prepare_abort(&mut entry, now_ms);
            if let Err(error) =
                prepare_completion_identities(&mut entry, txnv, &self.producer_ids).await
            {
                warn!(tid, %error, "txn reaper: failed to allocate completion identity");
                return None;
            }
            entry.clone()
        };
        if let Err(e) = self.put(prepared.clone(), txnv).await {
            warn!(tid, error = %e, "txn reaper: failed to persist PrepareAbort; skipping");
            return None;
        }
        Some(prepared)
    }

    // cargo-mutants: writes abort markers to live partition logs
    #[cfg_attr(test, mutants::skip)]
    async fn dispatch_abort_markers(&self, entry: &TxnEntry) -> bool {
        match self
            .dispatch_transaction_markers(entry, MarkerType::Abort)
            .await
        {
            Ok(()) => true,
            Err(error) => {
                warn!(
                    tid = %entry.transactional_id,
                    %error,
                    "txn reaper: abort marker fan-out failed; will retry"
                );
                false
            }
        }
    }

    // cargo-mutants: I/O over live entry locks + raft persistence
    #[cfg_attr(test, mutants::skip)]
    async fn complete_abort(
        &self,
        prepared: &TxnEntry,
        now_ms: i64,
        txnv: TxnVersion,
    ) -> Option<TxnEntry> {
        let tid = prepared.transactional_id.as_str();
        let handle = self.get(tid)?;
        let complete = {
            let mut entry = handle.lock().await;
            if !complete_abort_guard_ok(&entry, prepared) {
                // Someone advanced the entry underneath us; don't finalize.
                return None;
            }
            let (new_pid, new_epoch) = completion_producer_identity(&entry);
            apply_complete_abort(&mut entry, new_pid, new_epoch, now_ms);
            entry.clone()
        };
        if let Err(e) = self.put(complete.clone(), txnv).await {
            warn!(tid, error = %e, "txn reaper: failed to persist CompleteAbort; skipping");
            return None;
        }
        Some(complete)
    }
}
