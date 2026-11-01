//! KIP-939 idle-transaction reaper: the timeout-driven abort path.
//!
//! The module holds the [`ReaperBackend`] seam, the pure transition helper of
//! an idle abort, the sweep that runs over that seam, and the live adapter
//! that runs it against the in-memory state map, the `__transaction_state`
//! log and the producer-id allocator.
//!
//! The sweep only decides and prepares. Kafka's
//! `TransactionCoordinator.abortTimedOutTransactions` appends `PrepareAbort`
//! and hands the markers to `TransactionMarkerChannelManager`, which sends
//! them in the background; the sweep never waits for one. This sweep does the
//! same: it makes the `PrepareAbort` record durable and queues the transaction
//! for the completion task ([`crate::txn::completion`]), which fans the markers
//! out, retries a partition that is not ready, and writes `CompleteAbort`. A
//! sweep that waited for each fan-out would wait up to
//! `MARKER_COMMIT_TIMEOUT` for every transaction whose partition cannot
//! commit, and every later transaction of the sweep would time out without an
//! abort for as long as that partition stayed stuck.

use {
    super::TxnCoordinator,
    crate::txn::{
        handlers::end_txn::prepare_server_abort_identities,
        state::{TxnEntry, TxnState},
        two_pc::{NO_TIMEOUT_MS, should_abort_idle_txn},
        version::TxnVersion,
    },
    async_trait::async_trait,
    std::sync::Arc,
    tokio::sync::{Mutex, OwnedMutexGuard},
    tracing::{debug, info, warn},
};

#[cfg(test)]
mod tests;

/// Live-dependency seam for the KIP-939 idle-transaction reaper.
///
/// `sweep_expired` decides, for each tid, whether to abort it, around three
/// irreducible side effects: a coordinator-ownership check, the
/// compare-and-swap-style persisted `Ongoing → PrepareAbort` transition, and
/// the hand-off of a durable `PrepareAbort` to the completion task. Each one
/// touches a live `__transaction_state` partition, the producer-id allocator,
/// or the completion queue.
///
/// This trait puts those effects behind a seam, so a unit test can drive the
/// sweep against a [`mockall`] mock. The live adapter is [`TxnCoordinator`]
/// itself.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait ReaperBackend: Send + Sync {
    /// Is this broker the transaction coordinator for `tid` right now?
    async fn is_coordinator_for(&self, tid: &str) -> bool;

    /// Moves `Ongoing → PrepareAbort` under `tid`'s entry lock, atomically.
    ///
    /// If the entry should abort as an idle transaction at `now_ms`, this
    /// method makes the transition, persists it, and returns the persisted
    /// snapshot. It returns an entry already in `PrepareAbort` as it is, so a
    /// sweep hands an abort that is still outstanding to the completion task
    /// again. It returns `None` when the entry is absent, when the entry must
    /// not be reaped, or when the persistence failed.
    async fn prepare_abort(&self, tid: &str, now_ms: i64, txnv: TxnVersion) -> Option<TxnEntry>;

    /// Queues `tid`, whose `PrepareAbort` record is durable, for the
    /// completion task, which writes its markers and its `CompleteAbort`.
    fn queue_completion(&self, tid: &str);
}

/// Applies the `Ongoing → PrepareAbort` mutation for an idle-reaped entry. It
/// changes the state and stamps `last_update_ms`. The function is pure, so a
/// unit test can kill the transition without any persistence.
fn apply_prepare_abort(entry: &mut TxnEntry, now_ms: i64) {
    entry.state = TxnState::PrepareAbort;
    entry.last_update_ms = now_ms;
}

/// A coordinator persist replaces the map value with a fresh `Arc`. A caller
/// that queued on the prior entry lock must not publish from that stale value.
fn handle_is_current(
    coordinator: &TxnCoordinator,
    tid: &str,
    handle: &Arc<Mutex<TxnEntry>>,
) -> bool {
    coordinator.is_current_entry(tid, handle)
}

/// Runs the reaper sweep.
///
/// The sweep is generic over the [`ReaperBackend`] seam, so a unit test can
/// drive it against a mock. For each candidate tid it checks ownership,
/// prepares the abort (compare-and-swap), and hands the durable `PrepareAbort`
/// to the completion task. It never waits for a marker. Returns the tids whose
/// abort it handed over, in iteration order.
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
            debug!(tid, "txn reaper: not the coordinator; skipping");
            continue;
        }
        let Some(prepared) = backend.prepare_abort(&tid, now_ms, txnv).await else {
            continue;
        };
        backend.queue_completion(&tid);
        info!(
            tid,
            producer_id = prepared.producer_id.get(),
            producer_epoch = prepared.producer_epoch,
            partitions = ?prepared.partitions,
            "txn reaper: queued the abort of a timed-out transaction for completion"
        );
        aborted.push(tid);
    }
    aborted
}

impl TxnCoordinator {
    async fn current_entry_guard(&self, tid: &str) -> Option<OwnedMutexGuard<TxnEntry>> {
        let handle = self.get(tid)?;
        let entry = Arc::clone(&handle).lock_owned().await;
        handle_is_current(self, tid, &handle).then_some(entry)
    }

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
    /// Each abort writes the same `PrepareAbort` an `EndTxn(committed=false)`
    /// writes and bumps the producer epoch, at every transaction version, so
    /// the timed-out producer is fenced. The completion task then writes the
    /// markers and the `CompleteAbort`; each later sweep hands it a
    /// `PrepareAbort` that is still outstanding again. Returns the tids whose
    /// abort it handed to the completion task.
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
        // across the async abort work; the sweep then drives the live
        // `ReaperBackend` (this coordinator), which re-acquires each entry's
        // lock.
        let candidates: Vec<String> = self.state.iter().map(|e| e.key().clone()).collect();
        sweep_with_backend(self, candidates, now_ms, txnv).await
    }
}

/// Live adapter that runs the real reaper side effects against the in-memory
/// state map, the `__transaction_state` partition log, the producer-id
/// allocator and the completion queue. Only the irreducible IO lives here.
/// The pure helper and the sweep above hold every decision.
#[async_trait]
impl ReaperBackend for TxnCoordinator {
    // cargo-mutants: thin adapter over inherent method / live lock state
    #[cfg_attr(test, mutants::skip)]
    async fn is_coordinator_for(&self, tid: &str) -> bool {
        Self::is_transaction_coordinator_for(self, tid).await
    }

    // cargo-mutants: I/O over live entry locks + raft persistence
    #[cfg_attr(test, mutants::skip)]
    async fn prepare_abort(&self, tid: &str, now_ms: i64, txnv: TxnVersion) -> Option<TxnEntry> {
        let _partition_lock = self.lock_state_partition_for(tid).await;
        let entry = self.current_entry_guard(tid).await?;
        if entry.state == TxnState::PrepareAbort {
            // A prepared 2PC transaction remains under its external
            // coordinator's control, including after recovery.
            return (entry.txn_timeout_ms != NO_TIMEOUT_MS).then(|| entry.clone());
        }
        if !should_abort_idle_txn(entry.state, entry.txn_timeout_ms, entry.start_ms, now_ms) {
            return None;
        }
        // Stage on a clone. Allocation or persistence failure must leave the
        // live entry exactly as it was before this sweep.
        let mut prepared = entry.clone();
        apply_prepare_abort(&mut prepared, now_ms);
        // Kafka `abortTimedOutTransactions`: `prepareFenceProducerEpoch`, then
        // an abort at the cluster's transaction version. It fences the
        // timed-out producer at every level: the completion bump at `TV_2`,
        // the fence's own bump below it.
        if let Err(error) =
            prepare_server_abort_identities(&mut prepared, txnv, &self.producer_ids).await
        {
            warn!(
                tid,
                producer_id = entry.producer_id.get(),
                producer_epoch = entry.producer_epoch,
                %error,
                "txn reaper: failed to allocate completion identity"
            );
            return None;
        }
        // The completion task compares the entry it completes against the
        // live one by full equality, so this returns the entry publication
        // holds.
        let persisted = self.put_under_state_partition_lock(prepared, txnv).await;
        let (producer_id, producer_epoch) = (entry.producer_id.get(), entry.producer_epoch);
        drop(entry);
        match persisted {
            Ok(persisted) => Some(persisted),
            Err(e) => {
                warn!(
                    tid,
                    producer_id,
                    producer_epoch,
                    error = %e,
                    "txn reaper: failed to persist PrepareAbort; skipping"
                );
                None
            }
        }
    }

    fn queue_completion(&self, tid: &str) { self.request_completion(tid); }
}
