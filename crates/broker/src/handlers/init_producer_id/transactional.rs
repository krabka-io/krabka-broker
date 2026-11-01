//! The transactional sub-path of `InitProducerId`: coordinator-side
//! allocation, epoch bump, and KIP-939 recovery of a prepared transaction.
//!
//! Once the request has passed the ACL preamble and this broker is confirmed
//! as the coordinator for the transactional id, everything that follows is one
//! state machine over the persisted `TxnEntry`. A fresh id allocates, a
//! prepared id is recovered, and a reused id aborts any ongoing transaction
//! before it bumps the epoch, so the transitions and the abort-marker fan-out
//! they depend on stay together.

use {
    super::identity::{ProducerIdentity, next_init_producer_identity, stage_recovery_identity},
    crate::{
        codes,
        error::BrokerError,
        txn::{
            coordinator::{
                TxnCoordinator,
                completion::{apply_completion_from, completion_decision, completion_for},
            },
            handlers::end_txn::{
                client_producer_identity, completion_producer_identity,
                prepare_server_abort_identities,
            },
            marker::MarkerType,
            state::{TxnEntry, TxnState},
            util::now_millis,
            version::TxnVersion,
        },
    },
    krabka_log::ProducerId,
    krabka_protocol::owned::init_producer_id_response::InitProducerIdResponse,
    krabka_verified::{
        transaction,
        transaction::{
            InitProducerIdIdentityDecision,
            TransactionReaperCompletionDecision as CompletionDecision,
        },
    },
    std::sync::Arc,
    tokio::sync::Mutex,
};

/// One transactional `InitProducerId` call, as [`handle_transactional`] runs
/// it.
#[derive(bon::Builder)]
pub(super) struct TransactionalInit<'a> {
    tid: &'a str,
    txnv: TxnVersion,
    txn_timeout: i32,
    #[builder(default = false)]
    enable_2pc: bool,
    #[builder(default = false)]
    keep_prepared_txn: bool,
    /// KIP-360: the identity the caller believes it holds,
    /// [`RequestedIdentity::NONE`] when it claims none.
    #[builder(default = RequestedIdentity::NONE, into)]
    request_identity: RequestedIdentity,
}

/// Transactional sub-path: allocate or bump-epoch for `tid`.
///
/// `request_identity` is the `(producer_id, producer_epoch)` the caller
/// claims, `(-1, -1)` when it claims none. Kafka's
/// `TransactionCoordinator.prepareInitProducerIdTransit` fences a stale claim
/// with `PRODUCER_FENCED` before it matches on the persisted state, so an
/// ongoing transaction is aborted only for the producer that owns it
/// (KIP-360). It does that inside `txnMetadata.inLock`, together with the
/// transit it prepares, and so does every branch below: the check and the
/// mutation it admits share one guard.
///
/// A transactional id with no entry is not fenced, whatever the caller
/// claims. Kafka's `isValidProducerId` opens with
/// `txnMetadata.producerEpoch == RecordBatch.NO_PRODUCER_EPOCH`, which the
/// metadata it just created always satisfies: a producer recovering from
/// `UNKNOWN_PRODUCER_ID` names its old identity, and the freshly allocated one
/// is what it gets back.
pub(super) async fn handle_transactional(
    coord: &Arc<TxnCoordinator>,
    init: TransactionalInit<'_>,
) -> Result<InitProducerIdResponse, BrokerError> {
    let now_ms = now_millis();
    let Some(existing) = coord.get(init.tid) else {
        return allocate_fresh(coord, &init, now_ms).await;
    };
    // KIP-360: a caller that names a producer identity must name the
    // live one, or the epoch this entry held before an epoch fence
    // whose abort failed. Kafka runs that check inside
    // `txnMetadata.inLock` together with the transit it prepares, so
    // every branch below re-runs it under the very lock that performs
    // its mutation. Two overlapping v3 calls that name the same live
    // identity therefore serialize on that lock, and the one that
    // loses finds the epoch the winner already advanced. A zombie
    // neither recovers a prepared transaction nor aborts the ongoing
    // transaction of the producer that fenced it.
    if init.keep_prepared_txn
        && let Some(response) = recover_prepared(coord, &init, &existing, now_ms).await?
    {
        return Ok(response);
    }
    // Reusing tid. Kafka fences the epoch of an ongoing transaction,
    // aborts it, and answers CONCURRENT_TRANSACTIONS. Otherwise it
    // bumps the epoch and answers the new identity.
    if let Some(response) = fence_ongoing(coord, &init, &existing, now_ms).await? {
        return Ok(response);
    }
    bump_epoch(coord, &init, existing, now_ms).await
}

/// Fresh tid — allocate a new producer id.
async fn allocate_fresh(
    coord: &TxnCoordinator,
    init: &TransactionalInit<'_>,
    now_ms: i64,
) -> Result<InitProducerIdResponse, BrokerError> {
    let tid = init.tid;
    let (pid, epoch) = coord.producer_ids.allocate().await?;
    let entry = TxnEntry::new_empty(tid.to_owned(), pid, epoch, init.txn_timeout, now_ms);
    // Kafka's `putTransactionStateIfNotExists` creates the metadata
    // under the state lock and hands a racing caller the winner's
    // object. That caller runs `prepareInitProducerIdTransit` on it:
    // `CONCURRENT_TRANSACTIONS` while the winner's transit is pending,
    // and an epoch bump once the winner's append has completed.
    // `allocate` yields, so another first `InitProducerId` for this id
    // can create the entry meanwhile; recheck under the partition's
    // write lock, which every append takes, so the two never persist
    // two identities for one id. The winner publishes only after its
    // append, so a loser here would find it completed and Kafka would
    // bump at once. It answers `CONCURRENT_TRANSACTIONS` instead, and
    // the client's retry finds the winner's entry and takes that bump.
    // The id ends at the same identity and timeout either way, one
    // round trip later, so this does not run the reuse path below
    // from here.
    let _state_partition_write = coord.lock_state_partition_for(tid).await;
    if coord.get(tid).is_some() {
        return Ok(concurrent_transactions_response());
    }
    coord
        .put_under_state_partition_lock(entry, init.txnv)
        .await?;
    Ok(identity_response(pid, epoch))
}

/// KIP-939 `keepPreparedTxn`: recover the `Ongoing` transaction on `existing`
/// under a fresh producer identity. `None` lets the call fall through to the
/// reuse path, for an entry in any other state that is neither pending nor
/// fenced.
async fn recover_prepared(
    coord: &TxnCoordinator,
    init: &TransactionalInit<'_>,
    existing: &Mutex<TxnEntry>,
    now_ms: i64,
) -> Result<Option<InitProducerIdResponse>, BrokerError> {
    let recovery = {
        let mut entry = existing.lock().await;
        if let Some(response) = pending_or_fenced_response(&entry, init.request_identity) {
            return Ok(Some(response));
        }
        if entry.state == TxnState::Ongoing {
            let ongoing_pid = entry.producer_id;
            let ongoing_epoch = entry.producer_epoch;
            if init.enable_2pc {
                entry.txn_timeout_ms = i32::MAX;
            }
            let ProducerIdentity {
                id: next_pid,
                epoch: next_epoch,
            } = stage_recovery_identity(&mut entry, &coord.producer_ids).await?;
            entry.last_update_ms = now_ms;
            Some((
                entry.clone(),
                next_pid,
                next_epoch,
                ongoing_pid,
                ongoing_epoch,
            ))
        } else {
            None
        }
    };
    if let Some((snapshot, next_pid, next_epoch, ongoing_pid, ongoing_epoch)) = recovery {
        coord.put(snapshot, init.txnv).await?;
        return Ok(Some(InitProducerIdResponse {
            error_code: codes::NONE,
            producer_id: next_pid.get(),
            producer_epoch: next_epoch,
            ongoing_txn_producer_id: ongoing_pid.get(),
            ongoing_txn_producer_epoch: ongoing_epoch,
            ..Default::default()
        }));
    }
    Ok(pending_or_fenced_response(
        &*existing.lock().await,
        init.request_identity,
    ))
}

/// Fence and abort the `Ongoing` transaction on the published entry. `None`
/// lets the call fall through to [`bump_epoch`], for an entry in any other
/// state; both guards this takes are released before it returns.
async fn fence_ongoing(
    coord: &TxnCoordinator,
    init: &TransactionalInit<'_>,
    existing: &Arc<Mutex<TxnEntry>>,
    now_ms: i64,
) -> Result<Option<InitProducerIdResponse>, BrokerError> {
    let TransactionalInit {
        tid,
        txnv,
        request_identity,
        ..
    } = *init;
    // Lock order: the state-partition write lock, then the entry
    // lock, as `EndTxn`, the reaper and the completion task take
    // them. Every append takes the partition lock, so the entry
    // read under it is the published one.
    let state_partition_write = coord.lock_state_partition_for(tid).await;
    let current = coord.get(tid).unwrap_or_else(|| Arc::clone(existing));
    let mut e = current.lock().await;
    if let Some(response) = pending_or_fenced_response(&e, request_identity) {
        return Ok(Some(response));
    }
    // A `Retry`-classified identity names the epoch this entry
    // held before its *last* bump, never its live one (`Bump`
    // covers that). If the live producer has since opened a new
    // `Ongoing` transaction at the bumped epoch -- with or
    // without another `InitProducerId` call in between -- this
    // stale retry must answer the identity already on the entry,
    // not fall into the abort below and tear down a transaction
    // it does not own. Kafka's `prepareIncrementProducerEpoch`
    // answers a retry from the identity alone, never from the
    // entry's state.
    if let Some(response) = retried_bump_response(&e, request_identity) {
        return Ok(Some(response));
    }
    if !matches!(e.state, TxnState::Ongoing) {
        return Ok(None);
    }
    // Transition to PrepareAbort; persist; dispatch markers.
    let (request_pid, fenced_from_epoch) = client_producer_identity(&e);
    // Stage on a clone: until the PrepareAbort record is
    // durable, other callers must still see Ongoing.
    let mut prepared = e.clone();
    prepared.state = TxnState::PrepareAbort;
    // Kafka `prepareFenceProducerEpoch` and the abort it runs
    // at the cluster's transaction version. The epoch of the
    // ongoing transaction is raised once before the markers, so
    // every abort marker fences the producer at its partitions,
    // and that epoch is never answered to the client. Below
    // `TV_2` the fence raises it and clears the last epoch, a
    // retry token from an earlier, unrelated bump that a
    // zombie could otherwise present as a retry of this fence.
    // At `TV_2` the completion bump is the only bump, and the
    // last epoch is the one the producer still holds.
    prepare_server_abort_identities(&mut prepared, txnv, &coord.producer_ids).await?;
    prepared.last_update_ms = now_ms;
    let persisted = coord
        .put_under_state_partition_lock(prepared.clone(), txnv)
        .await?;
    *e = persisted;
    drop(e);
    drop(state_partition_write);
    // `put` republishes the tid under a fresh handle, so the
    // one this call started from is no longer the entry a
    // concurrent `coord.get` finds. Everything below must act
    // on the published entry.
    let published = coord.get(tid).unwrap_or(current);
    if let Err(error) = dispatch_abort_markers(coord, &mut prepared).await {
        record_failed_fence(&published, fenced_from_epoch, txnv).await;
        // The PrepareAbort record is durable. Kafka answers
        // the fence with CONCURRENT_TRANSACTIONS and finishes
        // the abort in its marker channel; the producer
        // retries.
        tracing::warn!(
            tid,
            %error,
            "InitProducerId: abort marker fan-out failed; queued for completion"
        );
        coord.request_completion(tid);
        return Ok(Some(concurrent_transactions_response()));
    }
    complete_fenced_abort(coord, init, &prepared, request_pid).await;
    // Kafka answers the fenced producer
    // `CONCURRENT_TRANSACTIONS`. The client retries, and that
    // retry takes the epoch bump below on the completed
    // transaction.
    Ok(Some(concurrent_transactions_response()))
}

/// KIP-360: the epoch fence is persisted but the abort
/// it was prepared for did not complete. The producer
/// that owns the transaction still holds
/// `fenced_from_epoch`, so record it on the published
/// entry and let only that producer retry its
/// `InitProducerId`. Kafka keeps `hasFailedEpochFence`
/// in memory too: a coordinator that loses it fails
/// closed, and the producer is fenced. At `TV_2` the
/// fence raised nothing, so a retry raises nothing
/// twice and Kafka never sets the flag there.
async fn record_failed_fence(
    published: &Mutex<TxnEntry>,
    fenced_from_epoch: i16,
    txnv: TxnVersion,
) {
    let mut fenced = published.lock().await;
    fenced.last_producer_epoch = fenced_from_epoch;
    if !txnv.verified() {
        fenced.has_failed_epoch_fence = true;
    }
    drop(fenced);
}

/// Re-acquire + transition to `CompleteAbort`, staged on a clone so a failed
/// append leaves `PrepareAbort` for the completion task.
///
/// Every lock was released for the marker fan-out, so the completion task may
/// have finished this abort and a later request may have written over it. As
/// in `complete_prepared_transaction`, this takes the state-partition write
/// lock, then the entry lock, and appends only while the live entry is still
/// the exact `prepared` snapshot the markers were written for. Kafka's marker
/// channel likewise appends the `Complete*` transition only for the metadata
/// it prepared, and drops it otherwise. The fenced producer is answered
/// `CONCURRENT_TRANSACTIONS` either way.
async fn complete_fenced_abort(
    coord: &TxnCoordinator,
    init: &TransactionalInit<'_>,
    prepared: &TxnEntry,
    request_pid: ProducerId,
) {
    let tid = init.tid;
    let state_partition_write = coord.lock_state_partition_for(tid).await;
    let Some(live) = coord.get(tid) else {
        return;
    };
    let live = live.lock().await;
    let prepared_states = (TxnState::PrepareAbort, TxnState::CompleteAbort);
    if completion_decision(&live, prepared, prepared_states) != CompletionDecision::Proceed {
        return;
    }
    let mut completed = live.clone();
    let identity = completion_producer_identity(&completed);
    // Kafka's `prepareComplete`, which also clears the failed epoch fence
    // whose abort has now been written.
    apply_completion_from(
        &mut completed,
        TxnState::CompleteAbort,
        identity,
        request_pid,
        now_millis(),
    );
    // The entry stays locked until the completion is written.
    let written = coord
        .put_under_state_partition_lock(completed, init.txnv)
        .await;
    drop(live);
    drop(state_partition_write);
    if let Err(error) = written {
        tracing::warn!(
            tid,
            %error,
            "InitProducerId: CompleteAbort append failed; queued for completion"
        );
        coord.request_completion(tid);
    }
}

/// Bump epoch on the existing entry. Persist a new `TxnEntry` with new epoch,
/// `Empty` state, cleared partitions.
async fn bump_epoch(
    coord: &TxnCoordinator,
    init: &TransactionalInit<'_>,
    existing: Arc<Mutex<TxnEntry>>,
    now_ms: i64,
) -> Result<InitProducerIdResponse, BrokerError> {
    let TransactionalInit {
        tid,
        txnv,
        txn_timeout,
        request_identity,
        ..
    } = *init;
    // Lock order: the state-partition write lock, then the entry
    // lock, same as the abort above and every other append to this
    // tid's log. The entry lock stays held across the append itself
    // -- staged on a local clone, published into the entry only once
    // `coord.put_under_state_partition_lock` returns -- so a caller
    // already parked on this exact handle never observes the bumped
    // identity before it is durable (the bug class PR #1046 fixed for
    // `AddOffsetsToTxn`'s `add_offsets_partition`).
    let _state_partition_write = coord.lock_state_partition_for(tid).await;
    let current = coord.get(tid).unwrap_or(existing);
    let mut e3 = current.lock().await;
    // A `Dead` entry is one the KIP-98 expiry sweep marked under this
    // very lock before it appended the tid's tombstone. This call was
    // parked on the lock while that happened, so its handle is no
    // longer the coordinator's: reviving from it would persist a
    // producer identity for a transactional id whose tombstone is
    // already in the log, and race a second `InitProducerId` that
    // found no entry and took the fresh-id path above. Kafka answers
    // a metadata object mid-transition with `CONCURRENT_TRANSACTIONS`,
    // which the client retries; the retry finds no entry and
    // allocates cleanly.
    // The check comes before the identity one below because a `Dead`
    // entry holds no live mapping to be fenced against, exactly like
    // the tid that was never there: Kafka's `isValidProducerId` opens
    // by admitting freshly created metadata, and the retry this answer
    // asks for finds no entry and allocates.
    if e3.state == TxnState::Dead {
        return Ok(concurrent_transactions_response());
    }
    if let Some(response) = pending_completion_response(&e3, request_identity) {
        return Ok(response);
    }
    if let Some(response) = retried_bump_response(&e3, request_identity) {
        return Ok(response);
    }
    if is_fenced(&e3, request_identity) {
        return Ok(fenced_response());
    }
    let (previous_pid, previous_epoch) = client_producer_identity(&e3);
    let ProducerIdentity {
        id: new_pid,
        epoch: new_epoch,
    } = next_init_producer_identity(&e3, &coord.producer_ids).await?;
    let mut staged = TxnEntry::new_empty(tid.to_owned(), new_pid, new_epoch, txn_timeout, now_ms);
    // The transition keeps the transaction version of the record before
    // it (Kafka's `TransitionData` defaults to it).
    staged.client_transaction_version = e3.client_transaction_version;
    // Kafka's `prepareIncrementProducerEpoch` and
    // `prepareProducerIdRotation` record the epoch the entry held, so
    // a retry of this call is recognised. A caller that named no
    // identity records no last epoch.
    if request_identity.producer_id >= 0 {
        staged.last_producer_epoch = previous_epoch;
    }
    if new_pid != previous_pid {
        staged.prev_producer_id = previous_pid;
    }
    // Stage on a clone: until the append is durable, a caller already
    // parked on this entry's lock must still see the identity it held
    // before this bump.
    let persisted = coord
        .put_under_state_partition_lock(staged.clone(), txnv)
        .await?;
    *e3 = persisted;
    Ok(identity_response(e3.producer_id, e3.producer_epoch))
}

/// The producer id and epoch that an `InitProducerId` request names, as the
/// wire carries them. A producer that names no identity sends `-1` for both.
#[derive(Clone, Copy, derive_more::From)]
pub(super) struct RequestedIdentity {
    producer_id: i64,
    producer_epoch: i16,
}

impl RequestedIdentity {
    /// The identity of a producer that names none. It is not `(0, 0)`, which
    /// names a real producer and would be fenced against the entry.
    pub(super) const NONE: Self = Self {
        producer_id: -1,
        producer_epoch: -1,
    };
}

/// What `request_identity` may do to `entry` (KIP-360).
///
/// The caller holds `entry`'s lock, and the mutation the verdict admits runs
/// under that same guard.
fn identity_decision(
    entry: &TxnEntry,
    request_identity: RequestedIdentity,
) -> InitProducerIdIdentityDecision {
    let (entry_pid, entry_epoch) = client_producer_identity(entry);
    transaction::init_producer_id_identity_decision(
        entry_pid.get(),
        entry_epoch,
        entry.last_producer_epoch,
        entry.prev_producer_id.get(),
        request_identity.producer_id,
        request_identity.producer_epoch,
    )
}

/// Whether `request_identity` is fenced against `entry`'s live identity.
fn is_fenced(entry: &TxnEntry, request_identity: RequestedIdentity) -> bool {
    identity_decision(entry, request_identity) == InitProducerIdIdentityDecision::Fenced
}

/// Reject a pending transition before checking the live producer identity.
/// Callers hold the same entry lock while checking and staging their mutation.
fn pending_or_fenced_response(
    entry: &TxnEntry,
    request_identity: RequestedIdentity,
) -> Option<InitProducerIdResponse> {
    pending_completion_response(entry, request_identity)
        .or_else(|| is_fenced(entry, request_identity).then(fenced_response))
}

/// Kafka `prepareIncrementProducerEpoch`: a retry of a bump that already
/// happened answers the entry's identity and writes nothing.
fn retried_bump_response(
    entry: &TxnEntry,
    request_identity: RequestedIdentity,
) -> Option<InitProducerIdResponse> {
    if identity_decision(entry, request_identity) != InitProducerIdIdentityDecision::Retry {
        return None;
    }
    let (pid, epoch) = client_producer_identity(entry);
    Some(identity_response(pid, epoch))
}

/// Kafka `prepareInitProducerIdTransit` for an entry whose `Prepare*` record
/// is durable and not yet complete.
///
/// Kafka checks only the producer ID here, not the epoch
/// (`isValidProducerId`): a caller that names no producer ID, the entry's
/// producer ID, or the prior producer ID at an exhausted epoch gets
/// `CONCURRENT_TRANSACTIONS` and retries after the completion. Any other
/// producer ID is `PRODUCER_FENCED`. Returns `None` for any other state.
///
/// Before this check, `keepPreparedTxn=false` overwrote a `PrepareCommit` with
/// a new empty entry, which erased a commit decision whose markers some
/// partitions may already hold.
fn pending_completion_response(
    entry: &TxnEntry,
    RequestedIdentity {
        producer_id: request_pid,
        producer_epoch: request_epoch,
    }: RequestedIdentity,
) -> Option<InitProducerIdResponse> {
    completion_for(entry.state)?;
    let names_this_transaction = request_pid < 0
        || request_pid == entry.producer_id.get()
        || (entry.has_staged_producer_identity() && request_pid == entry.next_producer_id.get())
        || (request_pid == entry.prev_producer_id.get() && request_epoch >= i16::MAX - 1);
    Some(if names_this_transaction {
        concurrent_transactions_response()
    } else {
        fenced_response()
    })
}

/// Kafka's `initTransactionError(Errors.CONCURRENT_TRANSACTIONS)`.
fn concurrent_transactions_response() -> InitProducerIdResponse {
    InitProducerIdResponse {
        error_code: codes::CONCURRENT_TRANSACTIONS,
        producer_id: -1,
        producer_epoch: -1,
        ..Default::default()
    }
}

/// Kafka's `initTransactionError(Errors.PRODUCER_FENCED)`.
fn fenced_response() -> InitProducerIdResponse {
    InitProducerIdResponse {
        error_code: codes::PRODUCER_FENCED,
        producer_id: -1,
        producer_epoch: -1,
        ..Default::default()
    }
}

/// The answer that hands the caller the producer identity
/// `(producer_id, producer_epoch)`.
fn identity_response(producer_id: ProducerId, producer_epoch: i16) -> InitProducerIdResponse {
    InitProducerIdResponse {
        error_code: codes::NONE,
        // Unwrap the `ProducerId` into the raw-`i64` wire field.
        producer_id: producer_id.get(),
        producer_epoch,
        ..Default::default()
    }
}

async fn dispatch_abort_markers(
    coord: &TxnCoordinator,
    entry: &mut TxnEntry,
) -> Result<(), BrokerError> {
    coord
        .dispatch_transaction_markers(entry, MarkerType::Abort)
        .await
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            log_dir::partition_dir,
            partition::Partition,
            partition_registry::PartitionRegistry,
            producer_id_manager::ProducerIdManager,
            test_support::{open_partition, spawn_standalone_partition},
            txn::{
                bootstrap,
                coordinator::{
                    completion::{CompletionAttempt, apply_completion},
                    fanout_gate::MarkerFanoutMode,
                },
                state::TopicPartition,
                version::TxnVersion,
            },
        },
        assert2::{assert, check},
        krabka_ids::PartitionIndex,
        krabka_log::{Log, LogConfig, ProducerId},
        krabka_metadata::{MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicRecord},
        std::{fs, path::Path},
        tokio::{task, task::JoinHandle},
    };

    /// A coordinator seeded with a transaction, and the partition that a test
    /// reads back: `__transaction_state-0` for a completed transaction and the
    /// data partition for an ongoing one.
    struct SeededCoordinator {
        coordinator: Arc<TxnCoordinator>,
        partition: Arc<Partition>,
    }

    /// An `InitProducerId` answer: the error code, the producer id and the
    /// producer epoch.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct Answer {
        error_code: i16,
        producer_id: i64,
        producer_epoch: i16,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum LogMutation {
        Appended,
        Unchanged,
    }

    #[derive(derive_more::From, derive_more::Into, Clone, Copy)]
    struct ProducerEpoch(i16);

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct InitIdentitySetup {
        #[default(ProducerId(1000))]
        producer_id: ProducerId,
        #[default(ProducerEpoch(3))]
        producer_epoch: ProducerEpoch,
    }

    #[derive(Clone, Copy)]
    enum InitIdentity {
        Anonymous,
        Existing(InitIdentitySetup),
    }

    impl Default for InitIdentity {
        fn default() -> Self { Self::Existing(InitIdentitySetup::default()) }
    }

    /// `dispatch_abort_markers` appends an abort control-marker batch to each
    /// locally-led partition in the entry's partition set. Each append advances
    /// that partition's LEO by one. A whole-function `Ok(())` replacement would
    /// skip the dispatch entirely and leave the LEO at 0.
    async fn init_verified(
        coordinator: &Arc<TxnCoordinator>,
        tid: &str,
        identity: InitIdentity,
    ) -> Result<InitProducerIdResponse, BrokerError> {
        let identity = match identity {
            InitIdentity::Anonymous => (-1, -1),
            InitIdentity::Existing(setup) => (setup.producer_id.0, setup.producer_epoch.0),
        };
        handle_transactional(
            coordinator,
            TransactionalInit::builder()
                .tid(tid)
                .txnv(TxnVersion::Verified)
                .txn_timeout(60_000)
                .request_identity(identity)
                .build(),
        )
        .await
    }

    /// `InitProducerId` for `tid` at `txnv`, naming `identity`.
    async fn init_at(
        coordinator: &Arc<TxnCoordinator>,
        tid: &str,
        txnv: TxnVersion,
        identity: (i64, i16),
    ) -> InitProducerIdResponse {
        handle_transactional(
            coordinator,
            TransactionalInit::builder()
                .tid(tid)
                .txnv(txnv)
                .txn_timeout(60_000)
                .request_identity(identity)
                .build(),
        )
        .await
        .expect("init responds")
    }

    async fn init_near_exhaustion(
        coordinator: &Arc<TxnCoordinator>,
        tid: &str,
    ) -> InitProducerIdResponse {
        init_verified(
            coordinator,
            tid,
            InitIdentity::Existing(InitIdentitySetup {
                producer_epoch: ProducerEpoch(i16::MAX - 1),
                ..Default::default()
            }),
        )
        .await
        .expect("init responds")
    }

    #[tokio::test]
    async fn dispatch_abort_markers_appends_marker_to_local_partition() {
        let dir = tempfile::tempdir().unwrap();
        let partitions = Arc::new(PartitionRegistry::new());
        let coord = TxnCoordinator::new(
            krabka_audit::NodeId(1),
            Arc::clone(&partitions),
            Arc::new(ProducerIdManager::new()),
            50,
            krabka_units::mebibytes(1),
        );

        // Materialize a local partition for `__transaction_state`-style data.
        let part_dir = partition_dir(dir.path(), "orders", 0);
        fs::create_dir_all(&part_dir).unwrap();
        let log = Log::open(&part_dir, LogConfig::default()).unwrap();
        let part = spawn_standalone_partition()
            .log_dir(dir.path())
            .log(log)
            .call();
        assert!(part.log_end_offset() == 0);
        // The metadata reconcile installs this broker, node 1, as the leader.
        part.install_leader_change(1, 0).await;
        partitions.insert("orders".into(), PartitionIndex(0), Arc::clone(&part));

        // Build a txn entry that names this partition.
        let mut entry = TxnEntry::new_empty("tx-1".to_owned(), ProducerId(1000), 3, 60_000, 0);
        entry.partitions.insert(TopicPartition {
            topic: "orders".to_owned(),
            partition: PartitionIndex(0),
        });

        dispatch_abort_markers(&coord, &mut entry)
            .await
            .expect("dispatch markers");

        // The abort marker is a single control record → LEO advances to 1.
        assert!(
            part.log_end_offset() == 1,
            "abort marker must be appended (LEO 1), got {:?}",
            part.log_end_offset()
        );
    }

    /// Without remote transport, a partition that is not hosted locally must
    /// fail the abort. Advancing the transaction without its marker would leave
    /// an open transaction in the data partition.
    #[tokio::test]
    async fn dispatch_abort_markers_rejects_missing_remote_transport() {
        let partitions = Arc::new(PartitionRegistry::new());
        let coord = TxnCoordinator::new(
            krabka_audit::NodeId(1),
            partitions,
            Arc::new(ProducerIdManager::new()),
            50,
            krabka_units::mebibytes(1),
        );
        let mut entry = TxnEntry::new_empty("tx-2".to_owned(), ProducerId(2000), 0, 60_000, 0);
        entry.partitions.insert(TopicPartition {
            topic: "ghost".to_owned(),
            partition: PartitionIndex(0),
        });
        assert!(dispatch_abort_markers(&coord, &mut entry).await.is_err());
    }

    #[derive(Clone, Copy, Default)]
    enum InitScheduling {
        #[default]
        YieldAfterEach,
        SpawnTogether,
    }

    #[derive(derive_more::From, derive_more::Into, Clone, Copy)]
    struct InitCallCount(usize);

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct ConcurrentInitSetup {
        #[default("tid")]
        tid: &'static str,
        identity: InitIdentity,
        scheduling: InitScheduling,
        #[default(InitCallCount(2))]
        count: InitCallCount,
    }

    #[bon::builder]
    async fn spawn_inits(
        coordinator: &Arc<TxnCoordinator>,
        #[builder(default = "tid")] tid: &'static str,
        #[builder(default)] identity: InitIdentity,
        #[builder(default)] scheduling: InitScheduling,
        #[builder(into, default = InitCallCount (2))] count: InitCallCount,
    ) -> Vec<JoinHandle<Result<InitProducerIdResponse, BrokerError>>> {
        let setup = ConcurrentInitSetup {
            tid,
            identity,
            scheduling,
            count,
        };
        let mut calls = Vec::new();
        for _ in 0..setup.count.0 {
            let coordinator = Arc::clone(coordinator);
            calls.push(tokio::spawn(async move {
                init_verified(&coordinator, setup.tid, setup.identity).await
            }));
            if matches!(setup.scheduling, InitScheduling::YieldAfterEach) {
                task::yield_now().await;
            }
        }
        calls
    }

    /// Kafka's `transactional.id.expiration.ms` default.
    const EXPIRY_MS: i64 = 604_800_000;

    /// A coordinator that leads the single `__transaction_state` partition,
    /// with one committed transactional id already persisted into it.
    async fn coordinator_with_completed_transaction(dir: &Path, tid: &str) -> SeededCoordinator {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: bootstrap::TOPIC.to_owned(),
            topic_id: uuid::Uuid::from_u128(1),
            partitions: 1,
            replication_factor: 1,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: bootstrap::TOPIC.to_owned(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1)],
            isr: vec![NodeId(1)],
            ..Default::default()
        }));

        let partitions = Arc::new(PartitionRegistry::new());
        let part = open_partition().log_dir(dir).topic(bootstrap::TOPIC).call();
        partitions.insert(
            bootstrap::TOPIC.into(),
            PartitionIndex(0),
            Arc::clone(&part),
        );
        let coordinator = Arc::new(TxnCoordinator::new(
            NodeId(1),
            partitions,
            Arc::new(ProducerIdManager::new()),
            1,
            krabka_units::mebibytes(1),
        ));
        coordinator
            .refresh_leader_partitions(&image)
            .await
            .finished()
            .await;

        let mut entry = TxnEntry::new_empty(tid.to_owned(), ProducerId(1000), 3, 60_000, 0);
        entry.state = TxnState::CompleteCommit;
        entry.last_update_ms = 0;
        coordinator
            .put(entry, TxnVersion::Verified)
            .await
            .expect("seed __transaction_state");
        SeededCoordinator {
            coordinator,
            partition: part,
        }
    }

    /// Seeds one `TxnEntry` into a coordinator that already leads its
    /// `__transaction_state` partition.
    async fn seed(coordinator: &TxnCoordinator, entry: TxnEntry) {
        coordinator
            .put(entry, TxnVersion::Verified)
            .await
            .expect("seed __transaction_state");
    }

    /// A transactional id the coordinator has no entry for is answered with a
    /// freshly allocated identity even when the caller names one of its own.
    ///
    /// Kafka's `isValidProducerId` opens with `txnMetadata.producerEpoch ==
    /// RecordBatch.NO_PRODUCER_EPOCH`, and the metadata
    /// `handleInitProducerId` creates for an unknown id always satisfies it:
    /// the case the clause exists for is a producer recovering from
    /// `UNKNOWN_PRODUCER_ID`, which names the identity it last held and must
    /// get the new one back rather than `PRODUCER_FENCED`.
    #[tokio::test]
    async fn an_unknown_transactional_id_is_not_fenced_by_a_supplied_identity() {
        const SEEDED: &str = "tid-seeded";
        const UNKNOWN: &str = "tid-unknown";

        let dir = tempfile::tempdir().expect("tempdir");
        let SeededCoordinator {
            coordinator,
            partition: _part,
        } = coordinator_with_completed_transaction(dir.path(), SEEDED).await;
        check!(coordinator.get(UNKNOWN).is_none());

        let response = init_verified(
            &coordinator,
            UNKNOWN,
            InitIdentity::Existing(InitIdentitySetup {
                producer_id: ProducerId(4242),
                producer_epoch: ProducerEpoch(7),
            }),
        )
        .await
        .expect("init responds");

        check!(response.error_code == codes::NONE);
        check!(response.producer_id != 4242);
        check!(response.producer_epoch == 0);
    }

    /// A coordinator that leads `__transaction_state-0`, with one ongoing
    /// transaction at `(1000, 3)` over a local `orders-0` data partition. The
    /// data partition comes back so a test can read the abort marker.
    async fn coordinator_with_ongoing_transaction(dir: &Path, tid: &str) -> SeededCoordinator {
        let SeededCoordinator {
            coordinator,
            partition: _part,
        } = coordinator_with_completed_transaction(dir, tid).await;
        let data_dir = partition_dir(dir, "orders", 0);
        fs::create_dir_all(&data_dir).expect("create the data partition directory");
        let data = spawn_standalone_partition()
            .log_dir(dir)
            .log(Log::open(&data_dir, LogConfig::default()).expect("open the data log"))
            .call();
        // The metadata reconcile installs this broker, node 1, as the leader.
        data.install_leader_change(1, 0).await;
        coordinator
            .partitions
            .insert("orders".into(), PartitionIndex(0), Arc::clone(&data));
        let handle = coordinator.get(tid).expect("the seeded entry");
        let ongoing = {
            let mut entry = handle.lock().await;
            entry.state = TxnState::Ongoing;
            entry.partitions.insert(TopicPartition {
                topic: "orders".to_owned(),
                partition: PartitionIndex(0),
            });
            entry.clone()
        };
        coordinator
            .put(ongoing, TxnVersion::Verified)
            .await
            .expect("seed the ongoing transaction");
        SeededCoordinator {
            coordinator,
            partition: data,
        }
    }

    /// The producer epoch of the one control marker in `partition`.
    fn marker_producer_epoch(partition: &Partition) -> Option<i16> {
        let read = partition
            .read_log(krabka_log::Offset(0), krabka_units::mebibytes(1))
            .expect("read the data partition");
        read.batches.first().map(|batch| batch.producer_epoch)
    }

    /// Kafka `prepareInitProducerIdTransit` on an `ONGOING` transaction:
    /// `prepareFenceProducerEpoch` names the epoch above the live one, the
    /// coordinator aborts the transaction at that epoch, and the client gets
    /// `CONCURRENT_TRANSACTIONS`. The retry then bumps the epoch a second
    /// time.
    ///
    /// Below transaction version 2 the fence raises the epoch and keeps no last
    /// epoch, so the epoch the producer held is fenced. At version 2 the fence
    /// raises nothing and `prepareAbortOrCommit` bumps once, keeping the held
    /// epoch as the last epoch, so a retry that names it is answered the epoch
    /// the abort landed on.
    #[tokio::test]
    async fn an_ongoing_transaction_is_fenced_aborted_and_answered_concurrent() {
        struct Case {
            name: &'static str,
            txnv: TxnVersion,
            /// The epoch the abort completes at, which is also the epoch its
            /// markers carry.
            aborted_epoch: i16,
            /// The last epoch the completed abort records, `-1` for none.
            last_epoch: i16,
            /// What the producer that held the fenced epoch is answered when it
            /// names it again.
            stale: Answer,
            /// The epoch the retry of a producer that names no identity
            /// hands to the client.
            retried_epoch: i16,
        }
        let cases = [
            Case {
                name: "transaction version 1",
                txnv: TxnVersion::Flexible,
                aborted_epoch: 4,
                last_epoch: -1,
                stale: Answer {
                    error_code: codes::PRODUCER_FENCED,
                    producer_id: -1,
                    producer_epoch: -1,
                },
                retried_epoch: 5,
            },
            Case {
                name: "transaction version 2",
                txnv: TxnVersion::Verified,
                aborted_epoch: 4,
                last_epoch: 3,
                stale: Answer {
                    error_code: codes::NONE,
                    producer_id: 1000,
                    producer_epoch: 4,
                },
                retried_epoch: 5,
            },
        ];
        for case in cases {
            let dir = tempfile::tempdir().expect("tempdir");
            let SeededCoordinator {
                coordinator,
                partition: data,
            } = coordinator_with_ongoing_transaction(dir.path(), "tid-fenced").await;

            let fenced = init_at(&coordinator, "tid-fenced", case.txnv, (1000, 3)).await;
            check!(
                fenced == concurrent_transactions_response(),
                "{}",
                case.name
            );
            let entry = coordinator
                .get("tid-fenced")
                .expect("entry")
                .lock()
                .await
                .clone();
            check!(
                (
                    entry.state,
                    entry.producer_id,
                    entry.producer_epoch,
                    entry.last_producer_epoch
                ) == (
                    TxnState::CompleteAbort,
                    ProducerId(1000),
                    case.aborted_epoch,
                    case.last_epoch
                ),
                "{}",
                case.name
            );
            // The abort marker fenced the producer at its data partition: the
            // marker carries an epoch above the one the client still holds.
            check!(
                marker_producer_epoch(&data) == Some(case.aborted_epoch),
                "{}: marker epoch",
                case.name
            );

            // Kafka's `prepareIncrementProducerEpoch` matches an expected
            // epoch against the current and the last epoch only. The
            // producer that held the fenced epoch is fenced below
            // transaction version 2, where the fence keeps no last epoch, and
            // it is answered the epoch the abort landed on at version 2, where
            // that epoch is the last one.
            let stale = init_at(&coordinator, "tid-fenced", case.txnv, (1000, 3)).await;
            check!(
                stale
                    == InitProducerIdResponse {
                        error_code: case.stale.error_code,
                        producer_id: case.stale.producer_id,
                        producer_epoch: case.stale.producer_epoch,
                        ..Default::default()
                    },
                "{}: the stale identity",
                case.name
            );

            // `initTransactions()` names no identity, so the retry bumps the
            // epoch a second time, as the comment in
            // `prepareInitProducerIdTransit` says.
            let retried = init_at(&coordinator, "tid-fenced", case.txnv, (-1, -1)).await;
            check!(
                retried
                    == InitProducerIdResponse {
                        producer_id: 1000,
                        producer_epoch: case.retried_epoch,
                        ..Default::default()
                    },
                "{}: the retry",
                case.name
            );
        }
    }

    /// KIP-360: a retry of an epoch bump whose response was lost gets the
    /// epoch the coordinator already wrote, and bumps nothing. Kafka's
    /// `prepareIncrementProducerEpoch` recognises it by the last epoch, which
    /// every bump records.
    #[tokio::test]
    async fn a_retried_epoch_bump_answers_the_epoch_it_already_wrote() {
        const TID: &str = "tid-retried-bump";

        struct Step {
            name: &'static str,
            request: RequestedIdentity,
            answer: Answer,
            /// Whether the call appended a `__transaction_state` record.
            log_mutation: LogMutation,
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let SeededCoordinator {
            coordinator,
            partition: part,
        } = coordinator_with_completed_transaction(dir.path(), TID).await;
        let steps = [
            Step {
                name: "the live identity bumps the epoch",
                request: RequestedIdentity {
                    producer_id: 1000,
                    producer_epoch: 3,
                },
                answer: Answer {
                    error_code: codes::NONE,
                    producer_id: 1000,
                    producer_epoch: 4,
                },
                log_mutation: LogMutation::Appended,
            },
            Step {
                name: "the retry answers the bumped epoch and writes nothing",
                request: RequestedIdentity {
                    producer_id: 1000,
                    producer_epoch: 3,
                },
                answer: Answer {
                    error_code: codes::NONE,
                    producer_id: 1000,
                    producer_epoch: 4,
                },
                log_mutation: LogMutation::Unchanged,
            },
            Step {
                name: "the bumped epoch bumps again",
                request: RequestedIdentity {
                    producer_id: 1000,
                    producer_epoch: 4,
                },
                answer: Answer {
                    error_code: codes::NONE,
                    producer_id: 1000,
                    producer_epoch: 5,
                },
                log_mutation: LogMutation::Appended,
            },
            Step {
                name: "an older epoch is fenced",
                request: RequestedIdentity {
                    producer_id: 1000,
                    producer_epoch: 3,
                },
                answer: Answer {
                    error_code: codes::PRODUCER_FENCED,
                    producer_id: -1,
                    producer_epoch: -1,
                },
                log_mutation: LogMutation::Unchanged,
            },
            Step {
                name: "another producer id is fenced",
                request: RequestedIdentity {
                    producer_id: 1001,
                    producer_epoch: 5,
                },
                answer: Answer {
                    error_code: codes::PRODUCER_FENCED,
                    producer_id: -1,
                    producer_epoch: -1,
                },
                log_mutation: LogMutation::Unchanged,
            },
            Step {
                name: "a caller that names no identity bumps",
                request: RequestedIdentity {
                    producer_id: -1,
                    producer_epoch: -1,
                },
                answer: Answer {
                    error_code: codes::NONE,
                    producer_id: 1000,
                    producer_epoch: 6,
                },
                log_mutation: LogMutation::Appended,
            },
            Step {
                name: "and records no last epoch, so the old epoch is fenced",
                request: RequestedIdentity {
                    producer_id: 1000,
                    producer_epoch: 5,
                },
                answer: Answer {
                    error_code: codes::PRODUCER_FENCED,
                    producer_id: -1,
                    producer_epoch: -1,
                },
                log_mutation: LogMutation::Unchanged,
            },
        ];
        let mut expected = Vec::new();
        let mut actual = Vec::new();
        for step in steps {
            let before = part.log_end_offset().0;
            let response = handle_transactional(
                &coordinator,
                TransactionalInit::builder()
                    .tid(TID)
                    .txnv(TxnVersion::Verified)
                    .txn_timeout(60_000)
                    .request_identity((step.request.producer_id, step.request.producer_epoch))
                    .build(),
            )
            .await
            .expect("init responds");
            expected.push((step.name, step.answer, step.log_mutation));
            actual.push((
                step.name,
                Answer {
                    error_code: response.error_code,
                    producer_id: response.producer_id,
                    producer_epoch: response.producer_epoch,
                },
                if part.log_end_offset().0 > before {
                    LogMutation::Appended
                } else {
                    LogMutation::Unchanged
                },
            ));
        }
        assert!(actual == expected);
    }

    /// Kafka rotates the producer id once the epoch is exhausted, records the
    /// rotated id as the previous one, and admits the old id at that exhausted
    /// epoch as a retry (`isValidProducerId`).
    #[tokio::test]
    async fn a_rotation_at_the_exhausted_epoch_admits_the_old_identity_as_a_retry() {
        const TID: &str = "tid-rotated";

        let dir = tempfile::tempdir().expect("tempdir");
        let SeededCoordinator { coordinator, .. } =
            coordinator_with_completed_transaction(dir.path(), TID).await;
        {
            let handle = coordinator.get(TID).expect("the seeded entry");
            let mut entry = handle.lock().await;
            entry.producer_epoch = i16::MAX - 1;
        }

        let rotated = init_near_exhaustion(&coordinator, TID).await;
        check!(rotated.error_code == codes::NONE);
        check!(rotated.producer_id != 1000);
        check!(rotated.producer_epoch == 0);
        let entry = coordinator.get(TID).expect("entry").lock().await.clone();
        check!(entry.prev_producer_id == ProducerId(1000));
        check!(entry.last_producer_epoch == i16::MAX - 1);

        let retried = init_near_exhaustion(&coordinator, TID).await;
        check!(
            (
                retried.error_code,
                retried.producer_id,
                retried.producer_epoch
            ) == (codes::NONE, rotated.producer_id, 0)
        );
    }

    /// Two `InitProducerId` v3 calls that name the same live identity race for
    /// one transactional id. Exactly one bump lands: the identity check and
    /// the epoch bump it admits run under one lock, so the loser wakes to the
    /// epoch the winner already wrote.
    ///
    /// Kafka cannot tell that second call from the retry of a lost response,
    /// so `prepareIncrementProducerEpoch` answers both with the same identity
    /// and writes once (`expectedProducerEpoch == lastProducerEpoch`).
    ///
    /// With the check hoisted above the mutation, both calls read epoch 3,
    /// both bump, and the first caller's response is already stale when it
    /// reaches the client -- two live producers for one transactional id.
    ///
    /// The runtime is single-threaded and both tasks are stepped to their park
    /// on the entry lock with an explicit yield, so the interleaving is the
    /// same every run.
    #[tokio::test]
    async fn overlapping_inits_for_one_identity_answer_one_bumped_identity() {
        const TID: &str = "tid-overlapping-init";

        let dir = tempfile::tempdir().expect("tempdir");
        let SeededCoordinator { coordinator, .. } =
            coordinator_with_completed_transaction(dir.path(), TID).await;

        // Both calls park on the entry lock while the test holds it.
        let handle = coordinator.get(TID).expect("the seeded entry");
        let guard = handle.lock().await;

        let calls = spawn_inits()
            .coordinator(&coordinator)
            .tid(TID)
            .call()
            .await;
        drop(guard);

        let mut answered = Vec::new();
        for call in calls {
            let response = call.await.expect("init task").expect("init responds");
            answered.push((
                response.error_code,
                response.producer_id,
                response.producer_epoch,
            ));
        }
        check!(answered == vec![(codes::NONE, 1000, 4), (codes::NONE, 1000, 4)]);

        // One bump landed, not two.
        let entry = coordinator.get(TID).expect("entry").lock().await.clone();
        check!((entry.producer_id, entry.producer_epoch) == (ProducerId(1000), 4));
    }

    /// Two first `InitProducerId` calls race for one unknown transactional id.
    /// Kafka's `putTransactionStateIfNotExists` gives the loser the winner's
    /// metadata, which it finds mid-transition and answers
    /// `CONCURRENT_TRANSACTIONS`, so one identity exists. Both calls allocate a
    /// producer id, which yields; the test holds the partition's write lock so
    /// both are parked behind it when the winner creates the entry.
    #[tokio::test]
    async fn two_first_inits_for_one_unknown_id_create_one_identity() {
        const TID: &str = "tid-fresh-race";

        let dir = tempfile::tempdir().expect("tempdir");
        let SeededCoordinator { coordinator, .. } =
            coordinator_with_completed_transaction(dir.path(), "tid-seeded").await;
        check!(coordinator.get(TID).is_none());

        let write_lock = coordinator.lock_state_partition_for(TID).await;
        let calls = spawn_inits()
            .coordinator(&coordinator)
            .tid(TID)
            .identity(InitIdentity::Anonymous)
            .scheduling(InitScheduling::SpawnTogether)
            .call()
            .await;
        for _ in 0..10 {
            task::yield_now().await;
        }
        drop(write_lock);

        let mut answers = Vec::new();
        for call in calls {
            let response = call.await.expect("init task").expect("init responds");
            answers.push((
                response.error_code,
                response.producer_id,
                response.producer_epoch,
            ));
        }
        // NONE sorts before CONCURRENT_TRANSACTIONS.
        answers.sort_unstable();
        let entry = coordinator.get(TID).expect("the winner's entry");
        let entry = entry.lock().await.clone();
        check!(
            answers
                == vec![
                    (codes::NONE, entry.producer_id.get(), 0),
                    (codes::CONCURRENT_TRANSACTIONS, -1, -1),
                ]
        );
        check!(entry.producer_epoch == 0);
    }

    /// A failed abort fan-out records the epoch fence on the entry the
    /// coordinator publishes, not on the handle this call started from: `put`
    /// republishes the tid under a fresh `Arc`, so a write to the superseded
    /// handle is invisible to the retry that comes to read it.
    ///
    /// The `PrepareAbort` record is durable, so the call answers
    /// `CONCURRENT_TRANSACTIONS` and queues the abort for completion, as
    /// Kafka's fence abort does. While the abort is pending, every caller
    /// that names the transaction's producer ID gets the same answer. Once
    /// the abort completes, KIP-360 lets exactly the producer that still holds
    /// the pre-fence epoch retry, and a zombie that names any other epoch is
    /// fenced.
    ///
    /// Only below transaction version 2 does the entry carry the failed fence
    /// flag: at version 2 the fence raises no epoch, so Kafka never sets
    /// `hasFailedEpochFence` there, and the `PrepareAbort` record already keeps
    /// the pre-fence epoch as its last epoch.
    #[tokio::test]
    async fn a_failed_abort_fan_out_records_the_fence_and_completes_before_a_retry() {
        const TID: &str = "tid-failed-fence";

        // (transaction version, whether the fence is recorded as failed)
        for (txnv, failed_fence) in [(TxnVersion::Flexible, true), (TxnVersion::Verified, false)] {
            let dir = tempfile::tempdir().expect("tempdir");
            let SeededCoordinator { coordinator, .. } =
                coordinator_with_completed_transaction(dir.path(), TID).await;

            // An ongoing transaction over a partition this broker does not
            // host yet: the abort marker cannot be delivered, so the fan-out
            // fails after the epoch fence is already persisted.
            let mut ongoing = TxnEntry::new_empty(TID.to_owned(), ProducerId(1000), 3, 60_000, 0);
            ongoing.state = TxnState::Ongoing;
            ongoing.partitions.insert(TopicPartition {
                topic: "ghost".to_owned(),
                partition: PartitionIndex(0),
            });
            seed(&coordinator, ongoing).await;

            let init = |identity: (i64, i16)| {
                let coordinator = Arc::clone(&coordinator);
                async move {
                    handle_transactional(
                        &coordinator,
                        TransactionalInit::builder()
                            .tid(TID)
                            .txnv(txnv)
                            .txn_timeout(60_000)
                            .request_identity(identity)
                            .build(),
                    )
                    .await
                    .expect("InitProducerId responds")
                }
            };
            let concurrent = concurrent_transactions_response();

            check!(init((1000, 3)).await == concurrent, "{txnv:?}");
            let published = coordinator.get(TID).expect("entry").lock().await.clone();
            check!(published.has_failed_epoch_fence == failed_fence, "{txnv:?}");
            check!(published.last_producer_epoch == 3, "{txnv:?}");
            check!(published.state == TxnState::PrepareAbort, "{txnv:?}");

            // While the abort is pending, the producer ID decides, not the
            // epoch.
            for identity in [(1000, 2), (1000, 3), (-1, -1)] {
                check!(init(identity).await == concurrent, "{txnv:?} {identity:?}");
            }
            check!(init((2000, 0)).await == fenced_response(), "{txnv:?}");

            // The partition appears, and the completion task finishes the
            // abort.
            let ghost_dir = partition_dir(dir.path(), "ghost", 0);
            fs::create_dir_all(&ghost_dir).expect("create ghost partition dir");
            let ghost = spawn_standalone_partition()
                .log_dir(dir.path())
                .log(Log::open(&ghost_dir, LogConfig::default()).expect("open ghost log"))
                .topic("ghost")
                .call();
            ghost.install_leader_change(1, 0).await;
            coordinator
                .partitions
                .insert("ghost".into(), PartitionIndex(0), ghost);
            check!(
                coordinator.complete_prepared_transaction(TID, txnv).await
                    == CompletionAttempt::Completed,
                "{txnv:?}"
            );

            // Kafka's `prepareComplete` clears the failed fence, so a later
            // fence of this transactional id raises the epoch again.
            let completed = coordinator.get(TID).expect("entry").lock().await.clone();
            check!(!completed.has_failed_epoch_fence, "{txnv:?}");

            check!(
                init((1000, 2)).await.error_code == codes::PRODUCER_FENCED,
                "{txnv:?}"
            );
            check!(init((1000, 3)).await.error_code == codes::NONE, "{txnv:?}");
        }
    }

    /// The KIP-98 expiry sweep and an `InitProducerId` already parked on the
    /// entry's lock must never both persist an identity for one transactional
    /// id.
    ///
    /// The sweep unpublishes the entry from the coordinator's map while this
    /// call holds a clone of its `Arc`. Reviving through that detached handle
    /// would append a producer identity for a tid whose tombstone is already
    /// in the log, and a second `InitProducerId` that found no entry would
    /// allocate a competing one -- two live identities for one id, with
    /// whichever append landed last deciding the coordinator's state. The
    /// sweep marks the entry `Dead` under the same lock, so the parked call
    /// wakes to Kafka's `CONCURRENT_TRANSACTIONS` and retries onto the
    /// fresh-id path.
    ///
    /// The runtime is single-threaded and each task is stepped to its park
    /// with an explicit yield, so the interleaving is the same every run:
    /// the sweep queues on the lock first, the call queues behind it.
    #[tokio::test]
    async fn an_init_parked_on_the_expiry_sweep_does_not_revive_the_tombstoned_id() {
        const TID: &str = "tid-parked-init";

        let dir = tempfile::tempdir().expect("tempdir");
        let SeededCoordinator {
            coordinator,
            partition: part,
        } = coordinator_with_completed_transaction(dir.path(), TID).await;
        assert!(part.log_end_offset() == 1);

        // The test holds the entry lock, so both tasks below park on it in
        // the order they are stepped.
        let handle = coordinator.get(TID).expect("the seeded entry");
        let guard = handle.lock().await;

        let sweep = {
            let coordinator = Arc::clone(&coordinator);
            tokio::spawn(async move {
                coordinator
                    .expire_transactional_ids(EXPIRY_MS + 1, EXPIRY_MS)
                    .await
            })
        };
        task::yield_now().await;

        let init = {
            let coordinator = Arc::clone(&coordinator);
            tokio::spawn(
                async move { init_verified(&coordinator, TID, InitIdentity::Anonymous).await },
            )
        };
        task::yield_now().await;

        drop(guard);
        let expired = sweep.await.expect("sweep task");
        let response = init.await.expect("init task").expect("init responds");

        check!(expired == vec![TID.to_owned()]);
        check!(response == concurrent_transactions_response());
        // The id stays expired: nothing was published back into the map, and
        // the log ends at the tombstone the sweep appended.
        check!(coordinator.get(TID).is_none());
        check!(part.log_end_offset() == 2);
    }

    /// KIP-360 + producer id rotation: an `Ongoing` transaction fenced and
    /// aborted right at the epoch-exhaustion boundary rotates to a fresh
    /// producer id during the abort's own completion, exactly as a normal
    /// commit or abort does (`prepare_completion_identities`). The client
    /// that owned the exhausted epoch is not fenced: Kafka records that epoch
    /// as the last epoch of the rotation, and `isValidProducerId` admits the
    /// old id at an exhausted epoch, so its retry is answered the rotated pair.
    /// A caller that names no identity -- Kafka's `initTransactions()` --
    /// bumps the rotated pair.
    #[tokio::test]
    async fn an_ongoing_transaction_fenced_at_the_exhausted_epoch_rotates_the_producer_id() {
        const TID: &str = "tid-abort-rotation";

        let tmp = tempfile::tempdir().expect("tempdir");
        let SeededCoordinator { coordinator, .. } =
            coordinator_with_completed_transaction(tmp.path(), TID).await;

        // An Ongoing transaction at the exhausted epoch boundary, with no
        // partitions, so the abort's marker fan-out succeeds at once and its
        // own `prepare_completion_identities` rotates to a fresh producer id.
        let ongoing = TxnEntry {
            state: TxnState::Ongoing,
            ..TxnEntry::new_empty(TID.to_owned(), ProducerId(1000), i16::MAX - 1, 60_000, 0)
        };
        seed(&coordinator, ongoing).await;

        let fenced = init_near_exhaustion(&coordinator, TID).await;
        check!(fenced == concurrent_transactions_response());

        let entry = coordinator.get(TID).expect("entry").lock().await.clone();
        check!(entry.state == TxnState::CompleteAbort);
        check!(
            entry.producer_id != ProducerId(1000),
            "the exhausted epoch rotates the producer id"
        );
        // The pre-abort identity is the one recorded, not whatever the
        // abort's own completion step landed on.
        check!(entry.prev_producer_id == ProducerId(1000));

        // The producer that names its old, exhausted identity again is
        // recognised as a retry of the rotation, and answered the rotated pair
        // without a write.
        let stale = init_near_exhaustion(&coordinator, TID).await;
        check!(
            stale
                == InitProducerIdResponse {
                    producer_id: entry.producer_id.get(),
                    producer_epoch: entry.producer_epoch,
                    ..Default::default()
                }
        );

        // `initTransactions()` names no identity, so the retry gets the
        // rotated identity the abort's completion already staged.
        let retried = init_verified(&coordinator, TID, InitIdentity::Anonymous)
            .await
            .expect("init responds");
        check!(retried.error_code == codes::NONE);
        check!(retried.producer_id == entry.producer_id.get());
        check!(retried.producer_epoch == entry.producer_epoch + 1);
    }

    /// A zombie holding the retry token from an earlier, unrelated epoch
    /// bump must not be admitted once that generation has since been fenced
    /// and aborted.
    ///
    /// A legitimate bump from epoch 4 to 5 records 4 as the recognized retry
    /// token. The live producer then opens a transaction at epoch 5 without
    /// another `InitProducerId` call, exactly as `a_stale_retry_answers_...`
    /// sets up. A second `InitProducerId` call finds that transaction
    /// `Ongoing` and fences it: abort, `CompleteAbort` at epoch 6, and the
    /// epoch 5 the fenced producer held becomes the last epoch. The old
    /// token from the 4->5 bump names a generation the fence just ended, so
    /// a zombie that still holds it must be fenced, not answered the live
    /// identity as a recognized retry.
    #[tokio::test]
    async fn a_fence_clears_the_retry_token_from_an_earlier_bump() {
        const TID: &str = "tid-stale-token-across-fence";

        let dir = tempfile::tempdir().expect("tempdir");
        let SeededCoordinator { coordinator, .. } =
            coordinator_with_completed_transaction(dir.path(), TID).await;

        // The seeded entry starts at epoch 3. Bump it once (3 -> 4) so the
        // legitimate 4 -> 5 bump below has a live epoch to name.
        init_verified(
            &coordinator,
            TID,
            InitIdentity::Existing(InitIdentitySetup::default()),
        )
        .await
        .expect("first bump responds");

        // The legitimate 4 -> 5 bump: recorded epoch 4 as the retry token.
        let bumped = init_verified(
            &coordinator,
            TID,
            InitIdentity::Existing(InitIdentitySetup {
                producer_epoch: ProducerEpoch(4),
                ..Default::default()
            }),
        )
        .await
        .expect("bump responds");
        check!(
            (bumped.error_code, bumped.producer_id, bumped.producer_epoch)
                == (codes::NONE, 1000, 5)
        );
        {
            let handle = coordinator.get(TID).expect("bumped entry");
            let mut entry = handle.lock().await;
            check!(entry.last_producer_epoch == 4);
            entry.state = TxnState::Ongoing;
            entry.start_ms = 0;
        }

        // A second InitProducerId call finds the transaction Ongoing and
        // fences it, aborting at epoch 6.
        // (Kafka `prepareAbortOrCommit` at TV2: `epoch + 1`, `lastEpoch = epoch`.)
        let fenced = init_verified(
            &coordinator,
            TID,
            InitIdentity::Existing(InitIdentitySetup {
                producer_epoch: ProducerEpoch(5),
                ..Default::default()
            }),
        )
        .await
        .expect("init responds");
        check!(fenced == concurrent_transactions_response());
        let entry = coordinator.get(TID).expect("entry").lock().await.clone();
        // TV2 bumps once, and the epoch the fenced producer held is the last
        // epoch.
        check!(
            (entry.state, entry.producer_epoch, entry.last_producer_epoch)
                == (TxnState::CompleteAbort, 6, 5)
        );

        // The zombie holding the pre-bump token (4) belongs to the
        // generation the fence just ended and must be fenced, not admitted
        // as a retry of the entry the fence produced.
        let zombie = init_verified(
            &coordinator,
            TID,
            InitIdentity::Existing(InitIdentitySetup {
                producer_epoch: ProducerEpoch(4),
                ..Default::default()
            }),
        )
        .await
        .expect("init responds");
        check!(zombie.error_code == codes::PRODUCER_FENCED, "{zombie:?}");
    }

    /// KIP-360: a caller retrying its own lost `InitProducerId` response must
    /// never abort a transaction the live producer has since opened at the
    /// epoch that response bumped to, whether or not another
    /// `InitProducerId` call carried that epoch to the producer.
    ///
    /// Kafka's `prepareIncrementProducerEpoch` decides a retry from the
    /// identity alone, never from the entry's state: the retry answers the
    /// current identity and touches nothing.
    #[tokio::test]
    async fn a_stale_retry_answers_the_bumped_identity_without_aborting_the_live_transaction() {
        const TID: &str = "tid-stale-retry-vs-live-ongoing";

        let dir = tempfile::tempdir().expect("tempdir");
        let SeededCoordinator {
            coordinator,
            partition: part,
        } = coordinator_with_completed_transaction(dir.path(), TID).await;

        // Bumps epoch 3 -> 4 and records 3 as the last epoch, exactly as a
        // caller whose response was lost would leave it.
        let bumped = init_verified(
            &coordinator,
            TID,
            InitIdentity::Existing(InitIdentitySetup::default()),
        )
        .await
        .expect("bump responds");
        check!(
            (bumped.error_code, bumped.producer_id, bumped.producer_epoch)
                == (codes::NONE, 1000, 4)
        );
        check!(part.log_end_offset() == 2);

        // The live producer opens a transaction at the bumped epoch without
        // another `InitProducerId` call -- Kafka's implicit-begin path, where
        // `AddPartitionsToTxn` opens a transaction directly once a producer
        // holds a live epoch.
        {
            let handle = coordinator.get(TID).expect("bumped entry");
            let mut entry = handle.lock().await;
            entry.state = TxnState::Ongoing;
            entry.start_ms = 0;
        }

        // The zombie's retransmitted InitProducerId, still naming the
        // pre-bump epoch, arrives after the live transaction is already
        // open.
        let retried = init_verified(
            &coordinator,
            TID,
            InitIdentity::Existing(InitIdentitySetup::default()),
        )
        .await
        .expect("retry responds");

        check!(
            (
                retried.error_code,
                retried.producer_id,
                retried.producer_epoch
            ) == (codes::NONE, 1000, 4)
        );
        // Nothing appended: the retry answered from the entry alone.
        check!(part.log_end_offset() == 2);
        // The live transaction is untouched.
        let entry = coordinator.get(TID).expect("entry").lock().await.clone();
        check!(entry.state == TxnState::Ongoing);
    }

    /// PR #1046 fixed this exact bug class for `AddOffsetsToTxn`
    /// (`add_offsets_partition`): the shared entry must publish the bumped
    /// identity only through the append that makes it durable, into the very
    /// same handle a caller from before this call already holds -- never a
    /// detached copy, and never before `coord.put_under_state_partition_lock`
    /// returns `Ok`.
    ///
    /// This locks in the fix's shape (stage on a local clone, append while
    /// still holding the entry lock, publish into `*e3` only on success): a
    /// caller that captured the handle before the bump ran sees the durable
    /// outcome afterward, on the exact same `Arc`, with nothing appended
    /// beyond that one durable record.
    #[tokio::test]
    async fn a_caller_already_holding_the_handle_sees_the_durable_bumped_identity() {
        const TID: &str = "tid-durable-publish";

        let dir = tempfile::tempdir().expect("tempdir");
        let SeededCoordinator {
            coordinator,
            partition: part,
        } = coordinator_with_completed_transaction(dir.path(), TID).await;

        // Captured before the bump call, the way a concurrent caller parked
        // on this same tid's lock would hold it.
        let pre_call_handle = coordinator.get(TID).expect("the seeded entry");

        let response = init_verified(
            &coordinator,
            TID,
            InitIdentity::Existing(InitIdentitySetup::default()),
        )
        .await
        .expect("bump responds");
        check!(
            (
                response.error_code,
                response.producer_id,
                response.producer_epoch
            ) == (codes::NONE, 1000, 4)
        );

        // One append: the seed, then the bump.
        check!(part.log_end_offset() == 2);

        // The pre-existing handle reflects the same durable identity as the
        // response, not a stale snapshot and not a copy that only a fresh
        // `coord.get` would see.
        let via_old_handle = pre_call_handle.lock().await.clone();
        check!(via_old_handle.producer_id == ProducerId(1000));
        check!(via_old_handle.producer_epoch == 4);
        check!(via_old_handle.state == TxnState::Empty);
    }

    /// The `CompleteAbort` entry that the fence of an `Ongoing` transaction
    /// writes, compared field for field.
    ///
    /// The prior producer id is recorded against the identity the client held
    /// on the `Ongoing` entry -- the staged recovery identity when there is
    /// one -- not against the entry's own `producer_id`, which is where this
    /// path differs from the completion task.
    #[tokio::test]
    async fn a_fenced_abort_writes_the_complete_abort_entry() {
        const TID: &str = "tid-fenced-complete-abort";

        struct Case {
            name: &'static str,
            txnv: TxnVersion,
            ongoing: TxnEntry,
            expected: TxnEntry,
        }
        let ongoing = |epoch| TxnEntry {
            state: TxnState::Ongoing,
            ..TxnEntry::new_empty(TID.to_owned(), ProducerId(1000), epoch, 60_000, 0)
        };
        let complete_abort = |producer_id, producer_epoch| TxnEntry {
            state: TxnState::CompleteAbort,
            client_transaction_version: TxnVersion::Verified.level(),
            ..TxnEntry::new_empty(TID.to_owned(), producer_id, producer_epoch, 60_000, 0)
        };
        let cases = [
            Case {
                name: "no staged identity",
                txnv: TxnVersion::Verified,
                ongoing: ongoing(3),
                expected: TxnEntry {
                    last_producer_epoch: 3,
                    ..complete_abort(ProducerId(1000), 4)
                },
            },
            Case {
                name: "a staged recovery identity",
                txnv: TxnVersion::Verified,
                ongoing: TxnEntry {
                    next_producer_id: ProducerId(2000),
                    next_producer_epoch: 0,
                    ..ongoing(3)
                },
                expected: TxnEntry {
                    last_producer_epoch: 3,
                    ..complete_abort(ProducerId(2000), 1)
                },
            },
            Case {
                name: "below transaction version 2",
                txnv: TxnVersion::Classic,
                ongoing: ongoing(3),
                expected: TxnEntry {
                    client_transaction_version: TxnVersion::Classic.level(),
                    ..complete_abort(ProducerId(1000), 4)
                },
            },
        ];
        for case in cases {
            let tmp = tempfile::tempdir().expect("tempdir");
            let SeededCoordinator { coordinator, .. } =
                coordinator_with_completed_transaction(tmp.path(), TID).await;
            seed(&coordinator, case.ongoing).await;

            let fenced = handle_transactional(
                &coordinator,
                TransactionalInit::builder()
                    .tid(TID)
                    .txnv(case.txnv)
                    .txn_timeout(60_000)
                    .build(),
            )
            .await
            .expect("init responds");
            check!(
                fenced == concurrent_transactions_response(),
                "{}",
                case.name
            );

            let entry = coordinator.get(TID).expect("entry").lock().await.clone();
            let expected = TxnEntry {
                last_update_ms: entry.last_update_ms,
                ..case.expected
            };
            check!(entry == expected, "{}", case.name);
        }
    }

    /// The fence of an `Ongoing` transaction at the exhausted epoch rotates to
    /// a fresh producer id and records the old one as the prior producer id.
    #[tokio::test]
    async fn a_fenced_abort_at_the_exhausted_epoch_writes_the_rotated_entry() {
        const TID: &str = "tid-fenced-rotation";

        let tmp = tempfile::tempdir().expect("tempdir");
        let SeededCoordinator { coordinator, .. } =
            coordinator_with_completed_transaction(tmp.path(), TID).await;
        seed(&coordinator, TxnEntry {
            state: TxnState::Ongoing,
            ..TxnEntry::new_empty(TID.to_owned(), ProducerId(1000), i16::MAX - 1, 60_000, 0)
        })
        .await;

        let fenced = init_verified(&coordinator, TID, InitIdentity::Anonymous)
            .await
            .expect("init responds");
        check!(fenced == concurrent_transactions_response());

        let entry = coordinator.get(TID).expect("entry").lock().await.clone();
        let expected = TxnEntry {
            state: TxnState::CompleteAbort,
            prev_producer_id: ProducerId(1000),
            last_producer_epoch: i16::MAX - 1,
            client_transaction_version: TxnVersion::Verified.level(),
            last_update_ms: entry.last_update_ms,
            ..TxnEntry::new_empty(TID.to_owned(), ProducerId(0), 0, 60_000, 0)
        };
        check!(entry == expected);
    }

    /// The fence's `CompleteAbort` must not overwrite a write that landed
    /// while its marker fan-out ran.
    ///
    /// The fence persists `PrepareAbort` and releases every lock for the
    /// fan-out. Meanwhile the completion task can finish the same abort, and a
    /// second `InitProducerId` can then bump the epoch and hand that identity
    /// to its producer. Kafka's marker channel appends the `Complete*`
    /// transition only while the coordinator still holds the metadata it was
    /// prepared for (`TransactionMarkerChannelManager`'s `appendToLogCallback`
    /// re-reads `getTransactionState`, and `TransactionMetadata.prepareComplete`
    /// requires the pending `PrepareAbort`), so a late completion is dropped
    /// and the bumped identity stays. The fence answers
    /// `CONCURRENT_TRANSACTIONS`, as it does whenever it fences.
    #[tokio::test]
    async fn a_fenced_abort_keeps_a_write_made_during_its_marker_fan_out() {
        const TID: &str = "tid-fence-race";

        let dir = tempfile::tempdir().expect("tempdir");
        let SeededCoordinator { coordinator, .. } =
            coordinator_with_ongoing_transaction(dir.path(), TID).await;

        // Hold the fence at the start of its marker fan-out, with its
        // `PrepareAbort` durable.
        coordinator
            .marker_fanout_gate
            .set_mode(MarkerFanoutMode::Hold);
        let fence = {
            let coordinator = Arc::clone(&coordinator);
            tokio::spawn(
                async move { init_verified(&coordinator, TID, InitIdentity::default()).await },
            )
        };
        coordinator.marker_fanout_gate.wait_for_arrivals(1).await;
        let prepared = coordinator.get(TID).expect("entry").lock().await.clone();
        check!(prepared.state == TxnState::PrepareAbort);

        // The completion task finishes the abort: the record
        // `complete_prepared_transaction` appends.
        let mut completed = prepared;
        let identity = completion_producer_identity(&completed);
        apply_completion(
            &mut completed,
            TxnState::CompleteAbort,
            identity,
            now_millis(),
        );
        seed(&coordinator, completed).await;

        // A producer that names no identity bumps the completed transaction.
        let bumped = init_verified(&coordinator, TID, InitIdentity::Anonymous)
            .await
            .expect("init responds");
        check!(bumped.error_code == codes::NONE);
        let live = coordinator.get(TID).expect("entry").lock().await.clone();
        check!(
            (live.producer_id.get(), live.producer_epoch)
                == (bumped.producer_id, bumped.producer_epoch)
        );

        coordinator
            .marker_fanout_gate
            .set_mode(MarkerFanoutMode::Open);
        let fenced = fence.await.expect("fence task").expect("init responds");

        check!(fenced == concurrent_transactions_response());
        let after = coordinator.get(TID).expect("entry").lock().await.clone();
        assert!(
            after == live,
            "the fence's CompleteAbort overwrote the bumped identity"
        );
    }
}
