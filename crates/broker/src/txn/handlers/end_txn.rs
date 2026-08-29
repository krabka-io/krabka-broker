//! `EndTxn` (`api_key=26`). Finalises a transaction. The producer calls
//! `commitTransaction()` or `abortTransaction()`, which drives two state
//! transitions with a `WriteTxnMarkers` fan-out in between.
//!
//! ## Flow
//!
//! 1. Verify coordinator-ness, pid, epoch.
//! 2. `Ongoing` → `PrepareCommit` (or `PrepareAbort`); persist.
//! 3. Fan out `WriteTxnMarkers` to every involved partition's leader:
//!    - **local** leader  → `Partition::produce_batch`.
//!    - **remote** leader → `WriteTxnMarkersRequest` over the shared
//!      `InterBrokerClient` (runs inter-broker TLS / SASL as the listener demands).
//! 4. `PrepareCommit` → `CompleteCommit` (or `PrepareAbort` → `CompleteAbort`); persist.
//! 5. Return `NONE` to the producer.
//!
//! Wire format: v0-2 non-flexible, v3-5 flexible (tagged fields).
//! Request fields: `transactional_id`, `producer_id`, `producer_epoch`, `committed`.
//! Response fields: `throttle_time_ms`, `error_code`.

use bytes::Bytes;
use krabka_log::ProducerId;
use krabka_protocol::{Decode, owned::end_txn_request::EndTxnRequest};

use crate::{
    broker::Broker,
    codes,
    error::BrokerError,
    txn::{
        decision::{CompletionDecision, decide_end_txn_completion},
        state::TxnEntry,
        util::now_millis,
    },
};

mod marker_rpc;
mod markers;
mod prepare;
mod producer_identity;
mod reacquire;
mod response;
mod validation;

#[cfg(test)]
mod test_support;

// Only the #[cfg(test)] stateright models in txn/ reach this variant.
#[cfg(test)]
pub(crate) use self::producer_identity::prepare_completion_identities_with_fresh;
use self::{
    markers::dispatch_transaction_markers,
    prepare::prepare_transaction,
    response::{encode_err, encode_ok},
    validation::{EndTxnValidation, validate_end_txn},
};
pub(crate) use self::{
    markers::{MarkerDispatchContext, dispatch_markers},
    producer_identity::{
        client_producer_identity, completion_producer_identity, next_producer_identity,
        next_recovery_producer_identity, prepare_completion_identities,
    },
    reacquire::{ReacquireDecision, validate_complete_reacquire},
};

#[tracing::instrument(
    name = "handle_end_txn",
    level = "info",
    skip_all,
    fields(api = "EndTxn", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let coord = broker.txn_coordinator.clone();
    let controller = broker.controller.clone();
    let authorizer = broker.config.authorizer.as_ref();
    let mut cur: &[u8] = req_bytes;
    let req = EndTxnRequest::decode(&mut cur, version)?;

    // Refresh leader-partition view from the current metadata image
    // before checking coordinator-ness.
    let image = controller.current_image();
    let txnv = crate::txn::version::resolve_txn_version(&image);
    coord.refresh_leader_partitions(&image).await;

    let tid = req.transactional_id.as_str();
    let entry_mutex = match validate_end_txn(&coord, authorizer, &image, ctx, &req).await {
        Ok(EndTxnValidation::Proceed(entry)) => entry,
        Ok(EndTxnValidation::AlreadyComplete(pid, epoch)) => {
            return encode_ok(version, pid.get(), epoch);
        }
        Err(code) => return encode_err(version, code),
    };

    // ── Phase 1: Ongoing → Prepare{Commit,Abort} ──────────────────────

    let (marker_type, prepare, complete, prepare_snap) =
        match prepare_transaction(&coord, &entry_mutex, req.committed, txnv, tid).await {
            Ok(prepared) => prepared,
            Err(code) => return encode_err(version, code),
        };

    // ── Phase 2: Fan out WriteTxnMarkers ──────────────────────────────

    if let Err(code) = dispatch_transaction_markers(broker, &prepare_snap, marker_type, tid).await {
        return encode_err(version, code);
    }

    // ── Phase 3: Prepare{Commit,Abort} → Complete{Commit,Abort} ───────
    //
    // The entry lock was *intentionally* dropped before the Phase-2 marker
    // fan-out (network I/O to remote brokers); holding it across the fan-out
    // would serialize/deadlock the coordinator. That window lets a concurrent
    // caller (another EndTxn, an AddPartitionsToTxn, or an InitProducerId that
    // bumps the epoch) interleave on this same transactional-id.
    //
    // We must NOT re-lock the original `entry_mutex` captured at the top of the
    // handler: `coord.put` replaces the coordinator's map slot with a *fresh*
    // `Arc<Mutex<TxnEntry>>` on every persist (see `TxnCoordinator::put`), so a
    // concurrent caller operates on a different Arc than the one we hold. The
    // only authoritative view is the entry currently registered under `tid`.
    //
    // Re-fetch it and re-validate that nothing advanced underneath us BEFORE
    // writing Complete. If the producer was fenced (epoch bumped) or the state
    // was advanced by another caller, abort this handler's Complete write and
    // return the matching Kafka error instead of blindly overwriting.
    let Some(current_mutex) = coord.get(tid) else {
        // The entry vanished (e.g. expired/deleted) while markers were in
        // flight. Treat as a producer-mapping loss.
        return encode_err(version, codes::INVALID_PRODUCER_ID_MAPPING);
    };

    // The completion identity was selected and persisted with the Prepare
    // state. Phase 3 adopts that identity after marker fan-out; it does not
    // allocate or increment it again.
    let response_pid;
    let response_epoch;
    let (prepared_completion_pid, prepared_completion_epoch) =
        completion_producer_identity(&prepare_snap);

    let complete_snap: TxnEntry = {
        let mut entry = current_mutex.lock().await;
        // The Prepare record already contains both identities: the marker uses
        // the incremented epoch of the producer that wrote the transaction,
        // while the staged completion identity is returned to the client. This
        // revalidation prevents Phase 3 from adopting a stale staged identity.
        match decide_end_txn_completion(
            &entry,
            prepare_snap.producer_id,
            prepare_snap.producer_epoch,
            prepared_completion_pid,
            prepared_completion_epoch,
            prepare,
            complete,
        ) {
            CompletionDecision::Proceed {
                next_state,
                response_pid: new_pid,
                response_epoch: new_epoch,
            } => {
                if new_pid != ProducerId(req.producer_id) {
                    // Epoch rolled over to a new producer_id: record the prior id
                    // so the transition is traceable (KIP-890 PreviousProducerId).
                    entry.prev_producer_id = ProducerId(req.producer_id);
                }
                entry.state = next_state;
                entry.last_update_ms = now_millis();
                entry.producer_id = new_pid;
                entry.producer_epoch = new_epoch;
                entry.next_producer_id = ProducerId(-1);
                entry.next_producer_epoch = -1;
                entry.partitions.clear();
                response_pid = new_pid;
                response_epoch = new_epoch;
                entry.clone()
            }
            CompletionDecision::AlreadyComplete {
                response_pid: pid,
                response_epoch: epoch,
            } => {
                // Another caller already drove this exact transition to
                // completion (or we are an idempotent EndTxn retry that lost the
                // race). Report success without re-writing, returning the
                // persisted (possibly already-bumped) identity so a KIP-890
                // client that retried picks up the authoritative value.
                return encode_ok(version, pid.get(), epoch);
            }
            CompletionDecision::Reject(code) => {
                tracing::warn!(
                    tid,
                    expected_epoch = req.producer_epoch,
                    found_epoch = entry.producer_epoch,
                    expected_state = ?prepare,
                    found_state = ?entry.state,
                    error_code = code,
                    "EndTxn: entry changed underneath the marker fan-out; \
                     aborting Complete write"
                );
                return encode_err(version, code);
            }
        }
        // Lock dropped here.
    };

    // FINAL put: move `complete_snap` in (no use-after-move below) to avoid the
    // redundant full `TxnEntry` clone (incl. the partition / offset-commit-group
    // sets) that the intermediate phases pay.
    if let Err(e) = coord.put(complete_snap, txnv).await {
        tracing::error!(
            tid,
            state = ?complete,
            error = %e,
            "EndTxn: failed to persist CompleteCommit/CompleteAbort"
        );
        return encode_err(version, codes::UNKNOWN_SERVER_ERROR);
    }

    // Unwrap the post-completion `ProducerId` into the raw-`i64` wire response.
    encode_ok(version, response_pid.get(), response_epoch)
}
