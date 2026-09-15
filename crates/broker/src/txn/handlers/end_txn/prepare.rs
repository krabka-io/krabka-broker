//! Phase 1 of `EndTxn`: the `Ongoing` → `Prepare{Commit,Abort}` transition.
//! It stages the producer identity the client continues with after completion,
//! then persists the Prepare record before any marker leaves this broker.

use super::producer_identity::prepare_completion_identities;
use crate::{
    codes,
    txn::{
        decision::decide_phase1_transition,
        marker::MarkerType,
        state::{TxnEntry, TxnState},
        util::now_millis,
    },
};

pub(super) async fn prepare_transaction(
    coordinator: &crate::txn::coordinator::TxnCoordinator,
    entry: &std::sync::Arc<tokio::sync::Mutex<TxnEntry>>,
    committed: bool,
    version: crate::txn::version::TxnVersion,
    transactional_id: &str,
) -> Result<(MarkerType, TxnState, TxnState, TxnEntry), i16> {
    let marker_type = if committed {
        MarkerType::Commit
    } else {
        MarkerType::Abort
    };
    // Lock order: the state-partition write lock, then the entry lock. The
    // reaper and the completion task take them in the same order.
    let _state_partition_write = coordinator.lock_state_partition_for(transactional_id).await;
    let mut state = entry.lock().await;
    if let Some(code) = coordinator.coordinator_error(transactional_id).await {
        return Err(code);
    }
    if !coordinator.is_current_entry(transactional_id, entry) {
        // Another request persisted this transaction after validation read
        // it. Kafka answers a transition in progress the same way, and the
        // client retries.
        return Err(codes::CONCURRENT_TRANSACTIONS);
    }
    // Stage on a clone. Until the Prepare record is durable, every other
    // caller must still see the state before it.
    let mut staged = state.clone();
    let (prepare, complete) = decide_phase1_transition(&mut staged, committed)?;
    prepare_completion_identities(&mut staged, version, &coordinator.producer_ids)
        .await
        .map_err(|error| {
            tracing::error!(
                tid = transactional_id,
                %error,
                "EndTxn: failed to allocate completion producer identity"
            );
            codes::UNKNOWN_SERVER_ERROR
        })?;
    staged.last_update_ms = now_millis();
    if let Err(error) = coordinator
        .put_under_state_partition_lock(staged.clone(), version)
        .await
    {
        tracing::error!(
            tid = transactional_id,
            state = ?prepare,
            error = %error,
            "EndTxn: failed to persist PrepareCommit/PrepareAbort"
        );
        return Err(coordinator.append_error_code(transactional_id).await);
    }
    // The append published a new handle. A caller that already waits on this
    // one sees the durable Prepare state too.
    *state = staged.clone();
    Ok((marker_type, prepare, complete, staged))
}
