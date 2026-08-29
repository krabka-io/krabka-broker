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
    let (prepare, complete, snapshot) = {
        let mut state = entry.lock().await;
        let (prepare, complete) = decide_phase1_transition(&mut state, committed)?;
        prepare_completion_identities(&mut state, version, &coordinator.producer_ids)
            .await
            .map_err(|error| {
                tracing::error!(
                    tid = transactional_id,
                    %error,
                    "EndTxn: failed to allocate completion producer identity"
                );
                codes::UNKNOWN_SERVER_ERROR
            })?;
        state.last_update_ms = now_millis();
        (prepare, complete, state.clone())
    };
    if let Err(error) = coordinator.put(snapshot.clone(), version).await {
        tracing::error!(
            tid = transactional_id,
            state = ?prepare,
            error = %error,
            "EndTxn: failed to persist PrepareCommit/PrepareAbort"
        );
        return Err(codes::UNKNOWN_SERVER_ERROR);
    }
    Ok((marker_type, prepare, complete, snapshot))
}
