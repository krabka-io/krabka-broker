//! Pure decision core of the transaction coordinator's `EndTxn` path.
//!
//! The Phase-3 revalidation primitive lives in `handlers::end_txn` next to the
//! handler that also persists. This module combines it with the completion
//! identity selected during Phase 1 into one decision that the handler and the
//! stateright model can drive. See `decision_model.rs` and the design:
//! `crates/broker/docs/transaction-coordinator-design.md`.

use {
    super::{
        handlers::end_txn::{ReacquireDecision, validate_complete_reacquire},
        state::{TxnEntry, TxnState},
        util::ProducerIdentity,
    },
    crate::codes,
    krabka_log::ProducerId,
};

/// Phase 1 of `EndTxn`: validates the `Ongoing → Prepare{Commit,Abort}`
/// transition and applies it to `entry`.
///
/// It returns the `(prepare, complete)` states on success, or the Kafka error
/// code to return. The function is pure, and the caller persists `entry`
/// afterwards.
pub fn decide_phase1_transition(
    entry: &mut TxnEntry,
    committed: bool,
) -> Result<(TxnState, TxnState), i16> {
    let prepare = if committed {
        TxnState::PrepareCommit
    } else {
        TxnState::PrepareAbort
    };
    let complete = if committed {
        TxnState::CompleteCommit
    } else {
        TxnState::CompleteAbort
    };
    if !entry.state.can_transition_to(prepare) {
        return Err(codes::INVALID_TXN_STATE);
    }
    entry.state = prepare;
    Ok((prepare, complete))
}

/// Applies the `Prepare{Commit,Abort}` transition that the `EndTxn` state
/// table admitted, and returns the `(prepare, complete)` states.
///
/// The table of `handlers::end_txn::state_table` decides which states may
/// prepare, because Kafka's table is wider than `Ongoing` at transaction
/// version 2: an abort is accepted from `Empty`, `CompleteCommit` and
/// `CompleteAbort` as well.
pub fn apply_end_txn_prepare(entry: &mut TxnEntry, committed: bool) -> (TxnState, TxnState) {
    let (prepare, complete) = if committed {
        (TxnState::PrepareCommit, TxnState::CompleteCommit)
    } else {
        (TxnState::PrepareAbort, TxnState::CompleteAbort)
    };
    entry.state = prepare;
    (prepare, complete)
}

/// What Phase 3 of `EndTxn` expects to find when it re-reads the entry, and
/// what it is about to write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionExpectation {
    /// The producer identity that this handler validated and that prepared
    /// the transaction in Phase 1.
    pub(crate) prepared: ProducerIdentity,
    /// The completion identity staged with the Prepare record. Phase 3
    /// returns it to the producer. It may carry a bumped epoch or a rotated
    /// producer id.
    pub(crate) completion: ProducerIdentity,
    /// The `Prepare{Commit,Abort}` state that Phase 1 wrote.
    pub(crate) prepare: TxnState,
    /// The `Complete{Commit,Abort}` state that Phase 3 is about to write.
    pub(crate) complete: TxnState,
}

/// Outcome of [`decide_end_txn_completion`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionDecision {
    /// Finalise the transaction: write `next_state`, and return the identity
    /// to the producer. That identity may carry a bumped epoch.
    Proceed {
        next_state: TxnState,
        response_pid: ProducerId,
        response_epoch: i16,
    },
    /// The entry already reached the intended Complete state, from an
    /// idempotent retry or a lost race. Report success and do not finalise it
    /// again.
    AlreadyComplete {
        response_pid: ProducerId,
        response_epoch: i16,
    },
    /// The broker fenced the producer with an epoch bump, or the state
    /// advanced during the marker fan-out. Do NOT finalise, and return this
    /// Kafka error code.
    Reject(i16),
}

/// Phase 3 of `EndTxn`: after marker fan-out, revalidate the entry and decide
/// whether to adopt the completion identity that was persisted during Phase 1.
/// The decision does not allocate or increment an identity.
pub fn decide_end_txn_completion(
    entry: &TxnEntry,
    expectation: &CompletionExpectation,
) -> CompletionDecision {
    match validate_complete_reacquire(entry, expectation) {
        ReacquireDecision::Proceed => CompletionDecision::Proceed {
            next_state: expectation.complete,
            response_pid: expectation.completion.producer_id,
            response_epoch: expectation.completion.producer_epoch,
        },
        ReacquireDecision::AlreadyComplete => CompletionDecision::AlreadyComplete {
            response_pid: entry.producer_id,
            response_epoch: entry.producer_epoch,
        },
        ReacquireDecision::RejectStaleIdentity => {
            // Kafka's post-marker check splits the two: a producer id that is
            // neither the one that prepared the transaction nor the one the
            // completion rotates to is a lost mapping, and any other stale
            // identity is a fenced producer.
            let mapping_lost = entry.producer_id != expectation.prepared.producer_id
                && entry.producer_id != expectation.completion.producer_id;
            CompletionDecision::Reject(if mapping_lost {
                codes::INVALID_PRODUCER_ID_MAPPING
            } else {
                codes::PRODUCER_FENCED
            })
        }
        ReacquireDecision::RejectState => CompletionDecision::Reject(codes::INVALID_TXN_STATE),
    }
}

#[cfg(test)]
#[path = "decision_model.rs"]
mod decision_model;
