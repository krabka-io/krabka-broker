//! The Phase-3 guard of `EndTxn`. The entry lock is released across the marker
//! fan-out, so before the handler writes `Complete{Commit,Abort}` it re-reads
//! the coordinator's current entry and asks this module whether that entry is
//! still the one it prepared.

/// Decision for the Phase-3 (Complete) re-acquire re-validation. See
/// [`validate_complete_reacquire`].
pub use krabka_verified::transaction::TransactionCompletionDecision as ReacquireDecision;
use {
    crate::txn::{decision::CompletionExpectation, state::TxnEntry},
    krabka_verified::transaction::{
        TransactionIdentity, TransactionSnapshot, transaction_completion_decision,
    },
};

/// Re-validate, after re-acquiring the coordinator's *current* entry for a
/// transactional-id, that it is safe to finalise the transaction.
///
/// `expectation.prepared` is the producer identity this `EndTxn` handler
/// validated and acted on, and `expectation.completion` the identity it staged
/// for the producer. `expectation.prepare` is the state this handler wrote in
/// Phase 1. `expectation.complete` is the state it is about to write.
///
/// Returns:
/// - [`ReacquireDecision::RejectStaleIdentity`] if the pid or epoch no longer
///   matches, which means a concurrent `InitProducerId` fenced this handler.
/// - [`ReacquireDecision::AlreadyComplete`] if the entry has the exact
///   completion identity and state this handler intended. Another caller, or
///   an `EndTxn` retry, finished the transition, so a second finalise would be
///   a redundant overwrite.
/// - [`ReacquireDecision::RejectState`] if the state is anything other than the
///   `expectation.prepare` this handler left in place. For
///   example, a concurrent `AddPartitionsToTxn` advanced it to `Ongoing`, or
///   it moved into the *opposite* prepare/complete kind. The marker fan-out
///   then no longer reflects the live transaction, and this handler must not
///   finalise.
/// - [`ReacquireDecision::Proceed`] only when the epoch matches and the state
///   is still exactly `expectation.prepare`.
pub fn validate_complete_reacquire(
    entry: &TxnEntry,
    expectation: &CompletionExpectation,
) -> ReacquireDecision {
    transaction_completion_decision(
        TransactionSnapshot {
            pid: entry.producer_id.get(),
            epoch: entry.producer_epoch,
            state: entry.state.to_kafka_status(),
        },
        TransactionIdentity {
            pid: expectation.prepared.producer_id.get(),
            epoch: expectation.prepared.producer_epoch,
        },
        TransactionIdentity {
            pid: expectation.completion.producer_id.get(),
            epoch: expectation.completion.producer_epoch,
        },
        expectation.prepare.to_kafka_status(),
        expectation.complete.to_kafka_status(),
    )
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::txn::{
            handlers::end_txn::test_support::entry, state::TxnState, util::ProducerIdentity,
        },
        assert2::assert,
        krabka_log::ProducerId,
    };

    /// The expectation of a handler that prepared under `(prepared_pid,
    /// prepared_epoch)` and staged `(completion_pid, completion_epoch)`.
    fn expectation(
        (prepared_pid, prepared_epoch): (i64, i16),
        (completion_pid, completion_epoch): (i64, i16),
        prepare: TxnState,
        complete: TxnState,
    ) -> CompletionExpectation {
        CompletionExpectation {
            prepared: ProducerIdentity {
                producer_id: ProducerId(prepared_pid),
                producer_epoch: prepared_epoch,
            },
            completion: ProducerIdentity {
                producer_id: ProducerId(completion_pid),
                producer_epoch: completion_epoch,
            },
            prepare,
            complete,
        }
    }

    #[test]
    fn commit_reacquire_decision_matrix() {
        // Phase 1 left (pid=7, epoch=3, PrepareCommit); the reacquire always
        // asks to drive PrepareCommit → CompleteCommit.
        // (observed_pid, observed_epoch, observed_state, expected)
        let cases = [
            // Entry is exactly as Phase 1 left it: same pid/epoch, still in
            // Prepare — proceed.
            (7, 3, TxnState::PrepareCommit, ReacquireDecision::Proceed),
            // A concurrent InitProducerId bumped the epoch during the marker
            // fan-out. We must NOT overwrite with the stale epoch / Complete
            // state.
            (
                7,
                4,
                TxnState::PrepareCommit,
                ReacquireDecision::RejectStaleIdentity,
            ),
            // Producer id changed underneath us — fenced.
            (
                8,
                3,
                TxnState::PrepareCommit,
                ReacquireDecision::RejectStaleIdentity,
            ),
            // Another caller (or an EndTxn retry that lost the race) already
            // drove this exact transition. Report success, do not re-write.
            (
                7,
                3,
                TxnState::CompleteCommit,
                ReacquireDecision::AlreadyComplete,
            ),
            // A concurrent AddPartitionsToTxn re-opened the txn
            // (Complete→Ongoing reuse, or some other interleave). Our marker
            // fan-out no longer reflects the live transaction; refuse to
            // finalise.
            (7, 3, TxnState::Ongoing, ReacquireDecision::RejectState),
            // We prepared a Commit, but the entry is now in PrepareAbort — a
            // different finalisation kind raced us. Refuse to write
            // CompleteCommit.
            (7, 3, TxnState::PrepareAbort, ReacquireDecision::RejectState),
        ];
        for (pid, epoch, state, expected) in cases {
            let e = entry(pid, epoch, state);
            let decision = validate_complete_reacquire(
                &e,
                &expectation(
                    (7, 3),
                    (7, 3),
                    TxnState::PrepareCommit,
                    TxnState::CompleteCommit,
                ),
            );
            assert!(
                decision == expected,
                "observed pid {pid}, epoch {epoch}, state {state:?}"
            );
        }
    }

    #[test]
    fn abort_path_proceeds_and_is_idempotent() {
        // Mirror the abort branch: prepare=PrepareAbort, complete=CompleteAbort.
        let prep = entry(7, 3, TxnState::PrepareAbort);
        assert!(
            validate_complete_reacquire(
                &prep,
                &expectation(
                    (7, 3),
                    (7, 3),
                    TxnState::PrepareAbort,
                    TxnState::CompleteAbort
                )
            ) == ReacquireDecision::Proceed
        );
        let done = entry(7, 3, TxnState::CompleteAbort);
        assert!(
            validate_complete_reacquire(
                &done,
                &expectation(
                    (7, 3),
                    (7, 3),
                    TxnState::PrepareAbort,
                    TxnState::CompleteAbort
                )
            ) == ReacquireDecision::AlreadyComplete
        );
    }

    #[test]
    fn rolled_completion_is_idempotent_only_under_the_new_identity() {
        let completed = entry(11, 0, TxnState::CompleteCommit);
        assert!(
            validate_complete_reacquire(
                &completed,
                &expectation(
                    (7, i16::MAX),
                    (11, 0),
                    TxnState::PrepareCommit,
                    TxnState::CompleteCommit
                )
            ) == ReacquireDecision::AlreadyComplete
        );

        let stale = entry(7, i16::MAX, TxnState::CompleteCommit);
        assert!(
            validate_complete_reacquire(
                &stale,
                &expectation(
                    (7, i16::MAX),
                    (11, 0),
                    TxnState::PrepareCommit,
                    TxnState::CompleteCommit
                )
            ) == ReacquireDecision::RejectState
        );
    }
}
