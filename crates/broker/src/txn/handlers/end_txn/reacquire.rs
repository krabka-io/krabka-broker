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
        krabka_log::ProducerId,
    };

    fn identity(producer_id: i64, producer_epoch: i16) -> ProducerIdentity {
        ProducerIdentity {
            producer_id: ProducerId(producer_id),
            producer_epoch,
        }
    }

    /// Phase 1 left (pid=7, epoch=3) in `prepare` and staged the same identity
    /// for the completion.
    fn unrolled(prepare: TxnState, complete: TxnState) -> CompletionExpectation {
        CompletionExpectation {
            prepared: identity(7, 3),
            completion: identity(7, 3),
            prepare,
            complete,
        }
    }

    fn commit() -> CompletionExpectation {
        unrolled(TxnState::PrepareCommit, TxnState::CompleteCommit)
    }

    fn abort() -> CompletionExpectation {
        unrolled(TxnState::PrepareAbort, TxnState::CompleteAbort)
    }

    /// Phase 1 prepared a commit at the last epoch of pid 7, so the completion
    /// rotates to pid 11 at epoch 0.
    fn rolled_commit() -> CompletionExpectation {
        CompletionExpectation {
            prepared: identity(7, i16::MAX),
            completion: identity(11, 0),
            prepare: TxnState::PrepareCommit,
            complete: TxnState::CompleteCommit,
        }
    }

    #[yare::parameterized(
        // Entry is exactly as Phase 1 left it: same pid/epoch, still in
        // Prepare — proceed.
        commit_unchanged = { (7, 3, TxnState::PrepareCommit), commit(), ReacquireDecision::Proceed },
        // A concurrent InitProducerId bumped the epoch during the marker
        // fan-out. We must NOT overwrite with the stale epoch / Complete state.
        commit_epoch_bumped = {
            (7, 4, TxnState::PrepareCommit), commit(), ReacquireDecision::RejectStaleIdentity
        },
        // Producer id changed underneath us — fenced.
        commit_pid_changed = {
            (8, 3, TxnState::PrepareCommit), commit(), ReacquireDecision::RejectStaleIdentity
        },
        // Another caller (or an EndTxn retry that lost the race) already drove
        // this exact transition. Report success, do not re-write.
        commit_already_complete = {
            (7, 3, TxnState::CompleteCommit), commit(), ReacquireDecision::AlreadyComplete
        },
        // A concurrent AddPartitionsToTxn re-opened the txn (Complete→Ongoing
        // reuse, or some other interleave). Our marker fan-out no longer
        // reflects the live transaction; refuse to finalise.
        commit_reopened = { (7, 3, TxnState::Ongoing), commit(), ReacquireDecision::RejectState },
        // We prepared a Commit, but the entry is now in PrepareAbort — a
        // different finalisation kind raced us. Refuse to write CompleteCommit.
        commit_raced_by_abort = {
            (7, 3, TxnState::PrepareAbort), commit(), ReacquireDecision::RejectState
        },
        // The abort branch mirrors the commit branch.
        abort_unchanged = { (7, 3, TxnState::PrepareAbort), abort(), ReacquireDecision::Proceed },
        abort_already_complete = {
            (7, 3, TxnState::CompleteAbort), abort(), ReacquireDecision::AlreadyComplete
        },
        // A rolled completion is idempotent only under the new identity.
        rolled_complete_under_new_identity = {
            (11, 0, TxnState::CompleteCommit), rolled_commit(), ReacquireDecision::AlreadyComplete
        },
        rolled_complete_under_old_identity = {
            (7, i16::MAX, TxnState::CompleteCommit), rolled_commit(), ReacquireDecision::RejectState
        },
    )]
    fn reacquire_decision(
        observed: (i64, i16, TxnState),
        expectation: CompletionExpectation,
        want: ReacquireDecision,
    ) {
        let (pid, epoch, state) = observed;
        let observed = entry(pid, epoch, state);
        assert2::assert!(validate_complete_reacquire(&observed, &expectation) == want);
    }
}
