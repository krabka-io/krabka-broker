//! The Phase-3 guard of `EndTxn`. The entry lock is released across the marker
//! fan-out, so before the handler writes `Complete{Commit,Abort}` it re-reads
//! the coordinator's current entry and asks this module whether that entry is
//! still the one it prepared.

use krabka_log::ProducerId;

use crate::{
    codes,
    txn::state::{TxnEntry, TxnState},
};

/// Decision for the Phase-3 (Complete) re-acquire re-validation. See
/// [`validate_complete_reacquire`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReacquireDecision {
    /// State is exactly as this handler left it after Prepare. Write Complete.
    Proceed,
    /// The entry already advanced to the Complete state this handler intended,
    /// after an idempotent retry or a lost race. Report success and do not
    /// write again.
    AlreadyComplete,
    /// The entry changed in a way that means this handler must NOT write
    /// Complete. Return this Kafka error code to the producer.
    Reject(i16),
}

/// Re-validate, after re-acquiring the coordinator's *current* entry for a
/// transactional-id, that it is safe to finalise the transaction.
///
/// `expected_epoch` and `expected_pid` are the producer identity this `EndTxn`
/// handler validated and acted on. `prepare` is the state this handler wrote
/// in Phase 1. `complete` is the state it is about to write.
///
/// Returns:
/// - [`ReacquireDecision::Reject`] with `INVALID_PRODUCER_EPOCH` if the pid or
///   epoch no longer matches, which means a concurrent `InitProducerId` fenced
///   this handler. Apache Kafka maps a stale producer epoch on `EndTxn` to
///   `INVALID_PRODUCER_EPOCH`, also known as `PRODUCER_FENCED` for the newer
///   producer client.
/// - [`ReacquireDecision::AlreadyComplete`] if the entry is already in the
///   exact `complete` state this handler intended. Another caller, or an
///   `EndTxn` retry, finished the transition, so a second finalise would be a
///   redundant overwrite.
/// - [`ReacquireDecision::Reject`] with `INVALID_TXN_STATE` if the state is
///   anything other than the `prepare` this handler left in place. For
///   example, a concurrent `AddPartitionsToTxn` advanced it to `Ongoing`, or
///   it moved into the *opposite* prepare/complete kind. The marker fan-out
///   then no longer reflects the live transaction, and this handler must not
///   finalise.
/// - [`ReacquireDecision::Proceed`] only when the epoch matches and the state
///   is still exactly `prepare`.
pub(crate) fn validate_complete_reacquire(
    entry: &TxnEntry,
    expected_pid: ProducerId,
    expected_epoch: i16,
    prepare: TxnState,
    complete: TxnState,
) -> ReacquireDecision {
    if entry.producer_id != expected_pid || entry.producer_epoch != expected_epoch {
        return ReacquireDecision::Reject(codes::INVALID_PRODUCER_EPOCH);
    }
    if entry.state == prepare {
        return ReacquireDecision::Proceed;
    }
    if entry.state == complete {
        return ReacquireDecision::AlreadyComplete;
    }
    ReacquireDecision::Reject(codes::INVALID_TXN_STATE)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::txn::handlers::end_txn::test_support::entry;

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
                ReacquireDecision::Reject(codes::INVALID_PRODUCER_EPOCH),
            ),
            // Producer id changed underneath us — fenced.
            (
                8,
                3,
                TxnState::PrepareCommit,
                ReacquireDecision::Reject(codes::INVALID_PRODUCER_EPOCH),
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
            (
                7,
                3,
                TxnState::Ongoing,
                ReacquireDecision::Reject(codes::INVALID_TXN_STATE),
            ),
            // We prepared a Commit, but the entry is now in PrepareAbort — a
            // different finalisation kind raced us. Refuse to write
            // CompleteCommit.
            (
                7,
                3,
                TxnState::PrepareAbort,
                ReacquireDecision::Reject(codes::INVALID_TXN_STATE),
            ),
        ];
        for (pid, epoch, state, expected) in cases {
            let e = entry(pid, epoch, state);
            let decision = validate_complete_reacquire(
                &e,
                ProducerId(7),
                3,
                TxnState::PrepareCommit,
                TxnState::CompleteCommit,
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
                ProducerId(7),
                3,
                TxnState::PrepareAbort,
                TxnState::CompleteAbort
            ) == ReacquireDecision::Proceed
        );
        let done = entry(7, 3, TxnState::CompleteAbort);
        assert!(
            validate_complete_reacquire(
                &done,
                ProducerId(7),
                3,
                TxnState::PrepareAbort,
                TxnState::CompleteAbort
            ) == ReacquireDecision::AlreadyComplete
        );
    }
}
