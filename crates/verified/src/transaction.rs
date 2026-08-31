//! Transaction-completion fencing after the `EndTxn` marker fan-out.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Whether an `EndTxn` caller may finalize the entry it prepared.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TransactionCompletionDecision {
    Proceed,
    AlreadyComplete,
    RejectStaleIdentity,
    RejectState,
}

/// The persisted identity and state observed after marker fan-out.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TransactionSnapshot {
    pub pid: i64,
    pub epoch: i16,
    pub state: i8,
}

/// A producer identity captured before marker fan-out.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TransactionIdentity {
    pub pid: i64,
    pub epoch: i16,
}

/// Revalidate the transaction entry after the marker fan-out released its lock.
#[cfg_attr(creusot, requires(prepare_state != complete_state))]
#[cfg_attr(
    creusot,
    ensures(
        current.pid == completion.pid
            && current.epoch == completion.epoch
            && current.state == complete_state
            ==> result == TransactionCompletionDecision::AlreadyComplete
    )
)]
#[cfg_attr(
    creusot,
    ensures(
        result == TransactionCompletionDecision::AlreadyComplete
            ==> current.pid == completion.pid
                && current.epoch == completion.epoch
                && current.state == complete_state
    )
)]
#[cfg_attr(
    creusot,
    ensures(
        current.pid == expected.pid
            && current.epoch == expected.epoch
            && current.state == prepare_state
            ==> result == TransactionCompletionDecision::Proceed
    )
)]
#[cfg_attr(
    creusot,
    ensures(
        result == TransactionCompletionDecision::Proceed
            ==> current.pid == expected.pid
                && current.epoch == expected.epoch
                && current.state == prepare_state
                && !(current.pid == completion.pid
                    && current.epoch == completion.epoch
                    && current.state == complete_state)
    )
)]
#[cfg_attr(
    creusot,
    ensures(
        !(current.pid == completion.pid
            && current.epoch == completion.epoch
            && current.state == complete_state)
            && (current.pid != expected.pid || current.epoch != expected.epoch)
            ==> result == TransactionCompletionDecision::RejectStaleIdentity
    )
)]
#[cfg_attr(
    creusot,
    ensures(
        result == TransactionCompletionDecision::RejectStaleIdentity
            ==> !(current.pid == completion.pid
                && current.epoch == completion.epoch
                && current.state == complete_state)
                && (current.pid != expected.pid || current.epoch != expected.epoch)
    )
)]
#[cfg_attr(
    creusot,
    ensures(
        current.pid == expected.pid
            && current.epoch == expected.epoch
            && current.state != prepare_state
            && !(current.pid == completion.pid
                && current.epoch == completion.epoch
                && current.state == complete_state)
            ==> result == TransactionCompletionDecision::RejectState
    )
)]
#[cfg_attr(
    creusot,
    ensures(
        result == TransactionCompletionDecision::RejectState
            ==> current.pid == expected.pid
                && current.epoch == expected.epoch
                && current.state != prepare_state
                && !(current.pid == completion.pid
                    && current.epoch == completion.epoch
                    && current.state == complete_state)
    )
)]
#[must_use]
pub fn transaction_completion_decision(
    current: TransactionSnapshot,
    expected: TransactionIdentity,
    completion: TransactionIdentity,
    prepare_state: i8,
    complete_state: i8,
) -> TransactionCompletionDecision {
    if current.pid == completion.pid
        && current.epoch == completion.epoch
        && current.state == complete_state
    {
        return TransactionCompletionDecision::AlreadyComplete;
    }
    if current.pid != expected.pid || current.epoch != expected.epoch {
        return TransactionCompletionDecision::RejectStaleIdentity;
    }
    if current.state == prepare_state {
        TransactionCompletionDecision::Proceed
    } else {
        TransactionCompletionDecision::RejectState
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    const PREPARE_COMMIT: i8 = 2;
    const COMPLETE_COMMIT: i8 = 4;

    #[test]
    fn completion_requires_the_prepared_identity_and_state() {
        use TransactionCompletionDecision::{Proceed, RejectStaleIdentity, RejectState};

        assert!(
            transaction_completion_decision(
                TransactionSnapshot {
                    pid: 7,
                    epoch: 3,
                    state: PREPARE_COMMIT,
                },
                TransactionIdentity { pid: 7, epoch: 3 },
                TransactionIdentity { pid: 7, epoch: 4 },
                PREPARE_COMMIT,
                COMPLETE_COMMIT,
            ) == Proceed
        );
        assert!(
            transaction_completion_decision(
                TransactionSnapshot {
                    pid: 7,
                    epoch: 4,
                    state: PREPARE_COMMIT,
                },
                TransactionIdentity { pid: 7, epoch: 3 },
                TransactionIdentity { pid: 7, epoch: 4 },
                PREPARE_COMMIT,
                COMPLETE_COMMIT,
            ) == RejectStaleIdentity
        );
        assert!(
            transaction_completion_decision(
                TransactionSnapshot {
                    pid: 7,
                    epoch: 3,
                    state: 1,
                },
                TransactionIdentity { pid: 7, epoch: 3 },
                TransactionIdentity { pid: 7, epoch: 4 },
                PREPARE_COMMIT,
                COMPLETE_COMMIT,
            ) == RejectState
        );
    }

    #[test]
    fn only_the_intended_completion_is_idempotent() {
        use TransactionCompletionDecision::{AlreadyComplete, RejectState};

        assert!(
            transaction_completion_decision(
                TransactionSnapshot {
                    pid: 11,
                    epoch: 0,
                    state: COMPLETE_COMMIT,
                },
                TransactionIdentity {
                    pid: 7,
                    epoch: i16::MAX,
                },
                TransactionIdentity { pid: 11, epoch: 0 },
                PREPARE_COMMIT,
                COMPLETE_COMMIT,
            ) == AlreadyComplete
        );
        assert!(
            transaction_completion_decision(
                TransactionSnapshot {
                    pid: 7,
                    epoch: i16::MAX,
                    state: COMPLETE_COMMIT,
                },
                TransactionIdentity {
                    pid: 7,
                    epoch: i16::MAX,
                },
                TransactionIdentity { pid: 11, epoch: 0 },
                PREPARE_COMMIT,
                COMPLETE_COMMIT,
            ) == RejectState
        );
    }
}
