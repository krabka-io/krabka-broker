//! Kafka's `EndTxn` state tables, one per transaction version.
//!
//! `TransactionCoordinator.endTransactionWithTV1` and its transaction-version-2
//! twin decide from the producer identity, the state of the entry and the
//! requested result. The tables differ: version 2 accepts an abort from
//! `Empty`, `CompleteCommit` and `CompleteAbort`, bumps the epoch for it and
//! writes an abort over an empty partition set, and it recognises the retry of
//! a completed transaction by the epoch before the bump. Version 1 has no
//! retry, and accepts a commit only from `CompleteCommit` and an abort only
//! from `CompleteAbort`, both without a write.
//!
//! The table in `TransactionCoordinator.scala` above `endTransaction` is what
//! this module implements, with `PF` = `PRODUCER_FENCED`,
//! `ITS` = `INVALID_TXN_STATE`, `NONE` = no error and no write, and
//! `EB` = no error and an epoch bump.

use krabka_log::ProducerId;

use super::producer_identity::client_producer_identity;
use crate::{
    codes,
    txn::state::{TxnEntry, TxnState},
};

/// What `EndTxn` does with the entry it found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EndTxnDecision {
    /// Prepare the transition to `state`. `no_partition_added` marks Kafka's
    /// `prepareAbortOrCommit(..., noPartitionAdded = true)`: an abort of a
    /// transaction that added no partition, which starts from the update time
    /// and has no partition to mark.
    Prepare {
        state: TxnState,
        no_partition_added: bool,
    },
    /// The transaction already reached the requested result. Answer `NONE`
    /// with the entry's identity and write nothing.
    AlreadyComplete,
    /// Answer this Kafka error code.
    Refuse(i16),
}

/// The `EndTxn` decision for one request against one entry.
///
/// `verified` is transaction version 2 and above, where completion bumps the
/// producer epoch.
pub(super) fn end_txn_decision(
    entry: &TxnEntry,
    (request_pid, request_epoch): (ProducerId, i16),
    committed: bool,
    verified: bool,
) -> EndTxnDecision {
    // The identity a client holds is the staged KIP-939 recovery identity when
    // the entry has one, and the live identity otherwise.
    let (entry_pid, entry_epoch) = client_producer_identity(entry);
    let retry_on_epoch_bump =
        entry_pid == request_pid && request_epoch.checked_add(1) == Some(entry_epoch);
    let retry_on_overflow =
        entry.prev_producer_id == request_pid && request_epoch == i16::MAX - 1 && entry_epoch == 0;
    let is_retry = verified && (retry_on_epoch_bump || retry_on_overflow);
    if verified {
        if entry_pid != request_pid && !retry_on_overflow {
            return EndTxnDecision::Refuse(codes::INVALID_PRODUCER_ID_MAPPING);
        }
        if !valid_verified_epoch(
            entry.state,
            (entry_epoch, request_epoch),
            (retry_on_epoch_bump, retry_on_overflow),
        ) {
            return EndTxnDecision::Refuse(codes::PRODUCER_FENCED);
        }
    } else {
        if entry_pid != request_pid {
            return EndTxnDecision::Refuse(codes::INVALID_PRODUCER_ID_MAPPING);
        }
        if entry_epoch != request_epoch {
            return EndTxnDecision::Refuse(codes::PRODUCER_FENCED);
        }
    }
    if verified {
        verified_state_decision(entry.state, committed, is_retry)
    } else {
        classic_state_decision(entry.state, committed)
    }
}

/// Kafka's `isValidEpoch` for transaction version 2: which epoch each state
/// accepts.
fn valid_verified_epoch(
    state: TxnState,
    (entry_epoch, request_epoch): (i16, i16),
    (retry_on_epoch_bump, retry_on_overflow): (bool, bool),
) -> bool {
    match state {
        TxnState::Ongoing | TxnState::Empty | TxnState::Dead => request_epoch == entry_epoch,
        TxnState::PrepareCommit | TxnState::PrepareAbort => retry_on_epoch_bump,
        TxnState::CompleteCommit | TxnState::CompleteAbort => {
            retry_on_epoch_bump || retry_on_overflow || request_epoch == entry_epoch
        }
    }
}

/// The transaction-version-2 table.
fn verified_state_decision(state: TxnState, committed: bool, is_retry: bool) -> EndTxnDecision {
    let prepare = |no_partition_added| EndTxnDecision::Prepare {
        state: if committed {
            TxnState::PrepareCommit
        } else {
            TxnState::PrepareAbort
        },
        no_partition_added,
    };
    match (state, committed, is_retry) {
        (TxnState::Ongoing, _, _) => prepare(false),
        // An abort of a transaction that added no partition is accepted, and
        // it bumps the epoch. That is how a KIP-890 client fences itself after
        // a failure it cannot classify.
        (TxnState::Empty, false, _) => EndTxnDecision::Prepare {
            state: TxnState::PrepareAbort,
            no_partition_added: true,
        },
        (TxnState::CompleteCommit, true, true) | (TxnState::CompleteAbort, false, true) => {
            EndTxnDecision::AlreadyComplete
        }
        (TxnState::CompleteCommit | TxnState::CompleteAbort, false, false) => {
            EndTxnDecision::Prepare {
                state: TxnState::PrepareAbort,
                no_partition_added: true,
            }
        }
        // A commit at the pre-abort epoch raced a coordinator-side abort, for
        // example on `transaction.timeout.ms`. It cannot have taken effect, so
        // Kafka answers the recoverable `PRODUCER_FENCED` (KAFKA-20785).
        (TxnState::CompleteAbort, true, true) => EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
        (TxnState::PrepareCommit, true, _) | (TxnState::PrepareAbort, false, _) => {
            EndTxnDecision::Refuse(codes::CONCURRENT_TRANSACTIONS)
        }
        _ => EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
    }
}

/// The table below transaction version 2.
fn classic_state_decision(state: TxnState, committed: bool) -> EndTxnDecision {
    match (state, committed) {
        (TxnState::Ongoing, _) => EndTxnDecision::Prepare {
            state: if committed {
                TxnState::PrepareCommit
            } else {
                TxnState::PrepareAbort
            },
            no_partition_added: false,
        },
        (TxnState::CompleteCommit, true) | (TxnState::CompleteAbort, false) => {
            EndTxnDecision::AlreadyComplete
        }
        (TxnState::PrepareCommit, true) | (TxnState::PrepareAbort, false) => {
            EndTxnDecision::Refuse(codes::CONCURRENT_TRANSACTIONS)
        }
        _ => EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
    }
}

#[cfg(test)]
mod tests;
