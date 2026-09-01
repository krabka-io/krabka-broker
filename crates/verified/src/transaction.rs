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

/// Sentinel persisted for a transaction controlled by an external 2PC owner.
pub const NO_TRANSACTION_TIMEOUT_MS: i32 = i32::MAX;

/// State fact needed by the idle-transaction reaper.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum IdleTransactionState {
    Ongoing,
    Other,
}

/// Resolve the persisted timeout without colliding with the 2PC sentinel.
#[requires(0 < min_timeout_ms@)]
#[requires(min_timeout_ms@ <= max_timeout_ms@)]
#[requires(max_timeout_ms@ < i32::MAX@)]
#[ensures(enable_2pc ==> result@ == i32::MAX@)]
#[ensures(!enable_2pc ==> min_timeout_ms@ <= result@ && result@ <= max_timeout_ms@)]
#[ensures(!enable_2pc && requested_ms@ < min_timeout_ms@ ==> result@ == min_timeout_ms@)]
#[ensures(!enable_2pc && requested_ms@ > max_timeout_ms@ ==> result@ == max_timeout_ms@)]
#[ensures(!enable_2pc && min_timeout_ms@ <= requested_ms@ && requested_ms@ <= max_timeout_ms@
    ==> result@ == requested_ms@)]
#[must_use]
pub fn resolve_transaction_timeout(
    enable_2pc: bool,
    requested_ms: i32,
    min_timeout_ms: i32,
    max_timeout_ms: i32,
) -> i32 {
    if enable_2pc {
        NO_TRANSACTION_TIMEOUT_MS
    } else if requested_ms < min_timeout_ms {
        min_timeout_ms
    } else if requested_ms > max_timeout_ms {
        max_timeout_ms
    } else {
        requested_ms
    }
}

/// Whether the idle reaper may abort one persisted transaction.
#[requires(0 < txn_timeout_ms@)]
#[ensures(state != IdleTransactionState::Ongoing ==> !result)]
#[ensures(txn_timeout_ms@ == i32::MAX@ ==> !result)]
#[ensures(result ==> state == IdleTransactionState::Ongoing
    && txn_timeout_ms@ != i32::MAX@
    && now_ms@ - start_ms@ >= txn_timeout_ms@)]
#[ensures(state == IdleTransactionState::Ongoing
    && txn_timeout_ms@ != i32::MAX@
    && now_ms@ - start_ms@ >= txn_timeout_ms@ ==> result)]
#[must_use]
pub fn should_abort_idle_transaction(
    state: IdleTransactionState,
    txn_timeout_ms: i32,
    start_ms: i64,
    now_ms: i64,
) -> bool {
    let ongoing = match state {
        IdleTransactionState::Ongoing => true,
        IdleTransactionState::Other => false,
    };
    if !ongoing || txn_timeout_ms == NO_TRANSACTION_TIMEOUT_MS || now_ms < start_ms {
        return false;
    }
    now_ms.saturating_sub(start_ms) >= i64::from(txn_timeout_ms)
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

/// Choose the producer identity exposed after transaction completion.
///
/// Verified normal completion reserves `i16::MAX` for the transaction marker,
/// while a staged recovery identity may use that epoch once before rotating.
#[cfg_attr(creusot, ensures(!verified ==> result == Some((pid, epoch))))]
#[cfg_attr(
    creusot,
    ensures(
        verified && !recovery && epoch@ < i16::MAX@ - 1 ==>
            match result {
                Some((result_pid, result_epoch)) =>
                    result_pid == pid && result_epoch@ == epoch@ + 1,
                None => false,
            }
    )
)]
#[cfg_attr(
    creusot,
    ensures(
        verified && recovery && epoch@ < i16::MAX@ ==>
            match result {
                Some((result_pid, result_epoch)) =>
                    result_pid == pid && result_epoch@ == epoch@ + 1,
                None => false,
            }
    )
)]
#[cfg_attr(
    creusot,
    ensures(
        verified
            && ((!recovery && epoch@ >= i16::MAX@ - 1)
                || (recovery && epoch@ >= i16::MAX@)) ==>
            match (result, fresh) {
                (Some((result_pid, result_epoch)), Some(fresh_pid)) =>
                    result_pid == fresh_pid && result_epoch@ == 0,
                (None, None) => true,
                _ => false,
            }
    )
)]
#[must_use]
pub fn next_producer_identity(
    verified: bool,
    recovery: bool,
    pid: i64,
    epoch: i16,
    fresh: Option<i64>,
) -> Option<(i64, i16)> {
    if !verified {
        return Some((pid, epoch));
    }
    let can_increment = if recovery {
        epoch < i16::MAX
    } else {
        epoch < i16::MAX - 1
    };
    if can_increment {
        Some((pid, epoch + 1))
    } else {
        fresh.map(|fresh_pid| (fresh_pid, 0))
    }
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
    fn two_pc_timeout_and_reaper_are_fail_closed() {
        assert2::assert!(resolve_transaction_timeout(true, -1, 2_000, 8_000) == i32::MAX);
        assert2::assert!(resolve_transaction_timeout(false, -1, 2_000, 8_000) == 2_000);
        assert2::assert!(resolve_transaction_timeout(false, i32::MAX, 2_000, 8_000) == 8_000);
        assert2::assert!(!should_abort_idle_transaction(
            IdleTransactionState::Ongoing,
            i32::MAX,
            0,
            i64::MAX,
        ));
        assert2::assert!(!should_abort_idle_transaction(
            IdleTransactionState::Other,
            1,
            0,
            i64::MAX,
        ));
        assert2::assert!(!should_abort_idle_transaction(
            IdleTransactionState::Ongoing,
            1,
            10,
            9,
        ));
        assert2::assert!(should_abort_idle_transaction(
            IdleTransactionState::Ongoing,
            1,
            10,
            11,
        ));
    }

    #[test]
    fn producer_identity_boundary_table() {
        let cases = [
            (false, false, i16::MAX, None, Some((7, i16::MAX))),
            (false, true, i16::MAX, Some(11), Some((7, i16::MAX))),
            (true, false, i16::MAX - 2, None, Some((7, i16::MAX - 1))),
            (true, false, i16::MAX - 1, None, None),
            (true, false, i16::MAX - 1, Some(11), Some((11, 0))),
            (true, true, i16::MAX - 1, None, Some((7, i16::MAX))),
            (true, true, i16::MAX, None, None),
            (true, true, i16::MAX, Some(11), Some((11, 0))),
        ];
        for (verified, recovery, epoch, fresh, expected) in cases {
            assert!(
                next_producer_identity(verified, recovery, 7, epoch, fresh) == expected,
                "verified={verified}, recovery={recovery}, epoch={epoch}, fresh={fresh:?}"
            );
        }
    }

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
