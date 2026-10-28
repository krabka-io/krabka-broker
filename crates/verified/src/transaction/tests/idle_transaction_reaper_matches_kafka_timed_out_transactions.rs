#[cfg(test)]
use std::vec;
use {super::*, assert2::assert};

#[cfg(test)]
#[rstest_bdd_macros::then(
    "transaction tests idle transaction reaper matches kafka timed out transactions: idle transaction reaper matches kafka timed out transactions"
)]
fn bdd_verify_idle_transaction_reaper_matches_kafka_timed_out_transactions() {
    use IdleTransactionState::{Ongoing, Other};

    // (state, timeout, start, now, expected).
    let cases = [
        // `txnStartTimestamp + txnTimeoutMs < now` is strict.
        (Ongoing, 60_000, 0, 60_001, true),
        (Ongoing, 60_000, 0, 60_000, false),
        (Ongoing, 60_000, 0, 59_999, false),
        (Ongoing, 1, 10, 11, false),
        (Ongoing, 1, 10, 12, true),
        // Only an ONGOING transaction times out.
        (Other, 1, 0, i64::MAX, false),
        // KIP-939: a two-phase-commit transaction never times out.
        (Ongoing, NO_TRANSACTION_TIMEOUT_MS, 0, i64::MAX, false),
        (
            Ongoing,
            NO_TRANSACTION_TIMEOUT_MS,
            i64::MIN,
            i64::MAX,
            false,
        ),
        // A backwards clock never aborts a nonnegative timeout.
        (Ongoing, 60_000, 100_000, 0, false),
        (Ongoing, 0, 10, 9, false),
        (Ongoing, 0, 10, 10, false),
        (Ongoing, 0, i64::MAX, i64::MIN, false),
        // A zero timeout expires one millisecond after the start.
        (Ongoing, 0, 10, 11, true),
        // A negative timeout from a foreign log record is arithmetic too.
        (Ongoing, -5, 10, 6, true),
        (Ongoing, -5, 10, 5, false),
        // The elapsed time does not wrap at the `i64` edges.
        (Ongoing, 60_000, i64::MIN, i64::MAX, true),
        (Ongoing, i32::MAX - 1, i64::MAX, i64::MIN, false),
    ];
    for (state, timeout, start, now, expected) in cases {
        assert!(
            should_abort_idle_transaction(state, timeout, start, now) == expected,
            "state={state:?}, timeout={timeout}, start={start}, now={now}"
        );
    }
}

/// Kafka `TransactionStateManager.timedOutTransactions`.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/transaction/tests/idle_transaction_reaper_matches_kafka_timed_out_transactions.feature",
    name = "idle transaction reaper matches kafka timed out transactions"
)]
fn idle_transaction_reaper_matches_kafka_timed_out_transactions() {}

#[cfg(test)]
#[rstest_bdd_macros::then(
    "transaction tests idle transaction reaper matches kafka timed out transactions: producer identity boundary table"
)]
fn bdd_verify_producer_identity_boundary_table() {
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

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/transaction/tests/idle_transaction_reaper_matches_kafka_timed_out_transactions.feature",
    name = "producer identity boundary table"
)]
fn producer_identity_boundary_table() {}

#[cfg(test)]
#[rstest_bdd_macros::then(
    "transaction tests idle transaction reaper matches kafka timed out transactions: init producer id identity bumps retries or fences"
)]
fn bdd_verify_init_producer_id_identity_bumps_retries_or_fences() {
    use InitProducerIdIdentityDecision::{Bump, BumpWithoutIdentity, Fenced, Retry};

    // (entry pid, entry epoch, last epoch, prev pid, request pid,
    //  request epoch, expected).
    let cases = [
        (
            7_i64,
            4_i16,
            -1_i16,
            -1_i64,
            -1_i64,
            -1_i16,
            BumpWithoutIdentity,
        ),
        (7, 4, -1, -1, -1, 4, BumpWithoutIdentity),
        (7, 4, -1, -1, 7, 4, Bump),
        (7, 5, 4, -1, 7, 4, Retry),
        (7, 5, 4, -1, 7, 3, Fenced),
        (7, 4, -1, -1, 7, 5, Fenced),
        (7, 4, -1, -1, 9, 4, Fenced),
        // A producer id rotated at the epoch ceiling: the old id retries
        // with its exhausted epoch and gets the rotated identity back.
        (
            11,
            0,
            EXHAUSTED_PRODUCER_EPOCH,
            7,
            7,
            EXHAUSTED_PRODUCER_EPOCH,
            Retry,
        ),
        (11, 0, EXHAUSTED_PRODUCER_EPOCH, 7, 7, i16::MAX, Fenced),
        (11, 0, EXHAUSTED_PRODUCER_EPOCH, 7, 7, 5, Fenced),
        (11, 0, -1, 7, 7, EXHAUSTED_PRODUCER_EPOCH, Fenced),
    ];
    for (entry_pid, entry_epoch, last_epoch, prev_pid, pid, epoch, expected) in cases {
        assert!(
            init_producer_id_identity_decision(
                entry_pid,
                entry_epoch,
                last_epoch,
                prev_pid,
                pid,
                epoch,
            ) == expected,
            "entry=({entry_pid}, {entry_epoch}), last={last_epoch}, \
             prev={prev_pid}, request=({pid}, {epoch})"
        );
    }
}

#[cfg(test)]
/// Kafka `isValidProducerId` and `prepareIncrementProducerEpoch`.
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/transaction/tests/idle_transaction_reaper_matches_kafka_timed_out_transactions.feature",
    name = "init producer id identity bumps retries or fences"
)]
fn init_producer_id_identity_bumps_retries_or_fences() {}

#[cfg(test)]
#[rstest_bdd_macros::then(
    "transaction tests idle transaction reaper matches kafka timed out transactions: completion requires the prepared identity and state for {example}"
)]
fn bdd_verify_completion_requires_the_prepared_identity_and_state(example: String) {
    type Example = (i16, i8, TransactionCompletionDecision);
    let (epoch, state, expected): Example = match example.as_str() {
        "proceeds" => (3, PREPARE_COMMIT, TransactionCompletionDecision::Proceed),
        "stale identity" => (
            4,
            PREPARE_COMMIT,
            TransactionCompletionDecision::RejectStaleIdentity,
        ),
        "wrong state" => (3, ONGOING, TransactionCompletionDecision::RejectState),
        _ => panic!("unknown BDD example: {example}"),
    };

    assert2::assert!(
        transaction_completion_decision(
            TransactionSnapshot {
                pid: 7,
                epoch,
                state
            },
            TransactionIdentity { pid: 7, epoch: 3 },
            TransactionIdentity { pid: 7, epoch: 4 },
            PREPARE_COMMIT,
            COMPLETE_COMMIT,
        ) == expected
    );
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/transaction/tests/idle_transaction_reaper_matches_kafka_timed_out_transactions.feature",
    name = "completion requires the prepared identity and state"
)]
fn completion_requires_the_prepared_identity_and_state(example: String) { let _ = example; }

#[cfg(test)]
#[rstest_bdd_macros::then(
    "transaction tests idle transaction reaper matches kafka timed out transactions: only the intended completion is idempotent"
)]
fn bdd_verify_only_the_intended_completion_is_idempotent() {
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

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/transaction/tests/idle_transaction_reaper_matches_kafka_timed_out_transactions.feature",
    name = "only the intended completion is idempotent"
)]
fn only_the_intended_completion_is_idempotent() {}
