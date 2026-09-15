use assert2::assert;
use tempfile::tempdir;

use super::*;
use crate::{
    config::LogConfig,
    log::test_support::{commit_marker, sample_batch, transactional_batch},
};

const PID: i64 = 1000;
const CLOCK: (i64, i64) = (1_000, 86_400_000);

fn batch(epoch: i16, base_sequence: i32, is_transactional: bool) -> TransactionalBatch {
    TransactionalBatch {
        producer_id: ProducerId(PID),
        producer_epoch: epoch,
        base_sequence,
        is_transactional,
        is_control: false,
    }
}

/// The partition state a case starts from.
#[derive(Debug, Clone, Copy)]
enum Start {
    /// No producer state at all.
    Empty,
    /// A committed transaction at epoch 3, sequences 0 and 1.
    CommittedAtEpoch3,
    /// An open transaction at epoch 3, sequences 0 and 1.
    OpenAtEpoch3,
}

fn log_at(start: Start) -> (tempfile::TempDir, Log) {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    if matches!(start, Start::CommittedAtEpoch3 | Start::OpenAtEpoch3) {
        let mut data = transactional_batch(PID, 3, &["a", "b"]);
        data.base_sequence = 0;
        let guard = log
            .maybe_start_transaction_verification(batch(3, 0, true), false, CLOCK)
            .unwrap();
        log.check_transactional_append(batch(3, 0, true), guard)
            .unwrap();
        log.append(&mut data).unwrap();
    }
    if matches!(start, Start::CommittedAtEpoch3) {
        log.append(&mut commit_marker(PID, 3)).unwrap();
    }
    (dir, log)
}

/// Kafka's `UnifiedLog.maybeStartTransactionVerification`: a stale epoch is
/// refused, an open transaction at the batch epoch needs no guard, and any
/// other transactional batch gets one guard that a repeated start keeps.
#[test]
fn a_verification_starts_only_outside_an_open_transaction() {
    let cases = [
        ("no producer state", Start::Empty, 0, Ok(true)),
        ("after a commit", Start::CommittedAtEpoch3, 3, Ok(true)),
        ("open transaction", Start::OpenAtEpoch3, 3, Ok(false)),
        (
            "open transaction, stale epoch",
            Start::OpenAtEpoch3,
            2,
            Err(TransactionAppendRefusal::StaleProducerEpoch),
        ),
        (
            "open transaction, newer epoch",
            Start::OpenAtEpoch3,
            4,
            Ok(true),
        ),
    ];
    for (name, start, epoch, want_guard) in cases {
        let (_dir, mut log) = log_at(start);
        let first = log.maybe_start_transaction_verification(batch(epoch, 2, true), false, CLOCK);
        let again = log.maybe_start_transaction_verification(batch(epoch, 2, true), false, CLOCK);
        assert!(first == again, "{name}");
        assert!(
            first.map(|guard| guard != VerificationGuard::SENTINEL) == want_guard,
            "{name}"
        );
    }
}

/// The append-time check of `UnifiedLog.analyzeAndValidateProducerState` and
/// `ProducerAppendInfo`.
#[test]
fn an_append_needs_an_open_transaction_or_the_current_guard() {
    #[derive(Debug, Clone, Copy)]
    enum Presented {
        Sentinel,
        StartedGuard,
        /// A guard started, then a marker for the producer landed.
        GuardBeforeMarker,
    }
    let cases = [
        (
            "no transaction, no guard",
            Start::CommittedAtEpoch3,
            batch(3, 2, true),
            Presented::Sentinel,
            Err(TransactionAppendRefusal::InvalidTransactionState),
        ),
        (
            "no transaction, current guard",
            Start::CommittedAtEpoch3,
            batch(3, 2, true),
            Presented::StartedGuard,
            Ok(()),
        ),
        (
            "a marker cleared the guard",
            Start::OpenAtEpoch3,
            batch(3, 2, true),
            Presented::GuardBeforeMarker,
            Err(TransactionAppendRefusal::InvalidTransactionState),
        ),
        (
            "open transaction",
            Start::OpenAtEpoch3,
            batch(3, 2, true),
            Presented::Sentinel,
            Ok(()),
        ),
        (
            "stale epoch",
            Start::CommittedAtEpoch3,
            batch(2, 2, true),
            Presented::Sentinel,
            Err(TransactionAppendRefusal::StaleProducerEpoch),
        ),
        (
            "non-transactional batch in an open transaction",
            Start::OpenAtEpoch3,
            batch(3, 2, false),
            Presented::Sentinel,
            Err(TransactionAppendRefusal::InvalidTransactionState),
        ),
        (
            "non-transactional batch after a commit",
            Start::CommittedAtEpoch3,
            batch(3, 2, false),
            Presented::Sentinel,
            Ok(()),
        ),
    ];
    for (name, start, appended, presented, want) in cases {
        let (_dir, mut log) = log_at(start);
        let guard = match presented {
            Presented::Sentinel => VerificationGuard::SENTINEL,
            Presented::StartedGuard => log
                .maybe_start_transaction_verification(appended, false, CLOCK)
                .unwrap(),
            Presented::GuardBeforeMarker => {
                // The verification started for the next epoch, then the
                // coordinator ended the open transaction.
                let next = TransactionalBatch {
                    producer_epoch: 4,
                    ..appended
                };
                let guard = log
                    .maybe_start_transaction_verification(next, false, CLOCK)
                    .unwrap();
                log.append(&mut commit_marker(PID, 3)).unwrap();
                guard
            }
        };
        assert!(
            log.check_transactional_append(appended, guard) == want,
            "{name}"
        );
    }
}

/// Kafka's `ProducerAppendInfo.checkSequence` with a verification state.
#[test]
fn a_verified_first_batch_keeps_the_verified_sequence() {
    let cases = [
        (
            "transaction version 2, no batch, sequence 0",
            true,
            0,
            0,
            Ok(()),
        ),
        (
            "transaction version 2, no batch, sequence 5",
            true,
            5,
            5,
            Err(TransactionAppendRefusal::OutOfOrderSequence),
        ),
        (
            "transaction version 1, no batch, sequence 5",
            false,
            5,
            5,
            Ok(()),
        ),
        (
            "above the lowest verified sequence",
            false,
            5,
            7,
            Err(TransactionAppendRefusal::OutOfOrderSequence),
        ),
    ];
    for (name, supports_epoch_bump, verified_sequence, appended_sequence, want) in cases {
        let (_dir, mut log) = log_at(Start::Empty);
        let guard = log
            .maybe_start_transaction_verification(
                batch(0, verified_sequence, true),
                supports_epoch_bump,
                CLOCK,
            )
            .unwrap();
        assert!(
            log.check_transactional_append(batch(0, appended_sequence, true), guard) == want,
            "{name}"
        );
    }
}

/// The first transactional append clears the guard, and verification state
/// older than the expiration window is dropped when another one starts.
#[test]
fn an_append_and_the_expiration_window_clear_the_guard() {
    let (_dir, mut log) = log_at(Start::Empty);
    let guard = log
        .maybe_start_transaction_verification(batch(0, 0, true), false, CLOCK)
        .unwrap();
    let mut data = transactional_batch(PID, 0, &["a"]);
    data.base_sequence = 0;
    log.append(&mut data).unwrap();
    assert!(log.verification_states.is_empty());
    assert!(log.check_transactional_append(batch(0, 1, true), guard) == Ok(()));

    let other = TransactionalBatch {
        producer_id: ProducerId(2000),
        ..batch(0, 0, true)
    };
    log.maybe_start_transaction_verification(other, false, CLOCK)
        .unwrap();
    let mut plain = sample_batch(1);
    log.append(&mut plain).unwrap();
    let later = TransactionalBatch {
        producer_id: ProducerId(3000),
        ..batch(0, 0, true)
    };
    log.maybe_start_transaction_verification(later, false, (CLOCK.0 + CLOCK.1, CLOCK.1))
        .unwrap();
    assert!(log.verification_states.keys().copied().collect::<Vec<_>>() == vec![ProducerId(3000)]);
}
