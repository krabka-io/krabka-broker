use assert2::assert;

use super::*;

/// Kafka's `TransactionCoordinator.handleVerifyPartitionsInTransaction`, in
/// its order of checks.
#[test]
fn a_verify_only_answer_follows_kafka() {
    let producer = ProducerId(7);
    let cases = [
        (
            "another producer id",
            (ProducerId(8), 3, TxnState::Ongoing),
            true,
            codes::INVALID_PRODUCER_ID_MAPPING,
        ),
        (
            "another epoch",
            (producer, 4, TxnState::Ongoing),
            true,
            codes::PRODUCER_FENCED,
        ),
        (
            "prepare commit",
            (producer, 3, TxnState::PrepareCommit),
            true,
            codes::CONCURRENT_TRANSACTIONS,
        ),
        (
            "prepare abort",
            (producer, 3, TxnState::PrepareAbort),
            false,
            codes::CONCURRENT_TRANSACTIONS,
        ),
        (
            "partition in the transaction",
            (producer, 3, TxnState::Ongoing),
            true,
            codes::NONE,
        ),
        (
            "partition not in the transaction",
            (producer, 3, TxnState::Ongoing),
            false,
            codes::TRANSACTION_ABORTABLE,
        ),
        (
            "complete commit",
            (producer, 3, TxnState::CompleteCommit),
            false,
            codes::TRANSACTION_ABORTABLE,
        ),
    ];
    for (name, entry, contains, want) in cases {
        assert!(
            verification_code(entry, contains, (producer, 3)) == want,
            "{name}"
        );
    }
}
