use assert2::assert;

use super::*;

const PID: ProducerId = ProducerId(7);
const EPOCH: i16 = 4;

fn entry(state: TxnState) -> TxnEntry {
    let mut entry = TxnEntry::new_empty("tid-end".to_owned(), PID, EPOCH, 60_000, 0);
    entry.state = state;
    entry
}

/// The identity a request carries, relative to the entry.
#[derive(Debug, Clone, Copy)]
enum Identity {
    /// The entry's own epoch.
    Current,
    /// One below the entry's epoch: the retry of a completion that bumped it.
    Retry,
    /// Two below: neither the current epoch nor a retry.
    Stale,
    /// Another producer id at the entry's epoch.
    OtherProducer,
}

fn request(identity: Identity) -> (ProducerId, i16) {
    match identity {
        Identity::Current => (PID, EPOCH),
        Identity::Retry => (PID, EPOCH - 1),
        Identity::Stale => (PID, EPOCH - 2),
        Identity::OtherProducer => (ProducerId(PID.get() + 1), EPOCH),
    }
}

fn prepare(state: TxnState, no_partition_added: bool) -> EndTxnDecision {
    EndTxnDecision::Prepare {
        state,
        no_partition_added,
    }
}

/// The table above `endTransaction` in `TransactionCoordinator.scala`, both
/// halves, plus the identity checks that run before it.
#[test]
fn the_state_table_follows_the_transaction_version() {
    // (state, result, identity, version 1 decision, version 2 decision)
    let cases: [(TxnState, bool, Identity, EndTxnDecision, EndTxnDecision); 20] = [
        (
            TxnState::Ongoing,
            true,
            Identity::Current,
            prepare(TxnState::PrepareCommit, false),
            prepare(TxnState::PrepareCommit, false),
        ),
        (
            TxnState::Ongoing,
            false,
            Identity::Current,
            prepare(TxnState::PrepareAbort, false),
            prepare(TxnState::PrepareAbort, false),
        ),
        (
            TxnState::Ongoing,
            true,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
        ),
        // Empty: version 2 accepts an abort and bumps the epoch for it.
        (
            TxnState::Empty,
            false,
            Identity::Current,
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
            prepare(TxnState::PrepareAbort, true),
        ),
        (
            TxnState::Empty,
            false,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
        ),
        (
            TxnState::Empty,
            true,
            Identity::Current,
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
        ),
        (
            TxnState::Empty,
            true,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
        ),
        // CompleteAbort.
        (
            TxnState::CompleteAbort,
            false,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::AlreadyComplete,
        ),
        (
            TxnState::CompleteAbort,
            false,
            Identity::Current,
            EndTxnDecision::AlreadyComplete,
            prepare(TxnState::PrepareAbort, true),
        ),
        (
            TxnState::CompleteAbort,
            true,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
        ),
        (
            TxnState::CompleteAbort,
            true,
            Identity::Current,
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
        ),
        // CompleteCommit.
        (
            TxnState::CompleteCommit,
            true,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::AlreadyComplete,
        ),
        (
            TxnState::CompleteCommit,
            true,
            Identity::Current,
            EndTxnDecision::AlreadyComplete,
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
        ),
        (
            TxnState::CompleteCommit,
            false,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
        ),
        (
            TxnState::CompleteCommit,
            false,
            Identity::Current,
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
            prepare(TxnState::PrepareAbort, true),
        ),
        // The prepare states.
        (
            TxnState::PrepareCommit,
            true,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::Refuse(codes::CONCURRENT_TRANSACTIONS),
        ),
        (
            TxnState::PrepareCommit,
            false,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
        ),
        (
            TxnState::PrepareAbort,
            false,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::Refuse(codes::CONCURRENT_TRANSACTIONS),
        ),
        // The identity checks.
        (
            TxnState::Ongoing,
            true,
            Identity::OtherProducer,
            EndTxnDecision::Refuse(codes::INVALID_PRODUCER_ID_MAPPING),
            EndTxnDecision::Refuse(codes::INVALID_PRODUCER_ID_MAPPING),
        ),
        (
            TxnState::Ongoing,
            true,
            Identity::Stale,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
        ),
    ];

    let mut expected = Vec::new();
    let mut actual = Vec::new();
    for (state, committed, identity, classic, verified) in cases {
        let row = format!(
            "{state:?} {} {identity:?}",
            if committed { "commit" } else { "abort" }
        );
        expected.push((row.clone(), classic, verified));
        actual.push((
            row,
            end_txn_decision(&entry(state), request(identity), committed, false),
            end_txn_decision(&entry(state), request(identity), committed, true),
        ));
    }
    assert!(actual == expected);
}

/// The producer id from before a rotation retries with the exhausted epoch,
/// and transaction version 2 admits it (`retryOnOverflow`).
#[test]
fn a_rotated_producer_id_retries_with_the_exhausted_epoch() {
    let mut rotated = entry(TxnState::CompleteCommit);
    rotated.producer_id = ProducerId(11);
    rotated.producer_epoch = 0;
    rotated.prev_producer_id = PID;
    let overflow = (PID, i16::MAX - 1);

    // (result, version 2 decision)
    let cases = [
        (true, EndTxnDecision::AlreadyComplete),
        (false, EndTxnDecision::Refuse(codes::INVALID_TXN_STATE)),
    ];
    for (committed, expected) in cases {
        assert!(
            end_txn_decision(&rotated, overflow, committed, true) == expected,
            "committed={committed}"
        );
        // Below version 2 the same request names neither the entry's producer
        // id nor its epoch.
        assert!(
            end_txn_decision(&rotated, overflow, committed, false)
                == EndTxnDecision::Refuse(codes::INVALID_PRODUCER_ID_MAPPING),
            "committed={committed} below version 2"
        );
    }
}

/// A KIP-939 recovery stages a new identity on the entry. That identity is the
/// one the recovered client holds, so it is the one that may end the
/// transaction, and the identity from before the recovery is fenced.
#[test]
fn a_staged_recovery_identity_is_the_one_that_ends_the_transaction() {
    let mut staged = entry(TxnState::Ongoing);
    staged.next_producer_id = PID;
    staged.next_producer_epoch = EPOCH + 1;
    for verified in [false, true] {
        assert!(
            end_txn_decision(&staged, (PID, EPOCH + 1), true, verified)
                == prepare(TxnState::PrepareCommit, false),
            "verified={verified}: the staged identity"
        );
        assert!(
            end_txn_decision(&staged, (PID, EPOCH), true, verified)
                == EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            "verified={verified}: the identity from before the recovery"
        );
    }
}
