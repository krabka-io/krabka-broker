//! Producer-identity selection for a transactional `InitProducerId`.
//!
//! Both entry points answer the same question from a different starting
//! point: which `(producer_id, producer_epoch)` pair the coordinator hands
//! back when a transactional id is re-initialised, and which pair it stages on
//! the entry for a KIP-939 recovery. The epoch-bump, the rollover to a fresh
//! producer id at `i16::MAX`, and the `transaction.version` split all live
//! here so the coordinator flow reads as one sequence of state transitions.

use crate::{error::BrokerError, txn::state::TxnEntry};

// cargo-mutants: identity allocation can cross the metadata controller;
// transaction integration exercises both epoch-bump and rollover paths.
#[cfg_attr(test, mutants::skip)]
pub(super) async fn next_init_producer_identity(
    entry: &TxnEntry,
    txnv: crate::txn::version::TxnVersion,
    producer_ids: &crate::producer_id_manager::ProducerIdManager,
) -> Result<(krabka_log::ProducerId, i16), BrokerError> {
    let (pid, epoch) = crate::txn::handlers::end_txn::client_producer_identity(entry);
    if txnv.verified() {
        crate::txn::handlers::end_txn::next_producer_identity(txnv, pid, epoch, producer_ids).await
    } else {
        match epoch.checked_add(1) {
            Some(next_epoch) => Ok((pid, next_epoch)),
            None => Ok(producer_ids.allocate().await?),
        }
    }
}

pub(super) async fn stage_recovery_identity(
    entry: &mut TxnEntry,
    producer_ids: &crate::producer_id_manager::ProducerIdManager,
) -> Result<(krabka_log::ProducerId, i16), BrokerError> {
    let already_staged = entry.has_staged_producer_identity();
    let (client_pid, client_epoch) = crate::txn::handlers::end_txn::client_producer_identity(entry);
    let (next_pid, next_epoch) = if already_staged {
        crate::txn::handlers::end_txn::next_recovery_producer_identity(
            client_pid,
            client_epoch,
            producer_ids,
        )
        .await?
    } else {
        crate::txn::handlers::end_txn::next_producer_identity(
            crate::txn::version::TxnVersion::TwoPhase,
            client_pid,
            client_epoch,
            producer_ids,
        )
        .await?
    };
    entry.next_producer_id = next_pid;
    entry.next_producer_epoch = next_epoch;
    Ok((next_pid, next_epoch))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_log::ProducerId;

    use super::*;
    use crate::txn::state::TxnState;

    #[tokio::test]
    async fn recovery_identity_advances_through_max_then_rotates() {
        let ids = crate::producer_id_manager::ProducerIdManager::new();
        let mut entry = TxnEntry::new_empty("tid-recover".into(), ProducerId(7), 3, i32::MAX, 0);
        entry.state = TxnState::Ongoing;

        assert!(stage_recovery_identity(&mut entry, &ids).await.unwrap() == (ProducerId(7), 4));
        assert!(stage_recovery_identity(&mut entry, &ids).await.unwrap() == (ProducerId(7), 5));
        assert!(entry.producer_id == 7);
        assert!(entry.producer_epoch == 3);

        entry.next_producer_id = ProducerId(7);
        entry.next_producer_epoch = i16::MAX - 1;
        assert!(
            stage_recovery_identity(&mut entry, &ids).await.unwrap() == (ProducerId(7), i16::MAX)
        );
        let (rotated_pid, rotated_epoch) = stage_recovery_identity(&mut entry, &ids).await.unwrap();
        assert!(rotated_pid != 7);
        assert!(rotated_epoch == 0);
        assert!(entry.producer_id == 7);
        assert!(entry.producer_epoch == 3);

        entry.next_producer_id = ProducerId(-1);
        entry.next_producer_epoch = -1;
        entry.producer_epoch = i16::MAX - 1;
        let (initial_rotated_pid, initial_rotated_epoch) =
            stage_recovery_identity(&mut entry, &ids).await.unwrap();
        assert!(initial_rotated_pid != 7);
        assert!(initial_rotated_epoch == 0);
    }
}
