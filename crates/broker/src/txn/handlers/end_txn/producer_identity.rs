//! The producer identity a transaction continues with once it completes.
//! KIP-890 bumps the epoch on completion and rotates to a fresh producer id at
//! the epoch boundary the transaction marker reserves, so `EndTxn` and
//! `InitProducerId` share these rules from one place.

use krabka_log::ProducerId;

use crate::{
    error::BrokerError,
    txn::{state::TxnEntry, version::TxnVersion},
};

/// KIP-890: the `(producer_id, producer_epoch)` a producer continues with after
/// a transaction completes.
///
/// - Below `TV_2`: unchanged — the epoch only moves on `InitProducerId` reuse.
/// - `TV >= 2`, normal: same `producer_id`, `epoch + 1` — bumping on completion
///   fences a zombie holding the old epoch without a fresh `InitProducerId`.
/// - `TV >= 2`, marker-epoch boundary (`epoch >= i16::MAX - 1`): `i16::MAX` is
///   reserved for the transaction marker, so a *new* `producer_id` is allocated
///   at epoch 0 before the client can receive the reserved epoch. The caller
///   records the old id as `prev_producer_id`; `EndTxn` v5 returns the new pair.
pub(crate) async fn next_producer_identity(
    txnv: TxnVersion,
    pid: ProducerId,
    epoch: i16,
    ids: &crate::producer_id_manager::ProducerIdManager,
) -> Result<(ProducerId, i16), BrokerError> {
    let fresh = if txnv.verified() && epoch >= i16::MAX - 1 {
        Some(ids.allocate().await?.0)
    } else {
        None
    };
    Ok(next_producer_identity_with_fresh(txnv, pid, epoch, fresh)
        .expect("fresh producer ID supplied at the rotation boundary"))
}

fn next_producer_identity_with_fresh(
    txnv: TxnVersion,
    pid: ProducerId,
    epoch: i16,
    fresh: Option<ProducerId>,
) -> Option<(ProducerId, i16)> {
    krabka_verified::transaction::next_producer_identity(
        txnv.verified(),
        false,
        pid.0,
        epoch,
        fresh.map(|producer_id| producer_id.0),
    )
    .map(|(producer_id, epoch)| (ProducerId(producer_id), epoch))
}

/// KIP-939 recovery identities have already moved past the original producer
/// identity that must retain `i16::MAX` for its transaction marker. A staged
/// recovery identity can therefore advance through `i16::MAX`; only a later
/// recovery or completion rotates it to a fresh producer ID.
pub(crate) async fn next_recovery_producer_identity(
    pid: ProducerId,
    epoch: i16,
    ids: &crate::producer_id_manager::ProducerIdManager,
) -> Result<(ProducerId, i16), BrokerError> {
    let fresh = if epoch == i16::MAX {
        Some(ids.allocate().await?.0)
    } else {
        None
    };
    Ok(
        next_recovery_producer_identity_with_fresh(pid, epoch, fresh)
            .expect("fresh producer ID supplied at the recovery rotation boundary"),
    )
}

fn next_recovery_producer_identity_with_fresh(
    pid: ProducerId,
    epoch: i16,
    fresh: Option<ProducerId>,
) -> Option<(ProducerId, i16)> {
    krabka_verified::transaction::next_producer_identity(
        true,
        true,
        pid.0,
        epoch,
        fresh.map(|producer_id| producer_id.0),
    )
    .map(|(producer_id, epoch)| (ProducerId(producer_id), epoch))
}

pub(crate) fn client_producer_identity(entry: &TxnEntry) -> (ProducerId, i16) {
    if entry.has_staged_producer_identity() {
        (entry.next_producer_id, entry.next_producer_epoch)
    } else {
        (entry.producer_id, entry.producer_epoch)
    }
}

pub(crate) fn completion_producer_identity(entry: &TxnEntry) -> (ProducerId, i16) {
    client_producer_identity(entry)
}

pub(crate) async fn prepare_completion_identities(
    entry: &mut TxnEntry,
    txnv: TxnVersion,
    ids: &crate::producer_id_manager::ProducerIdManager,
) -> Result<(), BrokerError> {
    let had_recovery_identity = entry.has_staged_producer_identity();
    let (_, client_epoch) = client_producer_identity(entry);
    let at_rotation_boundary = if had_recovery_identity {
        client_epoch == i16::MAX
    } else {
        client_epoch >= i16::MAX - 1
    };
    let fresh = if txnv.verified() && at_rotation_boundary {
        Some(ids.allocate().await?.0)
    } else {
        None
    };
    prepare_completion_identities_with_fresh(entry, txnv, fresh)
        .expect("fresh producer ID supplied at the rotation boundary");
    Ok(())
}

pub(crate) fn prepare_completion_identities_with_fresh(
    entry: &mut TxnEntry,
    txnv: TxnVersion,
    fresh: Option<ProducerId>,
) -> Option<()> {
    if !txnv.verified() {
        return Some(());
    }

    let had_recovery_identity = entry.has_staged_producer_identity();
    let (client_pid, client_epoch) = client_producer_identity(entry);
    let (completion_pid, completion_epoch) = if had_recovery_identity {
        next_recovery_producer_identity_with_fresh(client_pid, client_epoch, fresh)?
    } else {
        next_producer_identity_with_fresh(txnv, client_pid, client_epoch, fresh)?
    };

    // The transaction marker fences the identity that wrote the transaction.
    // i16::MAX is reserved for this final marker epoch.
    entry.producer_epoch = entry.producer_epoch.saturating_add(1);

    if had_recovery_identity || completion_pid != entry.producer_id {
        entry.next_producer_id = completion_pid;
        entry.next_producer_epoch = completion_epoch;
    } else {
        entry.next_producer_id = ProducerId(-1);
        entry.next_producer_epoch = -1;
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::txn::{handlers::end_txn::test_support::entry, state::TxnState};

    #[tokio::test]
    async fn epoch_bumps_only_at_tv2() {
        use crate::txn::version::TxnVersion;
        let ids = crate::producer_id_manager::ProducerIdManager::new();
        let cases = [
            // Below TV_2 (Classic, Flexible): pid + epoch unchanged.
            (TxnVersion::Classic, (ProducerId(7), 3)),
            (TxnVersion::Flexible, (ProducerId(7), 3)),
            // TV_2+ non-overflow: same pid, epoch + 1.
            (TxnVersion::Verified, (ProducerId(7), 4)),
            (TxnVersion::TwoPhase, (ProducerId(7), 4)),
        ];
        for (version, want) in cases {
            assert!(
                next_producer_identity(version, ProducerId(7), 3, &ids)
                    .await
                    .unwrap()
                    == want,
                "txn version {version:?}"
            );
        }
    }

    #[tokio::test]
    async fn epoch_overflow_at_tv2_allocates_new_pid_at_epoch_zero() {
        use crate::txn::version::TxnVersion;
        let ids = crate::producer_id_manager::ProducerIdManager::new();
        // MAX is reserved for the marker epoch, so the client rotates at
        // MAX-1 and receives a fresh producer_id at epoch 0.
        let (new_pid, new_epoch) =
            next_producer_identity(TxnVersion::Verified, ProducerId(7), i16::MAX - 1, &ids)
                .await
                .unwrap();
        assert!(new_pid != 7);
        assert!(new_epoch == 0);
        // The allocator hands out a distinct pid on the next overflow too.
        let (next_pid, _) =
            next_producer_identity(TxnVersion::Verified, ProducerId(7), i16::MAX, &ids)
                .await
                .unwrap();
        assert!(next_pid != new_pid);
        // Below TV_2 at i16::MAX: no roll, epoch stays (no bump path taken).
        assert!(
            next_producer_identity(TxnVersion::Classic, ProducerId(7), i16::MAX, &ids)
                .await
                .unwrap()
                == (ProducerId(7), i16::MAX)
        );
    }

    #[tokio::test]
    async fn normal_completion_rotates_before_the_reserved_marker_epoch() {
        let ids = crate::producer_id_manager::ProducerIdManager::new();
        let mut entry = entry(7, i16::MAX - 1, TxnState::PrepareCommit);

        prepare_completion_identities(&mut entry, TxnVersion::Verified, &ids)
            .await
            .unwrap();

        assert!(entry.producer_epoch == i16::MAX);
        let (completion_pid, completion_epoch) = completion_producer_identity(&entry);
        assert!(completion_pid != 7);
        assert!(completion_epoch == 0);
    }

    #[tokio::test]
    async fn legacy_completion_does_not_allocate_at_the_tv2_boundary() {
        let ids = crate::producer_id_manager::ProducerIdManager::new();
        let mut entry = entry(7, i16::MAX - 1, TxnState::PrepareCommit);

        prepare_completion_identities(&mut entry, TxnVersion::Classic, &ids)
            .await
            .unwrap();

        assert!(completion_producer_identity(&entry) == (ProducerId(7), i16::MAX - 1));
        assert!(
            ids.allocate().await.unwrap() == (ProducerId(0), 0),
            "legacy completion must not consume a fresh producer ID"
        );
    }

    #[tokio::test]
    async fn prepared_recovery_uses_marker_identity_and_fences_the_recovery_client() {
        let ids = crate::producer_id_manager::ProducerIdManager::new();
        let mut entry = entry(7, 3, TxnState::PrepareCommit);
        entry.next_producer_id = ProducerId(7);
        entry.next_producer_epoch = 4;

        prepare_completion_identities(&mut entry, TxnVersion::TwoPhase, &ids)
            .await
            .unwrap();

        assert!(entry.producer_id == 7);
        assert!(entry.producer_epoch == 4, "marker identity must advance");
        assert!(completion_producer_identity(&entry) == (ProducerId(7), 5));
    }

    #[tokio::test]
    async fn prepared_recovery_can_use_max_epoch_before_rotating() {
        let ids = crate::producer_id_manager::ProducerIdManager::new();
        let mut entry = entry(7, i16::MAX - 1, TxnState::PrepareCommit);
        entry.next_producer_id = ProducerId(11);
        entry.next_producer_epoch = i16::MAX - 1;

        prepare_completion_identities(&mut entry, TxnVersion::TwoPhase, &ids)
            .await
            .unwrap();

        assert!(entry.producer_epoch == i16::MAX);
        assert!(completion_producer_identity(&entry) == (ProducerId(11), i16::MAX));

        entry.next_producer_epoch = i16::MAX;
        prepare_completion_identities(&mut entry, TxnVersion::TwoPhase, &ids)
            .await
            .unwrap();
        let (rotated_pid, rotated_epoch) = completion_producer_identity(&entry);
        assert!(rotated_pid != 11);
        assert!(rotated_epoch == 0);
    }
}
