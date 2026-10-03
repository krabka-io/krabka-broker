use super::*;

pub(super) fn st(id: i8) -> TxnState {
    TxnState::from_kafka_status(id).expect("valid TxnState id in model")
}

/// Reconstructs the real `TxnEntry` from the projection, so the real decision
/// fns behave as in a live run.
pub(super) fn rebuild(s: &TxnProj) -> TxnEntry {
    let mut e = TxnEntry::new_empty("tid".to_string(), ProducerId(s.pid), s.epoch, 60_000, 1);
    e.state = st(s.state);
    e
}

/// Writes the persisted fields of `entry` back into the projection.
pub(super) fn project(s: &mut TxnProj, entry: &TxnEntry) {
    s.pid = entry.producer_id.get();
    s.epoch = entry.producer_epoch;
    s.state = entry.state.to_kafka_status();
}

pub(super) fn finalize(s: &mut TxnProj, generation: i16, complete: TxnState) {
    s.finalized.push(Finalized {
        generation,
        committed: complete == TxnState::CompleteCommit,
    });
    s.finalized.sort_unstable();
}

pub(super) fn is_prepared(state: TxnState) -> bool {
    matches!(state, TxnState::PrepareCommit | TxnState::PrepareAbort)
}

/// Kafka's fence of an `Ongoing` transaction, restated from
/// `TransactionMetadata` (`prepareFenceProducerEpoch`, then `prepareAbortOrCommit`
/// from `endTransaction(isFromClient = false)` at the cluster's version)
/// instead of read back from the function under test. The producer epoch moves
/// from `held` to `held + 1` once, and the producer continues at that epoch.
/// `TV_2` records the epoch it held as the last epoch, and stamps `TV_2`. Below
/// it the last epoch is cleared and the record carries `TV_0`.
pub(super) fn fenced_as_kafka(entry: &TxnEntry, held: i16, version: TxnVersion) -> bool {
    let verified = version == TxnVersion::Verified;
    entry.producer_epoch == held + 1
        && entry.last_producer_epoch == if verified { held } else { -1 }
        && entry.client_transaction_version == if verified { 2 } else { 0 }
        && completion_producer_identity(entry) == (entry.producer_id, held + 1)
}
