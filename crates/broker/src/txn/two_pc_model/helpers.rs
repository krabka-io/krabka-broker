use super::*;

pub(super) fn st(id: i8) -> TxnState {
    TxnState::from_kafka_status(id).expect("valid TxnState id in model")
}

/// Reconstructs the real `TxnEntry` so the real decision functions behave as
/// in a live run. Partitions do not change these decisions.
pub(super) fn rebuild(s: &TwoPcProj) -> TxnEntry {
    let mut e = TxnEntry::new_empty(
        "tid".to_string(),
        ProducerId(s.pid),
        s.epoch,
        s.timeout_ms,
        1,
    );
    e.state = st(s.state);
    e.start_ms = s.start_ms;
    e
}

/// Writes the persisted fields of `entry` back into the projection.
pub(super) fn project(s: &mut TwoPcProj, entry: &TxnEntry) {
    s.pid = entry.producer_id.get();
    s.epoch = entry.producer_epoch;
    s.state = entry.state.to_kafka_status();
    s.timeout_ms = entry.txn_timeout_ms;
    s.start_ms = entry.start_ms;
}

/// Kafka's `timedOutTransactions` rule, restated from the request rather than
/// the persisted timeout: an `Ongoing` transaction of a producer that did not
/// ask for 2PC times out once `start + requested < now`, in exact arithmetic.
pub(super) fn kafka_timed_out(s: &TwoPcProj, now_ms: i64) -> bool {
    st(s.state) == TxnState::Ongoing
        && !s.enable_2pc
        && i128::from(s.start_ms) + i128::from(s.requested_ms) < i128::from(now_ms)
}

/// Kafka's abort of an `Ongoing` transaction by the coordinator itself,
/// restated from `TransactionMetadata` (`prepareFenceProducerEpoch`, then
/// `prepareAbortOrCommit` from `endTransaction(isFromClient = false)` at the
/// cluster's version) instead of read back from the function under test. The
/// producer epoch moves from `held` to `held + 1` once, and the producer
/// continues at that epoch. `TV_2` records the epoch it held as the last epoch,
/// and stamps `TV_2`. Below it the last epoch is cleared and the record carries
/// `TV_0`.
pub(super) fn fenced_as_kafka(entry: &TxnEntry, held: i16, version: TxnVersion) -> bool {
    let verified = version == TxnVersion::Verified;
    entry.producer_epoch == held + 1
        && entry.last_producer_epoch == if verified { held } else { -1 }
        && entry.client_transaction_version == if verified { 2 } else { 0 }
        && completion_producer_identity(entry) == (entry.producer_id, held + 1)
}
