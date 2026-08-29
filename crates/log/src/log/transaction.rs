//! Last-stable-offset bookkeeping and the effect of one transaction end
//! marker on the in-memory transaction state.
//!
//! An open transaction holds the LSO at its first offset, so every append
//! that is not transactional data moves the LSO through the one helper
//! here and a commit or abort marker closes the transaction through the
//! other.

use krabka_ids::{Offset, ProducerId};
use krabka_protocol::records::RecordBatch;

use super::{
    Log,
    control::{
        ABORT_CONTROL_TYPE, COMMIT_CONTROL_TYPE, parse_control_marker_coordinator_epoch,
        parse_control_marker_type,
    },
};
use crate::{error::LogError, txn_index::AbortedTxn};

impl Log {
    /// Move the last-stable-offset to the log end when no transaction is open.
    ///
    /// An open transaction holds the LSO at its first offset. Every append
    /// that is not transactional data goes through this method, so a barrier
    /// marker and an ordinary data batch move the LSO the same way.
    pub(super) fn advance_lso_when_no_open_transaction(&mut self) {
        if self.pending.is_empty() {
            self.lso = self.log_end_offset();
        }
    }

    /// Apply one transaction end marker to the in-memory transaction state.
    ///
    /// The marker closes `producer_id`'s open transaction on this partition.
    /// An ABORT marker appends the transaction's offset range to the active
    /// `.txnindex`. A COMMIT marker stamps the transaction's data ranges with
    /// `transaction_stamp`, or with the next stamp from the installed source.
    ///
    /// This method never runs for a barrier marker.
    pub(super) fn apply_transaction_marker(
        &mut self,
        batch: &RecordBatch,
        producer_id: ProducerId,
        transaction_stamp: Option<u64>,
    ) -> Result<(), LogError> {
        // Read the inner control record: key = (version: i16, type: i16) BE.
        let marker_type = batch
            .records
            .first()
            .and_then(|record| record.key.as_deref())
            .and_then(parse_control_marker_type);
        if producer_id.get() >= 0
            && let Some(epoch) = batch
                .records
                .first()
                .and_then(|record| record.value.as_deref())
                .and_then(parse_control_marker_coordinator_epoch)
        {
            self.coordinator_epochs.insert(producer_id, epoch);
        }
        if marker_type == Some(ABORT_CONTROL_TYPE)
            && let Some(start) = self.pending.get(&producer_id).copied()
        {
            let last = Offset(batch.base_offset + i64::from(batch.last_offset_delta));
            self.active_txn_index.append(AbortedTxn {
                start_offset: start,
                last_offset: last,
                producer_id,
            })?;
        }
        let stamp_ranges = self
            .pending_stamp_ranges
            .get(&producer_id)
            .cloned()
            .unwrap_or_default();
        if marker_type == Some(COMMIT_CONTROL_TYPE) && !stamp_ranges.is_empty() {
            let stamp = transaction_stamp
                .or_else(|| self.stamp_source.as_ref().map(|source| source.next_stamp()));
            if let Some(stamp) = stamp {
                for (base, last) in stamp_ranges {
                    self.record_stamp_value(base, last, stamp)?;
                }
            }
        }
        // Keep the in-memory transaction state until all durable sidecar
        // writes succeed. A caller can then retry a marker whose log append
        // succeeded but whose index update failed.
        self.pending.remove(&producer_id);
        self.pending_stamp_ranges.remove(&producer_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use krabka_ids::LeaderEpoch;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        config::LogConfig,
        log::test_support::{
            abort_marker, commit_marker, sample_batch, test_batch_at, transactional_batch,
            verbatim_from,
        },
        txn_index::TxnIndex,
    };

    // ---- transactional LSO / txnindex tests ----

    #[test]
    fn transactional_batch_holds_lso() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        // First, a non-txn batch — LSO advances past it.
        let mut b0 = sample_batch(1);
        log.append(&mut b0).unwrap();
        assert2::assert!(log.lso() == log.log_end_offset());

        // Now an in-flight txn batch — LSO stays.
        let mut b1 = transactional_batch(1000, 0, &["a", "b"]); // pid=1000 epoch=0
        let old_lso = log.lso();
        log.append(&mut b1).unwrap();
        assert2::assert!(log.lso() == old_lso);

        // Commit marker — LSO catches up.
        let mut commit = commit_marker(1000, 0);
        log.append(&mut commit).unwrap();
        assert2::assert!(log.lso() == log.log_end_offset());
    }

    #[test]
    fn negative_producer_ids_never_create_transaction_state() {
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            log.set_stamp_source(std::sync::Arc::new(
                crate::stamp_source::MonotonicStampSource::new(40, 1),
            ))
            .unwrap();

            let mut owned = sample_batch(1);
            owned.producer_id = -2;
            owned.producer_epoch = 0;
            owned.base_sequence = 0;
            owned.attributes = owned.attributes.with_transactional(true);
            log.append(&mut owned).unwrap();
            assert2::assert!(log.lso() == Offset(1));
            assert2::assert!(log.stamp_for_offset(Offset(0)) == Some(40));

            let mut marker = commit_marker(-2, 0);
            log.append(&mut marker).unwrap();
            assert2::assert!(log.producer_transaction_state(ProducerId(-2)) == (-1, None));

            let mut verbatim = test_batch_at(0);
            verbatim.producer_id = -3;
            verbatim.producer_epoch = 0;
            verbatim.base_sequence = 0;
            verbatim.attributes = verbatim.attributes.with_transactional(true);
            let (_wire, batch) = verbatim_from(&verbatim, LeaderEpoch(0));
            log.append_verbatim(&batch).unwrap();
            assert2::assert!(log.lso() == Offset(3));
            assert2::assert!(log.stamp_for_offset(Offset(2)) == Some(41));
            assert2::assert!(log.producer_state_snapshot().is_empty());
        }

        let reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!(reopened.lso() == reopened.log_end_offset());
        assert2::assert!(reopened.producer_transaction_state(ProducerId(-2)) == (-1, None));
        assert2::assert!(reopened.producer_transaction_state(ProducerId(-3)) == (-1, None));
        assert2::assert!(reopened.producer_state_snapshot().is_empty());
    }

    #[test]
    fn abort_marker_writes_txnindex_entry() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut t = transactional_batch(1000, 0, &["a", "b", "c"]);
        log.append(&mut t).unwrap();

        let mut a = abort_marker(1000, 0);
        log.append(&mut a).unwrap();

        let idx = TxnIndex::open(dir.path().join("00000000000000000000.txnindex")).unwrap();
        let entries = idx.entries();
        // Txn batch was the first append: start_offset = 0.
        // last_offset = abort marker's base_offset + last_offset_delta = 3 + 0 = 3.
        // (The 3-record txn batch occupies offsets 0-2; the marker lands at offset 3.)
        assert2::assert!(
            entries
                == [AbortedTxn {
                    start_offset: Offset(0),
                    last_offset: Offset(3),
                    producer_id: ProducerId(1000),
                }]
        );
    }

    // The aborted-txn `last_offset` is `marker.base_offset +
    // marker.last_offset_delta`. Using a marker that spans TWO offsets
    // (`last_offset_delta = 1`) pins the `+`: the txn batch occupies offsets
    // 0..=2, the abort marker lands at base_offset 3 with delta 1, so the
    // recorded `last_offset` is `3 + 1 = 4`. Mutating `+`→`-` would record 2.
    #[test]
    fn abort_marker_last_offset_uses_base_plus_delta() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut t = transactional_batch(1000, 0, &["a", "b", "c"]);
        log.append(&mut t).unwrap(); // offsets 0..=2

        // Abort marker spanning two offsets (delta 1): base 3, last 4.
        let mut a = abort_marker(1000, 0);
        a.last_offset_delta = 1;
        log.append(&mut a).unwrap();

        let idx = TxnIndex::open(dir.path().join("00000000000000000000.txnindex")).unwrap();
        assert2::assert!(
            idx.entries()
                == [AbortedTxn {
                    start_offset: Offset(0),
                    last_offset: Offset(4), // 3 + 1, not 3 - 1
                    producer_id: ProducerId(1000),
                }]
        );
    }

    // LSO tracking (owned path) keys on `is_transactional() && !pid.is_none()`.
    // A NON-transactional batch that carries a valid producer_id (idempotent
    // producer, pid >= 0) must NOT be treated as an open txn: LSO advances to
    // log_end. Mutating `&&`→`||` would hold LSO at the batch base (0).
    #[test]
    fn non_txn_batch_with_valid_pid_advances_lso() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        // Idempotent (not transactional) producer: pid >= 0, no transactional
        // attribute bit set.
        let mut b = sample_batch(2);
        b.producer_id = 55;
        b.producer_epoch = 0;
        assert2::assert!(!b.attributes.is_transactional());
        log.append(&mut b).unwrap();
        // Not an open txn → LSO advances to log_end (2), not held at 0.
        assert2::assert!(log.lso() == Offset(2));
        assert2::assert!(log.log_end_offset() == Offset(2));
    }

    #[test]
    fn lso_held_by_remaining_producer_after_partial_commit() {
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();

        // Open two producers' transactions in parallel.
        let mut t1 = transactional_batch(1000, 0, &["a", "b"]);
        log.append(&mut t1).unwrap();
        let mut t2 = transactional_batch(2000, 0, &["c"]);
        log.append(&mut t2).unwrap();
        let lso_after_open = log.lso();

        // Commit producer 1000. LSO must still be held back by 2000.
        let mut c1 = commit_marker(1000, 0);
        log.append(&mut c1).unwrap();
        assert2::assert!(log.lso() == lso_after_open);

        // Commit producer 2000. LSO advances to log_end_offset.
        let mut c2 = commit_marker(2000, 0);
        log.append(&mut c2).unwrap();
        assert2::assert!(log.lso() == log.log_end_offset());
    }
}
