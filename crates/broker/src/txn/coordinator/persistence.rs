//! Persistence and recovery of the coordinator's transaction state.
//!
//! One path appends a `TxnEntry` to its `__transaction_state` partition as a
//! byte-exact Kafka `TransactionLogKey` / `TransactionLogValue` record pair and
//! then publishes it to the in-memory map. A second appends a null-valued
//! record under that same key, which is how KIP-98 expires a transactional id.
//! The third replays every locally-led `__transaction_state` partition on
//! broker start to rebuild that map, tombstones included.

use std::sync::{Arc, atomic::Ordering};

use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_metadata::MetadataImage;
use krabka_protocol::records::{Record, RecordBatch};
use tokio::sync::Mutex;
use tracing::info;

use super::{TxnCoordinator, pid_index::RecoveredTransactions};
use crate::{
    error::BrokerError,
    txn::{bootstrap, state::TxnEntry},
};

impl TxnCoordinator {
    pub(super) async fn lock_state_partition_for(
        &self,
        tid: &str,
    ) -> tokio::sync::MutexGuard<'_, ()> {
        let partition = self.partition_for(tid);
        let index = usize::try_from(partition.get())
            .expect("transaction state partition index must be nonnegative");
        self.state_partition_writes[index].lock().await
    }

    /// Persists `entry` to the matching `__transaction_state` partition log,
    /// then updates the in-memory map. The partition's writer task appends the
    /// batch, in order with all other produce appends.
    ///
    /// `txnv` is the finalized `transaction.version` that the caller resolved
    /// from the live metadata image. It selects the byte-exact Kafka
    /// `TransactionLogValue` format: v0 for `TV_0`, and v1 for `TV >= 1`.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Txn`] if the partition is not locally held
    /// or the append fails.
    #[tracing::instrument(
        name = "txn_coordinator_put",
        level = "debug",
        skip_all,
        fields(tid = %entry.transactional_id, producer_id = entry.producer_id.0),
        err,
    )]
    pub(crate) async fn put(
        &self,
        entry: TxnEntry,
        txnv: crate::txn::version::TxnVersion,
    ) -> Result<(), BrokerError> {
        let _state_partition_write = self.lock_state_partition_for(&entry.transactional_id).await;
        self.put_under_state_partition_lock(entry, txnv).await
    }

    /// Persists one entry while the caller holds its state-partition write
    /// lock. The reaper uses this form to make its exact recheck and append one
    /// serialized operation.
    pub(super) async fn put_under_state_partition_lock(
        &self,
        entry: TxnEntry,
        txnv: crate::txn::version::TxnVersion,
    ) -> Result<(), BrokerError> {
        let tid = entry.transactional_id.clone();
        let p = self.partition_for(&tid);
        let part = self
            .partitions
            .get(bootstrap::TOPIC, p)
            .ok_or_else(|| BrokerError::Txn(format!("__transaction_state-{p} not local")))?;
        self.validate_pid_install(&entry)?;

        // Byte-exact Kafka TransactionLogKey(v0) + TransactionLogValue(v0/v1).
        let key = crate::txn::log_record::encode_key(&tid);
        let value = crate::txn::log_record::encode_value(&entry, txnv.flexible_records());

        let mut batch = RecordBatch::default();
        batch.records.push(Record {
            offset_delta: 0,
            key: Some(Bytes::from(key)),
            value: Some(Bytes::from(value)),
            ..Default::default()
        });
        batch.last_offset_delta = 0;

        part.produce_batch(batch).await?;

        let _pid_install = self
            .pid_install
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Recheck after the append: another transaction can complete its own
        // durable append while this one awaits I/O, but publication must never
        // overwrite that transaction's PID ownership.
        self.validate_pid_install(&entry)?;
        Self::evict_superseded_pids(&self.pid_to_tid, &entry);
        self.pid_to_tid
            .insert(entry.producer_id, entry.transactional_id.clone());
        if !entry.next_producer_id.is_none() {
            self.pid_to_tid
                .insert(entry.next_producer_id, entry.transactional_id.clone());
        }
        self.state.insert(tid, Arc::new(Mutex::new(entry)));
        Ok(())
    }

    /// Appends a `TransactionLogKey` tombstone for `entry`'s transactional id,
    /// then drops that id from the in-memory map and from the producer-id
    /// reverse index.
    ///
    /// The record is a null-valued record under the same byte-exact
    /// `TransactionLogKey(v0)` that [`Self::put`] writes, which is how Kafka
    /// expires a transactional id: compaction reclaims the tid's history, and
    /// [`Self::recover`] already reads a null value as a delete.
    ///
    /// `entry` is the live entry, and the caller **holds its lock**. That is
    /// what makes the append and the in-memory drop one step: every path that
    /// revives a known tid mutates the entry under that same lock, so no
    /// revival can land between them and no reviving record can end up before
    /// this tombstone in the log.
    ///
    /// A failed append leaves the coordinator exactly as it was, and the next
    /// sweep retries.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Txn`] if the partition is not locally held, or
    /// the append error if the append fails.
    // cargo-mutants: append to a live partition log + live DashMap state
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(
        name = "txn_coordinator_tombstone",
        level = "debug",
        skip_all,
        fields(tid = %entry.transactional_id),
        err,
    )]
    pub(crate) async fn tombstone(&self, entry: &TxnEntry) -> Result<(), BrokerError> {
        let tid = entry.transactional_id.as_str();
        let p = self.partition_for(tid);
        let part = self
            .partitions
            .get(bootstrap::TOPIC, p)
            .ok_or_else(|| BrokerError::Txn(format!("__transaction_state-{p} not local")))?;

        let mut batch = RecordBatch::default();
        batch.records.push(Record {
            offset_delta: 0,
            key: Some(Bytes::from(crate::txn::log_record::encode_key(tid))),
            value: None,
            ..Default::default()
        });
        batch.last_offset_delta = 0;

        part.produce_batch(batch).await?;

        self.state.remove(tid);
        Self::evict_entry_pids(&self.pid_to_tid, entry);
        Ok(())
    }

    /// Replays every locally-led `__transaction_state` partition into the
    /// in-memory state map. `Broker::start` calls it.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] without publishing a partial recovery image if
    /// a partition is missing, a read or decode fails, a record is misplaced,
    /// an offset overflows, or two transactions claim one producer ID.
    // cargo-mutants: orchestration only. It reads each locally-led
    // `__transaction_state` partition off disk and feeds every record to
    // `apply_recovered_record` / `RecoveredTransactions`, which carry the
    // decisions and are mutation-tested in their own right; what is left here is
    // the log walk itself, which has no in-process signal of its own.
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(name = "txn_coordinator_recover", level = "info", skip_all, err)]
    pub(crate) async fn recover(&self, image: &MetadataImage) -> Result<(), BrokerError> {
        self.recovery_valid.store(false, Ordering::Release);
        self.install_leader_partitions(image).await;

        let mut local_partitions: Vec<PartitionIndex> = self
            .leader_partitions
            .read()
            .await
            .iter()
            .copied()
            .collect();
        local_partitions.sort_unstable_by_key(|partition| partition.get());

        let recovery = (|| -> Result<RecoveredTransactions, BrokerError> {
            let mut recovered = RecoveredTransactions::default();
            for p in local_partitions {
                let part = self.partitions.get(bootstrap::TOPIC, p).ok_or_else(|| {
                    BrokerError::Txn(format!("__transaction_state-{p} not local during recovery"))
                })?;

                let mut offset = part.log_start_offset();
                loop {
                    let out = part.read_log(offset, self.recovery_read_max)?;
                    if out.batches.is_empty() {
                        break;
                    }

                    for batch in &out.batches {
                        if batch.base_offset < offset.0 {
                            return Err(BrokerError::Txn(format!(
                                "__transaction_state-{p} replay regressed from {} to {}",
                                offset.0, batch.base_offset
                            )));
                        }
                        for rec in &batch.records {
                            let key_bytes = rec.key.as_ref().ok_or_else(|| {
                                BrokerError::Txn(format!(
                                    "__transaction_state-{p} record is missing its key"
                                ))
                            })?;
                            let tid = crate::txn::log_record::decode_key(key_bytes)?;
                            let partition_matches = self.partition_for(&tid) == p;
                            let Some(value_bytes) = rec.value.as_ref() else {
                                if !partition_matches {
                                    return Err(BrokerError::Txn(format!(
                                        "transaction {tid} tombstone is in the wrong state partition"
                                    )));
                                }
                                recovered.apply_tombstone(&tid);
                                continue;
                            };
                            let entry = crate::txn::log_record::decode_value(value_bytes, tid)?;
                            recovered.apply_value(entry, partition_matches)?;
                        }
                        offset = recovery_next_offset(batch.base_offset, batch.last_offset_delta)?;
                    }
                }
            }
            Ok(recovered)
        })();

        let recovered = match recovery {
            Ok(recovered) => recovered,
            Err(error) => {
                self.leader_partitions.write().await.clear();
                let _pid_install = self
                    .pid_install
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                self.state.clear();
                self.pid_to_tid.clear();
                return Err(error);
            }
        };
        let _pid_install = self
            .pid_install
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.state.clear();
        self.pid_to_tid.clear();
        for (tid, entry) in recovered.state {
            self.state.insert(tid, Arc::new(Mutex::new(entry)));
        }
        for (pid, tid) in recovered.pid_to_tid {
            self.pid_to_tid.insert(pid, tid);
        }
        self.recovery_valid.store(true, Ordering::Release);

        info!(
            tids_loaded = self.state.len(),
            "TxnCoordinator recovery complete"
        );
        Ok(())
    }
}

fn recovery_next_offset(base: i64, last_delta: i32) -> Result<Offset, BrokerError> {
    let delta = i64::from(last_delta);
    let next = base
        .checked_add(delta)
        .and_then(|last| last.checked_add(1))
        .filter(|_| delta >= 0)
        .ok_or_else(|| BrokerError::Txn("transaction-state replay offset overflow".into()))?;
    Ok(Offset(next))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{Offset, recovery_next_offset};

    #[test]
    fn recovery_offset_advance_is_checked_and_monotonic() {
        assert!(recovery_next_offset(7, 2).unwrap() == Offset(10));
        assert!(recovery_next_offset(7, -1).is_err());
        assert!(recovery_next_offset(i64::MAX, 0).is_err());
    }
}
