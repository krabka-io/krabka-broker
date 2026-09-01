//! Persistence and recovery of the coordinator's transaction state.
//!
//! One path appends a `TxnEntry` to its `__transaction_state` partition as a
//! byte-exact Kafka `TransactionLogKey` / `TransactionLogValue` record pair and
//! then publishes it to the in-memory map. A second appends a null-valued
//! record under that same key, which is how KIP-98 expires a transactional id.
//! The third replays every locally-led `__transaction_state` partition on
//! broker start to rebuild that map, tombstones included.

use std::sync::Arc;

use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_metadata::MetadataImage;
use krabka_protocol::records::{Record, RecordBatch};
use tokio::sync::Mutex;
use tracing::{info, warn};

use super::TxnCoordinator;
use crate::{
    error::BrokerError,
    txn::{bootstrap, state::TxnEntry},
};

impl TxnCoordinator {
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
        let tid = entry.transactional_id.clone();
        let p = self.partition_for(&tid);
        let part = self
            .partitions
            .get(bootstrap::TOPIC, p)
            .ok_or_else(|| BrokerError::Txn(format!("__transaction_state-{p} not local")))?;

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
    /// Returns [`BrokerError`] if a read of a partition's log fails with an
    /// error other than a read past the end. A read past the end is a normal
    /// "partition is empty" condition.
    // The `base_offset + last_offset_delta + 1` next-batch offset advance is
    // only reachable by replaying real committed `__transaction_state` batches
    // from an on-disk `Log`; there is no pure seam over the read loop, so the
    // arithmetic is exercised by the live recovery / differential suite.
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(name = "txn_coordinator_recover", level = "info", skip_all, err)]
    pub(crate) async fn recover(&self, image: &MetadataImage) -> Result<(), BrokerError> {
        self.refresh_leader_partitions(image).await;

        let local_partitions: Vec<PartitionIndex> = self
            .leader_partitions
            .read()
            .await
            .iter()
            .copied()
            .collect();

        for p in local_partitions {
            let Some(part) = self.partitions.get(bootstrap::TOPIC, p) else {
                // Partition is not yet open locally (no log dir / not yet created).
                continue;
            };

            let mut offset = part.log_start_offset();
            loop {
                let out = match part.read_log(offset, self.recovery_read_max) {
                    Ok(o) => o,
                    // OffsetTooLow can happen when the partition just opened
                    // with no data written yet (log_start == log_end == 0
                    // but the log returns empty in that case). Treat any
                    // read error as "nothing to replay here" to be safe.
                    Err(e) => {
                        warn!(
                            partition = p.get(),
                            error = %e,
                            "read error during __transaction_state recovery; skipping partition"
                        );
                        break;
                    }
                };

                if out.batches.is_empty() {
                    break;
                }

                for batch in &out.batches {
                    for rec in &batch.records {
                        let Some(key_bytes) = rec.key.as_ref() else {
                            warn!(
                                partition = p.get(),
                                "__transaction_state record missing key; skipping"
                            );
                            continue;
                        };
                        let tid = match crate::txn::log_record::decode_key(key_bytes) {
                            Ok(t) => t,
                            Err(e) => {
                                warn!(
                                    partition = p.get(),
                                    error = %e,
                                    "invalid TransactionLogKey in __transaction_state; skipping"
                                );
                                continue;
                            }
                        };
                        let Some(value_bytes) = rec.value.as_ref() else {
                            // Tombstone (null value) deletes txn state for this
                            // tid, and with it every producer-id mapping the
                            // value records before it built. Dropping the state
                            // entry alone would leave the reverse index -- and
                            // so this broker's start-up footprint -- growing
                            // with every transactional id ever expired.
                            if let Some((_, handle)) = self.state.remove(&tid) {
                                let entry = handle.lock().await;
                                Self::evict_entry_pids(&self.pid_to_tid, &entry);
                            }
                            continue;
                        };
                        let entry = match crate::txn::log_record::decode_value(value_bytes, tid) {
                            Ok(e) => e,
                            Err(e) => {
                                warn!(
                                    partition = p.get(),
                                    error = %e,
                                    "invalid TransactionLogValue in __transaction_state; skipping"
                                );
                                continue;
                            }
                        };
                        self.pid_to_tid
                            .insert(entry.producer_id, entry.transactional_id.clone());
                        if !entry.next_producer_id.is_none() {
                            self.pid_to_tid
                                .insert(entry.next_producer_id, entry.transactional_id.clone());
                        }
                        self.state
                            .insert(entry.transactional_id.clone(), Arc::new(Mutex::new(entry)));
                    }
                    offset = Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
                }
            }
        }

        info!(
            tids_loaded = self.state.len(),
            "TxnCoordinator recovery complete"
        );
        Ok(())
    }
}
