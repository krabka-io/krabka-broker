//! The producer-keyed state of one partition, as Kafka's
//! `ProducerStateManager` keeps it: each producer's snapshot entry, the data
//! batches it appended before its last one, and its transaction verification.
//!
//! The transaction bookkeeping that the last stable offset, the `.txnindex`
//! and the stamp indexes depend on stays on [`Log`](super::Log).

use {
    super::{
        active_producers::ActiveProducer,
        control::{
            TransactionMarkerFlags, batch_control_marker_coordinator_epoch,
            transaction_marker_flags,
        },
        open::{DataProducerAppend, ProducerTail},
        verification::VerificationState,
    },
    crate::{
        compact::ProducerLastRecord,
        error::LogError,
        producer_snapshot::{ProducerBatchMetadata, ProducerSnapshotEntry, RecoveredProducer},
    },
    krabka_ids::{Offset, ProducerId},
    krabka_protocol::records::RecordBatch,
    std::collections::{HashMap, VecDeque},
};

/// Every piece of state the log keeps per producer id.
#[derive(Debug, Default)]
pub(super) struct ProducerStateManager {
    /// Producer sequence, epoch, and transaction metadata persisted in
    /// Kafka-compatible `.snapshot` files at segment boundaries.
    pub(super) entries: HashMap<ProducerId, ProducerSnapshotEntry>,

    /// Up to four data batches each producer appended before the last one
    /// its entry holds, at that entry's epoch, oldest first. With the last
    /// batch they are Kafka's retained `BatchMetadata`. No snapshot carries
    /// them, so a reopen rebuilds them from the replayed tail alone.
    pub(super) earlier_batches: HashMap<ProducerId, VecDeque<ProducerBatchMetadata>>,

    /// KIP-890 verification state per producer, from the start of a
    /// transaction verification to the producer's next transactional append.
    /// See [`VerificationGuard`](crate::VerificationGuard).
    pub(super) verifications: HashMap<ProducerId, VerificationState>,
}

impl ProducerStateManager {
    /// Forget every producer, for a hard reset or a producer-state reload.
    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.earlier_batches.clear();
        self.verifications.clear();
    }

    /// Remove every producer that has no open transaction and whose last
    /// timestamp is `expiration_ms` or more before `now_ms`, with its retained
    /// batches, and every verification that is that old. Kafka's
    /// `ProducerStateManager.removeExpiredProducers`.
    pub(super) fn expire(&mut self, now_ms: i64, expiration_ms: i64) {
        let expired: Vec<ProducerId> = self
            .entries
            .values()
            .filter(|entry| {
                entry.current_txn_first_offset.is_none()
                    && now_ms.saturating_sub(entry.timestamp) >= expiration_ms
            })
            .map(|entry| entry.producer_id)
            .collect();
        for producer_id in expired {
            self.entries.remove(&producer_id);
            self.earlier_batches.remove(&producer_id);
        }
        self.verifications
            .retain(|_, state| now_ms.saturating_sub(state.created_ms) < expiration_ms);
    }

    /// Every producer's entry as an [`ActiveProducer`], in producer id order.
    pub(super) fn active_producers(&self) -> Vec<ActiveProducer> {
        let mut producers: Vec<ActiveProducer> = self
            .entries
            .values()
            .map(|entry| ActiveProducer {
                producer_id: entry.producer_id,
                producer_epoch: entry.producer_epoch,
                last_sequence: entry.last_sequence,
                last_timestamp: entry.timestamp,
                coordinator_epoch: entry.coordinator_epoch,
                current_txn_start_offset: entry.current_txn_first_offset,
            })
            .collect();
        producers.sort_unstable_by_key(|producer| producer.producer_id);
        producers
    }

    /// Every producer's last data offset, if any, and its epoch.
    pub(super) fn last_records(&self) -> HashMap<ProducerId, ProducerLastRecord> {
        self.entries
            .values()
            .map(|entry| {
                let last_record = ProducerLastRecord {
                    last_data_offset: entry.last_batch().map(|batch| batch.last_offset),
                    producer_epoch: entry.producer_epoch,
                };
                (entry.producer_id, last_record)
            })
            .collect()
    }

    /// Every producer's entry with the batches it retains before its last.
    pub(super) fn recovered(&self) -> Vec<RecoveredProducer> {
        self.entries
            .values()
            .map(|entry| RecoveredProducer {
                entry: *entry,
                earlier: self
                    .earlier_batches
                    .get(&entry.producer_id)
                    .map_or_default(|earlier| earlier.iter().copied().collect()),
            })
            .collect()
    }

    /// The retained batch of `producer_id` that a batch at `producer_epoch`
    /// with this sequence range repeats. Kafka's
    /// `ProducerStateEntry.findDuplicateBatch`.
    pub(super) fn find_duplicate_batch(
        &self,
        producer_id: ProducerId,
        producer_epoch: i16,
        (base_sequence, last_offset_delta): (i32, i32),
    ) -> Option<ProducerBatchMetadata> {
        self.entries.get(&producer_id)?.find_duplicate_batch(
            self.earlier_batches
                .get(&producer_id)
                .into_iter()
                .flatten()
                .copied(),
            producer_epoch,
            (base_sequence, last_offset_delta),
        )
    }

    /// Record a control batch of `producer_id`, which is not negative, in
    /// its entry. An end marker also closes the producer's transaction and
    /// takes its coordinator epoch.
    ///
    /// # Errors
    /// Returns the marker decoding error. The entry already holds the
    /// marker's producer epoch and timestamp by then.
    pub(super) fn apply_marker(
        &mut self,
        producer_id: ProducerId,
        batch: &RecordBatch,
    ) -> Result<(), LogError> {
        let entry = self
            .entries
            .entry(producer_id)
            .or_insert_with(|| ProducerSnapshotEntry::empty(producer_id, batch.producer_epoch));
        if entry.producer_epoch != batch.producer_epoch {
            // Kafka clears the retained data-batch metadata when an end
            // marker advances the producer epoch (transaction version 2).
            self.earlier_batches.remove(&producer_id);
            entry.last_sequence = -1;
            entry.last_offset = Offset(-1);
            entry.offset_delta = 0;
        }
        entry.producer_epoch = batch.producer_epoch;
        entry.timestamp = batch.max_timestamp;
        let TransactionMarkerFlags {
            is_abort,
            is_commit,
        } = transaction_marker_flags(batch)?;
        if is_abort || is_commit {
            entry.current_txn_first_offset = None;
        }
        if (is_abort || is_commit)
            && let Some(epoch) = batch_control_marker_coordinator_epoch(batch)?
        {
            entry.coordinator_epoch = epoch;
        }
        Ok(())
    }

    /// Record one data batch in its producer's entry: `tail` is where the
    /// batch ends, as `Log::data_producer_tail` computed it.
    pub(super) fn apply_data_batch(&mut self, append: DataProducerAppend, tail: ProducerTail) {
        let DataProducerAppend {
            producer_id,
            producer_epoch,
            last_offset_delta,
            base_offset,
            timestamp,
            is_transactional,
            ..
        } = append;
        let ProducerTail {
            last_sequence,
            last_offset,
        } = tail;
        let entry = self
            .entries
            .entry(producer_id)
            .or_insert_with(|| ProducerSnapshotEntry::empty(producer_id, producer_epoch));
        let mut earlier = self.earlier_batches.remove(&producer_id);
        entry.add_data_batch(&mut earlier, producer_epoch, ProducerBatchMetadata {
            last_sequence,
            last_offset,
            offset_delta: last_offset_delta,
            timestamp,
        });
        if let Some(earlier) = earlier {
            self.earlier_batches.insert(producer_id, earlier);
        }
        if is_transactional && entry.current_txn_first_offset.is_none() {
            entry.current_txn_first_offset = Some(base_offset);
        }
    }
}
