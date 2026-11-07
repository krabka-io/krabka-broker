//! The producer-keyed state of one partition, as Kafka's
//! `ProducerStateManager` keeps it: each producer's snapshot entry, the data
//! batches it appended before its last one, and its transaction verification.
//!
//! The transaction bookkeeping that the last stable offset, the `.txnindex`
//! and the stamp indexes depend on stays on [`Log`](super::Log).

use {
    super::verification::VerificationState,
    crate::producer_snapshot::{ProducerBatchMetadata, ProducerSnapshotEntry},
    krabka_ids::ProducerId,
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
}
