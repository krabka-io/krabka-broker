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
