//! Per-(topic, partition) producer-sequence tracking. Drives the
//! idempotent-producer dedup / out-of-order / epoch-fence checks in
//! `handlers::produce`.

use std::sync::Arc;

use dashmap::DashMap;
use krabka_ids::PartitionIndex;
use krabka_log::ProducerId;
use krabka_protocol::records::increment_sequence;
use tokio::sync::Mutex;

use crate::partition::LogOffset;

#[cfg(test)]
#[macro_use]
mod commit_macro;
mod decision;
mod entry;
mod expiry;
mod recovery;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use self::decision::check_pure;
pub use self::{
    decision::Decision,
    entry::{PartitionProducerState, ProducerEntry},
};

/// Per-partition idempotent-producer state, nested under the owning
/// topic. The partition index (`i32`, `Copy`) is the key, so per-call
/// lookups allocate nothing. The outer topic map is keyed by `String`, but
/// its `get`/`entry` accept a borrowed `&str`. That map allocates the owned
/// topic key only on the first produce to a topic it has not seen before.
type PartitionMap = DashMap<PartitionIndex, Arc<Mutex<PartitionProducerState>>>;

#[derive(Debug, Default)]
pub struct ProducerState {
    by_topic: Arc<DashMap<String, Arc<PartitionMap>>>,
}

impl ProducerState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            by_topic: Arc::new(DashMap::new()),
        }
    }

    /// Commit a successful append into the tracker.
    pub async fn commit(
        &self,
        topic: &str,
        partition: PartitionIndex,
        producer: (i64, i16),
        sequence: (i32, i32),
        append: (LogOffset, i64),
    ) {
        let (producer_id, producer_epoch) = producer;
        let (base_sequence, last_offset_delta) = sequence;
        let (base_offset, last_timestamp) = append;
        let handle = self.handle(topic, partition);
        let mut s = handle.lock().await;
        let last_sequence = increment_sequence(base_sequence, last_offset_delta);
        let last_offset = base_offset + i64::from(last_offset_delta);
        s.entries.insert(
            ProducerId(producer_id),
            ProducerEntry {
                epoch: producer_epoch,
                last_sequence,
                last_offset,
                base_offset,
                last_timestamp,
                last_activity_ms: crate::txn::util::now_millis(),
            },
        );
    }

    /// Drop idempotent-producer entries whose last accepted batch was
    /// truncated off the log, that is `last_offset >= offset`.
    ///
    /// The broker calls this after it truncates the partition log below the
    /// recorded batch. Two paths do that: KIP-320 divergence truncation on
    /// rejoin, and an `OFFSET_OUT_OF_RANGE` reset.
    ///
    /// Without this call, the broker deduplicates a producer that retries a
    /// batch from the truncated tail against a `base_offset` that is no longer
    /// in the log. The `acks=all` HW gate
    /// (`await_hw_at_least(base_offset + delta + 1)`) then waits forever for a
    /// high watermark that can never reach the truncated offset. That is a
    /// permanent produce stall after failover. When this function drops the
    /// entry, the retry re-appends fresh instead. This mirrors Kafka's
    /// `ProducerStateManager.truncateAndReload`. It does not create state for a
    /// partition that the broker has never tracked.
    pub async fn truncate(&self, topic: &str, partition: PartitionIndex, offset: LogOffset) {
        let Some(parts) = self.by_topic.get(topic).map(|e| e.value().clone()) else {
            return;
        };
        let Some(handle) = parts.get(&partition).map(|e| e.value().clone()) else {
            return;
        };
        let mut s = handle.lock().await;
        s.entries.retain(|_pid, e| e.last_offset < offset);
    }

    /// Resolve the per-partition state handle, and create it on a miss.
    ///
    /// The outer topic lookup borrows `&str`. It allocates an owned `String`
    /// key only on the first lookup of that topic. The inner partition lookup
    /// is keyed by `i32` and never allocates.
    fn handle(&self, topic: &str, partition: PartitionIndex) -> Arc<Mutex<PartitionProducerState>> {
        // `get` first to avoid allocating the topic `String` on the hot
        // path (the topic almost always already exists).
        let parts = if let Some(existing) = self.by_topic.get(topic) {
            existing.value().clone()
        } else {
            self.by_topic
                .entry(topic.to_string())
                .or_insert_with(|| Arc::new(DashMap::new()))
                .value()
                .clone()
        };
        parts
            .entry(partition)
            .or_insert_with(|| Arc::new(Mutex::new(PartitionProducerState::default())))
            .value()
            .clone()
    }

    /// Read-only snapshot of every active producer entry on
    /// `(topic, partition)`.
    ///
    /// This function returns an empty list when the partition has no entries.
    /// That means no idempotent or transactional producer has produced to it
    /// yet. The `DescribeProducers` admin handler (`api_key=61`, KIP-664)
    /// calls it to show per-partition producer state to admin clients such as
    /// `kafka-admin --describe-producers`.
    ///
    /// The snapshot drops the mutex before it returns, so callers do not
    /// hold the per-partition lock across response encoding.
    pub async fn snapshot(
        &self,
        topic: &str,
        partition: PartitionIndex,
    ) -> Vec<(i64, ProducerEntry)> {
        // Cheaper to bypass `handle` (which inserts on miss): a snapshot
        // for an unknown partition should report "no producers", not
        // wire up an empty entry. The borrowed `&str` / `i32` lookups
        // allocate nothing and map a miss to an empty result.
        let Some(topic_ref) = self.by_topic.get(topic) else {
            return Vec::new();
        };
        let parts = topic_ref.value().clone();
        drop(topic_ref);
        let Some(part_ref) = parts.get(&partition) else {
            return Vec::new();
        };
        let handle = part_ref.value().clone();
        drop(part_ref);
        let state = handle.lock().await;
        // Keep the public return `i64`: unwrap the map's `ProducerId` key at the
        // snapshot boundary (the `DescribeProducers` handler writes it straight
        // into the raw-`i64` wire field).
        state
            .entries
            .iter()
            .map(|(pid, e)| (pid.get(), *e))
            .collect()
    }
}

#[cfg(test)]
#[path = "producer_state_model.rs"]
mod producer_state_model;
