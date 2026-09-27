//! The `producer.id.expiration.ms` inactivity window: which producers still
//! count as active, and eviction of the ones that have gone quiet.
//!
//! The log cleaner reads the active set so compaction keeps a live producer's
//! last batch, and a broker maintenance loop calls the eviction so the
//! per-partition maps do not grow without bound.

use std::{collections::HashMap, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_units::{Time, convert::TimeExt as _};
use tokio::sync::Mutex;

use super::{PartitionMap, PartitionProducerState, ProducerState};
use crate::partition::LogOffset;

impl ProducerState {
    /// Snapshot of currently-active producers on `(topic, partition)`.
    ///
    /// The map holds `producer_id` → that producer's last-accepted-batch
    /// `base_offset`. A producer is "active" unless
    /// [`ProducerEntry::is_expired`](super::ProducerEntry::is_expired) holds,
    /// the predicate Kafka's `producer.id.expiration.ms` sweep uses. This
    /// function excludes expired producers.
    ///
    /// The cleaner calls it to build a `CompactionContext`. The cleaner must
    /// keep an active producer's last batch with `RETAIN_EMPTY` even when
    /// compaction removes all of its records, so the producer's
    /// sequence/epoch state survives.
    ///
    /// This function returns an empty map for an unknown `(topic, partition)`.
    ///
    /// The caller is the partition writer task's `WriterMessage::Compact`
    /// handler, which fills the `CompactionContext::active_producers` set.
    /// `spawn_partition` threads the broker-wide `ProducerState` into
    /// `partition_writer::run` for that handler.
    pub async fn active_snapshot(
        &self,
        topic: &str,
        partition: PartitionIndex,
        now_ms: i64,
        expiration: Time,
    ) -> HashMap<i64, LogOffset> {
        // Mirror `snapshot`: avoid inserting an empty entry for an unknown
        // partition (the borrowed lookups allocate nothing on a miss).
        let Some(topic_ref) = self.by_topic.get(topic) else {
            return HashMap::new();
        };
        let parts = topic_ref.value().clone();
        drop(topic_ref);
        let Some(part_ref) = parts.get(&partition) else {
            return HashMap::new();
        };
        let handle = part_ref.value().clone();
        drop(part_ref);
        let state = handle.lock().await;
        // Public return stays `HashMap<i64, i64>`; unwrap the `ProducerId` key at
        // the boundary (the caller re-wraps into the log seam's `ProducerId`).
        state
            .entries
            .iter()
            .filter(|(_pid, e)| !e.is_expired(now_ms, expiration.millis_i64()))
            .map(|(pid, e)| (pid.get(), e.base_offset))
            .collect()
    }

    /// Evict idempotent-producer entries that are expired at `now_ms`.
    ///
    /// This is Kafka's `ProducerStateManager.removeExpiredProducers` for
    /// `producer.id.expiration.ms`, whose default is `86_400_000` ms = 24h.
    /// Kafka's `isProducerExpired` ages an entry by its `lastTimestamp`, the
    /// max timestamp of its last batch (or of the marker after it), not by
    /// the broker clock at the append, and never expires a producer whose
    /// transaction on the partition is still open.
    ///
    /// This function removes empty partition maps and empty topic maps once
    /// their last entry expires, so stale `(topic, partition)` keys do not
    /// leak. It returns the number of producer-id entries it evicted.
    ///
    /// This function gives the mechanism only. The periodic caller is a
    /// broker maintenance loop, wired separately.
    pub async fn expire_older_than(&self, now_ms: i64, ttl: Time) -> usize {
        let ttl_ms = ttl.millis_i64();
        let mut evicted = 0usize;
        // Snapshot the (topic -> partition-map) refs first so we don't
        // hold a DashMap shard guard across the per-partition `.await`.
        let topics: Vec<(String, Arc<PartitionMap>)> = self
            .by_topic
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        for (topic, parts) in topics {
            let partition_refs: Vec<(PartitionIndex, Arc<Mutex<PartitionProducerState>>)> = parts
                .iter()
                .map(|e| (*e.key(), e.value().clone()))
                .collect();
            for (partition, handle) in partition_refs {
                let mut state = handle.lock().await;
                let before = state.entries.len();
                state
                    .entries
                    .retain(|_pid, entry| !entry.is_expired(now_ms, ttl_ms));
                evicted += before - state.entries.len();
                let now_empty = state.entries.is_empty();
                drop(state);
                if now_empty {
                    // Only drop the partition slot if it's *still* empty
                    // under the removal guard, so a concurrent commit that
                    // re-populated it isn't lost.
                    parts.remove_if(&partition, |_, h| {
                        h.try_lock().is_ok_and(|s| s.entries.is_empty())
                    });
                }
            }
            // Drop the topic slot if all its partitions are gone.
            self.by_topic.remove_if(&topic, |_, p| p.is_empty());
        }
        evicted
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_log::ProducerId;
    use krabka_units::{millis, secs};

    use super::*;
    use crate::producer_state::Decision;

    /// How a row's producer reaches the sweep.
    #[derive(Clone, Copy, Debug)]
    enum History {
        /// One idempotent batch.
        Idempotent,
        /// One transactional batch, with no marker yet.
        OpenTransaction,
        /// One transactional batch, then its commit marker at the batch's
        /// timestamp.
        ClosedTransaction,
    }

    /// #908: Kafka's `ProducerStateManager.isProducerExpired` ages an entry by
    /// the timestamp of its last batch (or marker), never by the broker's
    /// clock at the append, and never expires a producer whose transaction on
    /// the partition is still open. The expiry window is 5 s and every batch
    /// carries timestamp `t0`; the append itself happens now, on the wall
    /// clock, long after `t0`.
    #[tokio::test]
    async fn expiry_ages_by_the_batch_timestamp_and_spares_open_transactions() {
        const T0: i64 = 1_000;
        const EXPIRY_MS: i64 = 5_000;
        let cases = [
            (
                "idle for the whole window",
                History::Idempotent,
                T0 + EXPIRY_MS,
                1,
            ),
            (
                "one millisecond short",
                History::Idempotent,
                T0 + EXPIRY_MS - 1,
                0,
            ),
            (
                "open transaction, ten windows later",
                History::OpenTransaction,
                T0 + 10 * EXPIRY_MS,
                0,
            ),
            (
                "committed transaction, one window later",
                History::ClosedTransaction,
                T0 + EXPIRY_MS,
                1,
            ),
        ];
        for (label, history, now_ms, evicted) in cases {
            let s = ProducerState::new();
            let transactional = !matches!(history, History::Idempotent);
            s.commit(
                "t",
                PartitionIndex(0),
                (7, 0),
                (0, 0),
                (0, T0, transactional),
            )
            .await;
            if matches!(history, History::ClosedTransaction) {
                s.mirror_log_entries(
                    "t",
                    PartitionIndex(0),
                    vec![krabka_log::ProducerSnapshotEntry {
                        producer_id: ProducerId(7),
                        producer_epoch: 0,
                        last_sequence: 0,
                        last_offset: krabka_log::Offset(0),
                        offset_delta: 0,
                        timestamp: T0,
                        coordinator_epoch: 0,
                        current_txn_first_offset: None,
                    }],
                )
                .await;
            }
            let survivors: Vec<i64> = if evicted == 0 { vec![7] } else { vec![] };
            check!(
                (
                    s.expire_older_than(now_ms, Time::from_millis(EXPIRY_MS))
                        .await,
                    s.snapshot("t", PartitionIndex(0))
                        .await
                        .into_iter()
                        .map(|(pid, _)| pid)
                        .collect::<Vec<_>>(),
                ) == (evicted, survivors),
                "{label}"
            );
        }
    }

    #[tokio::test]
    async fn active_snapshot_excludes_expired_includes_active() {
        let s = ProducerState::new();
        // pid 1: last batch base_offset 10 at t=1_000; pid 2: base_offset 20
        // at t=9_500.
        commit!(s, "t", PartitionIndex(0), 1, 0, 0, 0, 10, 1_000).await;
        commit!(s, "t", PartitionIndex(0), 2, 0, 0, 0, 20, 9_500).await;
        // now = 10_000, expiration = 5_000 → pid 1 (age 9_000) excluded;
        // pid 2 (age 500) included with its base_offset.
        let snap = s
            .active_snapshot("t", PartitionIndex(0), 10_000, secs(5))
            .await;
        let expected: HashMap<i64, i64> = maplit::hashmap! {2 => 20};
        assert!(snap == expected);
        // Unknown partition / topic → empty without panicking.
        for (topic, partition) in [("t", PartitionIndex(99)), ("nope", PartitionIndex(0))] {
            assert!(
                s.active_snapshot(topic, partition, 10_000, secs(5)).await == HashMap::new(),
                "case: {topic}/{partition}"
            );
        }
    }

    #[tokio::test]
    async fn expire_drops_empty_partition_and_topic_slots() {
        let s = ProducerState::new();
        commit!(s, "t", PartitionIndex(0), 1, 0, 0, 0, 0, 0).await;
        let evicted = s.expire_older_than(1_000_000, millis(1)).await;
        // The empty partition and topic maps are pruned (the empty topic slot
        // must be removed), and a subsequent produce still works after pruning.
        check!(evicted == 1);
        check!(s.by_topic.get("t").is_none());
        check!(s.check("t", PartitionIndex(0), 1, 0, 0, 0).await == Decision::Append);
    }
}
