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
    /// `base_offset`. A producer is "active" when
    /// `now_ms - last_activity_ms <= expiration_ms`. That is Kafka's
    /// `producer.id.expiration.ms` inactivity window. This function excludes
    /// expired producers.
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
            .filter(|(_pid, e)| {
                now_ms.saturating_sub(e.last_activity_ms) <= expiration.millis_i64()
            })
            .map(|(pid, e)| (pid.get(), e.base_offset))
            .collect()
    }

    /// Evict idempotent-producer entries whose last activity is older
    /// than `ttl` relative to `now_ms`.
    ///
    /// This mirrors Kafka's `producer.id.expiration.ms`, whose default is
    /// `86_400_000` ms = 24h. Kafka expires by *inactivity*. An entry that
    /// keeps receiving produces stays. An entry that has gone quiet past the
    /// window goes, so the map does not grow unbounded.
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
                    .retain(|_pid, entry| now_ms.saturating_sub(entry.last_activity_ms) < ttl_ms);
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

    #[tokio::test]
    async fn expire_evicts_only_idle_entries() {
        let s = ProducerState::new();
        // Two producers on the same partition with controlled activity
        // timestamps: we commit, then overwrite last_activity_ms directly
        // to simulate age without sleeping.
        commit!(s, "t", PartitionIndex(0), 1, 0, 0, 0, 0, 0).await;
        commit!(s, "t", PartitionIndex(0), 2, 0, 0, 0, 0, 0).await;
        {
            let h = s.handle("t", PartitionIndex(0));
            let mut st = h.lock().await;
            st.entries.get_mut(&ProducerId(1)).unwrap().last_activity_ms = 1_000; // old
            st.entries.get_mut(&ProducerId(2)).unwrap().last_activity_ms = 9_000; // recent
        }
        // now = 10_000, ttl = 5_000 → pid 1 (age 9_000) expires, pid 2
        // (age 1_000) survives.
        let evicted = s.expire_older_than(10_000, secs(5)).await;
        assert!(evicted == 1);
        let snap = s.snapshot("t", PartitionIndex(0)).await;
        assert!(snap.len() == 1);
        assert!(snap[0].0 == 2, "only the recently-active producer survives");
    }

    #[tokio::test]
    async fn expire_evicts_entry_at_exact_ttl_boundary() {
        let s = ProducerState::new();
        commit!(s, "t", PartitionIndex(0), 1, 0, 0, 0, 0, 0).await;
        {
            let h = s.handle("t", PartitionIndex(0));
            h.lock()
                .await
                .entries
                .get_mut(&ProducerId(1))
                .unwrap()
                .last_activity_ms = 5_000;
        }

        let evicted = s.expire_older_than(10_000, secs(5)).await;
        assert!(evicted == 1);
        assert!(s.snapshot("t", PartitionIndex(0)).await.is_empty());
    }

    #[tokio::test]
    async fn active_snapshot_excludes_expired_includes_active() {
        let s = ProducerState::new();
        // pid 1: last batch base_offset 10; pid 2: base_offset 20.
        commit!(
            s,
            "t",
            PartitionIndex(0),
            1,
            0,
            0,
            0,
            /* base_offset */ 10,
            0,
        )
        .await;
        commit!(
            s,
            "t",
            PartitionIndex(0),
            2,
            0,
            0,
            0,
            /* base_offset */ 20,
            0,
        )
        .await;
        {
            let h = s.handle("t", PartitionIndex(0));
            let mut st = h.lock().await;
            st.entries.get_mut(&ProducerId(1)).unwrap().last_activity_ms = 1_000; // old
            st.entries.get_mut(&ProducerId(2)).unwrap().last_activity_ms = 9_500; // recent
        }
        // now = 10_000, expiration = 5_000 → pid 1 (age 9_000 > 5_000)
        // excluded; pid 2 (age 500 <= 5_000) included with its base_offset.
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
        {
            let h = s.handle("t", PartitionIndex(0));
            h.lock()
                .await
                .entries
                .get_mut(&ProducerId(1))
                .unwrap()
                .last_activity_ms = 0;
        }
        let evicted = s.expire_older_than(1_000_000, millis(1)).await;
        // The empty partition and topic maps are pruned (the empty topic slot
        // must be removed), and a subsequent produce still works after pruning.
        check!(evicted == 1);
        check!(s.by_topic.get("t").is_none());
        check!(s.check("t", PartitionIndex(0), 1, 0, 0, 0).await == Decision::Append);
    }
}
