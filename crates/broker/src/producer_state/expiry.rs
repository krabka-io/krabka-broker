//! The `producer.id.expiration.ms` inactivity window: eviction of the
//! producers that have gone quiet.
//!
//! A broker maintenance loop calls the eviction so the per-partition maps do
//! not grow without bound. The log cleaner does not read this tracker: it reads
//! the producer state of the partition log, which a follower also updates
//! (`krabka_log::Log::compact`).
#[cfg(test)]
use crate::producer_state::test_support::commit_fixture;
use {
    super::{PartitionMap, PartitionProducerState, ProducerState},
    crate::config_keys::node_or_cluster_default,
    krabka_ids::PartitionIndex,
    krabka_metadata::{MetadataImage, NodeId},
    krabka_units::{Time, convert::TimeExt},
    std::sync::Arc,
    tokio::sync::Mutex,
};

/// Kafka's `TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_CONFIG`, a dynamic
/// broker config.
pub(crate) const PRODUCER_ID_EXPIRATION_MS: &str = "producer.id.expiration.ms";

/// The `producer.id.expiration.ms` that `node` applies as of `image`: its own
/// dynamic value, else the cluster-wide one, else `static_value`.
///
/// Kafka's `DynamicProducerStateManagerConfig` writes the effective value into
/// the `ProducerStateManagerConfig` that every log shares, so the expiry sweep
/// and the transaction checks of the next request both read it. Kafka defines
/// the key as a `LONG` of at least 1 and refuses any other value at alter
/// time, so when the dynamic value that is set does not parse as one,
/// `static_value` applies.
///
/// This is read on each use, so a dynamic change takes effect for the next
/// request and the next sweep.
pub(crate) fn producer_id_expiration(
    image: &MetadataImage,
    node: NodeId,
    static_value: Time,
) -> Time {
    node_or_cluster_default(image, node, PRODUCER_ID_EXPIRATION_MS)
        .and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|millis| *millis >= 1)
        .map_or(static_value, <Time as TimeExt>::from_millis)
}

/// A topic and its partition map, copied out so no `DashMap` guard is held.
struct TopicSlot {
    topic: String,
    partitions: Arc<PartitionMap>,
}

/// A partition and its producer-state handle, copied out of its map.
struct PartitionSlot {
    partition: PartitionIndex,
    handle: Arc<Mutex<PartitionProducerState>>,
}

impl ProducerState {
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
        let topics: Vec<TopicSlot> = self
            .by_topic
            .iter()
            .map(|e| TopicSlot {
                topic: e.key().clone(),
                partitions: e.value().clone(),
            })
            .collect();
        for TopicSlot {
            topic,
            partitions: parts,
        } in topics
        {
            let partition_refs: Vec<PartitionSlot> = parts
                .iter()
                .map(|e| PartitionSlot {
                    partition: *e.key(),
                    handle: e.value().clone(),
                })
                .collect();
            for PartitionSlot { partition, handle } in partition_refs {
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
    use {
        super::*, crate::producer_state::Decision, assert2::check, krabka_log::ProducerId,
        krabka_units::millis,
    };

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
                s.mirror_log_entries("t", PartitionIndex(0), vec![
                    krabka_log::ProducerSnapshotEntry {
                        producer_id: ProducerId(7),
                        producer_epoch: 0,
                        last_sequence: 0,
                        last_offset: krabka_log::Offset(0),
                        offset_delta: 0,
                        timestamp: T0,
                        coordinator_epoch: 0,
                        current_txn_first_offset: None,
                    },
                ])
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
    async fn expire_drops_empty_partition_and_topic_slots() {
        let s = ProducerState::new();
        commit_fixture()
            .state(&s)
            .producer(1)
            .delta(0)
            .timestamp(0)
            .call()
            .await;
        let evicted = s.expire_older_than(1_000_000, millis(1)).await;
        // The empty partition and topic maps are pruned (the empty topic slot
        // must be removed), and a subsequent produce still works after pruning.
        check!(evicted == 1);
        check!(s.by_topic.get("t").is_none());
        check!(s.check("t", PartitionIndex(0), 1, 0, 0, 0).await == Decision::Append);
    }

    /// A dynamic `producer.id.expiration.ms` of one broker, or of the cluster.
    #[derive(Clone, Copy, Debug)]
    struct DynamicValue {
        node: NodeId,
        value: &'static str,
    }

    /// This node, whose effective value the rows resolve.
    const NODE: NodeId = NodeId(1);

    fn cluster(value: &'static str) -> DynamicValue {
        DynamicValue {
            node: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
            value,
        }
    }

    fn broker(node: NodeId, value: &'static str) -> DynamicValue { DynamicValue { node, value } }

    /// Kafka's `DynamicProducerStateManagerConfig`: this broker's dynamic
    /// `producer.id.expiration.ms` beats the cluster-wide one, which beats the
    /// static value. A value that is not a `LONG` of at least 1, which Kafka
    /// refuses at alter time, does not apply: the static value does, as for
    /// `transaction.partition.verification.enable`. Another broker's value is
    /// not this broker's.
    #[yare::parameterized(
        static_only = { &[], 86_400_000 },
        cluster_default = { &[cluster("60000")], 60_000 },
        cluster_default_blanks = { &[cluster(" 60000 ")], 60_000 },
        per_broker = { &[broker(NODE, "1000")], 1_000 },
        per_broker_over_cluster = { &[cluster("60000"), broker(NODE, "1000")], 1_000 },
        another_broker = { &[broker(NodeId(2), "1000")], 86_400_000 },
        unparsable_per_broker_falls_to_static = {
            &[cluster("60000"), broker(NODE, "soon")], 86_400_000
        },
        unparsable_cluster = { &[cluster("soon")], 86_400_000 },
        zero = { &[cluster("0")], 86_400_000 },
        negative = { &[broker(NODE, "-5")], 86_400_000 },
        one = { &[broker(NODE, "1")], 1 },
    )]
    fn the_expiration_reads_the_broker_then_the_cluster_then_the_static_value(
        records: &[DynamicValue],
        want_ms: i64,
    ) {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for DynamicValue { node, value } in records {
            image.apply(&krabka_metadata::MetadataRecord::V1BrokerConfig(
                krabka_metadata::BrokerConfigRecord {
                    node_id: *node,
                    config_name: PRODUCER_ID_EXPIRATION_MS.to_owned(),
                    config_value: Some((*value).to_owned()),
                },
            ));
        }

        let got = producer_id_expiration(&image, NODE, millis(86_400_000));

        check!(got.millis_i64() == want_ms);
    }
}
