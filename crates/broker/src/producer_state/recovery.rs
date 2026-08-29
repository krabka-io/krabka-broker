//! Installation of producer state that was rebuilt from a partition's durable
//! log.
//!
//! Startup and follower-prefix hydration call these functions before a
//! partition becomes request-visible, so a recovered `ProducerState` carries
//! the sequence and epoch state that survived a restart or a remote-tier copy.

use std::{collections::HashMap, sync::Arc};

use dashmap::DashMap;
use krabka_ids::PartitionIndex;
use krabka_log::ProducerId;
use tokio::sync::Mutex;

use super::{PartitionProducerState, ProducerEntry, ProducerState};

impl ProducerState {
    /// Replace one partition's producer sequence state with the state rebuilt
    /// from its durable log.
    ///
    /// Startup calls this for disk-backed and diskless partitions before the
    /// partition writer starts. `Log::open` has already loaded the latest
    /// valid Kafka-compatible producer snapshot and replayed its uncovered
    /// tail, including state whose source segment was removed locally after
    /// remote-tier copy.
    ///
    /// # Errors
    /// This currently cannot fail; the result remains fallible so startup can
    /// preserve its existing error boundary if snapshot projection gains a
    /// checked conversion later.
    pub async fn rebuild_from_log(
        &self,
        topic: &str,
        partition: PartitionIndex,
        log: &krabka_log::Log,
    ) -> Result<(), krabka_log::LogError> {
        self.rebuild_from_snapshot(topic, partition, log.producer_state_snapshot())
            .await;
        Ok(())
    }

    pub(crate) async fn rebuild_from_snapshot(
        &self,
        topic: &str,
        partition: PartitionIndex,
        snapshot: Vec<krabka_log::ProducerSnapshotEntry>,
    ) {
        self.handle(topic, partition).lock().await.entries = entries_from_snapshot(snapshot);
    }

    /// Install recovered producer state before a partition becomes
    /// request-visible as leader.
    ///
    /// Unlike [`Self::rebuild_from_snapshot`], this replaces the map handle
    /// synchronously. The partition does not exist in `PartitionRegistry` yet,
    /// so no request can have acquired the new handle. Vacant materialization
    /// can therefore make follower-prefix hydration and idempotent-producer
    /// recovery one atomic publication boundary from the request path's point
    /// of view.
    pub(crate) fn install_snapshot_before_materialization(
        &self,
        topic: &str,
        partition: PartitionIndex,
        snapshot: Vec<krabka_log::ProducerSnapshotEntry>,
    ) {
        let parts = if let Some(existing) = self.by_topic.get(topic) {
            existing.value().clone()
        } else {
            self.by_topic
                .entry(topic.to_string())
                .or_insert_with(|| Arc::new(DashMap::new()))
                .value()
                .clone()
        };
        parts.insert(
            partition,
            Arc::new(Mutex::new(PartitionProducerState {
                entries: entries_from_snapshot(snapshot),
            })),
        );
    }
}

fn entries_from_snapshot(
    snapshot: Vec<krabka_log::ProducerSnapshotEntry>,
) -> HashMap<ProducerId, ProducerEntry> {
    let recovered_at = crate::txn::util::now_millis();
    snapshot
        .into_iter()
        .map(|entry| {
            let base_offset = if entry.last_offset >= 0 {
                entry.last_offset.0 - i64::from(entry.offset_delta)
            } else {
                // A marker-only producer has no retained data batch.
                -1
            };
            (
                entry.producer_id,
                ProducerEntry {
                    epoch: entry.producer_epoch,
                    last_sequence: entry.last_sequence,
                    last_offset: entry.last_offset.0,
                    base_offset,
                    last_timestamp: entry.timestamp,
                    last_activity_ms: recovered_at,
                },
            )
        })
        .collect()
}
