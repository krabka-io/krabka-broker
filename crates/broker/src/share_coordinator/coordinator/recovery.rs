//! Replay of the locally-led `__share_group_state` partitions back into the
//! in-memory delivery state.
//!
//! `Broker::start` calls `recover` once. It refreshes the leadership set and
//! then folds every `ShareSnapshot`, `ShareUpdate`, and tombstone record of
//! each led partition into the state map. This path only reads the log, so it
//! lives apart from the write path in `persist`.

use std::sync::Arc;

use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_metadata::MetadataImage;
use tokio::sync::Mutex;
use tracing::{info, warn};

#[cfg(test)]
mod tests;

use super::{ShareCoordinator, ShareStateKey3};
use crate::{
    error::BrokerError,
    share_coordinator::{
        bootstrap,
        persistence::{
            KEY_SHARE_SNAPSHOT, KEY_SHARE_UPDATE, ShareSnapshotValue, ShareStateKey,
            ShareUpdateValue, parse_state_key,
        },
        state::SharePartitionState,
    },
};

impl ShareCoordinator {
    /// Replays every locally-led `__share_group_state` partition.
    ///
    /// The replayed records go into the in-memory state map. `Broker::start`
    /// calls this method.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] if the leadership refresh fails. The method logs
    /// a per-partition read error and then skips that partition, as if it holds
    /// nothing to replay.
    pub(crate) async fn recover(&self, image: &MetadataImage) -> Result<(), BrokerError> {
        self.refresh_leader_partitions(image).await;
        self.replay_led_partitions().await;
        info!(
            keys_loaded = self.state.len(),
            "ShareCoordinator recovery complete"
        );
        Ok(())
    }

    /// Replays the log of every currently-led `__share_group_state` partition.
    ///
    /// The replayed records go into the in-memory state map. This method
    /// assumes that `refresh_leader_partitions` already filled
    /// `leader_partitions`.
    async fn replay_led_partitions(&self) {
        let local_partitions: Vec<PartitionIndex> = self
            .leader_partitions
            .read()
            .await
            .iter()
            .copied()
            .collect();

        let read_max = self.config.recovery_read_max;

        for p in local_partitions {
            let Some(part) = self.partitions.get(bootstrap::TOPIC, p) else {
                continue;
            };

            let mut offset = part.log_start_offset();
            loop {
                let out = match part.read_log(offset, read_max) {
                    Ok(o) => o,
                    Err(e) => {
                        warn!(
                            partition = p.get(),
                            error = %e,
                            "read error during __share_group_state recovery; skipping partition"
                        );
                        break;
                    }
                };

                if out.batches.is_empty() {
                    break;
                }

                for batch in &out.batches {
                    for rec in &batch.records {
                        let rec_offset = Offset(batch.base_offset + i64::from(rec.offset_delta));
                        let Some(key_bytes) = rec.key.as_ref() else {
                            continue;
                        };
                        let key = match parse_state_key(key_bytes) {
                            Ok(k) => k,
                            Err(e) => {
                                warn!(
                                    partition = p.get(),
                                    error = %e,
                                    "invalid share-state key; skipping record"
                                );
                                continue;
                            }
                        };
                        let map_key = (key.group_id.clone(), key.topic_id, key.partition);

                        // Tombstone: drop the in-memory entry.
                        let Some(value) = rec.value.as_ref() else {
                            self.state.remove(&map_key);
                            continue;
                        };

                        self.replay_value(&key, &map_key, value, rec_offset, p);
                    }
                    offset = Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
                }
            }
        }
    }

    /// Folds one replayed record value into the in-memory state map.
    ///
    /// A snapshot record resets the state and records `last_snapshot_offset`.
    /// An update record applies a delta.
    fn replay_value(
        &self,
        key: &ShareStateKey,
        map_key: &ShareStateKey3,
        value: &Bytes,
        rec_offset: Offset,
        partition: PartitionIndex,
    ) {
        let entry = self
            .state
            .entry(map_key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(SharePartitionState::default())))
            .value()
            .clone();
        // Recovery runs single-threaded before the coordinator is shared, so
        // the lock is uncontended; `try_lock` keeps `recover` non-async here.
        let mut st = entry
            .try_lock()
            .expect("share-state recovery lock uncontended");

        match key.record_type {
            KEY_SHARE_SNAPSHOT => match ShareSnapshotValue::decode(value) {
                Ok(snap) => {
                    st.apply_snapshot(&snap);
                    st.last_snapshot_offset = rec_offset;
                }
                Err(e) => warn!(
                    partition = partition.get(),
                    error = %e,
                    "invalid ShareSnapshot value; skipping record"
                ),
            },
            KEY_SHARE_UPDATE => match ShareUpdateValue::decode(value) {
                Ok(upd) => st.apply_update(&upd),
                Err(e) => warn!(
                    partition = partition.get(),
                    error = %e,
                    "invalid ShareUpdate value; skipping record"
                ),
            },
            other => warn!(
                partition = partition.get(),
                record_type = other,
                "unknown share-state record type"
            ),
        }
    }
}
