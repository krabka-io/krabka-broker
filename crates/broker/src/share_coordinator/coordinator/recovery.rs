//! The load of a led `__share_group_state` partition: a replay of its log
//! into the in-memory delivery state.
//!
//! [`ShareCoordinator::refresh_leader_partitions`] starts one load task for
//! each partition that this broker starts to lead. The task folds every
//! `ShareSnapshot`, `ShareUpdate`, and tombstone record of the partition into
//! a private map. It installs the map and marks the partition active only if
//! no newer term of the partition started in the meantime. This path only
//! reads the log, so it lives apart from the write path in `persist`.

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_metadata::MetadataImage;
use tokio::sync::Mutex;
use tracing::{info, warn};

#[cfg(test)]
mod tests;

use super::{LoadStatus, ShareCoordinator, ShareStateKey3};
use crate::{
    error::BrokerError,
    partition::Partition,
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
    /// Applies the leadership of `image` and waits until every load that it
    /// started has ended.
    ///
    /// `Broker::start` calls this method, so the partitions that this broker
    /// leads at start serve requests as soon as the broker is up.
    ///
    /// # Errors
    ///
    /// This method does not fail at present. A partition whose replay fails
    /// is logged and stays failed until the next refresh loads it again.
    pub(crate) async fn recover(
        self: &Arc<Self>,
        image: &MetadataImage,
    ) -> Result<(), BrokerError> {
        self.refresh_leader_partitions(image).await.finished().await;
        info!(
            keys_loaded = self.state.len(),
            "ShareCoordinator recovery complete"
        );
        Ok(())
    }

    /// Replays `state_partition` and installs the result for term
    /// `generation`.
    ///
    /// The replay reads the log on the blocking thread pool, because
    /// `Partition::read_log` takes the log mutex and reads from disk.
    pub(super) async fn load_partition(&self, state_partition: PartitionIndex, generation: u64) {
        let read_max = self.config.recovery_read_max;
        let replayed = match self.partitions.get(bootstrap::TOPIC, state_partition) {
            Some(part) => tokio::task::spawn_blocking(move || {
                replay_partition(&part, state_partition, read_max)
            })
            .await
            .unwrap_or_else(|error| {
                Err(BrokerError::Share(format!(
                    "__share_group_state-{state_partition} replay task failed: {error}"
                )))
            }),
            None => Err(BrokerError::Share(format!(
                "__share_group_state-{state_partition} is not open locally"
            ))),
        };
        self.install_load(state_partition, generation, replayed)
            .await;
    }

    /// Ends the load of term `generation` of `state_partition`.
    ///
    /// The install runs under the write guard of the leadership map, and only
    /// while the partition still loads that term. A replayed map goes into the
    /// state map, and the partition becomes active. A failed replay installs
    /// nothing, and the partition becomes failed: it answers
    /// `NOT_COORDINATOR`, as Kafka's `CoordinatorRuntime` answers for a
    /// `FAILED` shard, and the next refresh loads it again. A partial map is
    /// never served.
    pub(super) async fn install_load(
        &self,
        state_partition: PartitionIndex,
        generation: u64,
        replayed: Result<HashMap<ShareStateKey3, SharePartitionState>, BrokerError>,
    ) {
        let mut led = self.leader_partitions.write().await;
        let Some(entry) = led
            .get_mut(&state_partition)
            .filter(|entry| entry.generation == generation && entry.status == LoadStatus::Loading)
        else {
            info!(
                partition = state_partition.get(),
                "__share_group_state load superseded; replayed state dropped"
            );
            return;
        };
        match replayed {
            Ok(replayed) => {
                let keys_loaded = replayed.len();
                for (key, state) in replayed {
                    self.state.insert(key, Arc::new(Mutex::new(state)));
                }
                entry.status = LoadStatus::Active;
                info!(
                    partition = state_partition.get(),
                    keys_loaded, "__share_group_state partition loaded"
                );
            }
            Err(error) => {
                entry.status = LoadStatus::Failed;
                warn!(
                    partition = state_partition.get(),
                    %error,
                    "__share_group_state load failed; the next refresh loads it again"
                );
            }
        }
    }
}

/// Reads the whole log of `state_partition` and folds it into a new map.
///
/// # Errors
///
/// Returns the read error of the log. The caller must not serve a partial
/// replay.
fn replay_partition(
    part: &Partition,
    state_partition: PartitionIndex,
    read_max: krabka_units::ByteSize,
) -> Result<HashMap<ShareStateKey3, SharePartitionState>, BrokerError> {
    let mut replayed = HashMap::new();
    let mut offset = part.log_start_offset();
    loop {
        let out = part.read_log(offset, read_max)?;

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
                            partition = state_partition.get(),
                            error = %e,
                            "invalid share-state key; skipping record"
                        );
                        continue;
                    }
                };
                let map_key = (key.group_id.clone(), key.topic_id, key.partition);

                // Tombstone: drop the entry.
                let Some(value) = rec.value.as_ref() else {
                    replayed.remove(&map_key);
                    continue;
                };

                let st = replayed.entry(map_key).or_default();
                replay_value(st, &key, value, rec_offset, state_partition);
            }
            offset = Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
        }
    }
    Ok(replayed)
}

/// Folds one replayed record value into `st`.
///
/// A snapshot record resets the state and records `last_snapshot_offset`. An
/// update record applies a delta.
fn replay_value(
    st: &mut SharePartitionState,
    key: &ShareStateKey,
    value: &Bytes,
    rec_offset: Offset,
    partition: PartitionIndex,
) {
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
