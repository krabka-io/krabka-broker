//! The `ShareCoordinator` state machine: `initialize`, `write`, `read`,
//! `read_summary`, and `delete`.
//!
//! These are the five operations the KIP-932 persister RPCs drive. They hold
//! the validation and epoch-fencing rules of Kafka's `ShareCoordinatorShard`,
//! and pick the record each operation appends. Every record goes through
//! [`ShareCoordinator::append_state_record`], which appends it and then
//! applies it to the in-memory state exactly as the log replay in `recovery`
//! does.

use std::sync::Arc;

use krabka_log::Offset;
use krabka_metadata::MetadataImage;
use tokio::sync::{Mutex, MutexGuard};
use tracing::warn;

use super::{
    LeaderEpoch, ShareCoordinator, ShareErrorCode, ShareStateError, ShareStateSummary, ShareWrite,
    StateEpoch, UNINITIALIZED_START_OFFSET, message,
};
use crate::{
    codes,
    share_coordinator::{
        persistence::{
            KEY_SHARE_SNAPSHOT, KEY_SHARE_UPDATE, ShareSnapshotValue, ShareStateKey,
            ShareUpdateValue, StateBatch, UNKNOWN_DELIVERY_COMPLETE_COUNT,
        },
        state::{SharePartitionState, combine_state_batches},
    },
};

#[cfg(test)]
mod tests;

/// One `__share_group_state` record value that an operation appends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StateRecord {
    Snapshot(ShareSnapshotValue),
    Update(ShareUpdateValue),
}

/// The progress fields of a write, as Kafka's
/// `WriteShareGroupStateRequestData.PartitionData` carries them into
/// `generateShareStateRecord`.
struct Progress<'a> {
    leader_epoch: LeaderEpoch,
    start_offset: Offset,
    delivery_complete_count: i32,
    batches: &'a [StateBatch],
}

impl ShareCoordinator {
    /// Serves an `InitializeShareGroupState` partition, as Kafka's
    /// `ShareCoordinatorShard.initializeState` does.
    ///
    /// The checks run in Kafka's order (`maybeGetInitializeStateError`): a
    /// negative partition or state epoch, a stored state epoch above the
    /// request, and a topic partition that `image` does not hold. A request
    /// that repeats the stored state epoch and start offset is a no-op. Any
    /// other request writes a `ShareSnapshot` with the next snapshot epoch
    /// (`0` for a new key), leader epoch `0`, no batches, and a delivery
    /// complete count of `-1` for an uninitialized start offset and `0`
    /// otherwise.
    ///
    /// # Errors
    ///
    /// Returns [`ShareStateError::Refused`] when a check fails, and
    /// [`ShareStateError::Operation`] when this broker is not the active
    /// coordinator of the key or the append fails.
    pub(crate) async fn initialize(
        &self,
        image: &MetadataImage,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        state_epoch: StateEpoch,
        start_offset: Offset,
    ) -> Result<(), ShareStateError> {
        let state_partition = self.state_partition_for(group, &topic_id, partition);
        let _led = self
            .active(state_partition)
            .await
            .map_err(ShareStateError::inactive)?;

        if partition < 0 {
            return Err(invalid_request(message::NEGATIVE_PARTITION_ID));
        }
        if state_epoch < 0 {
            return Err(invalid_request(message::NEGATIVE_STATE_EPOCH));
        }
        let entry = self.entry(group, topic_id, partition);
        let mut stored = match &entry {
            Some(entry) => Some(entry.lock().await),
            None => None,
        };
        if stored
            .as_ref()
            .is_some_and(|st| st.fence_state_epoch > state_epoch)
        {
            return Err(ShareStateError::Refused {
                code: codes::FENCED_STATE_EPOCH,
                message: message::FENCED_STATE_EPOCH,
            });
        }
        check_topic_partition(image, topic_id, partition)?;
        if stored.as_ref().is_some_and(|st| {
            st.fence_state_epoch == state_epoch && st.start_offset == start_offset
        }) {
            return Ok(());
        }

        let now = self.now_ms();
        let snapshot = ShareSnapshotValue {
            snapshot_epoch: stored
                .as_ref()
                .map_or(0, |st| st.snapshot_epoch.wrapping_add(1)),
            state_epoch,
            leader_epoch: 0,
            start_offset,
            delivery_complete_count: if start_offset.0 == UNINITIALIZED_START_OFFSET {
                UNKNOWN_DELIVERY_COMPLETE_COUNT
            } else {
                0
            },
            create_timestamp: now,
            write_timestamp: now,
            state_batches: Vec::new(),
        };
        if let Some(st) = stored.as_mut() {
            return self
                .append_state_record(
                    st,
                    group,
                    topic_id,
                    partition,
                    StateRecord::Snapshot(snapshot),
                )
                .await;
        }
        let key = state_key(KEY_SHARE_SNAPSHOT, group, topic_id, partition);
        let offset = self
            .persist_record(state_partition, key, Some(snapshot.encode()))
            .await
            .map_err(|e| {
                warn!(error = %e, "share initialize persist failed");
                e.share_error()
            })?;
        self.state.insert(
            (group.to_string(), topic_id, partition),
            Arc::new(Mutex::new(SharePartitionState::from_snapshot(
                &snapshot, offset,
            ))),
        );
        Ok(())
    }

    /// Applies a `WriteShareGroupState` partition, as Kafka's
    /// `ShareCoordinatorShard.writeState` does.
    ///
    /// The checks run in Kafka's order (`maybeGetWriteStateError`): a
    /// negative partition, leader epoch or state epoch, an uninitialized key,
    /// a recorded leader epoch or state epoch above the request, and a topic
    /// partition that `image` does not hold. The record is the one
    /// `generateShareStateRecord` picks: see
    /// [`ShareCoordinator::share_state_record`].
    ///
    /// # Errors
    ///
    /// Returns [`ShareStateError::Refused`] when a check fails, and
    /// [`ShareStateError::Operation`] when this broker is not the active
    /// coordinator of the key or the append fails.
    pub(crate) async fn write(
        &self,
        image: &MetadataImage,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        request: ShareWrite,
    ) -> Result<(), ShareStateError> {
        let state_partition = self.state_partition_for(group, &topic_id, partition);
        let _led = self
            .active(state_partition)
            .await
            .map_err(ShareStateError::inactive)?;

        if partition < 0 {
            return Err(invalid_request(message::NEGATIVE_PARTITION_ID));
        }
        if request.leader_epoch < 0 {
            return Err(invalid_request(message::NEGATIVE_LEADER_EPOCH));
        }
        if request.state_epoch < 0 {
            return Err(invalid_request(message::NEGATIVE_STATE_EPOCH));
        }
        let Some(entry) = self.entry(group, topic_id, partition) else {
            return Err(invalid_request(
                message::WRITE_UNINITIALIZED_SHARE_PARTITION,
            ));
        };
        let mut st = entry.lock().await;
        if st.fence_leader_epoch > request.leader_epoch {
            return Err(ShareStateError::Refused {
                code: codes::FENCED_LEADER_EPOCH,
                message: message::FENCED_LEADER_EPOCH,
            });
        }
        if st.fence_state_epoch > request.state_epoch {
            return Err(ShareStateError::Refused {
                code: codes::FENCED_STATE_EPOCH,
                message: message::FENCED_STATE_EPOCH,
            });
        }
        check_topic_partition(image, topic_id, partition)?;

        // A write never changes the leader epoch. Only a read raises it.
        let record = self.share_state_record(
            &st,
            &Progress {
                leader_epoch: st.leader_epoch,
                start_offset: request.start_offset,
                delivery_complete_count: request.delivery_complete_count,
                batches: &request.batches,
            },
        );
        self.append_state_record(&mut st, group, topic_id, partition, record)
            .await
    }

    /// Serves a `ReadShareGroupState` partition, as Kafka's
    /// `ShareCoordinatorShard.readStateAndMaybeUpdateLeaderEpoch` does.
    ///
    /// The checks run in Kafka's order (`maybeGetReadStateError`): a negative
    /// partition or leader epoch, an uninitialized key, a recorded leader
    /// epoch above the request, and a topic partition that `image` does not
    /// hold. When `leader_epoch` differs from the recorded leader epoch, the
    /// method appends the record of a write with the new leader epoch and the
    /// stored progress before it answers. A later write from a
    /// share-partition leader with an older epoch is then fenced.
    ///
    /// # Errors
    ///
    /// As [`ShareCoordinator::write`].
    pub(crate) async fn read(
        &self,
        image: &MetadataImage,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        leader_epoch: LeaderEpoch,
    ) -> Result<SharePartitionState, ShareStateError> {
        let state_partition = self.state_partition_for(group, &topic_id, partition);
        let _led = self
            .active(state_partition)
            .await
            .map_err(ShareStateError::inactive)?;

        if partition < 0 {
            return Err(invalid_request(message::NEGATIVE_PARTITION_ID));
        }
        if leader_epoch < 0 {
            return Err(invalid_request(message::NEGATIVE_LEADER_EPOCH));
        }
        let Some(entry) = self.entry(group, topic_id, partition) else {
            return Err(invalid_request(message::READ_UNINITIALIZED_SHARE_PARTITION));
        };
        let mut st = entry.lock().await;
        if st.fence_leader_epoch > leader_epoch {
            return Err(ShareStateError::Refused {
                code: codes::FENCED_LEADER_EPOCH,
                message: message::FENCED_LEADER_EPOCH,
            });
        }
        check_topic_partition(image, topic_id, partition)?;

        let current = st.clone();
        if st.fence_leader_epoch == leader_epoch {
            return Ok(current);
        }
        let record = self.share_state_record(
            &st,
            &Progress {
                leader_epoch,
                start_offset: st.start_offset,
                delivery_complete_count: st.delivery_complete_count,
                batches: &[],
            },
        );
        self.append_state_record(&mut st, group, topic_id, partition, record)
            .await?;
        Ok(current)
    }

    /// The record of a write, as Kafka's `generateShareStateRecord` builds
    /// it.
    ///
    /// The start offset never goes back, and the delivery complete count is
    /// picked by [`delivery_complete_count`]. Once the key has had
    /// `snapshot_update_records_per_snapshot` updates, the record is a
    /// `ShareSnapshot` with the next snapshot epoch, the stored batches
    /// combined with the written ones, and fresh timestamps. Otherwise it is
    /// a `ShareUpdate` that holds only the written batches, combined among
    /// themselves and clipped at the start offset.
    fn share_state_record(&self, st: &SharePartitionState, progress: &Progress<'_>) -> StateRecord {
        let start_offset = progress.start_offset.max(st.start_offset);
        let delivery_complete_count =
            delivery_complete_count(st, progress.start_offset, progress.delivery_complete_count);
        if st.updates_since_snapshot >= self.config.snapshot_update_records_per_snapshot {
            let now = self.now_ms();
            StateRecord::Snapshot(ShareSnapshotValue {
                snapshot_epoch: st.snapshot_epoch.wrapping_add(1),
                state_epoch: st.state_epoch,
                leader_epoch: progress.leader_epoch,
                start_offset,
                delivery_complete_count,
                create_timestamp: now,
                write_timestamp: now,
                state_batches: combine_state_batches(
                    &st.state_batches,
                    progress.batches,
                    start_offset,
                ),
            })
        } else {
            StateRecord::Update(ShareUpdateValue {
                snapshot_epoch: st.snapshot_epoch,
                leader_epoch: progress.leader_epoch,
                start_offset,
                delivery_complete_count,
                state_batches: combine_state_batches(&[], progress.batches, start_offset),
            })
        }
    }

    /// Appends `record` for the key and applies it to `st` after the append
    /// succeeds, as the replay of the record does.
    pub(super) async fn append_state_record(
        &self,
        st: &mut MutexGuard<'_, SharePartitionState>,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        record: StateRecord,
    ) -> Result<(), ShareStateError> {
        let state_partition = self.state_partition_for(group, &topic_id, partition);
        let (record_type, value) = match &record {
            StateRecord::Snapshot(snapshot) => (KEY_SHARE_SNAPSHOT, snapshot.encode()),
            StateRecord::Update(update) => (KEY_SHARE_UPDATE, update.encode()),
        };
        let key = state_key(record_type, group, topic_id, partition);
        let offset = self
            .persist_record(state_partition, key, Some(value))
            .await
            .map_err(|e| {
                warn!(error = %e, "share state persist failed");
                e.share_error()
            })?;
        match &record {
            StateRecord::Snapshot(snapshot) => st.apply_snapshot(
                snapshot,
                offset,
                self.config.snapshot_update_records_per_snapshot,
            ),
            StateRecord::Update(update) => st.apply_update(update),
        }
        Ok(())
    }

    /// The state cell of `(group, topic_id, partition)`, when the key has
    /// state.
    pub(super) fn entry(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<Arc<Mutex<SharePartitionState>>> {
        self.state
            .get(&(group.to_string(), topic_id, partition))
            .map(|entry| entry.value().clone())
    }

    /// Test-only: the in-memory state of a key, with no status check and no
    /// side effect.
    #[cfg(test)]
    pub(crate) async fn state_for_test(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<SharePartitionState> {
        let entry = self.entry(group, topic_id, partition)?;
        let st = entry.lock().await;
        Some(st.clone())
    }

    /// Returns `(state_epoch, leader_epoch, start_offset, delivery_complete_count)`.
    ///
    /// Returns `Ok(None)` when the key has no state.
    ///
    /// # Errors
    ///
    /// Returns `COORDINATOR_LOAD_IN_PROGRESS` or `NOT_COORDINATOR` when the
    /// state partition of the key is not active on this broker.
    pub(crate) async fn read_summary(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<Option<ShareStateSummary>, ShareErrorCode> {
        let state_partition = self.state_partition_for(group, &topic_id, partition);
        let _led = self.active(state_partition).await?;
        let Some(handle) = self.entry(group, topic_id, partition) else {
            return Ok(None);
        };
        let st = handle.lock().await;
        Ok(Some((
            st.state_epoch,
            st.leader_epoch,
            st.start_offset,
            st.delivery_complete_count,
        )))
    }

    /// Serves a `DeleteShareGroupState` partition, as Kafka's
    /// `ShareCoordinatorShard.deleteState` does.
    ///
    /// The checks run in Kafka's order (`maybeGetDeleteStateError`): a
    /// negative partition, then a topic partition that `image` does not hold.
    /// A key with no state is not an error, and nothing is appended for it.
    /// Otherwise the method writes a tombstone with the snapshot key and
    /// drops the in-memory entry.
    ///
    /// # Errors
    ///
    /// As [`ShareCoordinator::initialize`].
    pub(crate) async fn delete(
        &self,
        image: &MetadataImage,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<(), ShareStateError> {
        let state_partition = self.state_partition_for(group, &topic_id, partition);
        let _led = self
            .active(state_partition)
            .await
            .map_err(ShareStateError::inactive)?;
        if partition < 0 {
            return Err(invalid_request(message::NEGATIVE_PARTITION_ID));
        }
        check_topic_partition(image, topic_id, partition)?;
        let Some(entry) = self.entry(group, topic_id, partition) else {
            return Ok(());
        };
        let _st = entry.lock().await;
        self.tombstone(state_partition, group, topic_id, partition)
            .await
    }

    /// Appends the tombstone of a key and drops its in-memory entry. The
    /// caller holds the key lock.
    pub(super) async fn tombstone(
        &self,
        state_partition: krabka_ids::PartitionIndex,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<(), ShareStateError> {
        let key = state_key(KEY_SHARE_SNAPSHOT, group, topic_id, partition);
        self.persist_record(state_partition, key, None)
            .await
            .map_err(|e| {
                warn!(error = %e, "share delete persist failed");
                e.share_error()
            })?;
        self.state.remove(&(group.to_string(), topic_id, partition));
        Ok(())
    }
}

/// The key of a record of `record_type` for `(group, topic_id, partition)`.
fn state_key(record_type: i16, group: &str, topic_id: uuid::Uuid, partition: i32) -> ShareStateKey {
    ShareStateKey {
        record_type,
        group_id: group.to_string(),
        topic_id,
        partition,
    }
}

/// A refusal with `INVALID_REQUEST` and Kafka's validation `message`.
fn invalid_request(message: &'static str) -> ShareStateError {
    ShareStateError::Refused {
        code: codes::INVALID_REQUEST,
        message,
    }
}

/// Refuses a topic id that `image` does not know, or a partition that the
/// topic does not have, with `UNKNOWN_TOPIC_OR_PARTITION`.
fn check_topic_partition(
    image: &MetadataImage,
    topic_id: uuid::Uuid,
    partition: i32,
) -> Result<(), ShareStateError> {
    let known = image
        .topic_by_id(&topic_id)
        .is_some_and(|topic| image.partition(&topic.name, partition).is_some());
    if known {
        Ok(())
    } else {
        Err(ShareStateError::Refused {
            code: codes::UNKNOWN_TOPIC_OR_PARTITION,
            message: message::UNKNOWN_TOPIC_OR_PARTITION,
        })
    }
}

/// The delivery complete count of a write, as Kafka's
/// `generateShareStateRecord` picks it: the greater count when the request
/// start offset equals the stored one, the stored count when the request
/// start offset is smaller, and the request count when it is greater.
fn delivery_complete_count(
    stored: &SharePartitionState,
    request_start_offset: Offset,
    request_count: i32,
) -> i32 {
    match request_start_offset.cmp(&stored.start_offset) {
        std::cmp::Ordering::Equal => request_count.max(stored.delivery_complete_count),
        std::cmp::Ordering::Less => stored.delivery_complete_count,
        std::cmp::Ordering::Greater => request_count,
    }
}
