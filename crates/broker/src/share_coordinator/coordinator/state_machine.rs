//! The `ShareCoordinator` state machine: `initialize`, `write`, `read`,
//! `read_summary`, and `delete`.
//!
//! These are the five operations the KIP-932 persister RPCs drive. They hold
//! the epoch-fencing rules, the in-memory delivery-state updates, and the
//! decision to fold a `ShareSnapshot`. They sit apart from the durable append
//! in `persist` and the log replay in `recovery`, so that the fencing
//! semantics read on their own.

use std::sync::Arc;

use krabka_log::Offset;
use krabka_metadata::MetadataImage;
use tokio::sync::Mutex;
use tracing::warn;

use super::{
    LeaderEpoch, ShareCoordinator, ShareErrorCode, ShareStateError, ShareStateSummary, ShareWrite,
    StateEpoch, message,
};
use crate::{
    codes,
    share_coordinator::{
        persistence::{
            KEY_SHARE_SNAPSHOT, KEY_SHARE_UPDATE, ShareSnapshotValue, ShareStateKey,
            ShareUpdateValue,
        },
        state::SharePartitionState,
    },
};

// The state-machine methods are consumed by the persister RPC handlers and
// the group-lifecycle hook.
impl ShareCoordinator {
    /// Initializes the share state for `(group, topic_id, partition)`.
    ///
    /// The new state starts at `state_epoch` and `start_offset`. This method
    /// fences with `FENCED_STATE_EPOCH` if a state with
    /// `state_epoch >= new state_epoch` already exists. If not, it writes a
    /// `ShareSnapshot` record and seeds the in-memory state.
    ///
    /// # Errors
    ///
    /// Returns the per-partition error code on a fenced epoch. Returns
    /// `COORDINATOR_NOT_AVAILABLE` if the persist fails. Returns
    /// `COORDINATOR_LOAD_IN_PROGRESS` or `NOT_COORDINATOR` when the state
    /// partition of the key is not active on this broker.
    pub(crate) async fn initialize(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        state_epoch: StateEpoch,
        start_offset: Offset,
    ) -> Result<(), ShareErrorCode> {
        let map_key = (group.to_string(), topic_id, partition);
        let state_partition = self.state_partition_for(group, &topic_id, partition);
        let _led = self.active(state_partition).await?;

        if let Some(existing) = self.state.get(&map_key) {
            let cur = existing.value().clone();
            let guard = cur.lock().await;
            if guard.state_epoch >= state_epoch {
                return Err(crate::codes::FENCED_STATE_EPOCH);
            }
        }

        let snapshot = ShareSnapshotValue {
            snapshot_epoch: 0,
            state_epoch,
            leader_epoch: 0,
            start_offset,
            delivery_complete_count: 0,
            state_batches: Vec::new(),
        };
        let key = ShareStateKey {
            record_type: KEY_SHARE_SNAPSHOT,
            group_id: group.to_string(),
            topic_id,
            partition,
        };
        let offset = self
            .persist_record(state_partition, key, Some(snapshot.encode()))
            .await
            .map_err(|e| {
                warn!(error = %e, "share initialize persist failed");
                e.share_error().code()
            })?;

        let mut st = SharePartitionState::default();
        st.apply_snapshot(&snapshot);
        st.last_snapshot_offset = offset;
        self.state.insert(map_key, Arc::new(Mutex::new(st)));
        Ok(())
    }

    /// Applies a `WriteShareGroupState` partition, as Kafka's
    /// `ShareCoordinatorShard.writeState` does.
    ///
    /// The checks run in Kafka's order (`maybeGetWriteStateError`): a
    /// negative partition, leader epoch or state epoch, an uninitialized key,
    /// a stored leader epoch or state epoch above the request, and a topic
    /// partition that `image` does not hold. The `ShareUpdate` record keeps
    /// the stored state epoch and leader epoch, never lets the start offset go
    /// back, and picks the delivery complete count as
    /// `generateShareStateRecord` does. The in-memory state changes only after
    /// the append succeeds. Every `snapshot_update_records_per_snapshot`
    /// updates the method also folds a full `ShareSnapshot` and prunes the
    /// redundant log prefix.
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
        if st.leader_epoch > request.leader_epoch {
            return Err(ShareStateError::Refused {
                code: codes::FENCED_LEADER_EPOCH,
                message: message::FENCED_LEADER_EPOCH,
            });
        }
        if st.state_epoch > request.state_epoch {
            return Err(ShareStateError::Refused {
                code: codes::FENCED_STATE_EPOCH,
                message: message::FENCED_STATE_EPOCH,
            });
        }
        check_topic_partition(image, topic_id, partition)?;

        let update = ShareUpdateValue {
            snapshot_epoch: st.snapshot_epoch,
            // A write never changes the leader epoch. Only a read raises it.
            leader_epoch: st.leader_epoch,
            start_offset: request.start_offset.max(st.start_offset),
            delivery_complete_count: delivery_complete_count(
                &st,
                request.start_offset,
                request.delivery_complete_count,
            ),
            state_batches: request.batches,
        };
        let folded = self
            .append_update(&mut st, group, topic_id, partition, update)
            .await?;
        // Release the per-key lock before pruning so the per-partition scan
        // can lock sibling keys.
        drop(st);
        if folded {
            self.maybe_prune(state_partition).await;
        }
        Ok(())
    }

    /// Serves a `ReadShareGroupState` partition, as Kafka's
    /// `ShareCoordinatorShard.readStateAndMaybeUpdateLeaderEpoch` does.
    ///
    /// The checks run in Kafka's order (`maybeGetReadStateError`): a negative
    /// partition or leader epoch, an uninitialized key, a stored leader epoch
    /// above the request, and a topic partition that `image` does not hold.
    /// When `leader_epoch` differs from the stored leader epoch, the method
    /// appends a `ShareUpdate` with the new leader epoch before it answers. A
    /// later write from a share-partition leader with an older epoch is then
    /// fenced.
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
        if st.leader_epoch > leader_epoch {
            return Err(ShareStateError::Refused {
                code: codes::FENCED_LEADER_EPOCH,
                message: message::FENCED_LEADER_EPOCH,
            });
        }
        check_topic_partition(image, topic_id, partition)?;

        let current = st.clone();
        if st.leader_epoch == leader_epoch {
            return Ok(current);
        }
        let update = ShareUpdateValue {
            snapshot_epoch: st.snapshot_epoch,
            leader_epoch,
            start_offset: st.start_offset,
            delivery_complete_count: st.delivery_complete_count,
            state_batches: Vec::new(),
        };
        let folded = self
            .append_update(&mut st, group, topic_id, partition, update)
            .await?;
        drop(st);
        if folded {
            self.maybe_prune(state_partition).await;
        }
        Ok(current)
    }

    /// The state cell of `(group, topic_id, partition)`, when the key has
    /// state.
    fn entry(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<Arc<Mutex<SharePartitionState>>> {
        self.state
            .get(&(group.to_string(), topic_id, partition))
            .map(|entry| entry.value().clone())
    }

    /// Appends `update` and applies it to `st` after the append succeeds.
    ///
    /// When the update count crosses the snapshot threshold, the method also
    /// folds a `ShareSnapshot`. It returns `true` when it folded one, so the
    /// caller prunes the log after it releases the key lock. A failed fold
    /// does not fail the update.
    async fn append_update(
        &self,
        st: &mut tokio::sync::MutexGuard<'_, SharePartitionState>,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        update: ShareUpdateValue,
    ) -> Result<bool, ShareStateError> {
        let state_partition = self.state_partition_for(group, &topic_id, partition);
        let key = ShareStateKey {
            record_type: KEY_SHARE_UPDATE,
            group_id: group.to_string(),
            topic_id,
            partition,
        };
        self.persist_record(state_partition, key, Some(update.encode()))
            .await
            .map_err(|e| {
                warn!(error = %e, "share update persist failed");
                e.share_error()
            })?;
        st.apply_update(&update);

        if st.updates_since_snapshot < self.config.snapshot_update_records_per_snapshot {
            return Ok(false);
        }
        let Some(snapshot) = st.to_snapshot() else {
            warn!(
                group,
                partition, "share snapshot skipped because its epoch is exhausted"
            );
            return Ok(false);
        };
        let snap_key = ShareStateKey {
            record_type: KEY_SHARE_SNAPSHOT,
            group_id: group.to_string(),
            topic_id,
            partition,
        };
        match self
            .persist_record(state_partition, snap_key, Some(snapshot.encode()))
            .await
        {
            Ok(offset) => {
                st.apply_snapshot(&snapshot);
                st.last_snapshot_offset = offset;
                Ok(true)
            }
            Err(e) => {
                // The update itself was durable; a missed snapshot fold is
                // recoverable on the next threshold crossing.
                warn!(error = %e, "share snapshot persist failed");
                Ok(false)
            }
        }
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
    /// As [`ShareCoordinator::read`].
    pub(crate) async fn read_summary(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<Option<ShareStateSummary>, ShareErrorCode> {
        let state_partition = self.state_partition_for(group, &topic_id, partition);
        let _led = self.active(state_partition).await?;
        let map_key = (group.to_string(), topic_id, partition);
        let Some(handle) = self.state.get(&map_key).map(|entry| entry.value().clone()) else {
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

    /// Deletes the share state for `(group, topic_id, partition)`.
    ///
    /// This method writes a tombstone with the snapshot key and a null value.
    /// It then drops the in-memory entry.
    ///
    /// # Errors
    ///
    /// Returns `COORDINATOR_NOT_AVAILABLE` if the tombstone persist fails, and
    /// the codes of [`ShareCoordinator::read`] when the state partition is not
    /// active.
    pub(crate) async fn delete(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<(), ShareErrorCode> {
        let map_key = (group.to_string(), topic_id, partition);
        let state_partition = self.state_partition_for(group, &topic_id, partition);
        let _led = self.active(state_partition).await?;
        let key = ShareStateKey {
            record_type: KEY_SHARE_SNAPSHOT,
            group_id: group.to_string(),
            topic_id,
            partition,
        };
        self.persist_record(state_partition, key, None)
            .await
            .map_err(|e| {
                warn!(error = %e, "share delete persist failed");
                e.share_error().code()
            })?;
        self.state.remove(&map_key);
        Ok(())
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

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use tempfile::tempdir;

    use super::*;
    use crate::share_coordinator::coordinator::test_support::{
        batch, coordinator, image_with_topic, lead_all, share_write,
    };

    const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([5; 16]);

    type WriteOutcome = Result<(), ShareStateError>;

    /// `(partition, (state_epoch, leader_epoch), (start_offset, dcc), drop the
    /// state partition log, expected result, expected summary)`.
    type WriteRow = (
        i32,
        (i32, i32),
        (i64, i32),
        bool,
        WriteOutcome,
        Option<ShareStateSummary>,
    );

    /// `(partition, leader_epoch, expected read as (state_epoch, start),
    /// expected write with leader epoch 3, stored leader epoch after reload)`.
    type ReadRow = (
        i32,
        i32,
        Result<(i32, i64), ShareStateError>,
        Option<WriteOutcome>,
        i32,
    );

    fn refused(code: ShareErrorCode, message: &'static str) -> ShareStateError {
        ShareStateError::Refused { code, message }
    }

    #[tokio::test]
    async fn initialize_then_summary() {
        let dir = tempdir().unwrap();
        let (coord, _reg) = coordinator(dir.path());
        lead_all(&coord).await;

        coord
            .initialize("g", TOPIC, 0, 5, Offset(100))
            .await
            .unwrap();

        let summary = coord.read_summary("g", TOPIC, 0).await;
        assert!(summary == Ok(Some((5, 0, Offset(100), 0))));
    }

    #[tokio::test]
    async fn initialize_fences_stale_state_epoch() {
        let dir = tempdir().unwrap();
        let (coord, _reg) = coordinator(dir.path());
        lead_all(&coord).await;

        coord.initialize("g", TOPIC, 0, 5, Offset(0)).await.unwrap();
        let err = coord
            .initialize("g", TOPIC, 0, 5, Offset(0))
            .await
            .unwrap_err();
        assert!(err == codes::FENCED_STATE_EPOCH);
    }

    #[tokio::test]
    async fn delete_removes_state() {
        let dir = tempdir().unwrap();
        let (coord, _reg) = coordinator(dir.path());
        lead_all(&coord).await;

        coord.initialize("g", TOPIC, 0, 1, Offset(0)).await.unwrap();
        assert!(coord.read_summary("g", TOPIC, 0).await.unwrap().is_some());
        coord.delete("g", TOPIC, 0).await.unwrap();
        assert!(coord.read_summary("g", TOPIC, 0).await.unwrap().is_none());
    }

    /// Stored state for the read and write tables, on topic `TOPIC` with two
    /// partitions: partition 0 at state epoch 2, leader epoch 3, start offset
    /// 10 and delivery complete count 4, and partition 2 (not in the image)
    /// at state epoch 2.
    async fn seeded(dir: &std::path::Path) -> (ShareCoordinator, MetadataImage) {
        let (coord, _reg) = coordinator(dir);
        lead_all(&coord).await;
        let image = image_with_topic(TOPIC, 2);
        coord
            .initialize("g", TOPIC, 0, 2, Offset(10))
            .await
            .unwrap();
        coord
            .initialize("g", TOPIC, 2, 2, Offset(10))
            .await
            .unwrap();
        coord.read(&image, "g", TOPIC, 0, 3).await.unwrap();
        coord
            .write(&image, "g", TOPIC, 0, share_write((2, 3), (10, 4), vec![]))
            .await
            .unwrap();
        assert!(coord.read_summary("g", TOPIC, 0).await == Ok(Some((2, 3, Offset(10), 4))));
        (coord, image)
    }

    /// `WriteShareGroupState` as Kafka's `ShareCoordinatorShard.writeState`
    /// answers it, and the stored summary after each write.
    #[tokio::test]
    async fn write_matches_kafka_checks_and_record_rules() {
        let unchanged = Some((2, 3, Offset(10), 4));
        // (partition, (state_epoch, leader_epoch), (start_offset, dcc),
        //  drop the state partition log, expected result, expected summary)
        let rows: [WriteRow; 13] = [
            (
                0,
                (2, 3),
                (10, 2),
                false,
                Ok(()),
                Some((2, 3, Offset(10), 4)),
            ),
            (
                0,
                (2, 3),
                (5, 9),
                false,
                Ok(()),
                Some((2, 3, Offset(10), 4)),
            ),
            (
                0,
                (2, 3),
                (20, 1),
                false,
                Ok(()),
                Some((2, 3, Offset(20), 1)),
            ),
            (
                0,
                (7, 3),
                (10, 4),
                false,
                Ok(()),
                Some((2, 3, Offset(10), 4)),
            ),
            (
                0,
                (2, 9),
                (10, 4),
                false,
                Ok(()),
                Some((2, 3, Offset(10), 4)),
            ),
            (
                0,
                (2, 3),
                (10, -1),
                false,
                Ok(()),
                Some((2, 3, Offset(10), 4)),
            ),
            (
                0,
                (1, 3),
                (10, 4),
                false,
                Err(refused(
                    codes::FENCED_STATE_EPOCH,
                    message::FENCED_STATE_EPOCH,
                )),
                unchanged,
            ),
            (
                0,
                (2, 2),
                (10, 4),
                false,
                Err(refused(
                    codes::FENCED_LEADER_EPOCH,
                    message::FENCED_LEADER_EPOCH,
                )),
                unchanged,
            ),
            (
                1,
                (0, 0),
                (0, 0),
                false,
                Err(refused(
                    codes::INVALID_REQUEST,
                    message::WRITE_UNINITIALIZED_SHARE_PARTITION,
                )),
                None,
            ),
            (
                -1,
                (2, 3),
                (10, 4),
                false,
                Err(refused(
                    codes::INVALID_REQUEST,
                    message::NEGATIVE_PARTITION_ID,
                )),
                None,
            ),
            (
                0,
                (2, -1),
                (10, 4),
                false,
                Err(refused(
                    codes::INVALID_REQUEST,
                    message::NEGATIVE_LEADER_EPOCH,
                )),
                unchanged,
            ),
            (
                2,
                (2, 3),
                (10, 4),
                false,
                Err(refused(
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                    message::UNKNOWN_TOPIC_OR_PARTITION,
                )),
                Some((2, 0, Offset(10), 0)),
            ),
            (
                0,
                (2, 3),
                (30, 0),
                true,
                Err(ShareStateError::Operation {
                    code: codes::COORDINATOR_NOT_AVAILABLE,
                    message: message::UNKNOWN_TOPIC_OR_PARTITION,
                }),
                unchanged,
            ),
        ];

        for (index, (partition, epochs, progress, drop_log, expected, summary)) in
            rows.into_iter().enumerate()
        {
            let dir = tempdir().unwrap();
            let (coord, image) = seeded(dir.path()).await;
            if drop_log {
                let state_partition = coord.state_partition_for("g", &TOPIC, partition);
                coord
                    .partitions
                    .remove(crate::share_coordinator::bootstrap::TOPIC, state_partition);
            }
            let result = coord
                .write(
                    &image,
                    "g",
                    TOPIC,
                    partition,
                    share_write(epochs, progress, vec![batch(progress.0, progress.0 + 9)]),
                )
                .await;
            check!(result == expected, "row {index}");
            check!(
                coord.read_summary("g", TOPIC, partition).await == Ok(summary),
                "row {index}"
            );
        }
    }

    #[tokio::test]
    async fn write_refuses_a_negative_state_epoch() {
        let dir = tempdir().unwrap();
        let (coord, image) = seeded(dir.path()).await;
        let result = coord
            .write(&image, "g", TOPIC, 0, share_write((-1, 3), (10, 4), vec![]))
            .await;
        assert!(
            result
                == Err(refused(
                    codes::INVALID_REQUEST,
                    message::NEGATIVE_STATE_EPOCH
                ))
        );
    }

    /// `ReadShareGroupState` as Kafka's
    /// `readStateAndMaybeUpdateLeaderEpoch` answers it, then a write with
    /// leader epoch 3, and the stored leader epoch after a reload of the log.
    #[tokio::test]
    async fn read_fences_and_persists_the_leader_epoch() {
        // (partition, leader_epoch, expected read as (state_epoch, start),
        //  expected write with leader epoch 3, stored leader epoch after
        //  reload)
        let rows: [ReadRow; 7] = [
            (0, 3, Ok((2, 10)), Some(Ok(())), 3),
            (
                0,
                4,
                Ok((2, 10)),
                Some(Err(refused(
                    codes::FENCED_LEADER_EPOCH,
                    message::FENCED_LEADER_EPOCH,
                ))),
                4,
            ),
            (
                0,
                2,
                Err(refused(
                    codes::FENCED_LEADER_EPOCH,
                    message::FENCED_LEADER_EPOCH,
                )),
                Some(Ok(())),
                3,
            ),
            (
                1,
                0,
                Err(refused(
                    codes::INVALID_REQUEST,
                    message::READ_UNINITIALIZED_SHARE_PARTITION,
                )),
                None,
                3,
            ),
            (
                -1,
                0,
                Err(refused(
                    codes::INVALID_REQUEST,
                    message::NEGATIVE_PARTITION_ID,
                )),
                None,
                3,
            ),
            (
                0,
                -1,
                Err(refused(
                    codes::INVALID_REQUEST,
                    message::NEGATIVE_LEADER_EPOCH,
                )),
                None,
                3,
            ),
            (
                2,
                0,
                Err(refused(
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                    message::UNKNOWN_TOPIC_OR_PARTITION,
                )),
                None,
                3,
            ),
        ];

        for (index, (partition, leader_epoch, expected, write, reloaded_leader_epoch)) in
            rows.into_iter().enumerate()
        {
            let dir = tempdir().unwrap();
            let (coord, image) = seeded(dir.path()).await;
            let read = coord
                .read(&image, "g", TOPIC, partition, leader_epoch)
                .await
                .map(|st| (st.state_epoch, st.start_offset.0));
            check!(read == expected, "row {index}");
            if let Some(expected_write) = write {
                let written = coord
                    .write(
                        &image,
                        "g",
                        TOPIC,
                        partition,
                        share_write((2, 3), (10, 4), vec![]),
                    )
                    .await;
                check!(written == expected_write, "row {index}");
            }
            coord.reload_all_partitions_for_test().await;
            let summary = coord.read_summary("g", TOPIC, 0).await;
            check!(
                summary.map(|s| s.map(|(_, leader, ..)| leader)) == Ok(Some(reloaded_leader_epoch)),
                "row {index}"
            );
        }
    }

    /// A failed append leaves the in-memory state as it was.
    #[tokio::test]
    async fn failed_read_append_changes_no_state() {
        let dir = tempdir().unwrap();
        let (coord, image) = seeded(dir.path()).await;
        let state_partition = coord.state_partition_for("g", &TOPIC, 0);
        coord
            .partitions
            .remove(crate::share_coordinator::bootstrap::TOPIC, state_partition);

        let read = coord.read(&image, "g", TOPIC, 0, 8).await;
        assert!(
            read == Err(ShareStateError::Operation {
                code: codes::COORDINATOR_NOT_AVAILABLE,
                message: message::UNKNOWN_TOPIC_OR_PARTITION,
            })
        );
        assert!(coord.read_summary("g", TOPIC, 0).await == Ok(Some((2, 3, Offset(10), 4))));
    }
}
