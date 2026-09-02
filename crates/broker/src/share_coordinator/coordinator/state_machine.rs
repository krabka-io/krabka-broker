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
use tokio::sync::Mutex;
use tracing::warn;

use super::{LeaderEpoch, ShareCoordinator, ShareErrorCode, ShareStateSummary, StateEpoch};
use crate::share_coordinator::{
    persistence::{
        KEY_SHARE_SNAPSHOT, KEY_SHARE_UPDATE, ShareSnapshotValue, ShareStateKey, ShareUpdateValue,
        StateBatch,
    },
    state::SharePartitionState,
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
    /// `COORDINATOR_NOT_AVAILABLE` if the persist fails.
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
                crate::codes::COORDINATOR_NOT_AVAILABLE
            })?;

        let mut st = SharePartitionState::default();
        st.apply_snapshot(&snapshot);
        st.last_snapshot_offset = offset;
        self.state.insert(map_key, Arc::new(Mutex::new(st)));
        Ok(())
    }

    /// Applies a `WriteShareGroupState` delta.
    ///
    /// This method fences a stale `state_epoch` with `FENCED_STATE_EPOCH` and a
    /// stale `leader_epoch` with `FENCED_LEADER_EPOCH`. If not fenced, it
    /// applies the update and persists a `ShareUpdate`. Every
    /// `snapshot_update_records_per_snapshot` updates it also folds a full
    /// `ShareSnapshot` and prunes the redundant log prefix.
    ///
    /// # Errors
    ///
    /// Returns the per-partition error code on a fenced epoch. Returns
    /// `COORDINATOR_NOT_AVAILABLE` if the persist fails.
    pub(crate) async fn write(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        epochs: (StateEpoch, LeaderEpoch),
        progress: (Offset, i32),
        batches: Vec<StateBatch>,
    ) -> Result<(), ShareErrorCode> {
        let (state_epoch, leader_epoch) = epochs;
        let (start_offset, delivery_complete_count) = progress;
        let map_key = (group.to_string(), topic_id, partition);
        let state_partition = self.state_partition_for(group, &topic_id, partition);

        let entry = self
            .state
            .entry(map_key)
            .or_insert_with(|| Arc::new(Mutex::new(SharePartitionState::default())))
            .value()
            .clone();
        let mut st = entry.lock().await;

        // Epoch fencing. A write carrying a state_epoch below the durable one
        // is rejected; a stale leader_epoch (lower than the recorded one) is
        // also rejected.
        if state_epoch < st.state_epoch {
            return Err(crate::codes::FENCED_STATE_EPOCH);
        }
        if leader_epoch < st.leader_epoch {
            return Err(crate::codes::FENCED_LEADER_EPOCH);
        }
        st.state_epoch = state_epoch;

        let update = ShareUpdateValue {
            snapshot_epoch: st.snapshot_epoch,
            leader_epoch,
            start_offset,
            delivery_complete_count,
            state_batches: batches,
        };
        st.apply_update(&update);

        let key = ShareStateKey {
            record_type: KEY_SHARE_UPDATE,
            group_id: group.to_string(),
            topic_id,
            partition,
        };
        self.persist_record(state_partition, key, Some(update.encode()))
            .await
            .map_err(|e| {
                warn!(error = %e, "share write persist failed");
                crate::codes::COORDINATOR_NOT_AVAILABLE
            })?;

        // Snapshot fold + prune once the update count crosses the threshold.
        if st.updates_since_snapshot >= self.config.snapshot_update_records_per_snapshot {
            let Some(snapshot) = st.to_snapshot() else {
                warn!(
                    group,
                    partition, "share snapshot skipped because its epoch is exhausted"
                );
                return Ok(());
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
                    // Release the per-key lock before pruning so the
                    // per-partition scan below can lock sibling keys.
                    drop(st);
                    self.maybe_prune(state_partition).await;
                }
                Err(e) => {
                    warn!(error = %e, "share snapshot persist failed");
                    // The update itself was durable; a missed snapshot fold is
                    // recoverable on the next threshold crossing.
                }
            }
        }

        Ok(())
    }

    /// Clones the current state for `(group, topic_id, partition)`.
    pub(crate) async fn read(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<SharePartitionState> {
        let map_key = (group.to_string(), topic_id, partition);
        let handle = self.state.get(&map_key)?.value().clone();
        let st = handle.lock().await;
        Some(st.clone())
    }

    /// Returns `(state_epoch, leader_epoch, start_offset, delivery_complete_count)`.
    pub(crate) async fn read_summary(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<ShareStateSummary> {
        let map_key = (group.to_string(), topic_id, partition);
        let handle = self.state.get(&map_key)?.value().clone();
        let st = handle.lock().await;
        Some((
            st.state_epoch,
            st.leader_epoch,
            st.start_offset,
            st.delivery_complete_count,
        ))
    }

    /// Deletes the share state for `(group, topic_id, partition)`.
    ///
    /// This method writes a tombstone with the snapshot key and a null value.
    /// It then drops the in-memory entry.
    ///
    /// # Errors
    ///
    /// Returns `COORDINATOR_NOT_AVAILABLE` if the tombstone persist fails.
    pub(crate) async fn delete(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<(), ShareErrorCode> {
        let map_key = (group.to_string(), topic_id, partition);
        let state_partition = self.state_partition_for(group, &topic_id, partition);
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
                crate::codes::COORDINATOR_NOT_AVAILABLE
            })?;
        self.state.remove(&map_key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use tempfile::tempdir;

    use super::*;
    use crate::share_coordinator::coordinator::test_support::{batch, coordinator, lead_all};

    #[tokio::test]
    async fn initialize_then_read() {
        let dir = tempdir().unwrap();
        let (coord, _reg) = coordinator(dir.path());
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([3; 16]);

        coord.initialize("g", tid, 0, 5, Offset(100)).await.unwrap();

        let st = coord.read("g", tid, 0).await.expect("present");
        assert!(st.state_epoch == 5);
        assert!(st.start_offset == 100);
        let summary = coord.read_summary("g", tid, 0).await.expect("present");
        assert!(summary == (5, 0, Offset(100), 0));
    }

    #[tokio::test]
    async fn initialize_fences_stale_state_epoch() {
        let dir = tempdir().unwrap();
        let (coord, _reg) = coordinator(dir.path());
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([4; 16]);

        coord.initialize("g", tid, 0, 5, Offset(0)).await.unwrap();
        let err = coord
            .initialize("g", tid, 0, 5, Offset(0))
            .await
            .unwrap_err();
        assert!(err == crate::codes::FENCED_STATE_EPOCH);
    }

    #[tokio::test]
    async fn write_advances_spso_and_summary_matches() {
        let dir = tempdir().unwrap();
        let (coord, _reg) = coordinator(dir.path());
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([5; 16]);

        coord.initialize("g", tid, 0, 1, Offset(0)).await.unwrap();
        coord
            .write("g", tid, 0, (1, 2), (Offset(50), 7), vec![batch(50, 59)])
            .await
            .unwrap();

        let st = coord.read("g", tid, 0).await.expect("present");
        check!(st.state_epoch == 1);
        check!(st.leader_epoch == 2);
        check!(st.start_offset == 50);
        check!(st.delivery_complete_count == 7);
        check!(st.state_batches == vec![batch(50, 59)]);

        let summary = coord.read_summary("g", tid, 0).await.expect("present");
        assert!(summary == (1, 2, Offset(50), 7));
    }

    #[tokio::test]
    async fn write_fences_stale_state_epoch() {
        let dir = tempdir().unwrap();
        let (coord, _reg) = coordinator(dir.path());
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([6; 16]);

        coord.initialize("g", tid, 0, 5, Offset(0)).await.unwrap();
        let err = coord
            .write("g", tid, 0, (4, 0), (Offset(0), 0), vec![])
            .await
            .unwrap_err();
        assert!(err == crate::codes::FENCED_STATE_EPOCH);
    }

    #[tokio::test]
    async fn write_fences_stale_leader_epoch() {
        let dir = tempdir().unwrap();
        let (coord, _reg) = coordinator(dir.path());
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([7; 16]);

        coord.initialize("g", tid, 0, 1, Offset(0)).await.unwrap();
        coord
            .write("g", tid, 0, (1, 5), (Offset(0), 0), vec![])
            .await
            .unwrap();
        let err = coord
            .write("g", tid, 0, (1, 4), (Offset(0), 0), vec![])
            .await
            .unwrap_err();
        assert!(err == crate::codes::FENCED_LEADER_EPOCH);
    }

    #[tokio::test]
    async fn delete_removes_state() {
        let dir = tempdir().unwrap();
        let (coord, _reg) = coordinator(dir.path());
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([8; 16]);

        coord.initialize("g", tid, 0, 1, Offset(0)).await.unwrap();
        assert!(coord.read("g", tid, 0).await.is_some());
        coord.delete("g", tid, 0).await.unwrap();
        assert!(coord.read("g", tid, 0).await.is_none());
    }
}
