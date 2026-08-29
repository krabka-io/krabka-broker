//! The lazily loaded acquisition-state cells: the load-on-miss path, the
//! test-only peek, and the invalidation the admin offset RPCs use.
//!
//! This is the only module that inserts into or removes from the `leaders`
//! map, so the rule that no `DashMap` guard is held across an `.await` is
//! checkable by reading one file.

use std::sync::Arc;

use krabka_log::Offset;
use tokio::sync::Mutex;
use tracing::warn;

use super::SharePartitionLeaderManager;
use crate::share_partition::state::AcquisitionState;

impl SharePartitionLeaderManager {
    /// Gets the acquisition-state cell for `(group, topic_id, partition)`, and
    /// loads it lazily on a miss.
    ///
    /// On a cache miss the method reads the durable state from the persister
    /// and folds it into a fresh [`AcquisitionState`]. If no durable state
    /// exists, it uses an empty [`AcquisitionState`]. The method drops the
    /// `DashMap` guard before the load `.await`. A concurrent loader that loses
    /// the insert race adopts the cell of the winner.
    ///
    /// The `ShareFetch` and `ShareAcknowledge` handlers call this method.
    pub(crate) async fn get_or_load(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Arc<Mutex<AcquisitionState>> {
        let key = (group.to_string(), topic_id, partition);
        if let Some(cell) = self.leaders.get(&key) {
            return cell.value().clone();
        }

        // Miss: load from the persister WITHOUT holding any DashMap guard.
        let leader_epoch = self.leader_epoch_for(topic_id, partition);
        let loaded = match self.persister.read_state(group, topic_id, partition).await {
            Ok(Some(persisted)) => {
                let mut st = AcquisitionState::new(persisted.start_offset);
                st.load_from(
                    persisted.start_offset,
                    persisted.state_epoch,
                    leader_epoch,
                    persisted.delivery_complete_count,
                    &persisted.state_batches,
                );
                st
            }
            Ok(None) => {
                let mut st = AcquisitionState::new(Offset(0));
                st.leader_epoch = leader_epoch;
                st
            }
            Err(e) => {
                warn!(
                    group,
                    %topic_id, partition, error = %e,
                    "share-partition state load failed; starting from empty window"
                );
                let mut st = AcquisitionState::new(Offset(0));
                st.leader_epoch = leader_epoch;
                st
            }
        };

        let cell = Arc::new(Mutex::new(loaded));
        // Adopt the winner if another task loaded the same key concurrently.
        self.leaders.entry(key).or_insert(cell).value().clone()
    }

    /// Test-only: borrows the live acquisition cell without a persister load.
    ///
    /// Returns `None` if this node does not currently lead the partition or has
    /// not loaded the cell.
    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) fn peek_for_test(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<std::sync::Arc<tokio::sync::Mutex<AcquisitionState>>> {
        self.leaders
            .get(&(group.to_string(), topic_id, partition))
            .map(|c| c.value().clone())
    }

    /// Drops the cached acquisition-state cell for
    /// `(group, topic_id, partition)`.
    ///
    /// The next `get_or_load` then re-reads the durable SPSO. The admin offset
    /// RPCs call this method after `AlterShareGroupOffsets` or
    /// `DeleteShareGroupOffsets` rewrites the persister state. A later
    /// `ShareFetch` on this broker thus sees an in-flight reset. A cell on
    /// another broker refreshes on its own next load, which matches the classic
    /// offset-reset behavior.
    pub(crate) fn invalidate(&self, group: &str, topic_id: uuid::Uuid, partition: i32) {
        self.leaders
            .remove(&(group.to_string(), topic_id, partition));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;

    use crate::share_partition::manager::test_support::manager;

    #[tokio::test]
    async fn get_or_load_fresh_returns_empty_window_and_caches() {
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([21; 16]);

        let cell = mgr.get_or_load("g1", tid, 0).await;
        let st = cell.lock().await;
        assert!(st.start_offset == 0);
        assert!(!st.dirty);
        drop(st);
        // A second call returns the same cached cell.
        let cell2 = mgr.get_or_load("g1", tid, 0).await;
        assert!(Arc::ptr_eq(&cell, &cell2));
    }

    #[tokio::test]
    async fn invalidate_removes_cached_cell() {
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([24; 16]);

        // Populate the cache, then invalidate; a subsequent load yields a
        // fresh, distinct cell.
        let cell = mgr.get_or_load("g1", tid, 0).await;
        mgr.invalidate("g1", tid, 0);
        let cell2 = mgr.get_or_load("g1", tid, 0).await;
        assert!(!Arc::ptr_eq(&cell, &cell2));
    }
}
