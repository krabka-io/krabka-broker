//! The write-back of a dirty acquisition machine to the share coordinator.
//!
//! Persistence is best-effort and deliberately never fails a fetch or an ack,
//! so the whole retry contract, which is that `dirty` stays set until a durable
//! write succeeds, lives in this one method.

use tracing::warn;

use super::SharePartitionLeaderManager;
use crate::share_partition::state::AcquisitionState;

impl SharePartitionLeaderManager {
    /// Persists `st` if it is dirty, then clears the dirty flag.
    ///
    /// The method logs each error and then discards it. Persistence is
    /// best-effort. It never panics, and it never fails the fetch or the ack
    /// that called it.
    pub(crate) async fn persist_if_dirty(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        st: &mut AcquisitionState,
    ) {
        if !st.dirty {
            return;
        }
        let (start, dcc, batches) = st.to_persist_batches();
        match self
            .persister
            .write_state(
                group,
                topic_id,
                partition,
                (st.state_epoch, st.leader_epoch),
                (start, dcc),
                batches,
            )
            .await
        {
            // Clear `dirty` only on a durable write. On failure we leave it set
            // so the background sweeper (and the next fetch/ack) retries.
            Ok(()) => st.dirty = false,
            Err(e) => warn!(
                group,
                %topic_id, partition, error = %e,
                "share-partition state persist failed; will retry on next change"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_log::Offset;

    use crate::share_partition::manager::test_support::{LOCK, manager};

    #[tokio::test]
    async fn persist_if_dirty_is_noop_when_clean() {
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([22; 16]);

        let cell = mgr.get_or_load("g1", tid, 0).await;
        let mut st = cell.lock().await;
        assert!(!st.dirty);
        // Clean state: no-op, no panic, stays clean.
        mgr.persist_if_dirty("g1", tid, 0, &mut st).await;
        assert!(!st.dirty);
    }

    #[tokio::test]
    async fn persist_if_dirty_keeps_dirty_on_write_failure() {
        // Under MockSource the persister can't bootstrap the share-state topic,
        // so `write_state` errors. A failed durable write must leave `dirty`
        // set so the sweeper/next-ack retries (F4 durability fix).
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([25; 16]);

        let cell = mgr.get_or_load("g1", tid, 0).await;
        let mut st = cell.lock().await;
        // Make the state dirty with persistable content.
        st.materialize(Offset(4), 100);
        let _ = st.acquire("m1", 10, i32::MAX, std::time::Instant::now(), LOCK, 5);
        assert!(st.dirty);

        mgr.persist_if_dirty("g1", tid, 0, &mut st).await;
        // Write failed -> dirty stays set for retry.
        assert!(st.dirty);
    }
}
