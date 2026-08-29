//! The background acquisition-lock-timeout sweeper.
//!
//! The sweeper is the only caller that walks every live cell, so it is also the
//! one that has to snapshot the `DashMap` before it awaits. It sits apart from
//! the request path because it is the sole detached task the manager spawns.

use std::{sync::Arc, time::Duration};

use tokio::sync::Mutex;

use super::{LeaderKey, SharePartitionLeaderManager};
use crate::share_partition::state::AcquisitionState;

impl SharePartitionLeaderManager {
    /// Spawns the background acquisition-lock-timeout sweeper.
    ///
    /// The sweeper runs every `record_lock_duration / 2`, with a minimum of
    /// 100ms. On each run it snapshots the live cells. It clones their `Arc`s
    /// out of the `DashMap`, so it holds no guard across an `.await`. It then
    /// expires each timed-out lock and persists again the cells that changed.
    /// The sweeper runs detached for the lifetime of the broker.
    pub(crate) fn spawn_lock_sweeper(self: &Arc<Self>) {
        let mgr = Arc::clone(self);
        let period = (mgr.config.record_lock_duration / 2).max(Duration::from_millis(100));
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(period);
            loop {
                tick.tick().await;
                // Snapshot keys + cells, releasing all DashMap guards first.
                let cells: Vec<(LeaderKey, Arc<Mutex<AcquisitionState>>)> = mgr
                    .leaders
                    .iter()
                    .map(|e| (e.key().clone(), e.value().clone()))
                    .collect();
                let now = std::time::Instant::now();
                for ((group, topic_id, partition), cell) in cells {
                    let mut st = cell.lock().await;
                    st.expire_locks(now);
                    if st.dirty {
                        mgr.persist_if_dirty(&group, topic_id, partition, &mut st)
                            .await;
                    }
                }
            }
        });
    }
}
