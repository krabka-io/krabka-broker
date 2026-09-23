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
    /// Calculate the period between sweeps given the configured lock duration.
    #[must_use]
    pub(crate) fn sweeper_period(record_lock_duration: Duration) -> Duration {
        (record_lock_duration / 2).max(Duration::from_millis(100))
    }

    /// Spawns the background acquisition-lock-timeout sweeper.
    ///
    /// The sweeper runs every `record_lock_duration / 2`, with a minimum of
    /// 100ms. On each run it snapshots the live cells. It clones their `Arc`s
    /// out of the `DashMap`, so it holds no guard across an `.await`. It then
    /// expires each timed-out lock and persists again the cells that changed.
    /// The sweeper runs detached for the lifetime of the broker.
    pub(crate) fn spawn_lock_sweeper(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let mgr = Arc::clone(self);
        let period = Self::sweeper_period(mgr.config.record_lock_duration);
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
                        // A failed write keeps the state dirty for the next tick.
                        let _ = mgr
                            .persist_if_dirty(&group, topic_id, partition, Some(&cell), &mut st)
                            .await;
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use assert2::{assert, check};
    use krabka_log::Offset;

    use super::*;
    use crate::{
        coordinator::unified::share::config::ShareGroupConfig,
        share_partition::{manager::test_support::manager_with_config, state::RecordState},
    };

    #[test]
    fn sweeper_period_is_half_lock_duration_floored_at_100ms() {
        check!(
            SharePartitionLeaderManager::sweeper_period(Duration::from_secs(30))
                == Duration::from_secs(15)
        );
        check!(
            SharePartitionLeaderManager::sweeper_period(Duration::from_millis(100))
                == Duration::from_millis(100)
        );
        check!(
            SharePartitionLeaderManager::sweeper_period(Duration::from_millis(50))
                == Duration::from_millis(100)
        );
        check!(
            SharePartitionLeaderManager::sweeper_period(Duration::from_secs(1))
                == Duration::from_millis(500)
        );
    }

    #[tokio::test]
    async fn lock_sweeper_expires_overdue_locks() {
        let mgr = manager_with_config(ShareGroupConfig {
            record_lock_duration: Duration::from_millis(100),
            ..Default::default()
        });
        let topic_id = uuid::Uuid::new_v4();
        let partition = 0;
        let group = "test-group".to_string();

        let mut st = AcquisitionState::new(Offset(0));
        st.materialize(Offset(4), 100);
        let _ = st.acquire(
            "member-1",
            10,
            Offset(i64::MAX),
            Instant::now()
                .checked_sub(Duration::from_secs(1))
                .unwrap_or_else(Instant::now),
            Duration::from_millis(1),
            5,
        );
        st.dirty = false;

        let cell = Arc::new(Mutex::new(st));
        mgr.leaders
            .insert((group.clone(), topic_id, partition), cell.clone());

        let handle = mgr.spawn_lock_sweeper();

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut expired = false;
        while Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let locked = cell.lock().await;
            if locked
                .record_states()
                .iter()
                .all(|(_, s)| *s == RecordState::Available)
            {
                expired = true;
                break;
            }
        }
        handle.abort();
        assert!(expired, "sweeper did not expire overdue lock");
    }
}
