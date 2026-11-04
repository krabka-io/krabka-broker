//! The background acquisition-lock-timeout sweeper.
//!
//! The sweeper is the only caller that walks every live cell, so it is also the
//! one that has to snapshot the `DashMap` before it awaits. It sits apart from
//! the request path because it is the sole detached task the manager spawns.

use {
    super::{LeaderKey, SharePartitionLeaderManager},
    crate::share_partition::{group_settings::GroupShareSettings, state::AcquisitionState},
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::{sync::Mutex, time::interval},
};

/// A live share partition: its key and its acquisition state.
struct LeaderCell {
    key: LeaderKey,
    cell: Arc<Mutex<AcquisitionState>>,
}

impl SharePartitionLeaderManager {
    /// Spawns the background acquisition-lock-timeout sweeper.
    ///
    /// The sweeper runs every `record_lock_duration / 2` of the broker
    /// setting, with a minimum of 100ms. A group whose
    /// `share.record.lock.duration.ms` is shorter still sees its locks run out
    /// on time for every request, because each acquire pass expires locks
    /// first; the sweeper only bounds how long an idle partition keeps them.
    /// A lock that runs out at the group's `share.delivery.count.limit`
    /// archives its records. On each run it snapshots the live cells. It clones their `Arc`s
    /// out of the `DashMap`, so it holds no guard across an `.await`. It then
    /// expires each timed-out lock and persists again the cells that changed.
    /// Shutdown cancels and drains the sweeper, including a suspended cell pass.
    pub(crate) fn spawn_lock_sweeper(self: &Arc<Self>) {
        let mgr = Arc::clone(self);
        let period = (mgr.config.record_lock_duration / 2).max(Duration::from_millis(100));
        self.background_tasks.spawn(async move {
            let mut tick = interval(period);
            loop {
                tokio::select! {
                    biased;
                    () = mgr.shutdown.cancelled() => break,
                    _ = tick.tick() => {}
                }
                // Snapshot keys + cells, releasing all DashMap guards first.
                let cells: Vec<LeaderCell> = mgr
                    .leaders
                    .iter()
                    .map(|e| LeaderCell {
                        key: e.key().clone(),
                        cell: Arc::clone(e.value()),
                    })
                    .collect();
                let now = Instant::now();
                let image = mgr.controller.current_image();
                for LeaderCell {
                    key: (group, topic_id, partition),
                    cell,
                } in cells
                {
                    let settings = GroupShareSettings::resolve(&image, &group, &mgr.config);
                    let mut st = tokio::select! {
                        biased;
                        () = mgr.shutdown.cancelled() => return,
                        state = cell.lock() => state,
                    };
                    st.set_dlq_enabled(settings.dlq_enabled);
                    st.expire_locks(now, settings.delivery_count_limit);
                    if st.dirty {
                        // A failed write keeps the state dirty for the next tick.
                        tokio::select! {
                            biased;
                            () = mgr.shutdown.cancelled() => return,
                            _ = mgr.persist_if_dirty(&group, topic_id, partition, Some(&cell), &mut st) => {}
                        }
                    }
                    drop(st);
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::share_partition::manager::test_support::manager,
        assert2::assert,
        krabka_log::Offset,
        tokio::{task::yield_now, time::timeout},
    };

    #[tokio::test]
    async fn shutdown_cancels_a_sweep_waiting_for_a_cell_lock() {
        let manager = manager();
        let cell = Arc::new(Mutex::new(AcquisitionState::new(Offset(0))));
        manager
            .leaders
            .insert(("g".to_owned(), uuid::Uuid::nil(), 0), Arc::clone(&cell));
        let held = cell.lock().await;
        manager.spawn_lock_sweeper();
        // The interval's first tick is immediate. Let the worker reach the
        // locked cell, which remains locked throughout shutdown.
        yield_now().await;
        assert!(manager.background_tasks.len() == 1);
        timeout(Duration::from_secs(2), manager.shutdown())
            .await
            .expect("shutdown must cancel the blocked sweep");
        assert!(manager.background_tasks.len() == 0);
        drop(held);
    }
}
