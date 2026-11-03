//! Cancellation, draining and native-manager closure for the broker lifecycle.

use {
    super::SharePartitionLeaderManager,
    std::sync::Arc,
    tokio::{runtime::Handle, sync::watch},
};

pub struct StartupGuard(Option<Arc<SharePartitionLeaderManager>>);
impl StartupGuard {
    pub(crate) fn disarm(mut self) { self.0.take(); }
}
impl Drop for StartupGuard {
    fn drop(&mut self) {
        if let Some(manager) = self.0.take() {
            manager.shutdown_background();
        }
    }
}
impl SharePartitionLeaderManager {
    /// Cancel pending writes without archiving them, then stop the manager once.
    pub(crate) async fn shutdown(&self) {
        self.shutdown.cancel();
        self.background_tasks.close();
        self.background_tasks.wait().await;
        let mut stopped = self
            .stopped
            .get_or_init(|| async {
                let (done, receiver) = watch::channel(false);
                let sink = Arc::clone(&self.dlq);
                tokio::spawn(async move {
                    sink.stop().await;
                    let _ = done.send(true);
                });
                receiver
            })
            .await
            .clone();
        if stopped.wait_for(|done| *done).await.is_err() {
            tracing::error!("native DLQ manager stop task exited without completing cleanup");
        }
    }

    pub(crate) fn startup_guard(self: &Arc<Self>) -> StartupGuard {
        StartupGuard(Some(Arc::clone(self)))
    }

    /// Best effort for abandoned startup or a dropped handle; shutdown awaits it.
    pub(crate) fn shutdown_background(self: &Arc<Self>) {
        self.shutdown.cancel();
        if self.stopped.get().is_none()
            && let Ok(runtime) = Handle::try_current()
        {
            let manager = Arc::clone(self);
            runtime.spawn(async move {
                manager.shutdown().await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::share_partition::{
            dlq::{DlqError, DlqRequest, DlqSink},
            manager::test_support::manager_with_dlq,
            state::{
                AckType, AcquisitionState, DlqRange, RecordState,
                test_support::{DeadLetterQueue, acquire_window},
            },
        },
        assert2::assert,
        async_trait::async_trait,
        krabka_log::Offset,
        std::{
            future::pending,
            sync::{
                Arc,
                atomic::{AtomicUsize, Ordering},
            },
            time::Duration,
        },
        tokio::{
            sync::{Mutex, Notify},
            task::yield_now,
            time::timeout,
        },
    };

    #[derive(Default)]
    struct BlockingManager {
        entered: Notify,
        closed: Notify,
        cancelled: AtomicUsize,
        stops: AtomicUsize,
    }
    struct Cancelled<'a>(&'a AtomicUsize);
    impl Drop for Cancelled<'_> {
        fn drop(&mut self) { self.0.fetch_add(1, Ordering::SeqCst); }
    }
    #[async_trait]
    impl DlqSink for BlockingManager {
        async fn write(&self, _: DlqRequest) -> Result<(), DlqError> {
            let _cancelled = Cancelled(&self.cancelled);
            self.entered.notify_one();
            pending().await
        }

        async fn stop(&self) {
            self.stops.fetch_add(1, Ordering::SeqCst);
            self.closed.notify_one();
        }
    }

    #[tokio::test]
    async fn shutdown_cancels_native_future_without_terminal_progress() {
        let sink = Arc::new(BlockingManager::default());
        let manager = manager_with_dlq(Arc::<BlockingManager>::clone(&sink));
        let mut state = AcquisitionState::new(Offset(0));
        acquire_window()
            .state(&mut state)
            .end(1)
            .dead_letter_queue(DeadLetterQueue::Enabled)
            .call();
        state
            .acknowledge("m1", Offset(0), Offset(0), AckType::Reject, 5)
            .unwrap();
        let ranges = state.take_pending_dlq();
        let cell = Arc::new(Mutex::new(state));
        let key = ("g1".to_owned(), uuid::Uuid::nil(), 0);
        manager.dispatch_dead_letters(&key, &cell, ranges.clone());
        timeout(Duration::from_secs(2), sink.entered.notified())
            .await
            .unwrap();
        manager.shutdown().await;
        manager.shutdown().await;
        assert!(
            (
                sink.cancelled.load(Ordering::SeqCst),
                sink.stops.load(Ordering::SeqCst)
            ) == (1, 1)
        );
        let state = cell.lock().await;
        assert!(state.record_states() == vec![(0, RecordState::Archiving)]);
        let (start, _, batches) = state.to_persist_batches();
        let mut recovered = AcquisitionState::new(start);
        recovered.load_from(start, state.state_epoch, state.leader_epoch, &batches);
        assert!(
            recovered.take_pending_dlq()
                == vec![DlqRange {
                    first: Offset(0),
                    last: Offset(0),
                    delivery_count: 1,
                    cause: None,
                }]
        );
        drop(state);
        manager.dispatch_dead_letters(&key, &cell, ranges);
        yield_now().await;
        assert!(manager.background_tasks.len() == 0);
    }

    #[tokio::test]
    async fn abandoned_startup_stops_the_constructed_manager() {
        let sink = Arc::new(BlockingManager::default());
        let manager = manager_with_dlq(Arc::<BlockingManager>::clone(&sink));
        drop(manager.startup_guard());
        timeout(Duration::from_secs(2), sink.closed.notified())
            .await
            .unwrap();
        manager.shutdown().await;
        assert!(sink.stops.load(Ordering::SeqCst) == 1);
    }
    #[derive(Default)]
    struct ControlledStop {
        entered: Notify,
        release: Notify,
        calls: AtomicUsize,
    }
    #[async_trait]
    impl DlqSink for ControlledStop {
        async fn write(&self, _: DlqRequest) -> Result<(), DlqError> { Ok(()) }

        async fn stop(&self) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.release.notified().await;
        }
    }

    #[tokio::test]
    async fn cancelling_shutdown_waiter_does_not_repeat_native_stop() {
        let sink = Arc::new(ControlledStop::default());
        let manager = manager_with_dlq(Arc::<ControlledStop>::clone(&sink));
        let first = Arc::clone(&manager);
        let waiter = tokio::spawn(async move {
            first.shutdown().await;
        });
        timeout(Duration::from_secs(2), sink.entered.notified())
            .await
            .unwrap();
        waiter.abort();
        let _ = waiter.await;
        sink.release.notify_one();
        timeout(Duration::from_secs(2), manager.shutdown())
            .await
            .unwrap();
        assert!(sink.calls.load(Ordering::SeqCst) == 1);
    }
}
