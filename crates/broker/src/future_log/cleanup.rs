//! Cancellation and teardown of in-progress log-directory moves.
//!
//! A move ends here when a later `AlterReplicaLogDirs` redirects it to another
//! directory, when the broker shuts down, or when a `BrokerHandle` drops
//! without an awaited shutdown. Each path cancels the move's token and stops
//! its replicator task; a redirect also removes the staged future directory.

use {
    super::{FutureLogRegistry, FutureLogState, MoveError},
    krabka_ids::PartitionIndex,
    std::{
        fs,
        sync::{Arc, PoisonError},
    },
    tokio::task::JoinHandle,
};

impl FutureLogRegistry {
    /// Stop the move `existing` of `key`: cancel its token, abort and await
    /// its replicator task, remove its entry unless a later move replaced it,
    /// and delete its staged future directory.
    pub(super) async fn cancel(
        &self,
        key: &(String, PartitionIndex),
        existing: Arc<FutureLogState>,
    ) -> Result<(), MoveError> {
        existing.cancel.cancel();
        let task = existing
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        self.remove_if_same(key, &existing);
        let future_path = existing.future_path.clone();
        drop(existing);
        if future_path.exists() {
            fs::remove_dir_all(&future_path)?;
        }
        Ok(())
    }
}

fn take_move_tasks(future_logs: &FutureLogRegistry) -> Vec<JoinHandle<()>> {
    let states = future_logs.take_all();
    for state in &states {
        state.cancel.cancel();
    }
    states
        .into_iter()
        .filter_map(|state| {
            state
                .task
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
        })
        .collect()
}

/// Cancel and await every in-progress log-directory move. Broker shutdown
/// calls this after it has stopped accepting requests and before it stops the
/// partition writers that the move tasks use.
pub async fn shutdown_moves(future_logs: &FutureLogRegistry) {
    let tasks = take_move_tasks(future_logs);
    for task in &tasks {
        task.abort();
    }
    for task in tasks {
        let _ = task.await;
    }
}

/// Best-effort synchronous counterpart used when a
/// [`crate::broker::BrokerHandle`] is dropped without an awaited shutdown.
pub fn abort_moves(future_logs: &FutureLogRegistry) {
    for task in take_move_tasks(future_logs) {
        task.abort();
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::test_support::wait_tasks_started,
        assert2::assert,
        krabka_log::{Log, LogConfig},
        std::{
            future,
            path::Path,
            sync::{
                Mutex,
                atomic::{AtomicU64, Ordering},
            },
            time::Duration,
        },
        tempfile::tempdir,
        tokio::{task, time},
        tokio_util::sync::CancellationToken,
    };

    struct PendingMoveFixture {
        dir: tempfile::TempDir,
        log: Arc<Mutex<Log>>,
        logs: FutureLogRegistry,
        started: Arc<AtomicU64>,
        dropped: Arc<AtomicU64>,
    }
    fn pending_move_fixture() -> PendingMoveFixture {
        let dir = tempdir().expect("tempdir");
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).expect("open future log"),
        ));
        let logs = FutureLogRegistry::default();
        let started = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        PendingMoveFixture {
            dir,
            log,
            logs,
            started,
            dropped,
        }
    }

    struct DropCounter(Arc<AtomicU64>);

    impl Drop for DropCounter {
        fn drop(&mut self) { self.0.fetch_add(1, Ordering::SeqCst); }
    }

    fn pending_move(started: &Arc<AtomicU64>, dropped: &Arc<AtomicU64>) -> JoinHandle<()> {
        let started = Arc::clone(started);
        let dropped = Arc::clone(dropped);
        tokio::spawn(async move {
            let _drop_counter = DropCounter(dropped);
            started.fetch_add(1, Ordering::SeqCst);
            future::pending::<()>().await;
        })
    }

    /// A move staged in a new directory `name` under `parent`, with `task` as
    /// its replicator.
    fn staged_move(
        parent: &Path,
        name: &str,
        log: &Arc<Mutex<Log>>,
        task: Option<JoinHandle<()>>,
    ) -> Arc<FutureLogState> {
        let future_path = parent.join(name);
        fs::create_dir_all(&future_path).expect("create future dir");
        Arc::new(FutureLogState {
            target_log_dir: parent.to_path_buf(),
            future_path,
            future_log: Arc::clone(log),
            cancel: CancellationToken::new(),
            task: Mutex::new(task),
        })
    }

    #[tokio::test]
    async fn cancel_stops_the_task_and_removes_the_move() {
        let PendingMoveFixture {
            dir,
            log,
            logs,
            started,
            dropped,
        } = pending_move_fixture();
        let future_logs = Arc::new(logs);
        let key = ("t".to_owned(), PartitionIndex(0));
        let existing = staged_move(
            dir.path(),
            "t-0-future",
            &log,
            Some(pending_move(&started, &dropped)),
        );
        future_logs.insert(key.clone(), Arc::clone(&existing));
        wait_tasks_started(|| started.load(Ordering::SeqCst) == 1, "move task starts").await;
        let cancel = existing.cancel.clone();
        let future_path = existing.future_path.clone();

        future_logs
            .cancel(&key, existing)
            .await
            .expect("cancel the move");

        assert!(future_logs.is_empty());
        assert!(cancel.is_cancelled());
        assert!(dropped.load(Ordering::SeqCst) == 1);
        assert!(!future_path.exists());
    }

    #[tokio::test]
    async fn cancel_keeps_an_entry_a_later_move_registered() {
        let PendingMoveFixture { dir, log, logs, .. } = pending_move_fixture();
        let future_logs = Arc::new(logs);
        let key = ("t".to_owned(), PartitionIndex(0));
        let existing = staged_move(dir.path(), "t-0-future", &log, None);
        let later = staged_move(dir.path(), "later", &log, None);
        future_logs.insert(key.clone(), Arc::clone(&later));
        let future_path = existing.future_path.clone();

        future_logs
            .cancel(&key, existing)
            .await
            .expect("cancel the move");

        assert!(
            future_logs
                .get(&key)
                .is_some_and(|entry| Arc::ptr_eq(&entry, &later))
        );
        assert!(!future_path.exists());
        assert!(later.future_path.exists());
    }

    #[tokio::test]
    async fn shutdown_moves_cancels_and_awaits_every_task() {
        let PendingMoveFixture {
            dir,
            log: future_log,
            logs: future_logs,
            started,
            dropped,
        } = pending_move_fixture();
        let mut cancels = Vec::new();

        for partition in [PartitionIndex(0), PartitionIndex(1)] {
            let cancel = CancellationToken::new();
            let task = pending_move(&started, &dropped);
            future_logs.insert(
                ("t".to_owned(), partition),
                Arc::new(FutureLogState {
                    target_log_dir: dir.path().to_path_buf(),
                    future_path: dir.path().to_path_buf(),
                    future_log: Arc::clone(&future_log),
                    cancel: cancel.clone(),
                    task: Mutex::new(Some(task)),
                }),
            );
            cancels.push(cancel);
        }

        wait_tasks_started(|| started.load(Ordering::SeqCst) == 2, "move tasks start").await;

        time::timeout(Duration::from_secs(1), shutdown_moves(&future_logs))
            .await
            .expect("move task shutdown completes");

        assert!(future_logs.is_empty());
        assert!(cancels.iter().all(CancellationToken::is_cancelled));
        assert!(dropped.load(Ordering::SeqCst) == 2);
    }

    #[tokio::test]
    async fn abort_moves_cancels_and_aborts_every_task() {
        let PendingMoveFixture {
            dir,
            log: future_log,
            logs: future_logs,
            started,
            dropped,
        } = pending_move_fixture();
        let cancel = CancellationToken::new();
        let task = pending_move(&started, &dropped);
        future_logs.insert(
            ("t".to_owned(), PartitionIndex(0)),
            Arc::new(FutureLogState {
                target_log_dir: dir.path().to_path_buf(),
                future_path: dir.path().to_path_buf(),
                future_log,
                cancel: cancel.clone(),
                task: Mutex::new(Some(task)),
            }),
        );

        time::timeout(Duration::from_secs(1), async {
            while started.load(Ordering::SeqCst) != 1 {
                task::yield_now().await;
            }
        })
        .await
        .expect("move task starts");

        abort_moves(&future_logs);

        assert!(future_logs.is_empty());
        assert!(cancel.is_cancelled());
        time::timeout(Duration::from_secs(1), async {
            while dropped.load(Ordering::SeqCst) != 1 {
                task::yield_now().await;
            }
        })
        .await
        .expect("aborted move task drops");
    }
}
