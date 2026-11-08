//! Stopping the replica tasks the supervisor runs: the fetchers and the
//! diskless WAL followers.

use {
    super::{FetcherKey, FetcherTask, WalFollowerTask},
    crate::wal::quorum::registry::ShardId,
    std::time::Duration,
    tokio::{task::JoinHandle, time},
    tokio_util::sync::CancellationToken,
    tracing::{error, warn},
};

/// How long the supervisor waits for its replica tasks to stop on their own
/// before it aborts the ones still running.
pub(super) const TASK_STOP_TIMEOUT: Duration = Duration::from_secs(5);

/// A replica task to stop: the name its log lines carry, the token that asks
/// it to stop, its handle, and whether it may be aborted.
pub(super) struct StoppingTask {
    pub(super) label: String,
    pub(super) shutdown: CancellationToken,
    pub(super) handle: JoinHandle<()>,
    /// Whether the task may be aborted at the deadline. A fetcher may: it
    /// writes through the partition writer, so aborting it leaves no I/O
    /// behind. A WAL follower may not: it mutates its shard's files itself,
    /// and on a current-thread runtime that work runs on the blocking pool
    /// and would outlive an abort, racing the shard's removal or promotion.
    pub(super) abortable: bool,
}

impl StoppingTask {
    pub(super) fn fetcher(key: FetcherKey, task: FetcherTask) -> Self {
        Self {
            label: format!("fetcher {} for leader {}", key.fetcher_id, key.leader.0),
            shutdown: task.shutdown,
            handle: task.handle,
            abortable: true,
        }
    }

    pub(super) fn wal_follower(shard: ShardId, task: WalFollowerTask) -> Self {
        Self {
            label: format!(
                "WAL follower for topic {} partition {}",
                shard.topic_id, shard.partition.0
            ),
            shutdown: task.shutdown,
            handle: task.handle,
            abortable: false,
        }
    }
}

/// Stops replica tasks the way Kafka's
/// `AbstractFetcherManager.closeAllFetchers` does: it signals every task
/// first, then waits for each one, so a fetcher can close its session and
/// finish a writer round trip. An abortable task still running when `timeout`
/// has passed since the signal is aborted. Any other task is waited for until
/// it stops, with a warning at the deadline. A task that panicked is logged at
/// error.
pub(super) async fn stop_tasks(tasks: impl IntoIterator<Item = StoppingTask>, timeout: Duration) {
    let tasks = tasks.into_iter().collect::<Vec<_>>();
    for task in &tasks {
        task.shutdown.cancel();
    }
    let deadline = time::Instant::now() + timeout;
    for StoppingTask {
        label,
        mut handle,
        abortable,
        ..
    } in tasks
    {
        let joined = match time::timeout_at(deadline, &mut handle).await {
            Ok(joined) => joined,
            Err(_elapsed) if abortable => {
                warn!(
                    task = %label,
                    timeout = ?timeout,
                    "replica task did not stop after cancellation; aborting it"
                );
                handle.abort();
                handle.await
            }
            Err(_elapsed) => {
                warn!(
                    task = %label,
                    timeout = ?timeout,
                    "replica task did not stop after cancellation; still waiting for it"
                );
                handle.await
            }
        };
        if let Err(join_error) = joined
            && join_error.is_panic()
        {
            error!(task = %label, error = %join_error, "replica task panicked");
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::test_support::{LogCapture, LoggedEvent},
        assert2::check,
        std::{
            future,
            sync::{
                Arc,
                atomic::{AtomicBool, Ordering},
            },
        },
        tokio::task,
        tracing::{Instrument, Level, dispatcher},
    };

    fn stopping(shutdown: CancellationToken, handle: JoinHandle<()>) -> StoppingTask {
        StoppingTask {
            label: "test task".to_owned(),
            shutdown,
            handle,
            abortable: true,
        }
    }

    /// A task that, once signalled, runs `after_signal` and then marks itself
    /// finished: its token, its handle and the mark.
    fn finishing_task(
        after_signal: impl Future<Output = ()> + Send + 'static,
    ) -> (CancellationToken, JoinHandle<()>, Arc<AtomicBool>) {
        let shutdown = CancellationToken::new();
        let finished = Arc::new(AtomicBool::new(false));
        let handle = tokio::spawn({
            let shutdown = shutdown.clone();
            let finished = Arc::clone(&finished);
            async move {
                shutdown.cancelled().await;
                after_signal.await;
                finished.store(true, Ordering::SeqCst);
            }
        });
        (shutdown, handle, finished)
    }

    /// A task that may not be aborted, such as a WAL follower that owns its
    /// shard's files, is waited for past the deadline until it stops on its
    /// own, so nothing it started outlives the stop.
    #[tokio::test(start_paused = true)]
    async fn a_task_that_may_not_be_aborted_is_waited_for_past_the_deadline() {
        let (shutdown, handle, finished) = finishing_task(time::sleep(Duration::from_secs(10)));
        let task = StoppingTask {
            abortable: false,
            ..stopping(shutdown, handle)
        };

        stop_tasks([task], Duration::from_secs(1)).await;

        check!(finished.load(Ordering::SeqCst));
    }

    /// A task that stops on its signal runs to its end: it is not aborted
    /// before it can finish what it does after the signal.
    #[tokio::test]
    async fn a_cancelled_task_finishes_its_own_shutdown() {
        let (shutdown, handle, finished) = finishing_task(task::yield_now());

        stop_tasks([stopping(shutdown, handle)], TASK_STOP_TIMEOUT).await;

        check!(finished.load(Ordering::SeqCst));
    }

    /// A task that panicked, and one that ignores its signal past the
    /// deadline, are both reported, and shutdown still returns.
    #[tokio::test(start_paused = true)]
    async fn a_panicked_task_and_a_stuck_task_are_logged() {
        let capture = LogCapture::default();
        let _capturing = dispatcher::set_default(&capture.dispatch());
        let panicked = tokio::spawn(async { panic!("replica task exploded") });
        let stuck = tokio::spawn(future::pending::<()>());

        stop_tasks(
            [
                stopping(CancellationToken::new(), panicked),
                stopping(CancellationToken::new(), stuck),
            ],
            Duration::from_secs(1),
        )
        .instrument(LogCapture::span())
        .await;

        let logged = capture
            .events()
            .into_iter()
            .map(|LoggedEvent { level, message, .. }| (level, message))
            .collect::<Vec<_>>();
        check!(
            logged
                == vec![
                    (Level::ERROR, "replica task panicked".to_owned()),
                    (
                        Level::WARN,
                        "replica task did not stop after cancellation; aborting it".to_owned()
                    ),
                ]
        );
    }
}
