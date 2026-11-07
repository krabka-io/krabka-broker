//! Stopping the replica tasks the supervisor runs: the fetchers and the
//! diskless WAL followers.

use {
    std::time::Duration,
    tokio::{task::JoinHandle, time},
    tokio_util::sync::CancellationToken,
    tracing::{error, warn},
};

/// How long the supervisor waits for its replica tasks to stop on their own
/// before it aborts the ones still running.
pub(super) const TASK_STOP_TIMEOUT: Duration = Duration::from_secs(5);

/// Stops replica tasks the way Kafka's
/// `AbstractFetcherManager.closeAllFetchers` does: it signals every task
/// first, then waits for each one, so a fetcher can close its session and
/// finish a writer round trip. A task still running when `timeout` has passed
/// since the signal is aborted. A task that panicked is logged at error.
pub(super) async fn stop_tasks(
    tasks: impl IntoIterator<Item = (CancellationToken, JoinHandle<()>)>,
    timeout: Duration,
) {
    let tasks = tasks.into_iter().collect::<Vec<_>>();
    for (shutdown, _) in &tasks {
        shutdown.cancel();
    }
    let deadline = time::Instant::now() + timeout;
    for (_, mut handle) in tasks {
        let joined = match time::timeout_at(deadline, &mut handle).await {
            Ok(joined) => joined,
            Err(_elapsed) => {
                warn!(
                    timeout = ?timeout,
                    "replica task did not stop after cancellation; aborting it"
                );
                handle.abort();
                handle.await
            }
        };
        if let Err(join_error) = joined
            && join_error.is_panic()
        {
            error!(error = %join_error, "replica task panicked");
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

    /// A task that stops on its signal runs to its end: it is not aborted
    /// before it can finish what it does after the signal.
    #[tokio::test]
    async fn a_cancelled_task_finishes_its_own_shutdown() {
        let shutdown = CancellationToken::new();
        let finished = Arc::new(AtomicBool::new(false));
        let handle = tokio::spawn({
            let shutdown = shutdown.clone();
            let finished = Arc::clone(&finished);
            async move {
                shutdown.cancelled().await;
                task::yield_now().await;
                finished.store(true, Ordering::SeqCst);
            }
        });

        stop_tasks([(shutdown, handle)], TASK_STOP_TIMEOUT).await;

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
                (CancellationToken::new(), panicked),
                (CancellationToken::new(), stuck),
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
