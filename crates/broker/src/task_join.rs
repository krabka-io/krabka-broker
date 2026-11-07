//! Joining the tasks broker shutdown stops, and telling whether any of them
//! had panicked. A panic withholds the clean-shutdown proof.

use {std::future::Future, tokio::task::JoinError};

/// Await a task that shutdown stopped, and answer whether it ended without a
/// panic. A task that returned or was aborted ended cleanly. A panic is logged
/// here, because the task may have stopped halfway through changing state that
/// the clean-shutdown proof vouches for.
pub async fn joined_cleanly<T>(
    task: &str,
    join: impl Future<Output = Result<T, JoinError>>,
) -> bool {
    match join.await {
        Err(error) if error.is_panic() => {
            tracing::error!(task, %error, "a task panicked before shutdown; the next start recovers");
            false
        }
        _ => true,
    }
}
