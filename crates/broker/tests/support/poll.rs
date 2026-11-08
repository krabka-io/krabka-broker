//! Bounded wire-response retries, whose decisions can lag a committed metadata image.

use {
    std::{
        fmt::Debug,
        future::Future,
        time::{Duration, Instant},
    },
    tokio::time,
};

/// Return the first accepted response. The response predicate runs before the
/// timeout test, and request failures propagate.
///
/// # Errors
/// Returns the request error without retrying it.
///
/// # Panics
/// Panics with the final response when none is accepted before the deadline,
/// so a caller cannot mistake a timed-out poll for a ready one.
#[bon::builder]
pub async fn retry_response<R, E, F, Fut>(
    #[builder(default = Duration :: from_secs (10))] timeout: Duration,
    #[builder(default = Duration :: from_millis (50))] backoff: Duration,
    mut request: F,
    ready: impl Fn(&R) -> bool,
) -> Result<R, E>
where
    R: Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, E>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        let response = request().await?;
        if ready(&response) {
            return Ok(response);
        }
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_else(|| {
                panic!("response did not reach its expected state within {timeout:?}: {response:?}")
            });
        time::sleep(backoff.min(remaining)).await;
    }
}

/// Bound one cluster step with the caller's timeout and diagnostic.
pub async fn within<F: Future>(what: &str, timeout: Duration, future: F) -> F::Output {
    time::timeout(timeout, future)
        .await
        .unwrap_or_else(|_| panic!("{what} did not finish within {timeout:?}"))
}
