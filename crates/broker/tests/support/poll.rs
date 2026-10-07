//! Bounded wire-response retries, whose decisions can lag a committed metadata image.

use std::{
    future::Future,
    time::{Duration, Instant},
};

/// Return the first accepted response, or the final response after the original deadline.
/// The response predicate runs before the timeout test, and request failures propagate.
///
/// # Errors
/// Returns the request error without retrying it.
pub async fn retry_response<R, E, F, Fut>(
    timeout: Duration,
    backoff: Duration,
    mut request: F,
    ready: impl Fn(&R) -> bool,
) -> Result<R, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, E>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        let response = request().await?;
        if ready(&response) || Instant::now() > deadline {
            return Ok(response);
        }
        tokio::time::sleep(backoff).await;
    }
}
