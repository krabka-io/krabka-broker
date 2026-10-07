//! Visibility polling shared by transaction scenarios.
use std::time::{Duration, Instant};

use krabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};

/// Retain the 200ms polls and check completion before each poll, including the first.
pub(crate) async fn poll_values_until(
    consumer: &mut Consumer,
    timeout: Duration,
    done: impl Fn(&[String]) -> bool,
) -> Vec<String> {
    let mut seen = Vec::new();
    let deadline = Instant::now() + timeout;
    while !done(&seen) && Instant::now() < deadline {
        for record in consumer
            .poll(krabka_units::millis(200))
            .await
            .expect("poll")
        {
            seen.push(String::from_utf8_lossy(record.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    seen
}

pub(crate) async fn read_committed_through(
    bootstrap: &str,
    topic: &str,
    last: &str,
    timeout: Duration,
) -> Vec<String> {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.to_string())
        .group_id(format!("{topic}-reader"))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe([topic.to_string()])
        .build()
        .await
        .expect("consumer");
    let seen = poll_values_until(&mut consumer, timeout, |seen| {
        seen.last().map(String::as_str) == Some(last)
    })
    .await;
    consumer.close().await.expect("close consumer");
    seen
}
