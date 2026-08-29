//! Tests for the ticker itself: that it sweeps once at startup, re-arms on
//! the injected interval, and returns when the shutdown token is cancelled.

use assert2::assert;
use krabka_ids::PartitionIndex;
use krabka_units::secs;

use super::*;
use crate::cleaner::test_support::{compactable_partition, record_count};

#[tokio::test]
async fn run_ticks_until_shutdown() {
    use qubit_clock::{MockWaiterKind, sleep::MockSleeper};

    let dir = tempfile::tempdir().expect("log root");
    let registry = Arc::new(PartitionRegistry::new());
    let partition = compactable_partition(
        &dir,
        "run-compact",
        0,
        NodeId(7),
        krabka_log::CleanupPolicy::Compact,
    );
    let before = record_count(&partition);
    registry.insert(
        "run-compact".to_string(),
        PartitionIndex(0),
        Arc::clone(&partition),
    );

    // Drive the sweep cadence on a mock timeline instead of wall-clock time.
    let interval = secs(30);
    let sleeper = MockSleeper::new();
    let timeline = sleeper.timeline();
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run(
        Arc::clone(&registry),
        NodeId(7),
        CleanerConfig {
            interval,
            sleeper: Arc::new(sleeper),
            metadata: None,
        },
        shutdown.clone(),
        BrokerMetrics::new(),
    ));

    // The immediate t=0 tick runs a compaction sweep, then the loop re-arms
    // on `sleep_for_async(interval)`. Block (bounded real time, hang-guard
    // only) until that interval-sleep waiter is parked — it registers
    // strictly after the first sweep's `tick_all` returns, so the compaction
    // is fully applied by then. `wait_for_blocked_waiters` runs on a blocking
    // thread so it never stalls the current-thread runtime that must drive
    // the cleaner task and the partition writer actor to completion.
    let tl = timeline.clone();
    let parked = tokio::task::spawn_blocking(move || {
        tl.wait_for_blocked_waiters(MockWaiterKind::Sleep, 1, Duration::from_secs(5))
    })
    .await
    .unwrap();
    assert!(
        parked,
        "cleaner should park on the interval sleep after the first sweep"
    );
    assert!(
        record_count(&partition) < before,
        "immediate first sweep should compact the eligible partition"
    );

    // Advance one interval to fire a second sweep, then confirm the loop
    // re-parks — proving it keeps ticking on the injected cadence with no
    // wall-clock time (the second sweep is idempotent, so the log stays
    // compacted rather than shrinking further).
    timeline.advance(interval.to_std());
    let tl = timeline.clone();
    let parked_again = tokio::task::spawn_blocking(move || {
        tl.wait_for_blocked_waiters(MockWaiterKind::Sleep, 1, Duration::from_secs(5))
    })
    .await
    .unwrap();
    assert!(
        parked_again,
        "cleaner should re-park on the interval sleep after the second sweep"
    );
    assert!(record_count(&partition) < before, "log stays compacted");

    shutdown.cancel();
    task.await.expect("cleaner task exits");
}
