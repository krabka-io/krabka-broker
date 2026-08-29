//! The long poll that a `ShareFetch` runs when its first acquire pass took
//! nothing and the client asked to wait.
//!
//! It parks on the append and HW-advance notifies of the partitions this
//! broker leads, so a second pass only runs once there is something new to
//! see, or the client's `max_wait_ms` expired.

use std::{sync::Arc, time::Duration};

use tokio::sync::Notify;

use super::pending::PendingPartition;
use crate::broker::Broker;

type WaitFut = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// Parks on the append and HW-advance notifies of the partitions that this
/// broker can lead, under a single timeout. It mirrors the wait construction
/// in `fetch::long_poll_then_reread`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LongPollOutcome {
    NoPartitions,
    Notified,
    TimedOut,
}

pub(super) async fn long_poll(
    broker: &Broker,
    pending: &[PendingPartition],
    max_wait_ms: i32,
) -> LongPollOutcome {
    let notifies = pending
        .iter()
        .filter(|partition| partition.leadable)
        .filter(|partition| partition.fetchable)
        .filter_map(|partition| {
            partition.topic_name.as_deref().and_then(|name| {
                broker
                    .partitions
                    .get(name, krabka_ids::PartitionIndex(partition.partition_index))
            })
        })
        .flat_map(|partition| {
            [
                partition.append_notify.clone(),
                partition.hw_advance_notify.clone(),
            ]
        })
        .collect();
    let max_wait = Duration::from_millis(u64::from(u32::try_from(max_wait_ms).unwrap_or(0)));
    wait_for_notifications(notifies, max_wait).await
}

async fn wait_for_notifications(notifies: Vec<Arc<Notify>>, max_wait: Duration) -> LongPollOutcome {
    let Some(_) = notifies.first() else {
        return LongPollOutcome::NoPartitions;
    };
    let waits: Vec<WaitFut> = notifies
        .into_iter()
        .map(|n| Box::pin(async move { n.notified().await }) as WaitFut)
        .collect();
    match tokio::time::timeout(max_wait, futures_util::future::select_all(waits)).await {
        Ok(_) => LongPollOutcome::Notified,
        Err(_) => LongPollOutcome::TimedOut,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn notification_wait_reports_empty_wakeup_and_timeout() {
        assert!(
            wait_for_notifications(Vec::new(), Duration::from_secs(1)).await
                == LongPollOutcome::NoPartitions
        );

        let notified = Arc::new(Notify::new());
        notified.notify_one();
        assert!(
            wait_for_notifications(vec![notified], Duration::from_secs(1)).await
                == LongPollOutcome::Notified
        );

        assert!(
            wait_for_notifications(vec![Arc::new(Notify::new())], Duration::from_secs(1)).await
                == LongPollOutcome::TimedOut
        );
    }
}
