//! Per-broker local-retention ticker.
//!
//! Every `interval`, it walks the partition registry and dispatches
//! [`Partition::retain_log`](crate::partition::Partition::retain_log) for
//! every partition this broker hosts whose topic is not under a KFC-9 write
//! freeze. That call runs `krabka_log::Log::tick` on the partition's writer
//! actor: the time-based segment roll (`segment.ms`) followed by the
//! `retention.ms` and `retention.bytes` eviction of sealed segments.
//!
//! This is Kafka's `LogManager.cleanupLogs`, on `log.retention.check.interval.ms`,
//! and it is the half of log maintenance the [cleaner](crate::cleaner) does
//! not do. The two are separate loops because they are separate Kafka
//! settings with separate defaults, and because they gate on different things:
//! see [`sweep`] for the leadership and freeze semantics, which differ from
//! the cleaner's.
//!
//! This file holds the ticker and the configuration it reads; the sweep is in
//! [`sweep`].

use std::sync::Arc;

use krabka_units::{Time, convert::TimeExt as _};
use qubit_clock::{StdTimer, Timer};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use self::sweep::tick_all;
use crate::{metrics::BrokerMetrics, partition_registry::PartitionRegistry, time_util};

mod sweep;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

/// Tunables for [`run`].
#[derive(Clone)]
pub(crate) struct LogRetentionConfig {
    pub interval: Time,
    /// Relative timer that drives the retention-sweep cadence. Production uses
    /// [`qubit_clock::StdTimer`], which is real time. Tests inject a timer
    /// from a [`qubit_clock::ManualMonotonicClock`], so the sweep interval
    /// fires on a controlled manual timeline instead of wall-clock time.
    pub timer: Arc<dyn Timer>,
    /// The metadata authority the sweep reads the KFC-9 write-freeze registry
    /// from, re-read once per sweep so a freeze and a thaw both take effect on
    /// the next tick.
    ///
    /// `None` is a sweep with no metadata authority to ask. It resolves no
    /// freeze and leaves every partition eligible, which is the answer an
    /// empty registry gives anyway.
    pub metadata: Option<Arc<dyn crate::metadata_source::MetadataSource>>,
}

impl LogRetentionConfig {
    pub(crate) fn system(interval: Time) -> Self {
        Self {
            interval,
            timer: Arc::new(StdTimer::new()),
            metadata: None,
        }
    }
}

/// Spawned task entry point.
pub(crate) async fn run(
    partitions: Arc<PartitionRegistry>,
    cfg: LogRetentionConfig,
    shutdown: CancellationToken,
    metrics: BrokerMetrics,
) {
    // The cadence is driven as the cleaner's is -- each deadline armed only
    // after the sweep completes (`MissedTickBehavior::Delay` semantics), so a
    // slow sweep over a broker with thousands of partitions never triggers a
    // catch-up burst -- with one difference: the first deadline is a full
    // `interval` out rather than the cleaner's zero-duration t=0 tick.
    //
    // Kafka delays `cleanupLogs` past startup for the same reason (its
    // `log.initial.task.delay.ms`), and here the reason is sharper than
    // politeness: a partition's `LogConfig` is the broker-wide default until
    // `ReplicatorSupervisor::reconcile` pushes the topic's overrides through
    // `WriterMessage::SetLogConfig`. A sweep at t=0 would apply the default
    // seven-day `retention.ms` to a topic configured `retention.ms=-1`, and
    // deleting a segment is not a decision to make on a config that has not
    // arrived yet. Compaction has no such hazard, which is why the cleaner
    // still fires immediately.
    //
    // Arming and completing a deadline are both fallible, and a sweep whose
    // cadence is gone has nothing left to do, so either failure ends the task
    // rather than spinning on a timer that cannot be armed.
    const TASK: &str = "log retention";
    let timer = Arc::clone(&cfg.timer);
    let Some(mut tick) = time_util::arm(&*timer, cfg.interval.to_std(), TASK) else {
        return;
    };
    loop {
        tokio::select! {
            outcome = &mut tick => {
                if !time_util::fired(outcome, TASK) {
                    return;
                }
                // One image read per sweep. The freeze registry the sweep gates
                // on is whatever the metadata authority holds when the tick
                // starts, so a freeze applied mid-sweep takes effect on the
                // next one.
                let image = cfg.metadata.as_ref().map(|source| source.current_image());
                tick_all(&partitions, image.as_deref(), &metrics).await;
                let Some(next) = time_util::arm(&*timer, cfg.interval.to_std(), TASK) else {
                    return;
                };
                tick = next;
            }
            () = shutdown.cancelled() => {
                debug!("log retention task shutting down");
                return;
            }
        }
    }
}
