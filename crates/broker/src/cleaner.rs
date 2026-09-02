//! Per-broker log-compaction ticker.
//!
//! Every `interval`, it walks the partitions registry and dispatches
//! [`Partition::compact_log`](crate::partition::Partition::compact_log) for
//! every partition where all of these hold:
//!
//!   - the topic's `cleanup.policy` is `compact`,
//!   - this broker is currently the leader, and
//!   - no KFC-9 write freeze covers the topic.
//!
//! The compaction itself runs on the partition's writer actor, so appends and
//! compaction run in sequence.
//!
//! This file holds the ticker and the configuration it reads. The sweep that
//! those three conditions describe lives in [`sweep`].

use std::{sync::Arc, time::Duration};

use krabka_metadata::NodeId;
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
pub(crate) struct CleanerConfig {
    pub interval: Time,
    /// Relative timer that drives the compaction-sweep cadence. Production
    /// uses [`qubit_clock::StdTimer`], which is real time. Tests inject a
    /// timer from a [`qubit_clock::ManualMonotonicClock`], so the sweep
    /// interval fires on a controlled manual timeline instead of wall-clock
    /// time.
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

impl CleanerConfig {
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
    node_id: NodeId,
    cfg: CleanerConfig,
    shutdown: CancellationToken,
    metrics: BrokerMetrics,
) {
    // Drive the sweep cadence through the injected `Timer` (production: real
    // time; tests: a controlled manual timeline). A zero-duration first sleep
    // reproduces `tokio::time::interval`'s immediate t=0 tick, so the first sweep
    // fires at startup; each subsequent sleep is re-armed to `cfg.interval` only
    // after the sweep completes (`MissedTickBehavior::Delay` semantics — a slow
    // sweep never triggers a catch-up burst). Arming and completing a deadline
    // are both fallible, and a cleaner whose cadence is gone has nothing left to
    // do, so either failure ends the task rather than spinning on a timer that
    // cannot be armed. The timer is cloned into a local only to leave `cfg` free
    // for the sweep body; the tick future itself is `'static` and borrows
    // nothing.
    const TASK: &str = "log cleaner";
    let timer = Arc::clone(&cfg.timer);
    let Some(mut tick) = time_util::arm(&*timer, Duration::ZERO, TASK) else {
        return;
    };
    loop {
        tokio::select! {
            outcome = &mut tick => {
                if !time_util::fired(outcome, TASK) {
                    return;
                }
                // One image read per sweep. The registry the sweep gates on is
                // whatever the metadata authority holds when the tick starts,
                // so a freeze applied mid-sweep takes effect on the next one.
                let image = cfg.metadata.as_ref().map(|source| source.current_image());
                tick_all(&partitions, image.as_deref(), node_id, &metrics).await;
                let Some(next) = time_util::arm(&*timer, cfg.interval.to_std(), TASK) else {
                    return;
                };
                tick = next;
            }
            () = shutdown.cancelled() => {
                debug!("cleaner task shutting down");
                return;
            }
        }
    }
}
