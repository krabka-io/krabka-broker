//! The metric seam of the barrier coordinator.
//!
//! The coordinator reports each injection through this trait. The broker binds
//! it to the process metric registry. A unit test binds [`NoBarrierMetrics`],
//! so no test needs a live registry.

use krabka_ids::PartitionIndex;
use krabka_units::{Time, convert::TimeExt as _};

use crate::barrier::persistence::CutStatus;

/// What one finished injection reported.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct InjectionReport {
    /// The epoch the injection consumed.
    pub(crate) epoch: i64,
    /// Whether every target partition carries the marker.
    pub(crate) status: CutStatus,
    /// How many partitions carry the marker.
    pub(crate) marked: usize,
    /// How many partitions carry no marker.
    pub(crate) missing: usize,
    /// How long the injection took, from the first record to the cut record.
    pub(crate) elapsed: Time,
}

/// The counters and gauges that the barrier coordinator feeds.
pub(crate) trait BarrierMetrics: Send + Sync {
    /// The coordinator wrote the injection-start record of `epoch`.
    fn injection_started(&self, group: &str, epoch: i64);

    /// The coordinator published the cut of one injection.
    fn injection_completed(&self, group: &str, report: InjectionReport);

    /// One marker landed in a partition this broker leads, or in one a remote
    /// leader answered for.
    fn marker_written(&self, topic: &str);

    /// One marker append failed. The coordinator retries the partition until
    /// its deadline runs out.
    fn marker_append_failed(&self, topic: &str, partition: PartitionIndex);

    /// How many groups this broker coordinates now.
    fn groups_coordinated(&self, count: usize);
}

/// The [`BarrierMetrics`] the running broker uses.
///
/// It holds a [`BrokerMetrics`](crate::metrics::BrokerMetrics), which clones
/// cheaply, so the coordinator can own one behind an `Arc<dyn BarrierMetrics>`
/// without borrowing from the broker.
#[derive(Clone)]
pub(crate) struct BrokerBarrierMetrics {
    metrics: crate::metrics::BrokerMetrics,
}

impl BrokerBarrierMetrics {
    pub(crate) const fn new(metrics: crate::metrics::BrokerMetrics) -> Self {
        Self { metrics }
    }

    fn group(group: &str) -> crate::metrics::BarrierGroupLabel {
        crate::metrics::BarrierGroupLabel {
            group: group.to_owned(),
        }
    }
}

impl BarrierMetrics for BrokerBarrierMetrics {
    fn injection_started(&self, group: &str, _epoch: i64) {
        self.metrics
            .barrier_epochs_started_total
            .get_or_create(&Self::group(group))
            .inc();
    }

    fn injection_completed(&self, group: &str, report: InjectionReport) {
        let label = Self::group(group);
        // A partial cut is published, so it counts as an outcome, not as a
        // failure. The two counters separate the alertable case from the
        // healthy one.
        match report.status {
            CutStatus::Complete => self
                .metrics
                .barrier_epochs_committed_total
                .get_or_create(&label)
                .inc(),
            CutStatus::Partial => self
                .metrics
                .barrier_epochs_published_partial_total
                .get_or_create(&label)
                .inc(),
        };
        self.metrics
            .barrier_injection_duration_seconds
            .get_or_create(&label)
            .observe(report.elapsed.secs_f64());
        // The gauge names the newest cut this coordinator PUBLISHED, so it
        // moves here and not when the injection starts. A started epoch that
        // never publishes must not advance it.
        self.metrics
            .barrier_latest_epoch
            .get_or_create(&label)
            .set(report.epoch);
    }

    fn marker_written(&self, topic: &str) {
        self.metrics
            .barrier_markers_written_total
            .get_or_create(&crate::metrics::TopicLabel {
                topic: topic.to_owned(),
            })
            .inc();
    }

    fn marker_append_failed(&self, _topic: &str, _partition: PartitionIndex) {
        // No counter carries this yet. Every call site logs the topic, the
        // partition and the error at warn, and a run of failures shows up as a
        // partial cut on barrier_epochs_published_partial_total.
    }

    fn groups_coordinated(&self, count: usize) {
        self.metrics
            .barrier_groups_coordinated
            .set(i64::try_from(count).unwrap_or(i64::MAX));
    }
}

/// A [`BarrierMetrics`] that counts nothing.
///
/// It exists so a unit test needs no live metric registry.
#[cfg(test)]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct NoBarrierMetrics;

#[cfg(test)]
impl BarrierMetrics for NoBarrierMetrics {
    fn injection_started(&self, _group: &str, _epoch: i64) {}

    fn marker_written(&self, _topic: &str) {}
    fn injection_completed(&self, _group: &str, _report: InjectionReport) {}
    fn marker_append_failed(&self, _topic: &str, _partition: PartitionIndex) {}
    fn groups_coordinated(&self, _count: usize) {}
}
