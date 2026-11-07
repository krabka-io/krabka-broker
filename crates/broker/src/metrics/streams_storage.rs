//! Bounded native topology storage telemetry. No group IDs or plugin-supplied
//! strings become labels; the six operation/result combinations are broker-wide.
use {super::BrokerMetrics, prometheus_client::encoding::EncodeLabelSet, std::time::Duration};

/// The three asynchronous native topology storage callbacks.
#[derive(Debug, Clone, Copy)]
pub enum TopologyStorageOperation {
    Set,
    Get,
    Delete,
}

/// Closed operation/result label set with at most six combinations.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TopologyStorageLabel {
    operation: &'static str,
    result: &'static str,
}

impl TopologyStorageLabel {
    /// Labels for one completed native callback; no plugin strings are accepted.
    #[must_use]
    pub fn new(operation: TopologyStorageOperation, succeeded: bool) -> Self {
        Self {
            operation: match operation {
                TopologyStorageOperation::Set => "set",
                TopologyStorageOperation::Get => "get",
                TopologyStorageOperation::Delete => "delete",
            },
            result: if succeeded { "success" } else { "failure" },
        }
    }
}

impl BrokerMetrics {
    pub(crate) fn record_topology_storage(
        &self,
        operation: TopologyStorageOperation,
        succeeded: bool,
        duration: Duration,
    ) {
        let labels = TopologyStorageLabel::new(operation, succeeded);
        self.streams_topology_storage_operations
            .get_or_create(&labels)
            .inc();
        self.streams_topology_storage_duration_seconds
            .get_or_create(&labels)
            .observe(duration.as_secs_f64());
    }
}
