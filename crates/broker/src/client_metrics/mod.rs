//! KIP-714 client metrics receiver: the subscription config, the
//! client-instance registry, the OTLP decode, and the Prometheus and OTLP
//! sinks.

pub mod config;
pub mod ingestion;
pub mod manager;
pub mod otlp_sink;
pub mod prometheus_sink;
#[cfg(test)]
pub mod test_support;
pub use manager::ClientMetricsManager;
use {
    self::{
        otlp_sink::OtlpForwarder,
        prometheus_sink::{ClientMetricsCollector, SeriesCeilings, SeriesStats},
    },
    krabka_units::{Time, convert::TimeExt as _},
    prometheus_client::metrics::counter::Counter,
    std::{sync::Arc, time::Duration},
};

/// Broker-held bundle. It holds the manager, which owns the instance state and
/// the matching, plus the two sinks. The metrics registry shares the Prometheus
/// collector.
pub struct ClientMetrics {
    pub manager: ClientMetricsManager,
    pub prometheus: Arc<ClientMetricsCollector>,
    pub otlp: OtlpForwarder,
}

#[bon::bon]
impl ClientMetrics {
    /// `otlp_endpoint` is `None` when OTLP forwarding is disabled.
    #[builder]
    pub(crate) fn new(
        manager: ClientMetricsManager,
        otlp_endpoint: Option<String>,
        otlp_protocol: krabka_telemetry::OtlpProtocol,
        otlp_queue_capacity: usize,
        prometheus_snapshot_ttl: Time,
        #[builder(default)] prometheus_ceilings: SeriesCeilings,
        #[builder(default)] prometheus_stats: SeriesStats,
        #[builder(default)] otlp_dropped: Counter,
        #[builder(default)] otlp_failed: Counter,
    ) -> Self {
        let otlp = otlp_endpoint.map_or_else(OtlpForwarder::disabled, |ep| {
            OtlpForwarder::spawn(
                ep,
                otlp_protocol,
                otlp_queue_capacity,
                otlp_dropped,
                otlp_failed,
            )
        });
        Self {
            manager,
            prometheus: Arc::new(
                ClientMetricsCollector::builder()
                    .ttl(prometheus_snapshot_ttl.to_std())
                    .ceilings(prometheus_ceilings)
                    .stats(prometheus_stats)
                    .build(),
            ),
            otlp,
        }
    }

    pub(crate) async fn shutdown(&self) { self.otlp.shutdown().await; }

    pub(crate) fn connection_closed(&self, connection_id: &str) {
        if let Some(instance) = self.manager.connection_closed(connection_id) {
            self.prometheus.remove_instance(instance);
        }
    }

    /// One tick of the eviction ticker: expires idle client instances, as
    /// Kafka's expiration task does, and drops Prometheus series past their
    /// TTL.
    pub(crate) fn evict_stale(&self, stale_push_intervals: u32, stale_floor: Duration) {
        self.manager.evict_stale(stale_push_intervals, stale_floor);
        self.prometheus.evict_stale();
    }
}
