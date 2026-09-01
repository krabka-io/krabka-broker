//! Construction of a [`BrokerMetrics`] bundle and its registration with the
//! Prometheus registry. It holds the histogram bucket boundaries, the
//! unregistered constructor that gives every family its handle, and the
//! entry point that walks the registration groups in the children.

use std::sync::Arc;

use prometheus_client::{
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};
use tokio::sync::Mutex;

use crate::metrics::{BrokerMetrics, SharedRegistry};

mod requests_and_resources;
mod subsystems;
mod topics_and_replication;

#[cfg(test)]
mod tests;

/// Latency buckets (seconds) for the per-API `request_duration_seconds`
/// histogram. Spans ~100µs (idempotent `ApiVersions`) to 10s (a slow
/// controller round-trip or a throttled admin RPC), tuned so the common
/// Produce/Fetch band (0.5ms–50ms) lands on distinct buckets.
///
/// The `request_{local,remote,throttle}_duration_seconds` phases and
/// `quota_throttle_duration_seconds` share these boundaries deliberately. A
/// phase is a part of the total, and an operator checks the phases against the
/// total bucket by bucket; two bucket sets would make that comparison an
/// interpolation rather than a subtraction.
const REQUEST_DURATION_BUCKETS: [f64; 12] = [
    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 10.0,
];

/// Latency buckets (seconds) for `barrier_injection_duration_seconds`. One
/// injection appends a marker to every partition of a barrier group, and a
/// partition that another broker leads costs an inter-broker round trip. The
/// span runs from 5ms for a small single-broker group to 30s, which is the
/// default `barrier_injection_timeout`.
const BARRIER_INJECTION_DURATION_BUCKETS: [f64; 12] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Latency buckets (seconds) for `delivery_activation_lateness_seconds`.
/// KFC-1 bounds activation lateness at twice the topic's declared
/// `delivery_clock_uncertainty` plus one scheduler tick, so the value an
/// operator sees is normally a few hundred milliseconds at most: the span opens
/// at 1ms and resolves the sub-second band finely. The tail runs to 30s so a
/// broker with real clock skew, or one whose scheduler is starved of CPU, still
/// lands in a bucket instead of in `+Inf`.
const DELIVERY_ACTIVATION_LATENESS_BUCKETS: [f64; 12] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 10.0, 30.0,
];

impl BrokerMetrics {
    fn unregistered(registry: SharedRegistry) -> Self {
        Self {
            registry,
            topic_bytes_in: Family::default(),
            topic_bytes_out: Family::default(),
            topic_messages_in: Family::default(),
            topic_produce_requests: Family::default(),
            topic_fetch_requests: Family::default(),
            topic_failed_produce_requests: Family::default(),
            topic_failed_fetch_requests: Family::default(),
            partition_bytes_in: Family::default(),
            partition_bytes_out: Family::default(),
            replication_bytes_in: Family::default(),
            replication_bytes_out: Family::default(),
            partition_disk_bytes: Family::default(),
            share_group_backlog: Family::default(),
            partition_cpu_micros: Family::default(),
            partitions_led: Gauge::default(),
            partitions_total: Gauge::default(),
            under_replicated_partitions: Gauge::default(),
            under_min_isr_partition_count: Gauge::default(),
            offline_partitions_count: Gauge::default(),
            active_controller: Gauge::default(),
            ignored_static_voters: Gauge::default(),
            witness_role: Gauge::default(),
            leader_site_drift_partitions: Gauge::default(),
            voted_directory: Family::default(),
            controller_leader_changes_total: Counter::default(),
            isr_shrinks_total: Counter::default(),
            isr_expands_total: Counter::default(),
            incremental_fetch_sessions: Gauge::default(),
            incremental_fetch_session_evictions_total: Counter::default(),
            incremental_fetch_partitions_cached: Gauge::default(),
            client_software_versions: Family::default(),
            successful_authentication: Family::default(),
            failed_authentication: Family::default(),
            api_requests: Family::default(),
            unsupported_api_requests: Family::default(),
            request_duration_seconds: Family::new_with_constructor(|| {
                Histogram::new(REQUEST_DURATION_BUCKETS)
            }),
            request_local_duration_seconds: Family::new_with_constructor(|| {
                Histogram::new(REQUEST_DURATION_BUCKETS)
            }),
            request_remote_duration_seconds: Family::new_with_constructor(|| {
                Histogram::new(REQUEST_DURATION_BUCKETS)
            }),
            request_throttle_duration_seconds: Family::new_with_constructor(|| {
                Histogram::new(REQUEST_DURATION_BUCKETS)
            }),
            quota_throttle_duration_seconds: Family::new_with_constructor(|| {
                Histogram::new(REQUEST_DURATION_BUCKETS)
            }),
            in_flight_requests: Gauge::default(),
            active_connections: Gauge::default(),
            connection_closes: Family::default(),
            request_errors: Family::default(),
            tiered_storage_rlmm_topic_backed: Gauge::default(),
            tiered_storage_rlmm_bootstrap_attempts: Counter::default(),
            produce_message_conversions: Family::default(),
            fetch_message_conversions: Family::default(),
            unclean_leader_elections_total: Counter::default(),
            audit_events_total: Counter::default(),
            audit_write_failures_total: Counter::default(),
            audit_spool_depth: Gauge::default(),
            audit_spool_bytes: Gauge::default(),
            audit_records_spooled_total: Counter::default(),
            audit_records_replayed_total: Counter::default(),
            audit_records_dropped_total: Counter::default(),
            client_metrics_otlp_dropped_total: Counter::default(),
            client_metrics_otlp_failed_total: Counter::default(),
            log_cleaner_runs_total: Counter::default(),
            log_compactions_total: Family::default(),
            barrier_epochs_started_total: Family::default(),
            barrier_epochs_committed_total: Family::default(),
            barrier_epochs_published_partial_total: Family::default(),
            barrier_injection_duration_seconds: Family::new_with_constructor(|| {
                Histogram::new(BARRIER_INJECTION_DURATION_BUCKETS)
            }),
            barrier_latest_epoch: Family::default(),
            barrier_markers_written_total: Family::default(),
            barrier_groups_coordinated: Gauge::default(),
            delivery_watermark: Family::default(),
            delivery_pending_records: Family::default(),
            delivery_activation_lateness_seconds: Histogram::new(
                DELIVERY_ACTIVATION_LATENESS_BUCKETS,
            ),
            delivery_scheduler_wakeups_total: Counter::default(),
            schema_validation_rejections: Family::default(),
            schema_validation_cache_hits: Counter::default(),
            schema_validation_cache_misses: Counter::default(),
            delivery_clock_uncertainty_seconds: Gauge::default(),
            topic_freeze_rejections: Family::default(),
            topic_freezes_active: Gauge::default(),
            break_glass_proposals: Family::default(),
            break_glass_refusals: Family::default(),
            break_glass_bypassed: Family::default(),
            diskless_wal_durable_watermark: Family::default(),
            diskless_wal_voter_lag: Family::default(),
            diskless_wal_quorum_loss_events_total: Counter::default(),
            diskless_wal_flush_attempts_total: Counter::default(),
            diskless_wal_flush_bytes_total: Counter::default(),
            diskless_wal_flush_failures_total: Counter::default(),
            diskless_wal_index_projection_lag: Family::default(),
            diskless_wal_trim_frontier: Family::default(),
            diskless_wal_cold_read_hits_total: Counter::default(),
            diskless_wal_cold_read_misses_total: Counter::default(),
            diskless_wal_cold_read_errors_total: Counter::default(),
        }
    }

    /// Build and register every broker metric.
    #[must_use]
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn new() -> Self {
        let registry = Arc::new(Mutex::new(Registry::with_prefix("krabka_broker")));
        let metrics = Self::unregistered(registry);
        {
            let mut registry = metrics
                .registry
                .try_lock()
                .expect("fresh metrics registry cannot be locked");
            metrics.register_group_1(&mut registry);
            metrics.register_group_2(&mut registry);
            metrics.register_group_3(&mut registry);
            metrics.register_group_4(&mut registry);
            metrics.register_group_5(&mut registry);
            metrics.register_group_6(&mut registry);
            metrics.register_group_7(&mut registry);
            metrics.register_group_8(&mut registry);
        }
        metrics
    }
}

impl Default for BrokerMetrics {
    fn default() -> Self {
        Self::new()
    }
}
