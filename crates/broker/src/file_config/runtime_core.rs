//! The `[runtime]` appliers for the core broker timers and for replication.
//!
//! `apply_core` covers the startup, registration, audit, metrics, and RLMM
//! cadences; `apply_replication` covers the follower fetch loop's sizes and
//! backoffs. They share a module because both write the timings the broker
//! needs before it serves a single request.

use super::{
    FileConfigError, RuntimeFileConfig,
    validate::{
        invalid_runtime_value, positive_time, voter_request_time, whole_bytes_i32,
        whole_millis_i32_time,
    },
};

impl RuntimeFileConfig {
    pub(super) fn apply_core(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;

        set_runtime_time_millis!(
            runtime,
            startup_leader_wait_timeout,
            cfg.startup_leader_wait_timeout
        );
        set_runtime_time_millis!(
            runtime,
            self_registration_backoff_min,
            cfg.self_registration_backoff_min
        );
        set_runtime_time_millis!(
            runtime,
            self_registration_backoff_max,
            cfg.self_registration_backoff_max
        );
        set_runtime_time_millis!(runtime, observer_poll_interval, cfg.observer_poll_interval);
        set_runtime_time_millis!(
            runtime,
            audit_spool_replay_interval,
            cfg.audit_spool_replay_interval
        );
        set_runtime_time_millis!(
            runtime,
            audit_stats_poll_interval,
            cfg.audit_stats_poll_interval
        );
        set_runtime_time_millis!(
            runtime,
            audit_partition_wait_timeout,
            cfg.audit_partition_wait_timeout
        );
        set_runtime_time_millis!(runtime, liveness_tick_interval, cfg.liveness_tick_interval);
        set_runtime_time_millis!(runtime, gauge_poll_interval, cfg.gauge_poll_interval);
        set_runtime_time_millis!(runtime, isr_scan_interval, cfg.isr_scan_interval);
        set_runtime_time_millis!(runtime, cleaner_interval, cfg.cleaner_interval);
        set_runtime_time_millis!(
            runtime,
            log_retention_check_interval,
            cfg.log_retention_check_interval
        );
        set_runtime_time_millis!(
            runtime,
            future_log_move_retry_backoff,
            cfg.future_log_move_retry_backoff
        );
        set_runtime_time_millis!(
            runtime,
            client_metrics_eviction_tick,
            cfg.client_metrics_eviction_tick
        );
        set_runtime_time_millis!(
            runtime,
            client_metrics_stale_floor,
            cfg.client_metrics_stale_floor
        );
        set_runtime_time_millis!(
            runtime,
            client_metrics_default_interval,
            cfg.client_metrics_default_interval,
            positive_i32
        );
        set_runtime_size_bytes!(
            runtime,
            client_metrics_telemetry_max,
            cfg.client_metrics_telemetry_max,
            whole_bytes_i32
        );
        set_runtime_time_millis!(
            runtime,
            client_metrics_prom_snapshot_ttl,
            cfg.client_metrics_prom_snapshot_ttl
        );
        set_runtime_time_millis!(runtime, rlmm_reconcile_tick, cfg.rlmm_reconcile_tick);
        set_runtime_time_millis!(
            runtime,
            rlmm_bootstrap_backoff_initial,
            cfg.rlmm_bootstrap_backoff_initial
        );
        set_runtime_time_millis!(
            runtime,
            rlmm_bootstrap_backoff_max,
            cfg.rlmm_bootstrap_backoff_max
        );
        set_runtime_time_millis!(
            runtime,
            connection_creation_throttle_max,
            cfg.connection_creation_throttle_max
        );
        set_runtime_time_millis!(runtime, opa_http_timeout, cfg.opa_http_timeout);
        set_runtime_time_millis!(
            runtime,
            schema_registry_http_timeout,
            cfg.schema_registry_http_timeout
        );
        set_runtime_time_millis!(
            runtime,
            oauth_jwks_http_timeout,
            cfg.oauth_jwks_http_timeout
        );
        set_runtime_time_millis!(
            runtime,
            auto_join_retry_backoff,
            cfg.auto_join_retry_backoff
        );
        if let Some(value) = runtime.auto_join_voter_request_timeout {
            cfg.auto_join_voter_request_timeout =
                voter_request_time("auto_join_voter_request_timeout", value)?;
        }
        Ok(())
    }

    pub(super) fn apply_replication(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        if let Some(fetchers) = runtime.replica_fetchers {
            if fetchers == 0 {
                return Err(invalid_runtime_value(
                    "replica_fetchers",
                    "must be at least 1: a leader with no fetcher is never followed",
                ));
            }
            cfg.replication.fetchers = fetchers;
        }
        set_runtime_size_bytes!(
            runtime,
            replication_fetch_max,
            cfg.replication.fetch_max,
            whole_bytes_i32
        );
        set_runtime_time_millis!(
            runtime,
            replication_fetch_max_wait,
            cfg.replication.fetch_max_wait,
            positive_i32
        );
        set_runtime_size_bytes!(
            runtime,
            replication_fetch_min,
            cfg.replication.fetch_min,
            whole_bytes_i32
        );
        set_runtime_time_millis!(
            runtime,
            replication_throttle_exhausted_backoff,
            cfg.replication.throttle_exhausted_backoff
        );
        set_runtime_time_millis!(
            runtime,
            replication_send_error_backoff,
            cfg.replication.send_error_backoff
        );
        set_runtime_time_millis!(
            runtime,
            replication_unknown_topic_retry_delay,
            cfg.replication.unknown_topic_retry_delay
        );
        set_runtime_time_millis!(
            runtime,
            replication_epoch_fence_backoff,
            cfg.replication.epoch_fence_backoff
        );
        set_runtime_time_millis!(
            runtime,
            replication_unexpected_error_backoff,
            cfg.replication.unexpected_error_backoff
        );
        set_runtime_time_millis!(
            runtime,
            replication_reconnect_initial_delay,
            cfg.replication.reconnect_initial_delay
        );
        set_runtime_time_millis!(
            runtime,
            replication_reconnect_delay_cap,
            cfg.replication.reconnect_delay_cap
        );
        Ok(())
    }
}
