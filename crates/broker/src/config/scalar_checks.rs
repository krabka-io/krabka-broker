//! The positivity and range checks over the broker's runtime scalars: every
//! interval, budget, capacity, count and topic geometry value that must be
//! inside its bounds before the broker starts.

use krabka_units::{
    ByteSize, Ratio, Time,
    convert::{ByteSizeExt, RatioExt, TimeExt},
    millis,
};

use crate::{BrokerError, config::BrokerConfig};

#[cfg(test)]
mod tests;

impl BrokerConfig {
    pub(super) fn validate_positive_runtime_scalars(&self) -> Result<(), BrokerError> {
        for (name, value) in [
            (
                "startup_leader_wait_timeout",
                self.startup_leader_wait_timeout,
            ),
            (
                "self_registration_backoff_min",
                self.self_registration_backoff_min,
            ),
            (
                "self_registration_backoff_max",
                self.self_registration_backoff_max,
            ),
            ("observer_poll_interval", self.observer_poll_interval),
            (
                "audit_spool_replay_interval",
                self.audit_spool_replay_interval,
            ),
            ("audit_stats_poll_interval", self.audit_stats_poll_interval),
            (
                "audit_partition_wait_timeout",
                self.audit_partition_wait_timeout,
            ),
            ("liveness_tick_interval", self.liveness_tick_interval),
            ("gauge_poll_interval", self.gauge_poll_interval),
            ("isr_scan_interval", self.isr_scan_interval),
            ("cleaner_interval", self.cleaner_interval),
            (
                "diskless_wal_flush_interval",
                self.diskless_wal_flush_interval,
            ),
            (
                "diskless_wal_index_projection_timeout",
                self.diskless_wal_index_projection_timeout,
            ),
            (
                "future_log_move_retry_backoff",
                self.future_log_move_retry_backoff,
            ),
            (
                "client_metrics_eviction_tick",
                self.client_metrics_eviction_tick,
            ),
            (
                "client_metrics_stale_floor",
                self.client_metrics_stale_floor,
            ),
            (
                "client_metrics_prom_snapshot_ttl",
                self.client_metrics_prom_snapshot_ttl,
            ),
            ("rlmm_reconcile_tick", self.rlmm_reconcile_tick),
            (
                "rlmm_bootstrap_backoff_initial",
                self.rlmm_bootstrap_backoff_initial,
            ),
            (
                "rlmm_bootstrap_backoff_max",
                self.rlmm_bootstrap_backoff_max,
            ),
            (
                "connection_creation_throttle_max",
                self.connection_creation_throttle_max,
            ),
            ("opa_http_timeout", self.opa_http_timeout),
            (
                "schema_registry_http_timeout",
                self.schema_registry_http_timeout,
            ),
            ("oauth_jwks_http_timeout", self.oauth_jwks_http_timeout),
            ("auto_join_retry_backoff", self.auto_join_retry_backoff),
            (
                "coordinator_session_expiry_tick",
                self.coordinator_session_expiry_tick,
            ),
            (
                "coordinator_shutdown_ack_timeout",
                self.coordinator_shutdown_ack_timeout,
            ),
            (
                "classic_group_initial_rebalance_delay",
                self.classic_group_initial_rebalance_delay,
            ),
            ("sync_group_follower_wait", self.sync_group_follower_wait),
            (
                "unclean_recovery_aggressive_deadline",
                self.unclean_recovery_aggressive_deadline,
            ),
            (
                "unclean_recovery_balanced_deadline",
                self.unclean_recovery_balanced_deadline,
            ),
            (
                "operator_recovery_deadline",
                self.operator_recovery_deadline,
            ),
            ("quota_throttle_max", self.quota_throttle_max),
            (
                "controller_mutation_quota_window",
                self.controller_mutation_quota_window,
            ),
            (
                "controller_heartbeat_interval",
                self.controller_heartbeat_interval,
            ),
            (
                "remote_log_manager_interval",
                self.remote_log_manager_interval,
            ),
            (
                "producer_id_expiration_scan_interval",
                self.producer_id_expiration_scan_interval,
            ),
            (
                "client_metrics_default_interval",
                self.client_metrics_default_interval,
            ),
            ("heartbeat_interval", self.heartbeat_interval),
            ("replica_lag_time_max", self.replica_lag_time_max),
            (
                "delegation_token_max_lifetime",
                self.delegation_token_max_lifetime,
            ),
            (
                "delegation_token_expiry_check_interval",
                self.delegation_token_expiry_check_interval,
            ),
            (
                "delegation_token_default_renew_period",
                self.delegation_token_default_renew_period,
            ),
        ] {
            require_positive_time(name, value)?;
        }
        for (name, value) in [
            (
                "replication.fetch_max_wait",
                self.replication.fetch_max_wait,
            ),
            (
                "replication.throttle_exhausted_backoff",
                self.replication.throttle_exhausted_backoff,
            ),
            (
                "replication.send_error_backoff",
                self.replication.send_error_backoff,
            ),
            (
                "replication.unknown_topic_retry_delay",
                self.replication.unknown_topic_retry_delay,
            ),
            (
                "replication.epoch_fence_backoff",
                self.replication.epoch_fence_backoff,
            ),
            (
                "replication.unexpected_error_backoff",
                self.replication.unexpected_error_backoff,
            ),
            (
                "replication.reconnect_initial_delay",
                self.replication.reconnect_initial_delay,
            ),
            (
                "replication.reconnect_delay_cap",
                self.replication.reconnect_delay_cap,
            ),
        ] {
            require_positive_time(name, value)?;
        }
        for (name, value) in [
            (
                "client_metrics_telemetry_max",
                self.client_metrics_telemetry_max,
            ),
            (
                "metadata_max_bytes_between_snapshots",
                self.metadata_max_bytes_between_snapshots,
            ),
            ("replication.fetch_max", self.replication.fetch_max),
            ("replication.fetch_min", self.replication.fetch_min),
        ] {
            require_positive_size(name, value)?;
        }
        if self.metadata_snapshot_interval_records == 0 {
            return Err(BrokerError::InvalidRuntimeConfig(
                "metadata_snapshot_interval_records must be positive".into(),
            ));
        }
        krabka_kraft_core::snapshot_fetch::MetadataSnapshotFetchMax::new(
            self.metadata_snapshot_fetch_max,
        )
        .map_err(|error| {
            BrokerError::InvalidRuntimeConfig(format!(
                "metadata_snapshot_fetch_max is invalid: {error}"
            ))
        })?;
        Ok(())
    }

    pub(super) fn validate_additional_runtime_scalars(&self) -> Result<(), BrokerError> {
        // The timeout is carried verbatim in `AddRaftVoter.timeout_ms`, an
        // `int32` millisecond wire field, so it has to survive that narrowing.
        let voter_request_timeout_ms = self.auto_join_voter_request_timeout.millis_i64();
        if !(1..=i64::from(i32::MAX)).contains(&voter_request_timeout_ms) {
            return Err(BrokerError::InvalidRuntimeConfig(
                "auto_join_voter_request_timeout must be within 1..=i32::MAX milliseconds".into(),
            ));
        }
        if self.offsets_topic_metadata_wait_timeout < millis(1) {
            return Err(BrokerError::InvalidRuntimeConfig(
                "offsets_topic_metadata_wait_timeout must be at least 1ms".into(),
            ));
        }
        require_positive_size("observer_fetch_max", self.observer_fetch_max)?;
        for (name, value) in [
            (
                "self_registration_max_attempts",
                self.self_registration_max_attempts,
            ),
            (
                "client_metrics_stale_push_intervals",
                self.client_metrics_stale_push_intervals,
            ),
        ] {
            if value == 0 {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} must be positive"
                )));
            }
        }
        for (name, value) in [
            (
                "client_metrics_otlp_queue_capacity",
                self.client_metrics_otlp_queue_capacity,
            ),
            (
                "audit_event_queue_capacity",
                self.audit_event_queue_capacity,
            ),
            (
                "coordinator_actor_mailbox_capacity",
                self.coordinator_actor_mailbox_capacity,
            ),
            (
                "diskless_wal_local_replica_count",
                self.diskless_wal_local_replica_count,
            ),
            (
                "unclean_recovery_queue_capacity",
                self.unclean_recovery_queue_capacity,
            ),
            (
                "share_session_cache_max_when_unlimited",
                self.share_session_cache_max_when_unlimited,
            ),
            ("barrier_max_groups", self.barrier_max_groups),
            (
                "barrier_max_topics_per_group",
                self.barrier_max_topics_per_group,
            ),
            ("max_produce_group", self.max_produce_group),
            (
                "partition_writer_queue_depth",
                self.partition_writer_queue_depth,
            ),
        ] {
            if value == 0 {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} must be positive"
                )));
            }
        }
        if self.diskless_wal_local_replica_count.is_multiple_of(2) {
            return Err(BrokerError::InvalidRuntimeConfig(
                "diskless_wal_local_replica_count must be odd".into(),
            ));
        }
        if self.diskless_wal_trim_safety_lag < 0 {
            return Err(BrokerError::InvalidRuntimeConfig(
                "diskless_wal_trim_safety_lag must be nonnegative".into(),
            ));
        }
        for (name, value) in [
            ("audit_tail_read_max", self.audit_tail_read_max),
            (
                "diskless_wal_flush_max_size",
                self.diskless_wal_flush_max_size,
            ),
            ("share_recovery_read_max", self.share_recovery_read_max),
            ("barrier_recovery_read_max", self.barrier_recovery_read_max),
            ("socket_request_max", self.socket_request_max),
            ("sendfile_min", self.sendfile_min),
            ("socket_send_buffer", self.socket_send_buffer),
            ("socket_receive_buffer", self.socket_receive_buffer),
            ("acl_max_principal", self.acl_max_principal),
            ("acl_max_resource_name", self.acl_max_resource_name),
            (
                "telemetry_decompressed_output_floor",
                self.telemetry_decompressed_output_floor,
            ),
            (
                "telemetry_decompressed_output_ceiling",
                self.telemetry_decompressed_output_ceiling,
            ),
            (
                "future_log_move_read_chunk",
                self.future_log_move_read_chunk,
            ),
            (
                "transaction_recovery_read_max",
                self.transaction_recovery_read_max,
            ),
        ] {
            require_positive_size(name, value)?;
        }
        if self.telemetry_max_decompression_ratio <= <Ratio as RatioExt>::ZERO {
            return Err(BrokerError::InvalidRuntimeConfig(
                "telemetry_max_decompression_ratio must be positive".into(),
            ));
        }
        require_positive_time("producer_id_expiration", self.producer_id_expiration)?;
        if self.audit_tail_window_offsets <= 0 {
            return Err(BrokerError::InvalidRuntimeConfig(
                "audit_tail_window_offsets must be positive".into(),
            ));
        }
        for (name, value) in [
            (
                "default_min_insync_replicas",
                self.default_min_insync_replicas,
            ),
            (
                "share_state_num_partitions",
                self.share_coordinator.state_topic_num_partitions,
            ),
            (
                "offsets_topic_num_partitions",
                self.offsets_topic_num_partitions,
            ),
            (
                "transaction_state_num_partitions",
                self.transaction_state_num_partitions,
            ),
            (
                "barrier_state_num_partitions",
                self.barrier_state_num_partitions,
            ),
            ("barrier_retained_cuts", self.barrier_retained_cuts),
        ] {
            if value <= 0 {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} must be positive"
                )));
            }
        }
        for (name, value) in [
            ("transaction_min_timeout", self.transaction_min_timeout),
            ("transaction_max_timeout", self.transaction_max_timeout),
            (
                "barrier_min_injection_interval",
                self.barrier_min_injection_interval,
            ),
            ("barrier_injection_timeout", self.barrier_injection_timeout),
        ] {
            require_positive_time(name, value)?;
        }
        for (name, value) in [
            (
                "share_state_replication_factor",
                self.share_coordinator.state_topic_replication_factor,
            ),
            (
                "offsets_topic_replication_factor",
                self.offsets_topic_replication_factor,
            ),
            (
                "transaction_state_replication_factor",
                self.transaction_state_replication_factor,
            ),
            (
                "streams_internal_topic_replication_factor",
                self.streams_group.internal_topic_replication_factor,
            ),
            (
                "barrier_state_replication_factor",
                self.barrier_state_replication_factor,
            ),
        ] {
            if value <= 0 {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} must be positive"
                )));
            }
        }
        Ok(())
    }
}

/// Rejects a non-positive time extent. The error names the config field.
fn require_positive_time(name: &str, value: Time) -> Result<(), BrokerError> {
    if value <= <Time as TimeExt>::ZERO {
        return Err(BrokerError::InvalidRuntimeConfig(format!(
            "{name} must be positive"
        )));
    }
    Ok(())
}

/// Rejects a non-positive byte count. The error names the config field.
fn require_positive_size(name: &str, value: ByteSize) -> Result<(), BrokerError> {
    if value <= <ByteSize as ByteSizeExt>::ZERO {
        return Err(BrokerError::InvalidRuntimeConfig(format!(
            "{name} must be positive"
        )));
    }
    Ok(())
}
