//! The `[runtime]` applier for broker-wide operational policy.
//!
//! `apply_broker_policy` covers the metadata snapshot cadence, the controlled
//! shutdown and TLS reload timers, the leader-imbalance thresholds, the
//! connection ceilings, and the delegation-token lifetimes — the knobs that
//! govern the broker as a whole rather than one subsystem.

use super::{
    FileConfigError, RuntimeFileConfig,
    validate::{
        metadata_snapshot_fetch_max, nonnegative_time, positive_time, positive_u64,
        unit_interval_ratio, whole_bytes_u64, whole_millis_i64_time,
    },
};

impl RuntimeFileConfig {
    pub(super) fn apply_broker_policy(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        // Zero is a valid `partition_disk_scan_interval`: it disables the
        // scanner, so this one is not routed through the positive-only macro.
        if let Some(value) = runtime.partition_disk_scan_interval {
            cfg.partition_disk_scan_interval =
                nonnegative_time("partition_disk_scan_interval", value)?;
        }
        set_runtime_plain!(runtime, observer_lag_bound, cfg.observer_lag_bound);
        set_runtime_time_millis!(runtime, heartbeat_interval, cfg.heartbeat_interval);
        set_runtime_time_millis!(runtime, heartbeat_timeout, cfg.heartbeat_timeout);
        set_runtime_time_millis!(runtime, replica_lag_time_max, cfg.replica_lag_time_max);
        set_runtime_time_millis!(
            runtime,
            controller_election_timeout,
            cfg.controller_election_timeout
        );
        set_runtime_time_millis!(
            runtime,
            controller_heartbeat_interval,
            cfg.controller_heartbeat_interval
        );
        if runtime.controller_heartbeat_interval.is_some() {
            cfg.controller_heartbeat_interval_explicit = true;
        }
        if let Some(value) = runtime.controller_fetch_miss_limit {
            cfg.controller_fetch_miss_limit = krabka_raft::ControllerFetchMissLimit::new(value)
                .map_err(FileConfigError::InvalidConfig)?;
        }
        if let Some(value) = runtime.metadata_raft_command_queue_capacity {
            cfg.metadata_raft_command_queue_capacity =
                krabka_raft::MetadataRaftCommandQueueCapacity::new(value)
                    .map_err(FileConfigError::InvalidConfig)?;
        }
        if let Some(value) = runtime.metadata_raft_fetch_max {
            cfg.metadata_raft_fetch_max = krabka_raft::MetadataRaftFetchMax::try_from(value)
                .map_err(FileConfigError::InvalidConfig)?;
        }
        if let Some(value) = runtime.controlled_shutdown_drain_timeout {
            positive_time("controlled_shutdown_drain_timeout", value)?;
        }
        set_runtime_size_bytes!(
            runtime,
            metadata_max_between_snapshots,
            cfg.metadata_max_bytes_between_snapshots,
            whole_bytes_u64
        );
        // Zero disables the time-based snapshot cap, so it bypasses the
        // positive-only macro.
        if let Some(value) = runtime.metadata_max_snapshot_interval {
            cfg.metadata_max_snapshot_interval =
                nonnegative_time("metadata_max_snapshot_interval", value)?;
        }
        set_runtime_positive_u64!(
            runtime,
            metadata_snapshot_interval_records,
            cfg.metadata_snapshot_interval_records
        );
        set_runtime_size_bytes!(
            runtime,
            metadata_snapshot_fetch_max,
            cfg.metadata_snapshot_fetch_max,
            metadata_snapshot_fetch_max
        );
        // Zero disables the reaper, so it bypasses the positive-only macro.
        if let Some(value) = runtime.txn_abort_cleanup_interval {
            cfg.txn_abort_cleanup_interval = nonnegative_time("txn_abort_cleanup_interval", value)?;
        }
        set_runtime_time_secs!(runtime, txn_id_expiration, cfg.txn_id_expiration);
        // Zero disables the expiry sweep, so it bypasses the positive-only macro.
        if let Some(value) = runtime.txn_id_expiration_cleanup_interval {
            cfg.txn_id_expiration_cleanup_interval =
                nonnegative_time("txn_id_expiration_cleanup_interval", value)?;
        }
        set_runtime_time_secs!(
            runtime,
            leader_imbalance_check_interval,
            cfg.leader_imbalance_check_interval
        );
        if let Some(value) = runtime.leader_imbalance_per_broker {
            cfg.leader_imbalance_per_broker =
                unit_interval_ratio("leader_imbalance_per_broker", value)?;
        }
        // Zero disables the periodic TLS watcher, so it bypasses the
        // positive-only macro.
        if let Some(value) = runtime.tls_reload_interval {
            cfg.tls_reload_interval = nonnegative_time("tls_reload_interval", value)?;
        }
        set_runtime_plain!(
            runtime,
            max_incremental_fetch_session_cache_slots,
            cfg.max_incremental_fetch_session_cache_slots
        );
        set_runtime_plain!(runtime, max_connections, cfg.max_connections);
        set_runtime_plain!(runtime, max_connections_per_ip, cfg.max_connections_per_ip);
        set_runtime_time_millis!(
            runtime,
            delegation_token_max_lifetime,
            cfg.delegation_token_max_lifetime,
            positive_i64
        );
        set_runtime_time_millis!(
            runtime,
            delegation_token_expiry_check_interval,
            cfg.delegation_token_expiry_check_interval,
            positive_i64
        );
        set_runtime_time_millis!(
            runtime,
            delegation_token_default_renew_period,
            cfg.delegation_token_default_renew_period,
            positive_i64
        );
        set_runtime_time_millis!(
            runtime,
            remote_log_manager_interval,
            cfg.remote_log_manager_interval
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::convert::RatioExt as _;

    use crate::file_config::FileConfig;

    /// Kafka's `leader.imbalance.per.broker.percentage` lands as a [`Ratio`].
    #[test]
    fn leader_imbalance_percentage_lands_as_a_ratio() {
        for (raw, want) in [("0%", 0.0), ("10%", 0.10), ("55%", 0.55), ("100%", 1.0)] {
            let file: FileConfig = toml::from_str(&format!(
                "[runtime]\nleader_imbalance_per_broker = \"{raw}\"\n"
            ))
            .expect("parse runtime config");
            let mut cfg = crate::config::BrokerConfig::default();

            file.apply_to(&mut cfg).expect("apply runtime config");

            assert!(
                (cfg.leader_imbalance_per_broker.as_f64() - want).abs() < 1e-12,
                "{raw} should be {want}"
            );
        }
    }
}
