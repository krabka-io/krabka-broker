//! The overlay of the command line onto a `BrokerConfig`.
//!
//! The flags first become a `RuntimeFileConfig`, which carries the same
//! precedence rules as the operator's TOML file, and that config then applies
//! to the `BrokerConfig` the broker starts from.

use krabka_broker::{BrokerConfig, config::DEFAULT_CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT};
use krabka_client_core::{ClientFrameMax, ConnectionDispatchQueueCapacity};
use krabka_units::Time;

use crate::{cli::Args, runtime_args::RuntimeArgs};

impl RuntimeArgs {
    fn copy_core(&self, runtime: &mut krabka_broker::file_config::RuntimeFileConfig) {
        copy_plain_runtime!(
            self,
            runtime,
            startup_leader_wait_timeout,
            self_registration_backoff_min,
            self_registration_backoff_max,
            observer_poll_interval,
            audit_spool_replay_interval,
            audit_stats_poll_interval,
            audit_partition_wait_timeout,
            liveness_tick_interval,
            gauge_poll_interval,
            isr_scan_interval,
            cleaner_interval,
            future_log_move_retry_backoff,
            rlmm_reconcile_tick,
            rlmm_bootstrap_backoff_initial,
            rlmm_bootstrap_backoff_max,
            connection_creation_throttle_max,
            opa_http_timeout,
            oauth_jwks_http_timeout,
            auto_join_retry_backoff,
            auto_join_voter_request_timeout,
            self_registration_max_attempts,
            observer_fetch_max,
        );
    }

    fn copy_client_metrics(&self, runtime: &mut krabka_broker::file_config::RuntimeFileConfig) {
        copy_plain_runtime!(
            self,
            runtime,
            client_metrics_eviction_tick,
            client_metrics_stale_floor,
            client_metrics_default_interval,
            client_metrics_telemetry_max,
            client_metrics_prom_snapshot_ttl,
            client_metrics_stale_push_intervals,
        );
        copy_refined_runtime!(self, runtime, client_metrics_otlp_queue_capacity,);
    }

    fn copy_replication(&self, runtime: &mut krabka_broker::file_config::RuntimeFileConfig) {
        copy_plain_runtime!(
            self,
            runtime,
            replication_fetch_max_wait,
            replication_fetch_max,
            replication_fetch_min,
            replication_throttle_exhausted_backoff,
            replication_send_error_backoff,
            replication_unknown_topic_retry_delay,
            replication_epoch_fence_backoff,
            replication_unexpected_error_backoff,
            replication_reconnect_initial_delay,
            replication_reconnect_delay_cap,
        );
    }

    fn copy_coordinators(&self, runtime: &mut krabka_broker::file_config::RuntimeFileConfig) {
        copy_plain_runtime!(
            self,
            runtime,
            coordinator_session_expiry_tick,
            coordinator_shutdown_ack_timeout,
            consumer_group_session_timeout,
            consumer_group_heartbeat_interval,
            consumer_group_min_session_timeout,
            consumer_group_max_session_timeout,
            consumer_group_min_heartbeat_interval,
            consumer_group_max_heartbeat_interval,
            classic_group_initial_rebalance_delay,
            sync_group_follower_wait,
            share_recovery_read_max,
            diskless_wal_flush_interval,
            diskless_wal_flush_max_size,
            diskless_wal_hot_tail_max_size,
            diskless_wal_trim_safety_lag,
            diskless_wal_index_projection_timeout,
        );
        copy_refined_runtime!(
            self,
            runtime,
            consumer_group_max_size,
            coordinator_actor_mailbox_capacity,
            diskless_wal_local_replica_count,
            share_session_cache_max_when_unlimited,
            share_state_num_partitions,
            share_state_replication_factor,
        );
    }

    fn copy_storage_and_queues(&self, runtime: &mut krabka_broker::file_config::RuntimeFileConfig) {
        copy_plain_runtime!(
            self,
            runtime,
            unclean_recovery_aggressive_deadline,
            unclean_recovery_balanced_deadline,
            operator_recovery_deadline,
            quota_throttle_max,
            quota_window,
            controller_mutation_quota_window,
            offsets_topic_metadata_wait_timeout,
            producer_id_expiration,
            producer_id_expiration_scan_interval,
            transaction_min_timeout,
            transaction_max_timeout,
            offsets_retention,
            offsets_retention_check_interval,
            audit_tail_read_max,
            future_log_move_read_chunk,
            transaction_recovery_read_max,
        );
        copy_refined_runtime!(
            self,
            runtime,
            audit_event_queue_capacity,
            audit_tail_window_offsets,
            unclean_recovery_queue_capacity,
            max_produce_group,
            partition_writer_queue_depth,
            default_min_insync_replicas,
            offsets_topic_num_partitions,
            offsets_topic_replication_factor,
            transaction_state_num_partitions,
            transaction_state_replication_factor,
        );
    }

    fn copy_network_and_limits(&self, runtime: &mut krabka_broker::file_config::RuntimeFileConfig) {
        copy_plain_runtime!(
            self,
            runtime,
            socket_request_max,
            queued_max_requests,
            queued_max_request_bytes,
            sendfile_min,
            socket_send_buffer,
            socket_receive_buffer,
            log_read_buffer_cap,
            log_timestamp_scan_window,
            log_delivery_clock_uncertainty,
            message_max_bytes,
            acl_max_principal,
            acl_max_resource_name,
            telemetry_max_decompression_ratio,
            telemetry_decompressed_output_floor,
            telemetry_decompressed_output_ceiling,
            record_decompression_max_ratio,
            record_decompression_output_floor,
            record_decompression_output_ceiling,
        );
        runtime
            .inter_broker_server_name
            .clone_from(&self.inter_broker_server_name);
    }

    fn copy_group_protocols(&self, runtime: &mut krabka_broker::file_config::RuntimeFileConfig) {
        copy_plain_runtime!(
            self,
            runtime,
            share_group_session_timeout,
            share_group_heartbeat_interval,
            share_group_record_lock_duration,
            streams_group_session_timeout,
            streams_group_heartbeat_interval,
            streams_group_task_offset_interval,
        );
        copy_refined_runtime!(
            self,
            runtime,
            share_group_max_inflight_records,
            share_group_max_size,
            streams_group_max_size,
            streams_internal_topic_replication_factor,
        );
        copy_plain_runtime!(
            self,
            runtime,
            share_group_enable,
            share_group_max_delivery_attempts,
            streams_group_enable,
            streams_group_num_standby_replicas,
            streams_group_num_warmup_replicas,
            streams_group_acceptable_recovery_lag,
        );
        runtime.share_group_isolation_level = self.share_group_isolation_level.map(|value| {
            use krabka_broker::coordinator::unified::share::config::ShareIsolationLevel;
            match value {
                ShareIsolationLevel::ReadUncommitted => "read-uncommitted",
                ShareIsolationLevel::ReadCommitted => "read-committed",
            }
            .to_owned()
        });
        runtime.streams_group_assignor = self.streams_group_assignor.map(|value| {
            use krabka_broker::coordinator::unified::streams::config::StreamsAssignorKind;
            match value {
                StreamsAssignorKind::Auto => "auto",
                StreamsAssignorKind::Sticky => "sticky",
                StreamsAssignorKind::HighlyAvailable => "highly-available",
            }
            .to_owned()
        });
    }

    fn as_file_runtime(&self) -> krabka_broker::file_config::RuntimeFileConfig {
        let mut runtime = krabka_broker::file_config::RuntimeFileConfig::default();
        self.copy_core(&mut runtime);
        self.copy_client_metrics(&mut runtime);
        self.copy_replication(&mut runtime);
        self.copy_coordinators(&mut runtime);
        self.copy_storage_and_queues(&mut runtime);
        self.copy_network_and_limits(&mut runtime);
        self.copy_group_protocols(&mut runtime);
        runtime
    }
}

impl Args {
    fn runtime_overlay(&self) -> krabka_broker::file_config::RuntimeFileConfig {
        let mut runtime = self.runtime.as_file_runtime();
        copy_plain_runtime!(
            self,
            runtime,
            partition_disk_scan_interval,
            observer_lag_bound,
            metadata_max_bytes_between_snapshots,
            metadata_max_snapshot_interval,
            metadata_snapshot_interval_records,
            metadata_snapshot_fetch_max,
            txn_abort_cleanup_interval,
            txn_id_expiration,
            txn_id_expiration_cleanup_interval,
            leader_imbalance_check_interval,
            leader_imbalance_per_broker,
            tls_reload_interval,
            heartbeat_interval,
            heartbeat_timeout,
            replica_lag_time_max,
            controller_election_timeout,
            controller_heartbeat_interval,
            controller_fetch_miss_limit,
            metadata_raft_command_queue_capacity,
            metadata_raft_fetch_max,
            controlled_shutdown_drain_timeout,
            delegation_token_max_lifetime,
            delegation_token_expiry_check_interval,
            delegation_token_default_renew_period,
            remote_log_manager_interval,
            max_incremental_fetch_session_cache_slots,
            max_connections,
            max_connections_per_ip,
        );
        runtime
    }

    pub fn apply_runtime_to(
        &self,
        cfg: &mut BrokerConfig,
        file_shutdown: Option<Time>,
    ) -> Result<Time, String> {
        let runtime = self.runtime_overlay();
        let cli_shutdown = runtime.controlled_shutdown_drain_timeout;
        runtime.apply_to(cfg).map_err(|error| error.to_string())?;
        cfg.client_dispatch_queue_capacity =
            ConnectionDispatchQueueCapacity::new(self.runtime.client_dispatch_queue_capacity)
                .expect("validated by clap");
        cfg.client_frame_max =
            ClientFrameMax::try_from(self.runtime.client_frame_max).expect("validated by clap");
        cfg.validate().map_err(|error| error.to_string())?;
        Ok(cli_shutdown
            .or(file_shutdown)
            .unwrap_or(DEFAULT_CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT))
    }
}

// A `#[path]`-loaded module resolves its own children as siblings of itself, so
// the test module names its file.
#[cfg(test)]
#[path = "runtime_overlay/tests.rs"]
mod tests;
