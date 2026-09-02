//! The `[runtime]` TOML table and the entry point that applies it.
//!
//! [`RuntimeFileConfig`] mirrors every operational knob the broker reads from
//! `[runtime]`. Its [`apply_to`][RuntimeFileConfig::apply_to] method dispatches
//! to the per-domain appliers in the sibling `runtime_*` modules, which hold
//! the assignments themselves.

use krabka_units::{ByteSize, Ratio, Time};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::FileConfigError;

/// Validated operational policy loaded from `[runtime]`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeFileConfig {
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub startup_leader_wait_timeout: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub self_registration_backoff_min: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub self_registration_backoff_max: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub observer_poll_interval: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub audit_spool_replay_interval: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub audit_stats_poll_interval: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub audit_partition_wait_timeout: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub liveness_tick_interval: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub gauge_poll_interval: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub isr_scan_interval: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub cleaner_interval: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub future_log_move_retry_backoff: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub client_metrics_eviction_tick: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub client_metrics_stale_floor: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub client_metrics_default_interval: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub client_metrics_telemetry_max: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub client_metrics_prom_snapshot_ttl: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub rlmm_reconcile_tick: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub rlmm_bootstrap_backoff_initial: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub rlmm_bootstrap_backoff_max: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub connection_creation_throttle_max: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub opa_http_timeout: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub schema_registry_http_timeout: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub oauth_jwks_http_timeout: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub auto_join_retry_backoff: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub auto_join_voter_request_timeout: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub replication_fetch_max: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub replication_fetch_max_wait: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub replication_fetch_min: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub replication_throttle_exhausted_backoff: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub replication_send_error_backoff: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub replication_unknown_topic_retry_delay: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub replication_epoch_fence_backoff: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub replication_unexpected_error_backoff: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub replication_reconnect_initial_delay: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub replication_reconnect_delay_cap: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub coordinator_session_expiry_tick: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub coordinator_shutdown_ack_timeout: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub consumer_group_session_timeout: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub consumer_group_heartbeat_interval: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub consumer_group_min_session_timeout: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub consumer_group_max_session_timeout: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub consumer_group_min_heartbeat_interval: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub consumer_group_max_heartbeat_interval: Option<Time>,
    pub consumer_group_max_size: Option<usize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub classic_group_initial_rebalance_delay: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub sync_group_follower_wait: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub unclean_recovery_aggressive_deadline: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub unclean_recovery_balanced_deadline: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub operator_recovery_deadline: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub quota_throttle_max: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub controller_mutation_quota_window: Option<Time>,
    pub self_registration_max_attempts: Option<u32>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub observer_fetch_max: Option<ByteSize>,
    pub audit_event_queue_capacity: Option<usize>,
    pub audit_tail_window_offsets: Option<i64>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub audit_tail_read_max: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub offsets_topic_metadata_wait_timeout: Option<Time>,
    pub client_metrics_stale_push_intervals: Option<u32>,
    pub client_metrics_otlp_queue_capacity: Option<usize>,
    pub coordinator_actor_mailbox_capacity: Option<usize>,
    pub diskless_wal_local_replica_count: Option<usize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub diskless_wal_flush_interval: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub diskless_wal_flush_max_size: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub diskless_wal_hot_tail_max_size: Option<ByteSize>,
    pub diskless_wal_trim_safety_lag: Option<i64>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub diskless_wal_index_projection_timeout: Option<Time>,
    pub unclean_recovery_queue_capacity: Option<usize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub share_recovery_read_max: Option<ByteSize>,
    pub share_session_cache_max_when_unlimited: Option<usize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub log_read_buffer_cap: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub log_timestamp_scan_window: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub log_segment_bytes: Option<ByteSize>,
    /// Kafka's broker-wide `message.max.bytes`: the largest record batch a
    /// topic that sets no `max.message.bytes` accepts.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub message_max_bytes: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub log_delivery_clock_uncertainty: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub socket_request_max: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub sendfile_min: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub socket_send_buffer: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub socket_receive_buffer: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub acl_max_principal: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub acl_max_resource_name: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_ratio")]
    #[schemars(with = "Option<crate::file_config::schema_units::Ratio>")]
    pub telemetry_max_decompression_ratio: Option<krabka_units::Ratio>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub telemetry_decompressed_output_floor: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub telemetry_decompressed_output_ceiling: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_ratio")]
    #[schemars(with = "Option<crate::file_config::schema_units::Ratio>")]
    pub record_decompression_max_ratio: Option<Ratio>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub record_decompression_output_floor: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub record_decompression_output_ceiling: Option<ByteSize>,
    pub inter_broker_server_name: Option<String>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub producer_id_expiration: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub producer_id_expiration_scan_interval: Option<Time>,
    pub max_produce_group: Option<usize>,
    pub partition_writer_queue_depth: Option<usize>,
    pub default_min_insync_replicas: Option<i32>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub future_log_move_read_chunk: Option<ByteSize>,
    pub share_state_num_partitions: Option<i32>,
    pub share_state_replication_factor: Option<i16>,
    pub offsets_topic_num_partitions: Option<i32>,
    pub offsets_topic_replication_factor: Option<i16>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub offsets_retention: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub offsets_retention_check_interval: Option<Time>,
    pub transaction_state_num_partitions: Option<i32>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub transaction_recovery_read_max: Option<ByteSize>,
    pub transaction_state_replication_factor: Option<i16>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub transaction_min_timeout: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub transaction_max_timeout: Option<Time>,
    pub barrier_state_num_partitions: Option<i32>,
    pub barrier_state_replication_factor: Option<i16>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub barrier_min_injection_interval: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub barrier_injection_timeout: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub barrier_recovery_read_max: Option<ByteSize>,
    pub barrier_retained_cuts: Option<i32>,
    pub barrier_max_groups: Option<usize>,
    pub barrier_max_topics_per_group: Option<usize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub partition_disk_scan_interval: Option<Time>,
    pub observer_lag_bound: Option<u64>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub heartbeat_interval: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub heartbeat_timeout: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub replica_lag_time_max: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub controller_election_timeout: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub controller_heartbeat_interval: Option<Time>,
    pub controller_fetch_miss_limit: Option<u32>,
    pub metadata_raft_command_queue_capacity: Option<usize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub metadata_raft_fetch_max: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub controlled_shutdown_drain_timeout: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub metadata_max_between_snapshots: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub metadata_max_snapshot_interval: Option<Time>,
    pub metadata_snapshot_interval_records: Option<u64>,
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub metadata_snapshot_fetch_max: Option<ByteSize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub txn_abort_cleanup_interval: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub txn_id_expiration: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub txn_id_expiration_cleanup_interval: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub leader_imbalance_check_interval: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_ratio")]
    #[schemars(with = "Option<crate::file_config::schema_units::Ratio>")]
    pub leader_imbalance_per_broker: Option<krabka_units::Ratio>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub tls_reload_interval: Option<Time>,
    pub max_incremental_fetch_session_cache_slots: Option<usize>,
    pub max_connections: Option<usize>,
    pub max_connections_per_ip: Option<usize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub delegation_token_max_lifetime: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub delegation_token_expiry_check_interval: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub delegation_token_default_renew_period: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub remote_log_manager_interval: Option<Time>,

    pub share_group_enable: Option<bool>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub share_group_session_timeout: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub share_group_heartbeat_interval: Option<Time>,
    pub share_group_max_size: Option<usize>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub share_group_record_lock_duration: Option<Time>,
    pub share_group_max_delivery_attempts: Option<i16>,
    pub share_group_max_inflight_records: Option<i32>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub share_group_backlog_poll_interval: Option<Time>,
    pub share_group_isolation_level: Option<String>,
    pub streams_group_enable: Option<bool>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub streams_group_session_timeout: Option<Time>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub streams_group_heartbeat_interval: Option<Time>,
    pub streams_group_max_size: Option<usize>,
    pub streams_internal_topic_replication_factor: Option<i16>,
    pub streams_group_num_standby_replicas: Option<i32>,
    pub streams_group_num_warmup_replicas: Option<i32>,
    pub streams_group_acceptable_recovery_lag: Option<i64>,
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub streams_group_task_offset_interval: Option<Time>,
    pub streams_group_assignor: Option<String>,
}

impl RuntimeFileConfig {
    /// Apply every present runtime value, validating scalar boundaries.
    ///
    /// # Errors
    ///
    /// Returns [`FileConfigError::InvalidConfig`] for an invalid value.
    pub fn apply_to(
        mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        self.apply_core(cfg)?;
        self.apply_replication(cfg)?;
        self.apply_coordinators(cfg)?;
        self.apply_recovery_and_queues(cfg)?;
        self.apply_network_limits(cfg)?;
        self.apply_transactions(cfg)?;
        self.apply_barrier(cfg)?;
        self.apply_broker_policy(cfg)?;
        self.apply_share_group(cfg)?;
        self.apply_streams_group(cfg)
    }
}

#[cfg(test)]
mod tests;
