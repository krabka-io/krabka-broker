//! The broker's runtime-policy flags, as one flattened clap argument group.
//!
//! Every flag here overlays a field of `RuntimeFileConfig`, and the group is
//! large enough to keep apart from the rest of the command line.

use krabka_broker::config_value::{
    PositiveCount, PositiveI16, PositiveI32, PositiveI64, parse_positive_count, parse_positive_i16,
    parse_positive_i32, parse_positive_i64,
};
use krabka_client_core::{
    ClientFrameMax, ConnectionDispatchQueueCapacity, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
};
use krabka_units::{ByteSize, Ratio, Time};

fn parse_share_isolation(
    value: &str,
) -> Result<krabka_broker::coordinator::unified::share::config::ShareIsolationLevel, String> {
    use krabka_broker::coordinator::unified::share::config::ShareIsolationLevel;
    match value {
        "read-uncommitted" => Ok(ShareIsolationLevel::ReadUncommitted),
        "read-committed" => Ok(ShareIsolationLevel::ReadCommitted),
        _ => Err("expected `read-uncommitted` or `read-committed`".into()),
    }
}

fn parse_streams_assignor(
    value: &str,
) -> Result<krabka_broker::coordinator::unified::streams::config::StreamsAssignorKind, String> {
    use krabka_broker::coordinator::unified::streams::config::StreamsAssignorKind;
    match value {
        "auto" => Ok(StreamsAssignorKind::Auto),
        "sticky" => Ok(StreamsAssignorKind::Sticky),
        "highly-available" => Ok(StreamsAssignorKind::HighlyAvailable),
        _ => Err("expected `auto`, `sticky`, or `highly-available`".into()),
    }
}

#[derive(Debug, clap::Args)]
pub struct RuntimeArgs {
    #[arg(
        long,
        env = "KRABKA_BROKER_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    pub client_dispatch_queue_capacity: usize,
    #[arg(
        long,
        env = "KRABKA_BROKER_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_client_frame_max
    )]
    pub client_frame_max: ByteSize,
    #[arg(long, env = "KRABKA_STARTUP_LEADER_WAIT_TIMEOUT", value_parser = krabka_units::parse::positive_time)]
    pub startup_leader_wait_timeout: Option<Time>,
    #[arg(long, env = "KRABKA_SELF_REGISTRATION_BACKOFF_MIN", value_parser = krabka_units::parse::positive_time)]
    pub self_registration_backoff_min: Option<Time>,
    #[arg(long, env = "KRABKA_SELF_REGISTRATION_BACKOFF_MAX", value_parser = krabka_units::parse::positive_time)]
    pub self_registration_backoff_max: Option<Time>,
    #[arg(long, env = "KRABKA_OBSERVER_POLL_INTERVAL", value_parser = krabka_units::parse::positive_time)]
    pub observer_poll_interval: Option<Time>,
    #[arg(long, env = "KRABKA_AUDIT_SPOOL_REPLAY_INTERVAL", value_parser = krabka_units::parse::positive_time)]
    pub audit_spool_replay_interval: Option<Time>,
    #[arg(long, env = "KRABKA_AUDIT_STATS_POLL_INTERVAL", value_parser = krabka_units::parse::positive_time)]
    pub audit_stats_poll_interval: Option<Time>,
    #[arg(long, env = "KRABKA_AUDIT_PARTITION_WAIT_TIMEOUT", value_parser = krabka_units::parse::positive_time)]
    pub audit_partition_wait_timeout: Option<Time>,
    #[arg(long, env = "KRABKA_LIVENESS_TICK_INTERVAL", value_parser = krabka_units::parse::positive_time)]
    pub liveness_tick_interval: Option<Time>,
    #[arg(long, env = "KRABKA_GAUGE_POLL_INTERVAL", value_parser = krabka_units::parse::positive_time)]
    pub gauge_poll_interval: Option<Time>,
    #[arg(long, env = "KRABKA_ISR_SCAN_INTERVAL", value_parser = krabka_units::parse::positive_time)]
    pub isr_scan_interval: Option<Time>,
    #[arg(long, env = "KRABKA_CLEANER_INTERVAL", value_parser = krabka_units::parse::positive_time)]
    pub cleaner_interval: Option<Time>,
    #[arg(long, env = "KRABKA_FUTURE_LOG_MOVE_RETRY_BACKOFF", value_parser = krabka_units::parse::positive_time)]
    pub future_log_move_retry_backoff: Option<Time>,
    #[arg(long, env = "KRABKA_CLIENT_METRICS_EVICTION_TICK", value_parser = krabka_units::parse::positive_time)]
    pub client_metrics_eviction_tick: Option<Time>,
    #[arg(long, env = "KRABKA_CLIENT_METRICS_STALE_FLOOR", value_parser = krabka_units::parse::positive_time)]
    pub client_metrics_stale_floor: Option<Time>,
    #[arg(long, env = "KRABKA_CLIENT_METRICS_DEFAULT_INTERVAL", value_parser = krabka_units::parse::positive_time)]
    pub client_metrics_default_interval: Option<Time>,
    #[arg(long, env = "KRABKA_CLIENT_METRICS_TELEMETRY_MAX", value_parser = krabka_units::parse::positive_byte_size)]
    pub client_metrics_telemetry_max: Option<ByteSize>,
    #[arg(long, env = "KRABKA_CLIENT_METRICS_PROM_SNAPSHOT_TTL", value_parser = krabka_units::parse::positive_time)]
    pub client_metrics_prom_snapshot_ttl: Option<Time>,
    #[arg(long, env = "KRABKA_RLMM_RECONCILE_TICK", value_parser = krabka_units::parse::positive_time)]
    pub rlmm_reconcile_tick: Option<Time>,
    #[arg(long, env = "KRABKA_RLMM_BOOTSTRAP_BACKOFF_INITIAL", value_parser = krabka_units::parse::positive_time)]
    pub rlmm_bootstrap_backoff_initial: Option<Time>,
    #[arg(long, env = "KRABKA_RLMM_BOOTSTRAP_BACKOFF_MAX", value_parser = krabka_units::parse::positive_time)]
    pub rlmm_bootstrap_backoff_max: Option<Time>,
    #[arg(long, env = "KRABKA_CONNECTION_CREATION_THROTTLE_MAX", value_parser = krabka_units::parse::positive_time)]
    pub connection_creation_throttle_max: Option<Time>,
    #[arg(long, env = "KRABKA_OPA_HTTP_TIMEOUT", value_parser = krabka_units::parse::positive_time)]
    pub opa_http_timeout: Option<Time>,
    #[arg(long, env = "KRABKA_OAUTH_JWKS_HTTP_TIMEOUT", value_parser = krabka_units::parse::positive_time)]
    pub oauth_jwks_http_timeout: Option<Time>,
    #[arg(long, env = "KRABKA_AUTO_JOIN_RETRY_BACKOFF", value_parser = krabka_units::parse::positive_time)]
    pub auto_join_retry_backoff: Option<Time>,
    #[arg(long, env = "KRABKA_AUTO_JOIN_VOTER_REQUEST_TIMEOUT", value_parser = krabka_units::parse::positive_time)]
    pub auto_join_voter_request_timeout: Option<Time>,
    #[arg(long, env = "KRABKA_REPLICATION_FETCH_MAX", value_parser = krabka_units::parse::positive_byte_size)]
    pub replication_fetch_max: Option<ByteSize>,
    #[arg(long, env = "KRABKA_REPLICATION_FETCH_MAX_WAIT", value_parser = krabka_units::parse::positive_time)]
    pub replication_fetch_max_wait: Option<Time>,
    #[arg(long, env = "KRABKA_REPLICATION_FETCH_MIN", value_parser = krabka_units::parse::positive_byte_size)]
    pub replication_fetch_min: Option<ByteSize>,
    #[arg(long, env = "KRABKA_REPLICATION_THROTTLE_EXHAUSTED_BACKOFF", value_parser = krabka_units::parse::positive_time)]
    pub replication_throttle_exhausted_backoff: Option<Time>,
    #[arg(long, env = "KRABKA_REPLICATION_SEND_ERROR_BACKOFF", value_parser = krabka_units::parse::positive_time)]
    pub replication_send_error_backoff: Option<Time>,
    #[arg(long, env = "KRABKA_REPLICATION_UNKNOWN_TOPIC_RETRY_DELAY", value_parser = krabka_units::parse::positive_time)]
    pub replication_unknown_topic_retry_delay: Option<Time>,
    #[arg(long, env = "KRABKA_REPLICATION_EPOCH_FENCE_BACKOFF", value_parser = krabka_units::parse::positive_time)]
    pub replication_epoch_fence_backoff: Option<Time>,
    #[arg(long, env = "KRABKA_REPLICATION_UNEXPECTED_ERROR_BACKOFF", value_parser = krabka_units::parse::positive_time)]
    pub replication_unexpected_error_backoff: Option<Time>,
    #[arg(long, env = "KRABKA_REPLICATION_RECONNECT_INITIAL_DELAY", value_parser = krabka_units::parse::positive_time)]
    pub replication_reconnect_initial_delay: Option<Time>,
    #[arg(long, env = "KRABKA_REPLICATION_RECONNECT_DELAY_CAP", value_parser = krabka_units::parse::positive_time)]
    pub replication_reconnect_delay_cap: Option<Time>,
    #[arg(long, env = "KRABKA_COORDINATOR_SESSION_EXPIRY_TICK", value_parser = krabka_units::parse::positive_time)]
    pub coordinator_session_expiry_tick: Option<Time>,
    #[arg(long, env = "KRABKA_COORDINATOR_SHUTDOWN_ACK_TIMEOUT", value_parser = krabka_units::parse::positive_time)]
    pub coordinator_shutdown_ack_timeout: Option<Time>,
    #[arg(long, env = "KRABKA_CONSUMER_GROUP_SESSION_TIMEOUT", value_parser = krabka_units::parse::positive_time)]
    pub consumer_group_session_timeout: Option<Time>,
    #[arg(long, env = "KRABKA_CONSUMER_GROUP_HEARTBEAT_INTERVAL", value_parser = krabka_units::parse::positive_time)]
    pub consumer_group_heartbeat_interval: Option<Time>,
    #[arg(long, env = "KRABKA_CONSUMER_GROUP_MIN_SESSION_TIMEOUT", value_parser = krabka_units::parse::positive_time)]
    pub consumer_group_min_session_timeout: Option<Time>,
    #[arg(long, env = "KRABKA_CONSUMER_GROUP_MAX_SESSION_TIMEOUT", value_parser = krabka_units::parse::positive_time)]
    pub consumer_group_max_session_timeout: Option<Time>,
    #[arg(long, env = "KRABKA_CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL", value_parser = krabka_units::parse::positive_time)]
    pub consumer_group_min_heartbeat_interval: Option<Time>,
    #[arg(long, env = "KRABKA_CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL", value_parser = krabka_units::parse::positive_time)]
    pub consumer_group_max_heartbeat_interval: Option<Time>,
    #[arg(long, env = "KRABKA_CONSUMER_GROUP_MAX_SIZE", value_parser = parse_positive_count)]
    pub consumer_group_max_size: Option<PositiveCount>,
    #[arg(long, env = "KRABKA_CLASSIC_GROUP_INITIAL_REBALANCE_DELAY", value_parser = krabka_units::parse::positive_time)]
    pub classic_group_initial_rebalance_delay: Option<Time>,
    #[arg(long, env = "KRABKA_SYNC_GROUP_FOLLOWER_WAIT", value_parser = krabka_units::parse::positive_time)]
    pub sync_group_follower_wait: Option<Time>,
    #[arg(long, env = "KRABKA_UNCLEAN_RECOVERY_AGGRESSIVE_DEADLINE", value_parser = krabka_units::parse::positive_time)]
    pub unclean_recovery_aggressive_deadline: Option<Time>,
    #[arg(long, env = "KRABKA_UNCLEAN_RECOVERY_BALANCED_DEADLINE", value_parser = krabka_units::parse::positive_time)]
    pub unclean_recovery_balanced_deadline: Option<Time>,
    #[arg(long, env = "KRABKA_OPERATOR_RECOVERY_DEADLINE", value_parser = krabka_units::parse::positive_time)]
    pub operator_recovery_deadline: Option<Time>,
    #[arg(long, env = "KRABKA_QUOTA_THROTTLE_MAX", value_parser = krabka_units::parse::positive_time)]
    pub quota_throttle_max: Option<Time>,
    #[arg(long, env = "KRABKA_CONTROLLER_MUTATION_QUOTA_WINDOW", value_parser = krabka_units::parse::positive_time)]
    pub controller_mutation_quota_window: Option<Time>,
    #[arg(long, env = "KRABKA_SELF_REGISTRATION_MAX_ATTEMPTS", value_parser = clap::value_parser!(u32).range(1..))]
    pub self_registration_max_attempts: Option<u32>,
    #[arg(long, env = "KRABKA_OBSERVER_FETCH_MAX", value_parser = krabka_units::parse::positive_byte_size)]
    pub observer_fetch_max: Option<ByteSize>,
    #[arg(long, env = "KRABKA_AUDIT_EVENT_QUEUE_CAPACITY", value_parser = parse_positive_count)]
    pub audit_event_queue_capacity: Option<PositiveCount>,
    #[arg(long, env = "KRABKA_AUDIT_TAIL_WINDOW_OFFSETS", value_parser = parse_positive_i64)]
    pub audit_tail_window_offsets: Option<PositiveI64>,
    #[arg(long, env = "KRABKA_AUDIT_TAIL_READ_MAX", value_parser = krabka_units::parse::positive_byte_size)]
    pub audit_tail_read_max: Option<ByteSize>,
    #[arg(long, env = "KRABKA_OFFSETS_TOPIC_METADATA_WAIT_TIMEOUT", value_parser = krabka_units::parse::positive_time)]
    pub offsets_topic_metadata_wait_timeout: Option<Time>,
    #[arg(long, env = "KRABKA_CLIENT_METRICS_STALE_PUSH_INTERVALS", value_parser = clap::value_parser!(u32).range(1..))]
    pub client_metrics_stale_push_intervals: Option<u32>,
    #[arg(long, env = "KRABKA_CLIENT_METRICS_OTLP_QUEUE_CAPACITY", value_parser = parse_positive_count)]
    pub client_metrics_otlp_queue_capacity: Option<PositiveCount>,
    #[arg(long, env = "KRABKA_COORDINATOR_ACTOR_MAILBOX_CAPACITY", value_parser = parse_positive_count)]
    pub coordinator_actor_mailbox_capacity: Option<PositiveCount>,
    #[arg(long, env = "KRABKA_DISKLESS_WAL_LOCAL_REPLICA_COUNT", value_parser = parse_positive_count)]
    pub diskless_wal_local_replica_count: Option<PositiveCount>,
    #[arg(long, env = "KRABKA_DISKLESS_WAL_FLUSH_INTERVAL", value_parser = krabka_units::parse::positive_time)]
    pub diskless_wal_flush_interval: Option<Time>,
    #[arg(long, env = "KRABKA_DISKLESS_WAL_FLUSH_MAX_SIZE", value_parser = krabka_units::parse::positive_byte_size)]
    pub diskless_wal_flush_max_size: Option<ByteSize>,
    #[arg(long, env = "KRABKA_DISKLESS_WAL_HOT_TAIL_MAX_SIZE", value_parser = krabka_units::parse::positive_byte_size)]
    pub diskless_wal_hot_tail_max_size: Option<ByteSize>,
    #[arg(long, env = "KRABKA_DISKLESS_WAL_TRIM_SAFETY_LAG", value_parser = clap::value_parser!(i64).range(0..))]
    pub diskless_wal_trim_safety_lag: Option<i64>,
    #[arg(long, env = "KRABKA_DISKLESS_WAL_INDEX_PROJECTION_TIMEOUT", value_parser = krabka_units::parse::positive_time)]
    pub diskless_wal_index_projection_timeout: Option<Time>,
    #[arg(long, env = "KRABKA_UNCLEAN_RECOVERY_QUEUE_CAPACITY", value_parser = parse_positive_count)]
    pub unclean_recovery_queue_capacity: Option<PositiveCount>,
    #[arg(long, env = "KRABKA_SHARE_RECOVERY_READ_MAX", value_parser = krabka_units::parse::positive_byte_size)]
    pub share_recovery_read_max: Option<ByteSize>,
    #[arg(long, env = "KRABKA_SHARE_SESSION_CACHE_MAX_WHEN_UNLIMITED", value_parser = parse_positive_count)]
    pub share_session_cache_max_when_unlimited: Option<PositiveCount>,
    #[arg(long, env = "KRABKA_LOG_READ_BUFFER_CAP", value_parser = krabka_units::parse::positive_byte_size)]
    pub log_read_buffer_cap: Option<ByteSize>,
    #[arg(long, env = "KRABKA_LOG_TIMESTAMP_SCAN_WINDOW", value_parser = krabka_units::parse::positive_byte_size)]
    pub log_timestamp_scan_window: Option<ByteSize>,
    #[arg(long, env = "KRABKA_LOG_DELIVERY_CLOCK_UNCERTAINTY", value_parser = krabka_units::parse::positive_time)]
    pub log_delivery_clock_uncertainty: Option<Time>,
    #[arg(long, env = "KRABKA_MESSAGE_MAX_BYTES", value_parser = parse_kafka_int_byte_size)]
    pub message_max_bytes: Option<ByteSize>,
    #[arg(long, env = "KRABKA_SOCKET_REQUEST_MAX", value_parser = krabka_units::parse::positive_byte_size)]
    pub socket_request_max: Option<ByteSize>,
    #[arg(long, env = "KRABKA_SENDFILE_MIN", value_parser = krabka_units::parse::positive_byte_size)]
    pub sendfile_min: Option<ByteSize>,
    #[arg(long, env = "KRABKA_SOCKET_SEND_BUFFER", value_parser = krabka_units::parse::positive_byte_size)]
    pub socket_send_buffer: Option<ByteSize>,
    #[arg(long, env = "KRABKA_SOCKET_RECEIVE_BUFFER", value_parser = krabka_units::parse::positive_byte_size)]
    pub socket_receive_buffer: Option<ByteSize>,
    #[arg(long, env = "KRABKA_ACL_MAX_PRINCIPAL", value_parser = krabka_units::parse::positive_byte_size)]
    pub acl_max_principal: Option<ByteSize>,
    #[arg(long, env = "KRABKA_ACL_MAX_RESOURCE_NAME", value_parser = krabka_units::parse::positive_byte_size)]
    pub acl_max_resource_name: Option<ByteSize>,
    #[arg(long, env = "KRABKA_TELEMETRY_MAX_DECOMPRESSION_RATIO", value_parser = krabka_units::parse::ratio)]
    pub telemetry_max_decompression_ratio: Option<Ratio>,
    #[arg(long, env = "KRABKA_TELEMETRY_DECOMPRESSED_OUTPUT_FLOOR", value_parser = krabka_units::parse::positive_byte_size)]
    pub telemetry_decompressed_output_floor: Option<ByteSize>,
    #[arg(long, env = "KRABKA_TELEMETRY_DECOMPRESSED_OUTPUT_CEILING", value_parser = krabka_units::parse::positive_byte_size)]
    pub telemetry_decompressed_output_ceiling: Option<ByteSize>,
    #[arg(long, env = "KRABKA_RECORD_DECOMPRESSION_MAX_RATIO", value_parser = krabka_units::parse::positive_ratio)]
    pub record_decompression_max_ratio: Option<Ratio>,
    #[arg(long, env = "KRABKA_RECORD_DECOMPRESSION_OUTPUT_FLOOR", value_parser = krabka_units::parse::positive_byte_size)]
    pub record_decompression_output_floor: Option<ByteSize>,
    #[arg(long, env = "KRABKA_RECORD_DECOMPRESSION_OUTPUT_CEILING", value_parser = krabka_units::parse::positive_byte_size)]
    pub record_decompression_output_ceiling: Option<ByteSize>,
    #[arg(long, env = "KRABKA_INTER_BROKER_SERVER_NAME")]
    pub inter_broker_server_name: Option<String>,
    #[arg(long, env = "KRABKA_PRODUCER_ID_EXPIRATION", value_parser = krabka_units::parse::positive_time)]
    pub producer_id_expiration: Option<Time>,
    #[arg(long, env = "KRABKA_PRODUCER_ID_EXPIRATION_SCAN_INTERVAL", value_parser = krabka_units::parse::positive_time)]
    pub producer_id_expiration_scan_interval: Option<Time>,
    #[arg(long, env = "KRABKA_MAX_PRODUCE_GROUP", value_parser = parse_positive_count)]
    pub max_produce_group: Option<PositiveCount>,
    #[arg(long, env = "KRABKA_PARTITION_WRITER_QUEUE_DEPTH", value_parser = parse_positive_count)]
    pub partition_writer_queue_depth: Option<PositiveCount>,
    #[arg(long, env = "KRABKA_DEFAULT_MIN_INSYNC_REPLICAS", value_parser = parse_positive_i32)]
    pub default_min_insync_replicas: Option<PositiveI32>,
    #[arg(long, env = "KRABKA_FUTURE_LOG_MOVE_READ_CHUNK", value_parser = krabka_units::parse::positive_byte_size)]
    pub future_log_move_read_chunk: Option<ByteSize>,
    #[arg(long, env = "KRABKA_SHARE_STATE_NUM_PARTITIONS", value_parser = parse_positive_i32)]
    pub share_state_num_partitions: Option<PositiveI32>,
    #[arg(long, env = "KRABKA_SHARE_STATE_REPLICATION_FACTOR", value_parser = parse_positive_i16)]
    pub share_state_replication_factor: Option<PositiveI16>,
    #[arg(long, env = "KRABKA_OFFSETS_TOPIC_NUM_PARTITIONS", value_parser = parse_positive_i32)]
    pub offsets_topic_num_partitions: Option<PositiveI32>,
    #[arg(long, env = "KRABKA_OFFSETS_TOPIC_REPLICATION_FACTOR", value_parser = parse_positive_i16)]
    pub offsets_topic_replication_factor: Option<PositiveI16>,
    #[arg(long, env = "KRABKA_OFFSETS_RETENTION", value_parser = krabka_units::parse::positive_time)]
    pub offsets_retention: Option<Time>,
    #[arg(long, env = "KRABKA_OFFSETS_RETENTION_CHECK_INTERVAL", value_parser = krabka_units::parse::positive_time)]
    pub offsets_retention_check_interval: Option<Time>,
    #[arg(long, env = "KRABKA_TRANSACTION_STATE_NUM_PARTITIONS", value_parser = parse_positive_i32)]
    pub transaction_state_num_partitions: Option<PositiveI32>,
    #[arg(long, env = "KRABKA_TRANSACTION_RECOVERY_READ_MAX", value_parser = krabka_units::parse::positive_byte_size)]
    pub transaction_recovery_read_max: Option<ByteSize>,
    #[arg(long, env = "KRABKA_TRANSACTION_STATE_REPLICATION_FACTOR", value_parser = parse_positive_i16)]
    pub transaction_state_replication_factor: Option<PositiveI16>,
    #[arg(long, env = "KRABKA_TRANSACTION_MIN_TIMEOUT", value_parser = krabka_units::parse::positive_time)]
    pub transaction_min_timeout: Option<Time>,
    #[arg(long, env = "KRABKA_TRANSACTION_MAX_TIMEOUT", value_parser = krabka_units::parse::positive_time)]
    pub transaction_max_timeout: Option<Time>,

    #[arg(long, env = "KRABKA_SHARE_GROUP_ENABLE", action = clap::ArgAction::Set)]
    pub share_group_enable: Option<bool>,
    #[arg(long, env = "KRABKA_SHARE_GROUP_SESSION_TIMEOUT", value_parser = krabka_units::parse::positive_time)]
    pub share_group_session_timeout: Option<Time>,
    #[arg(long, env = "KRABKA_SHARE_GROUP_HEARTBEAT_INTERVAL", value_parser = krabka_units::parse::positive_time)]
    pub share_group_heartbeat_interval: Option<Time>,
    #[arg(long, env = "KRABKA_SHARE_GROUP_MAX_SIZE", value_parser = parse_positive_count)]
    pub share_group_max_size: Option<PositiveCount>,
    #[arg(long, env = "KRABKA_SHARE_GROUP_RECORD_LOCK_DURATION", value_parser = krabka_units::parse::positive_time)]
    pub share_group_record_lock_duration: Option<Time>,
    #[arg(long, env = "KRABKA_SHARE_GROUP_MAX_DELIVERY_ATTEMPTS", value_parser = clap::value_parser!(i16).range(1..))]
    pub share_group_max_delivery_attempts: Option<i16>,
    #[arg(long, env = "KRABKA_SHARE_GROUP_MAX_INFLIGHT_RECORDS", value_parser = parse_positive_i32)]
    pub share_group_max_inflight_records: Option<PositiveI32>,
    #[arg(long, env = "KRABKA_SHARE_GROUP_ISOLATION_LEVEL", value_parser = parse_share_isolation)]
    pub share_group_isolation_level:
        Option<krabka_broker::coordinator::unified::share::config::ShareIsolationLevel>,
    #[arg(long, env = "KRABKA_STREAMS_GROUP_ENABLE", action = clap::ArgAction::Set)]
    pub streams_group_enable: Option<bool>,
    #[arg(long, env = "KRABKA_STREAMS_GROUP_SESSION_TIMEOUT", value_parser = krabka_units::parse::positive_time)]
    pub streams_group_session_timeout: Option<Time>,
    #[arg(long, env = "KRABKA_STREAMS_GROUP_HEARTBEAT_INTERVAL", value_parser = krabka_units::parse::positive_time)]
    pub streams_group_heartbeat_interval: Option<Time>,
    #[arg(long, env = "KRABKA_STREAMS_GROUP_MAX_SIZE", value_parser = parse_positive_count)]
    pub streams_group_max_size: Option<PositiveCount>,
    #[arg(long, env = "KRABKA_STREAMS_INTERNAL_TOPIC_REPLICATION_FACTOR", value_parser = parse_positive_i16)]
    pub streams_internal_topic_replication_factor: Option<PositiveI16>,
    #[arg(long, env = "KRABKA_STREAMS_GROUP_NUM_STANDBY_REPLICAS", value_parser = clap::value_parser!(i32).range(0..))]
    pub streams_group_num_standby_replicas: Option<i32>,
    #[arg(long, env = "KRABKA_STREAMS_GROUP_NUM_WARMUP_REPLICAS", value_parser = clap::value_parser!(i32).range(0..))]
    pub streams_group_num_warmup_replicas: Option<i32>,
    #[arg(long, env = "KRABKA_STREAMS_GROUP_ACCEPTABLE_RECOVERY_LAG", value_parser = clap::value_parser!(i64).range(0..))]
    pub streams_group_acceptable_recovery_lag: Option<i64>,
    #[arg(long, env = "KRABKA_STREAMS_GROUP_TASK_OFFSET_INTERVAL", value_parser = krabka_units::parse::positive_time)]
    pub streams_group_task_offset_interval: Option<Time>,
    #[arg(long, env = "KRABKA_STREAMS_GROUP_ASSIGNOR", value_parser = parse_streams_assignor)]
    pub streams_group_assignor:
        Option<krabka_broker::coordinator::unified::streams::config::StreamsAssignorKind>,
}

fn parse_client_dispatch_queue_capacity(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    ConnectionDispatchQueueCapacity::new(value).map(ConnectionDispatchQueueCapacity::get)
}

/// Parse a byte count in the domain Kafka gives an `INT` config with
/// `atLeast(0)`, which is what `message.max.bytes` is.
///
/// `apache/kafka:4.3.1` starts on `message.max.bytes=0`, refuses `-1` with
/// "Value must be at least 0", and refuses `2147483648` with "Not a number of
/// type INT". The topic-level `max.message.bytes` this key defaults is the
/// same `INT`, so the flag, the TOML file, and `kafka-configs --alter` accept
/// and reject the same values rather than three overlapping domains.
fn parse_kafka_int_byte_size(value: &str) -> Result<ByteSize, String> {
    use krabka_units::convert::ByteSizeExt as _;

    const KAFKA_INT_MAX: u64 = 2_147_483_647;
    const DOMAIN: &str = "must be a whole number of bytes from 0 to 2147483647";

    let size =
        krabka_units::parse::non_negative_byte_size(value).map_err(|error| error.to_string())?;
    let bytes = size.bytes_u64();
    if ByteSize::from_bytes(bytes) != size || bytes > KAFKA_INT_MAX {
        return Err(DOMAIN.to_owned());
    }
    Ok(size)
}

fn parse_client_frame_max(value: &str) -> Result<ByteSize, String> {
    let value =
        krabka_units::parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    ClientFrameMax::try_from(value).map(ClientFrameMax::size)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use clap::Parser as _;
    use krabka_units::convert::ByteSizeExt;

    use crate::{cli::Args, test_support::env_guard};

    /// `--message-max-bytes` takes exactly the values Kafka's `INT` with
    /// `atLeast(0)` takes.
    ///
    /// `apache/kafka:4.3.1` starts on `message.max.bytes=0`, refuses `-1` with
    /// "Value must be at least 0", and refuses `2147483648` with "Not a number
    /// of type INT". The topic-level `max.message.bytes` this key defaults
    /// enforces the same domain, so an operator cannot set a broker-wide cap
    /// through the flag that `kafka-configs --alter` would refuse on a topic.
    #[test]
    fn message_max_bytes_takes_kafkas_int_at_least_zero() {
        let _guard = env_guard();

        for (value, expected) in [
            ("0B", Some(0)),
            ("2048B", Some(2048)),
            ("2147483647B", Some(2_147_483_647)),
            ("1MiB", Some(1_048_576)),
            ("-1B", None),
            ("2147483648B", None),
            ("2GiB", None),
            ("1.5B", None),
        ] {
            let parsed = Args::try_parse_from(["krabka-broker", "--message-max-bytes", value])
                .ok()
                .and_then(|args| args.runtime.message_max_bytes)
                .map(ByteSizeExt::bytes_u64);
            check!(parsed == expected, "--message-max-bytes={value}");
        }
    }
}
