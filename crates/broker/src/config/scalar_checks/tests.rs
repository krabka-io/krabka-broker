//! Behaviour tests for the runtime-scalar checks: which value each rejection
//! names, and the message it reports.

use assert2::assert;
use krabka_units::{gibibytes, nanos};

use super::*;
use crate::config::test_support::{RuntimeInvalidator, assert_invalid_runtime, base};

#[test]
fn rejects_non_positive_runtime_scalars() {
    let cases: [RuntimeInvalidator; 22] = [
        ("startup_leader_wait_timeout", |c| {
            c.startup_leader_wait_timeout = <Time as TimeExt>::ZERO;
        }),
        ("cleaner_interval", |c| {
            c.cleaner_interval = <Time as TimeExt>::ZERO;
        }),
        ("diskless_wal_flush_interval", |c| {
            c.diskless_wal_flush_interval = <Time as TimeExt>::ZERO;
        }),
        ("diskless_wal_index_projection_timeout", |c| {
            c.diskless_wal_index_projection_timeout = <Time as TimeExt>::ZERO;
        }),
        ("diskless_wal_flush_max_size", |c| {
            c.diskless_wal_flush_max_size = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("client_metrics_default_interval", |c| {
            c.client_metrics_default_interval = <Time as TimeExt>::ZERO;
        }),
        ("client_metrics_telemetry_max", |c| {
            c.client_metrics_telemetry_max = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("client_metrics_otlp_queue_capacity", |c| {
            c.client_metrics_otlp_queue_capacity = 0;
        }),
        ("replication.fetch_max", |c| {
            c.replication.fetch_max = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("replication.fetch_max_wait", |c| {
            c.replication.fetch_max_wait = <Time as TimeExt>::ZERO;
        }),
        ("replication.fetch_min", |c| {
            c.replication.fetch_min = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("replication.send_error_backoff", |c| {
            c.replication.send_error_backoff = <Time as TimeExt>::ZERO;
        }),
        ("heartbeat_interval", |c| {
            c.heartbeat_interval = <Time as TimeExt>::ZERO;
        }),
        ("replica_lag_time_max", |c| {
            c.replica_lag_time_max = <Time as TimeExt>::ZERO;
        }),
        ("controller_heartbeat_interval", |c| {
            c.controller_heartbeat_interval = <Time as TimeExt>::ZERO;
        }),
        ("metadata_max_bytes_between_snapshots", |c| {
            c.metadata_max_bytes_between_snapshots = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("metadata_snapshot_interval_records", |c| {
            c.metadata_snapshot_interval_records = 0;
        }),
        ("delegation_token_max_lifetime", |c| {
            c.delegation_token_max_lifetime = <Time as TimeExt>::ZERO;
        }),
        ("delegation_token_expiry_check_interval", |c| {
            c.delegation_token_expiry_check_interval = <Time as TimeExt>::ZERO;
        }),
        ("delegation_token_default_renew_period", |c| {
            c.delegation_token_default_renew_period = -millis(1);
        }),
        ("remote_log_manager_interval", |c| {
            c.remote_log_manager_interval = <Time as TimeExt>::ZERO;
        }),
        ("delegation_token_max_lifetime", |c| {
            c.delegation_token_max_lifetime = -millis(1);
        }),
    ];

    for (name, invalidate) in cases {
        let mut config = BrokerConfig::default();
        invalidate(&mut config);
        assert_invalid_runtime(&config, &format!("{name} must be positive"));
    }
}

#[test]
fn rejects_invalid_additional_runtime_scalars() {
    let cases: &[RuntimeInvalidator] = &[
        ("self_registration_max_attempts must be positive", |c| {
            c.self_registration_max_attempts = 0;
        }),
        ("observer_fetch_max must be positive", |c| {
            c.observer_fetch_max = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("audit_event_queue_capacity must be positive", |c| {
            c.audit_event_queue_capacity = 0;
        }),
        ("audit_tail_window_offsets must be positive", |c| {
            c.audit_tail_window_offsets = 0;
        }),
        ("audit_tail_read_max must be positive", |c| {
            c.audit_tail_read_max = <ByteSize as ByteSizeExt>::ZERO;
        }),
        (
            "offsets_topic_metadata_wait_timeout must be at least 1ms",
            |c| c.offsets_topic_metadata_wait_timeout = <Time as TimeExt>::ZERO,
        ),
        (
            "offsets_topic_metadata_wait_timeout must be at least 1ms",
            |c| c.offsets_topic_metadata_wait_timeout = nanos(1),
        ),
        (
            "client_metrics_stale_push_intervals must be positive",
            |c| c.client_metrics_stale_push_intervals = 0,
        ),
        ("coordinator_actor_mailbox_capacity must be positive", |c| {
            c.coordinator_actor_mailbox_capacity = 0;
        }),
        ("diskless_wal_local_replica_count must be positive", |c| {
            c.diskless_wal_local_replica_count = 0;
        }),
        ("diskless_wal_local_replica_count must be odd", |c| {
            c.diskless_wal_local_replica_count = 2;
        }),
        ("diskless_wal_trim_safety_lag must be nonnegative", |c| {
            c.diskless_wal_trim_safety_lag = -1;
        }),
        ("unclean_recovery_queue_capacity must be positive", |c| {
            c.unclean_recovery_queue_capacity = 0;
        }),
        ("share_recovery_read_max must be positive", |c| {
            c.share_recovery_read_max = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("transaction_recovery_read_max must be positive", |c| {
            c.transaction_recovery_read_max = <ByteSize as ByteSizeExt>::ZERO;
        }),
        (
            "share_session_cache_max_when_unlimited must be positive",
            |c| c.share_session_cache_max_when_unlimited = 0,
        ),
        ("socket_request_max must be positive", |c| {
            c.socket_request_max = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("sendfile_min must be positive", |c| {
            c.sendfile_min = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("socket_send_buffer must be positive", |c| {
            c.socket_send_buffer = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("socket_receive_buffer must be positive", |c| {
            c.socket_receive_buffer = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("acl_max_principal must be positive", |c| {
            c.acl_max_principal = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("acl_max_resource_name must be positive", |c| {
            c.acl_max_resource_name = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("telemetry_max_decompression_ratio must be positive", |c| {
            c.telemetry_max_decompression_ratio = <Ratio as RatioExt>::ZERO;
        }),
        (
            "telemetry_decompressed_output_floor must be positive",
            |c| c.telemetry_decompressed_output_floor = <ByteSize as ByteSizeExt>::ZERO,
        ),
        (
            "telemetry_decompressed_output_ceiling must be positive",
            |c| c.telemetry_decompressed_output_ceiling = <ByteSize as ByteSizeExt>::ZERO,
        ),
        ("producer_id_expiration must be positive", |c| {
            c.producer_id_expiration = <Time as TimeExt>::ZERO;
        }),
        (
            "producer_id_expiration_scan_interval must be positive",
            |c| c.producer_id_expiration_scan_interval = <Time as TimeExt>::ZERO,
        ),
        (
            "auto_join_voter_request_timeout must be within 1..=i32::MAX milliseconds",
            |c| {
                c.auto_join_voter_request_timeout = <Time as TimeExt>::ZERO;
            },
        ),
        (
            "auto_join_voter_request_timeout must be within 1..=i32::MAX milliseconds",
            |c| {
                c.auto_join_voter_request_timeout = nanos(1);
            },
        ),
        ("max_produce_group must be positive", |c| {
            c.max_produce_group = 0;
        }),
        ("partition_writer_queue_depth must be positive", |c| {
            c.partition_writer_queue_depth = 0;
        }),
        ("default_min_insync_replicas must be positive", |c| {
            c.default_min_insync_replicas = 0;
        }),
        ("future_log_move_read_chunk must be positive", |c| {
            c.future_log_move_read_chunk = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("share_state_num_partitions must be positive", |c| {
            c.share_coordinator.state_topic_num_partitions = 0;
        }),
        ("share_state_replication_factor must be positive", |c| {
            c.share_coordinator.state_topic_replication_factor = 0;
        }),
        ("offsets_topic_num_partitions must be positive", |c| {
            c.offsets_topic_num_partitions = 0;
        }),
        ("offsets_topic_replication_factor must be positive", |c| {
            c.offsets_topic_replication_factor = 0;
        }),
        ("transaction_state_num_partitions must be positive", |c| {
            c.transaction_state_num_partitions = 0;
        }),
        (
            "transaction_state_replication_factor must be positive",
            |c| c.transaction_state_replication_factor = 0,
        ),
        (
            "streams_internal_topic_replication_factor must be positive",
            |c| c.streams_group.internal_topic_replication_factor = 0,
        ),
        ("transaction_min_timeout must be positive", |c| {
            c.transaction_min_timeout = <Time as TimeExt>::ZERO;
        }),
        ("transaction_max_timeout must be positive", |c| {
            c.transaction_max_timeout = <Time as TimeExt>::ZERO;
        }),
        ("barrier_state_num_partitions must be positive", |c| {
            c.barrier_state_num_partitions = 0;
        }),
        ("barrier_state_replication_factor must be positive", |c| {
            c.barrier_state_replication_factor = 0;
        }),
        ("barrier_retained_cuts must be positive", |c| {
            c.barrier_retained_cuts = 0;
        }),
        ("barrier_max_groups must be positive", |c| {
            c.barrier_max_groups = 0;
        }),
        ("barrier_max_topics_per_group must be positive", |c| {
            c.barrier_max_topics_per_group = 0;
        }),
        ("barrier_recovery_read_max must be positive", |c| {
            c.barrier_recovery_read_max = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("barrier_min_injection_interval must be positive", |c| {
            c.barrier_min_injection_interval = <Time as TimeExt>::ZERO;
        }),
        ("barrier_injection_timeout must be positive", |c| {
            c.barrier_injection_timeout = <Time as TimeExt>::ZERO;
        }),
    ];

    for (expected, invalidate) in cases {
        let mut config = BrokerConfig::default();
        invalidate(&mut config);
        assert_invalid_runtime(&config, expected);
    }
}

#[test]
fn validate_rejects_a_non_positive_schema_registry_http_timeout() {
    let c = BrokerConfig {
        schema_registry_http_timeout: Time::ZERO,
        ..base()
    };
    assert!(c.validate().is_err());
}

#[test]
fn metadata_snapshot_fetch_max_cannot_raise_the_core_security_ceiling() {
    let cfg = BrokerConfig {
        metadata_snapshot_fetch_max: gibibytes(2),
        ..BrokerConfig::default()
    };

    let error = cfg.validate().expect_err("over-ceiling limit must fail");
    assert!(error.to_string().contains("metadata_snapshot_fetch_max"));
}
