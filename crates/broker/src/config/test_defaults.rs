//! The [`BrokerConfig::for_tests`] constructor: the same knobs as the
//! production defaults, retimed so an in-process fixture starts, fails over
//! and shuts down quickly.

use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

use krabka_compression::RecordDecompressionPolicy;
use krabka_log::LogConfig;
use krabka_raft::{
    BootstrapMode, ControllerFetchMissLimit, MetadataRaftCommandQueueCapacity,
    MetadataRaftFetchMax, NodeId,
};
use krabka_units::{
    Time, bytes, convert::TimeExt, fraction, gibibytes, hours, kibibytes, mebibytes, millis,
    minutes, secs,
};

use crate::{
    config::{
        BreakGlassConfig, BrokerConfig, DEFAULT_AUDIT_CHECKPOINT_EVERY,
        DEFAULT_AUDIT_CHECKPOINT_EVERY_N, DEFAULT_AUDIT_SPOOL_DIR, DEFAULT_AUDIT_SPOOL_MAX,
        DEFAULT_AUDIT_SPOOL_SYNC_EVERY_N, DEFAULT_AUDIT_TOPIC,
        DEFAULT_DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL, DEFAULT_DELEGATION_TOKEN_MAX_LIFETIME,
        DEFAULT_DELEGATION_TOKEN_RENEW_PERIOD, DEFAULT_DISKLESS_WAL_FLUSH_INTERVAL,
        DEFAULT_DISKLESS_WAL_FLUSH_MAX_SIZE, DEFAULT_DISKLESS_WAL_HOT_TAIL_MAX_SIZE,
        DEFAULT_DISKLESS_WAL_INDEX_PROJECTION_TIMEOUT, DEFAULT_DISKLESS_WAL_LOCAL_REPLICA_COUNT,
        DEFAULT_DISKLESS_WAL_TRIM_SAFETY_LAG, DEFAULT_JWKS_MIN_ON_DEMAND_PAUSE,
        DEFAULT_JWKS_REFRESH_INTERVAL, DEFAULT_LEADER_IMBALANCE_CHECK_INTERVAL,
        DEFAULT_LEADER_IMBALANCE_PER_BROKER, DEFAULT_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS,
        DEFAULT_METADATA_MAX_BYTES_BETWEEN_SNAPSHOTS, DEFAULT_METADATA_MAX_SNAPSHOT_INTERVAL,
        DEFAULT_METADATA_SNAPSHOT_FETCH_MAX, DEFAULT_METADATA_SNAPSHOT_INTERVAL_RECORDS,
        DEFAULT_OBSERVER_LAG_BOUND, DEFAULT_TXN_ID_EXPIRATION, FreezeConfig, NodeRole,
        ReplicationRuntimeConfig, RlmmKind, feature_flags::test_feature_flags, shared_epoch_ms,
    },
    operator_keys::OperatorKeys,
};

impl BrokerConfig {
    /// Builds a test config that listens on an OS-assigned port under a
    /// tempdir.
    #[must_use]
    /// # Panics
    /// Panics if the synchronized log state is poisoned.
    ///
    /// Panics if a segment that validated as nonempty is unexpectedly missing
    /// its required batch or index entry.
    #[allow(clippy::too_many_lines)]
    pub fn for_tests(log_dir: PathBuf) -> Self {
        let listen_addr: SocketAddr = "127.0.0.1:0".parse().expect("static");
        let controller_addr: SocketAddr = "127.0.0.1:0".parse().expect("static");
        let record_decompression = RecordDecompressionPolicy::default();
        Self {
            client_dispatch_queue_capacity:
                krabka_client_core::ConnectionDispatchQueueCapacity::default(),
            client_frame_max: krabka_client_core::ClientFrameMax::default(),
            startup_leader_wait_timeout: minutes(2),
            self_registration_backoff_min: millis(100),
            self_registration_backoff_max: secs(5),
            observer_poll_interval: millis(100),
            audit_spool_replay_interval: secs(2),
            audit_stats_poll_interval: secs(1),
            audit_partition_wait_timeout: secs(10),
            liveness_tick_interval: secs(1),
            gauge_poll_interval: secs(1),
            isr_scan_interval: secs(1),
            cleaner_interval: secs(30),
            future_log_move_retry_backoff: millis(50),
            client_metrics_eviction_tick: minutes(1),
            client_metrics_stale_floor: minutes(10),
            client_metrics_default_interval: minutes(5),
            client_metrics_otlp_queue_capacity: 256,
            client_metrics_telemetry_max: mebibytes(1),
            client_metrics_prom_snapshot_ttl: minutes(5),
            rlmm_reconcile_tick: secs(30),
            rlmm_bootstrap_backoff_initial: millis(250),
            rlmm_bootstrap_backoff_max: secs(10),
            connection_creation_throttle_max: secs(1),
            opa_http_timeout: secs(5),
            schema_registry_http_timeout: secs(5),
            oauth_jwks_http_timeout: secs(10),
            auto_join_retry_backoff: millis(500),
            auto_join_voter_request_timeout: secs(30),
            replication: ReplicationRuntimeConfig::default(),
            coordinator_session_expiry_tick: secs(1),
            coordinator_shutdown_ack_timeout: secs(5),
            classic_group_initial_rebalance_delay: secs(3),
            sync_group_follower_wait: secs(30),
            unclean_recovery_aggressive_deadline: secs(2),
            unclean_recovery_balanced_deadline: secs(30),
            operator_recovery_deadline: secs(25),
            quota_throttle_max: secs(1),
            controller_mutation_quota_window: secs(1),
            self_registration_max_attempts: 8,
            observer_fetch_max: mebibytes(1),
            audit_event_queue_capacity: 8_192,
            audit_tail_window_offsets: 4_096,
            audit_tail_read_max: mebibytes(1),
            offsets_topic_metadata_wait_timeout: secs(30),
            client_metrics_stale_push_intervals: 3,
            coordinator_actor_mailbox_capacity: 64,
            diskless_wal_local_replica_count: DEFAULT_DISKLESS_WAL_LOCAL_REPLICA_COUNT,
            diskless_wal_flush_interval: DEFAULT_DISKLESS_WAL_FLUSH_INTERVAL,
            diskless_wal_flush_max_size: DEFAULT_DISKLESS_WAL_FLUSH_MAX_SIZE,
            diskless_wal_hot_tail_max_size: DEFAULT_DISKLESS_WAL_HOT_TAIL_MAX_SIZE,
            diskless_wal_trim_safety_lag: DEFAULT_DISKLESS_WAL_TRIM_SAFETY_LAG,
            diskless_wal_index_projection_timeout: DEFAULT_DISKLESS_WAL_INDEX_PROJECTION_TIMEOUT,
            unclean_recovery_queue_capacity: 256,
            share_recovery_read_max: mebibytes(1),
            share_session_cache_max_when_unlimited: 10_000,
            socket_request_max: mebibytes(100),
            sendfile_min: kibibytes(32),
            socket_send_buffer: mebibytes(1),
            socket_receive_buffer: mebibytes(1),
            acl_max_principal: bytes(256),
            acl_max_resource_name: bytes(256),
            telemetry_max_decompression_ratio: fraction(100.0),
            telemetry_decompressed_output_floor: mebibytes(16),
            telemetry_decompressed_output_ceiling: gibibytes(1),
            record_decompression_max_ratio: record_decompression.max_ratio(),
            record_decompression_output_floor: record_decompression.output_floor(),
            record_decompression_output_ceiling: record_decompression.output_ceiling(),
            inter_broker_server_name: "localhost".to_string(),
            producer_id_expiration: hours(24),
            producer_id_expiration_scan_interval: minutes(10),
            max_produce_group: 1_024,
            partition_writer_queue_depth: 64,
            default_min_insync_replicas: 1,
            future_log_move_read_chunk: mebibytes(1),
            offsets_topic_num_partitions: 50,
            offsets_topic_replication_factor: 3,
            transaction_state_num_partitions: 50,
            transaction_recovery_read_max: mebibytes(1),
            transaction_state_replication_factor: 3,
            transaction_min_timeout: secs(1),
            transaction_max_timeout: minutes(15),
            barrier_state_num_partitions: 50,
            barrier_state_replication_factor: 3,
            barrier_min_injection_interval: secs(1),
            barrier_injection_timeout: secs(30),
            barrier_recovery_read_max: mebibytes(1),
            barrier_retained_cuts: 100,
            barrier_max_groups: 100,
            barrier_max_topics_per_group: 100,
            broker_id: 1,
            roles: vec![NodeRole::Controller, NodeRole::Broker],
            listen_addr,
            advertised_listener: "127.0.0.1:0".into(),
            log_dir,
            extra_log_dirs: Vec::new(),
            log_config: LogConfig::default(),
            stamp_source: None,
            node_id: NodeId(1),
            controller_listen_addr: controller_addr,
            controller_quorum_voters: vec![(NodeId(1), controller_addr.to_string())],
            controller_server_name: None,
            bootstrap_servers: vec![],
            directory_id: uuid::Uuid::from_u128(1),
            incarnation_id: uuid::Uuid::new_v4(),
            auto_join: false,
            observer_lag_bound: DEFAULT_OBSERVER_LAG_BOUND,
            heartbeat_interval: millis(200),
            heartbeat_timeout: secs(2),
            replica_lag_time_max: secs(2),
            // Short timings: single-node tests don't need quorum so split-vote
            // isn't a risk; multi-broker tests use these (via the shared
            // `support::start_n_node_with_retry` helper) so failover from a
            // dead controller leader completes well under the producer's
            // 10s timeout. The factor of ~10× vs. production defaults
            // is what makes `acks_all_completes_via_isr_shrink_when_follower_dead`
            // pass within its 5s assertion window.
            controller_election_timeout: millis(500),
            controller_heartbeat_interval: millis(100),
            controller_heartbeat_interval_explicit: false,
            controller_fetch_miss_limit: ControllerFetchMissLimit::default(),
            metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity::default(),
            metadata_raft_fetch_max: MetadataRaftFetchMax::default(),
            metadata_max_bytes_between_snapshots: DEFAULT_METADATA_MAX_BYTES_BETWEEN_SNAPSHOTS,
            metadata_max_snapshot_interval: DEFAULT_METADATA_MAX_SNAPSHOT_INTERVAL,
            metadata_snapshot_interval_records: DEFAULT_METADATA_SNAPSHOT_INTERVAL_RECORDS,
            metadata_snapshot_fetch_max: DEFAULT_METADATA_SNAPSHOT_FETCH_MAX,
            bootstrap_mode: BootstrapMode::Bootstrap,
            cluster_id: None,
            rack: None,
            replica_selector: crate::replica_selector::ReplicaSelectorKind::Leader,
            stretch: None,
            listeners: vec![],
            controller_listener_protocol: krabka_security::ListenerProtocol::Plaintext,
            inter_broker_listener_name: "PLAINTEXT".to_string(),
            inter_broker_credentials: None,
            inter_broker_principal_node_ids: HashMap::new(),
            plain_credentials: HashMap::new(),
            super_users: std::collections::HashSet::new(),
            authorizer: std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer),
            schema_validator: None,
            operator_keys: OperatorKeys::default(),
            freeze: FreezeConfig::default(),
            break_glass: BreakGlassConfig::default(),
            tls_config: None,
            enabled_sasl_mechanisms: vec![],
            oauthbearer_validator: krabka_security::OAuthBearerValidator::default(),
            gssapi: None,
            oauthbearer_jwks_endpoint: None,
            oauthbearer_jwks_refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
            oauthbearer_idp_tls_trust: None,
            oauthbearer_max_session_lifetime: None,
            oauthbearer_jwks_signal_rx: std::sync::Arc::new(std::sync::Mutex::new(None)),
            oauthbearer_jwks_last_successful_fetch_ms: shared_epoch_ms(),
            oauthbearer_jwks_last_on_demand_refresh_ms: shared_epoch_ms(),
            oauthbearer_jwks_min_on_demand_pause: DEFAULT_JWKS_MIN_ON_DEMAND_PAUSE,
            features: test_feature_flags(),
            // Reaper disabled in tests; suites that exercise it set it low.
            txn_abort_cleanup_interval: <Time as TimeExt>::ZERO,
            // The expiry itself keeps its production value, so a test that
            // ticks the sweep by hand sees the real window. The sweep task is
            // disabled, like the abort reaper above.
            txn_id_expiration: DEFAULT_TXN_ID_EXPIRATION,
            txn_id_expiration_cleanup_interval: <Time as TimeExt>::ZERO,
            next_gen_consumer_group: Box::new(
                crate::coordinator::unified::config::NextGenConfig::default(),
            ),
            share_group: Box::new(
                crate::coordinator::unified::share::config::ShareGroupConfig::default(),
            ),
            streams_group: Box::new(
                crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
            ),
            share_coordinator: Box::new(
                crate::share_coordinator::config::ShareCoordinatorConfig::default(),
            ),
            leader_imbalance_check_interval: DEFAULT_LEADER_IMBALANCE_CHECK_INTERVAL,
            leader_imbalance_per_broker: DEFAULT_LEADER_IMBALANCE_PER_BROKER,
            #[cfg(any(test, feature = "test-helpers"))]
            cleaner_interval_override: None,
            // Short interval so hot-reload tests don't wait long for a
            // watcher tick. Tests that don't care can ignore it.
            tls_reload_interval: millis(200),
            // Tests opt into the metrics endpoint individually by
            // setting this to `Some(127.0.0.1:0)`; sharing a default
            // port would race in parallel test runs.
            metrics_listen_addr: None,
            profiling: krabka_telemetry::profiling::ProfilingConfig::default(),
            client_metrics_otlp_endpoint: None,
            client_metrics_otlp_protocol: krabka_telemetry::OtlpProtocol::Grpc,
            // Disable the disk scanner by default in tests so the
            // background task doesn't tick during short-lived fixtures.
            // Integration tests enable this explicitly when needed.
            partition_disk_scan_interval: <Time as TimeExt>::ZERO,
            max_incremental_fetch_session_cache_slots:
                DEFAULT_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS,
            // Connection caps unlimited by default (Kafka's
            // Integer.MAX_VALUE); the enforcement path treats usize::MAX
            // as "no cap" and never increments the per-IP map.
            max_connections: usize::MAX,
            max_connections_per_ip: usize::MAX,
            // Tests opt into delegation tokens by setting
            // `delegation_token_secret_key`; default off keeps the
            // four DT RPCs returning DELEGATION_TOKEN_AUTH_DISABLED.
            delegation_token_secret_key: None,
            delegation_token_max_lifetime: DEFAULT_DELEGATION_TOKEN_MAX_LIFETIME,
            delegation_token_expiry_check_interval: DEFAULT_DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL,
            delegation_token_default_renew_period: DEFAULT_DELEGATION_TOKEN_RENEW_PERIOD,
            // Tiered storage off by default in tests.
            remote_storage_backend: None,
            // Tests that turn tiered storage on want quick offload, so the
            // for_tests default is well below the 30s production value.
            remote_log_manager_interval: secs(2),
            // Tests use the in-memory RLMM fixture.
            remote_log_metadata: RlmmKind::InMemory,
            // Ordinary mutable tiered storage in tests; WORM is opt-in.
            remote_storage_worm: None,
            // Audit enabled by default (secure-by-default / `FedRAMP` MLA).
            audit_enabled: true,
            audit_failure_mode: krabka_audit::AuditMode::FailOpen,
            audit_topic: DEFAULT_AUDIT_TOPIC.to_string(),
            audit_signing_key_path: None,
            audit_signing_key_id: None,
            audit_checkpoint_every_n: DEFAULT_AUDIT_CHECKPOINT_EVERY_N,
            audit_checkpoint_every: DEFAULT_AUDIT_CHECKPOINT_EVERY,
            audit_spool_dir: std::path::PathBuf::from(DEFAULT_AUDIT_SPOOL_DIR),
            audit_spool_max: DEFAULT_AUDIT_SPOOL_MAX,
            audit_spool_sync_every_n: DEFAULT_AUDIT_SPOOL_SYNC_EVERY_N,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::convert::{ByteSizeExt, RatioExt};

    use super::*;

    fn additional_policy_snapshot(config: BrokerConfig) -> [String; 31] {
        [
            config.self_registration_max_attempts.to_string(),
            config.observer_fetch_max.bytes_u64().to_string(),
            config.audit_event_queue_capacity.to_string(),
            config.audit_tail_window_offsets.to_string(),
            config.audit_tail_read_max.bytes_u64().to_string(),
            config
                .offsets_topic_metadata_wait_timeout
                .millis_i64()
                .to_string(),
            config.client_metrics_stale_push_intervals.to_string(),
            config.coordinator_actor_mailbox_capacity.to_string(),
            config.unclean_recovery_queue_capacity.to_string(),
            config.share_recovery_read_max.bytes_u64().to_string(),
            config.share_session_cache_max_when_unlimited.to_string(),
            config.socket_request_max.bytes_u64().to_string(),
            config.sendfile_min.bytes_u64().to_string(),
            config.socket_send_buffer.bytes_u64().to_string(),
            config.socket_receive_buffer.bytes_u64().to_string(),
            config.acl_max_principal.bytes_u64().to_string(),
            config.acl_max_resource_name.bytes_u64().to_string(),
            config
                .telemetry_max_decompression_ratio
                .as_f64()
                .to_string(),
            config
                .telemetry_decompressed_output_floor
                .bytes_u64()
                .to_string(),
            config
                .telemetry_decompressed_output_ceiling
                .bytes_u64()
                .to_string(),
            config.inter_broker_server_name,
            config.producer_id_expiration.millis_i64().to_string(),
            config
                .producer_id_expiration_scan_interval
                .millis_i64()
                .to_string(),
            config.max_produce_group.to_string(),
            config.partition_writer_queue_depth.to_string(),
            config.default_min_insync_replicas.to_string(),
            config.future_log_move_read_chunk.bytes_u64().to_string(),
            config
                .share_coordinator
                .state_topic_num_partitions
                .to_string(),
            config.transaction_state_num_partitions.to_string(),
            config.transaction_min_timeout.millis_i32().to_string(),
            config.transaction_max_timeout.millis_i32().to_string(),
        ]
    }

    #[test]
    fn additional_operational_policy_defaults_match_existing_behavior() {
        let actual = additional_policy_snapshot(BrokerConfig::default());
        assert!(
            actual
                == [
                    "8",
                    "1048576",
                    "8192",
                    "4096",
                    "1048576",
                    "30000",
                    "3",
                    "64",
                    "256",
                    "1048576",
                    "10000",
                    "104857600",
                    "32768",
                    "1048576",
                    "1048576",
                    "256",
                    "256",
                    "100",
                    "16777216",
                    "1073741824",
                    "localhost",
                    "86400000",
                    "600000",
                    "1024",
                    "64",
                    "1",
                    "1048576",
                    "50",
                    "50",
                    "1000",
                    "900000",
                ]
        );
        assert!(additional_policy_snapshot(BrokerConfig::for_tests(PathBuf::new())) == actual);
    }

    #[test]
    fn for_tests_uses_port_0() {
        let c = BrokerConfig::for_tests(PathBuf::from("/tmp"));
        assert!(c.listen_addr.port() == 0);
    }

    #[test]
    fn for_tests_uses_20_mib_metadata_snapshot_threshold() {
        let cfg = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert!(cfg.metadata_max_bytes_between_snapshots == mebibytes(20));
        assert!(cfg.metadata_max_bytes_between_snapshots.bytes_u64() == 20 * 1024 * 1024);
    }

    #[test]
    fn for_tests_uses_short_raft_timings_for_fast_failover() {
        let c = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        // Short enough that a 3-broker test can detect a dead leader and
        // re-elect within a few hundred ms — the failover tests
        // need failover well under their 10s producer timeout.
        assert!(c.controller_election_timeout <= millis(750));
        assert!(c.controller_heartbeat_interval <= millis(200));
    }

    #[test]
    fn for_tests_uses_bootstrap_mode() {
        let c = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert!(c.bootstrap_mode == BootstrapMode::Bootstrap);
    }
}
