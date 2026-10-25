//! The production defaults' shared values.
//!
//! [`BrokerConfig`](crate::config::BrokerConfig) derives
//! [`krabka_macros::FieldDefaults`], so each field's default sits on the
//! field itself. This module holds the addresses that more than one of those
//! defaults starts from, and the tests that pin the defaults.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// The data-plane address a default config listens on and advertises.
pub(super) const DEFAULT_LISTEN_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9092);

/// The controller address a default config listens on and names as its one
/// voter.
pub(super) const DEFAULT_CONTROLLER_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9093);

#[cfg(test)]
mod tests {
    use {
        crate::config::{
            BootstrapMode, BrokerConfig, DEFAULT_CONTROLLER_HEARTBEAT_INTERVAL,
            ReplicationRuntimeConfig,
        },
        assert2::assert,
        krabka_raft::NodeId,
        krabka_units::{bytes, gibibytes, mebibytes, millis, minutes, secs},
        std::{net::SocketAddr, path::PathBuf},
    };

    #[test]
    fn operational_policy_defaults_match_existing_behavior() {
        let config = BrokerConfig::default();

        assert!(
            (
                config.startup_leader_wait_timeout,
                config.self_registration_backoff_min,
                config.self_registration_backoff_max,
                config.observer_poll_interval,
                config.audit_spool_replay_interval,
                config.audit_stats_poll_interval,
                config.audit_partition_wait_timeout,
                config.liveness_tick_interval,
            ) == (
                minutes(2),
                millis(100),
                secs(5),
                millis(100),
                secs(2),
                secs(1),
                secs(10),
                secs(1),
            )
        );
        assert!(
            (
                config.gauge_poll_interval,
                config.isr_scan_interval,
                config.cleaner_interval,
                config.future_log_move_retry_backoff,
                config.client_metrics_eviction_tick,
                config.client_metrics_stale_floor,
            ) == (
                secs(1),
                secs(1),
                secs(30),
                millis(50),
                minutes(1),
                minutes(1)
            )
        );
        assert!(
            (
                config.client_metrics_default_interval,
                config.client_metrics_otlp_queue_capacity,
                config.client_metrics_telemetry_max,
                config.client_metrics_prom_snapshot_ttl,
                config.rlmm_reconcile_tick,
                config.rlmm_bootstrap_backoff_initial,
                config.rlmm_bootstrap_backoff_max,
                config.connection_creation_throttle_max,
                config.opa_http_timeout,
                config.oauth_jwks_http_timeout,
                config.auto_join_retry_backoff,
            ) == (
                minutes(5),
                256,
                mebibytes(1),
                minutes(5),
                secs(30),
                millis(250),
                secs(10),
                secs(1),
                secs(5),
                secs(10),
                millis(500),
            )
        );
        assert!(
            (
                config.client_metrics_prom_max_series,
                config.client_metrics_prom_max_series_per_principal,
                config.client_metrics_prom_max_series_per_client,
                config.client_metrics_prom_max_series_per_instance,
            ) == (10_000, 2_500, 1_000, 500)
        );
        assert!(
            config.replication
                == ReplicationRuntimeConfig {
                    // Kafka's `num.replica.fetchers` default.
                    fetchers: 1,
                    // Kafka's `replica.fetch.max.bytes` and
                    // `replica.fetch.response.max.bytes` defaults.
                    fetch_max: mebibytes(1),
                    fetch_response_max: mebibytes(10),
                    fetch_max_wait: millis(500),
                    fetch_min: bytes(1),
                    throttle_exhausted_backoff: millis(100),
                    send_error_backoff: secs(1),
                    unknown_topic_retry_delay: millis(100),
                    epoch_fence_backoff: millis(200),
                    unexpected_error_backoff: millis(500),
                    reconnect_initial_delay: millis(100),
                    reconnect_delay_cap: secs(5),
                }
        );
        assert!(
            (
                config.coordinator_session_expiry_tick,
                config.coordinator_shutdown_ack_timeout,
                config.classic_group_initial_rebalance_delay,
                config.sync_group_follower_wait,
                config.unclean_recovery_aggressive_deadline,
                config.unclean_recovery_balanced_deadline,
                config.operator_recovery_deadline,
                config.quota_throttle_max,
                config.controller_mutation_quota_window,
            ) == (
                secs(1),
                secs(5),
                secs(3),
                secs(30),
                secs(2),
                secs(30),
                secs(25),
                // Kafka's `ClientRequestQuotaManager` bounds the request-quota
                // throttle at one `quota.window.size.seconds` (#709).
                secs(1),
                // KIP-599: Kafka's `controller.quota.window.num` (11) x
                // `controller.quota.window.size.seconds` (1).
                secs(11),
            )
        );
    }

    #[test]
    fn barrier_defaults_match_the_documented_policy() {
        let config = BrokerConfig::default();
        let actual = (
            config.barrier_state_num_partitions,
            config.barrier_state_replication_factor,
            config.barrier_min_injection_interval,
            config.barrier_injection_timeout,
            config.barrier_recovery_read_max,
            config.barrier_retained_cuts,
            config.barrier_max_groups,
            config.barrier_max_topics_per_group,
        );

        assert!(actual == (50, 3, secs(1), secs(30), mebibytes(1), 100, 100, 100));
    }

    #[test]
    fn defaults_use_conservative_raft_timings() {
        let c = BrokerConfig::default();
        assert!(c.controller_fetch_timeout == secs(2));
        assert!(c.controller_election_timeout == secs(1));
        assert!(c.controller_heartbeat_interval == DEFAULT_CONTROLLER_HEARTBEAT_INTERVAL);
    }

    #[test]
    fn defaults_carry_no_schema_validator_and_a_five_second_registry_timeout() {
        let c = BrokerConfig::default();
        assert!(c.schema_validator.is_none());
        assert!(c.schema_registry_http_timeout == secs(5));

        let t = BrokerConfig::for_tests(PathBuf::from("/tmp/schema-registry-defaults"));
        assert!(t.schema_validator.is_none());
        assert!(t.schema_registry_http_timeout == secs(5));
    }

    #[test]
    fn default_metadata_snapshot_interval() {
        let cfg = BrokerConfig::default();
        assert!(cfg.metadata_snapshot_interval_records == 10_000);
        assert!(cfg.metadata_snapshot_fetch_max == gibibytes(1));
    }

    #[test]
    fn defaults_use_bootstrap_mode() {
        let c = BrokerConfig::default();
        assert!(c.bootstrap_mode == BootstrapMode::Bootstrap);
    }

    #[test]
    fn defaults_listen_on_loopback_and_name_the_controller_as_the_one_voter() {
        let c = BrokerConfig::default();
        let listen: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let controller: SocketAddr = "127.0.0.1:9093".parse().unwrap();
        assert!(
            (
                c.listen_addr,
                c.advertised_listener,
                c.controller_listen_addr,
                c.controller_quorum_voters,
            ) == (listen, "127.0.0.1:9092".to_owned(), controller, vec![(
                NodeId(1),
                "127.0.0.1:9093".to_owned()
            )],)
        );
    }
}
