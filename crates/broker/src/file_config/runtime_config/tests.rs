//! Cross-cutting tests for the `[runtime]` table.
//!
//! These exercise `RuntimeFileConfig` as a whole — the representative apply,
//! the quantity round trip across every millisecond and byte key, and the
//! relational checks that only run once the whole table is applied — so they
//! belong to the table's own module rather than to one domain applier.

use {
    crate::{
        config::BrokerConfig,
        file_config::{FileConfig, test_support::configured},
    },
    assert2::assert,
    krabka_units::{
        bytes,
        convert::{ByteSizeExt as _, TimeExt as _},
        days, hours, mebibytes, millis, minutes, secs,
    },
    std::time::Duration,
};

#[test]
fn runtime_file_config_applies_representative_values() {
    let cfg = configured(
        r#"
[runtime]
cleaner_interval = "7s"
isr_scan_interval = "800ms"
opa_http_timeout = "2500ms"
fetch_max_bytes = "2MiB"
replication_fetch_max = "2MiB"
replication_fetch_response_max = "20MiB"
replication_fetch_max_wait = "750ms"
replication_fetch_min = "2B"
diskless_wal_flush_interval = "125ms"
diskless_wal_flush_max_size = "4MiB"
diskless_wal_hot_tail_max_size = "16MiB"
diskless_wal_trim_safety_lag = 0
diskless_wal_index_projection_timeout = "3s"
controller_heartbeat_interval = "500ms"
controller_fetch_miss_limit = 7
metadata_raft_command_queue_capacity = 512
metadata_raft_fetch_max = "4MiB"
log_segment_bytes = "1MiB"
share_group_max_size = 17
share_group_backlog_poll_interval = "250ms"
streams_group_enable = false
streams_group_max_size = 19
"#,
        "parse runtime config",
    )
    .expect("apply runtime config");

    assert!(
        (
            cfg.cleaner_interval,
            cfg.isr_scan_interval,
            cfg.opa_http_timeout,
            cfg.replication.fetch_max,
            cfg.replication.fetch_response_max,
            cfg.replication.fetch_max_wait,
            cfg.replication.fetch_min,
            cfg.fetch_max_bytes,
        ) == (
            secs(7),
            millis(800),
            millis(2_500),
            mebibytes(2),
            mebibytes(20),
            millis(750),
            bytes(2),
            mebibytes(2),
        )
    );
    assert!(cfg.controller_heartbeat_interval_explicit);
    assert!(cfg.controller_heartbeat_interval == millis(500));
    assert!(cfg.controller_fetch_miss_limit.get() == 7);
    assert!(cfg.metadata_raft_command_queue_capacity.get() == 512);
    assert!(cfg.metadata_raft_fetch_max.bytes() == 4 * 1024 * 1024);
    assert!(cfg.log_config.segment_size == mebibytes(1));
    assert!(cfg.diskless_wal_flush_interval == millis(125));
    assert!(cfg.diskless_wal_flush_max_size == mebibytes(4));
    assert!(cfg.diskless_wal_hot_tail_max_size == mebibytes(16));
    assert!(cfg.diskless_wal_trim_safety_lag == 0);
    assert!(cfg.diskless_wal_index_projection_timeout == secs(3));
    assert!(cfg.share_group.max_size == 17);
    assert!(cfg.share_group.backlog_poll_interval == Duration::from_millis(250));
    assert!(!cfg.streams_group.enable);
    assert!(cfg.streams_group.max_size == 19);
}
/// Every time and byte-size runtime key must survive the round trip
/// TOML quantity → wire integer unchanged. This is the
/// regression the `krabka-units` adoption exists to prevent: a mapping
/// that reads `30000` as 30 000 *seconds*, or writes a 30 s timeout back
/// as `30`, changes a Kafka wire field by three orders of magnitude.
#[test]
fn runtime_millisecond_and_byte_keys_round_trip_through_quantities() {
    /// A runtime key and the integer it carries on the wire.
    #[derive(Debug, PartialEq, Eq)]
    struct Quantity {
        key: &'static str,
        value: i64,
    }

    let cfg = configured(
        r#"
[runtime]
heartbeat_interval = "3s"
heartbeat_timeout = "9s"
replica_lag_time_max = "30s"
transaction_max_timeout = "15min"
producer_id_expiration = "24h"
client_metrics_default_interval = "5min"
delegation_token_max_lifetime = "7d"
socket_request_max = "100MiB"
client_metrics_telemetry_max = "1MiB"
observer_fetch_max = "1MiB"
fetch_max_bytes = "56MiB"
replication_fetch_max_wait = "500ms"
replication_fetch_max = "1MiB"
replication_fetch_min = "1B"
"#,
        "parse runtime config",
    )
    .expect("apply runtime config");

    // Landed as dimensioned quantities, spelled in their natural units.
    assert!(cfg.heartbeat_interval == secs(3));
    assert!(cfg.heartbeat_timeout == secs(9));
    assert!(cfg.replica_lag_time_max == secs(30));
    assert!(cfg.transaction_max_timeout == minutes(15));
    assert!(cfg.producer_id_expiration == hours(24));
    assert!(cfg.client_metrics_default_interval == minutes(5));
    assert!(cfg.delegation_token_max_lifetime == days(7));
    assert!(cfg.socket_request_max == mebibytes(100));
    assert!(cfg.client_metrics_telemetry_max == mebibytes(1));
    assert!(cfg.observer_fetch_max == mebibytes(1));
    assert!(cfg.fetch_max_bytes == mebibytes(56));
    assert!(cfg.replication.fetch_max_wait == millis(500));
    assert!(cfg.replication.fetch_max == mebibytes(1));
    assert!(cfg.replication.fetch_min == bytes(1));

    // …and leave for the wire exactly the integers that came in.
    let millis: [Quantity; 8] = [
        Quantity {
            key: "heartbeat_interval",
            value: cfg.heartbeat_interval.millis_i64(),
        },
        Quantity {
            key: "heartbeat_timeout",
            value: cfg.heartbeat_timeout.millis_i64(),
        },
        Quantity {
            key: "replica_lag_time_max",
            value: cfg.replica_lag_time_max.millis_i64(),
        },
        Quantity {
            key: "transaction_max_timeout",
            value: i64::from(cfg.transaction_max_timeout.millis_i32()),
        },
        Quantity {
            key: "producer_id_expiration",
            value: cfg.producer_id_expiration.millis_i64(),
        },
        Quantity {
            key: "client_metrics_default_interval",
            value: i64::from(cfg.client_metrics_default_interval.millis_i32()),
        },
        Quantity {
            key: "delegation_token_max_lifetime",
            value: cfg.delegation_token_max_lifetime.millis_i64(),
        },
        // Truncating, exactly as `build_fetch_request` narrows it for the
        // `FetchRequest.max_wait_ms` wire field.
        Quantity {
            key: "replication_fetch_max_wait",
            value: cfg.replication.fetch_max_wait.millis_i64_trunc(),
        },
    ];
    assert!(
        millis
            == [
                Quantity {
                    key: "heartbeat_interval",
                    value: 3_000
                },
                Quantity {
                    key: "heartbeat_timeout",
                    value: 9_000
                },
                Quantity {
                    key: "replica_lag_time_max",
                    value: 30_000
                },
                Quantity {
                    key: "transaction_max_timeout",
                    value: 900_000
                },
                Quantity {
                    key: "producer_id_expiration",
                    value: 86_400_000
                },
                Quantity {
                    key: "client_metrics_default_interval",
                    value: 300_000
                },
                Quantity {
                    key: "delegation_token_max_lifetime",
                    value: 604_800_000
                },
                Quantity {
                    key: "replication_fetch_max_wait",
                    value: 500
                },
            ]
    );
    let sizes: [Quantity; 6] = [
        Quantity {
            key: "socket_request_max",
            value: cfg.socket_request_max.bytes_i64(),
        },
        Quantity {
            key: "client_metrics_telemetry_max",
            value: i64::from(cfg.client_metrics_telemetry_max.bytes_i32()),
        },
        Quantity {
            key: "observer_fetch_max",
            value: cfg.observer_fetch_max.bytes_i64(),
        },
        Quantity {
            key: "fetch_max_bytes",
            value: i64::from(cfg.fetch_max_bytes.bytes_i32()),
        },
        Quantity {
            key: "replication_fetch_max",
            value: i64::from(cfg.replication.fetch_max.bytes_i32()),
        },
        Quantity {
            key: "replication_fetch_min",
            value: i64::from(cfg.replication.fetch_min.bytes_i32()),
        },
    ];
    assert!(
        sizes
            == [
                Quantity {
                    key: "socket_request_max",
                    value: 104_857_600
                },
                Quantity {
                    key: "client_metrics_telemetry_max",
                    value: 1_048_576
                },
                Quantity {
                    key: "observer_fetch_max",
                    value: 1_048_576
                },
                Quantity {
                    key: "fetch_max_bytes",
                    value: 58_720_256
                },
                Quantity {
                    key: "replication_fetch_max",
                    value: 1_048_576
                },
                Quantity {
                    key: "replication_fetch_min",
                    value: 1
                },
            ]
    );
}
#[test]
fn runtime_file_config_rejects_relational_conflicts() {
    let cases = [(
        "[runtime]\nreplication_fetch_min = \"3B\"\nreplication_fetch_response_max = \"2B\"\n",
        "replication fetch minimum",
    )];

    for (source, message) in cases {
        let error =
            configured(source, "parse runtime config").expect_err("relational conflict must fail");
        assert!(error.to_string().contains(message));
    }
}

/// Kafka's `group.initial.rebalance.delay.ms` is `atLeast(0)`, so the file
/// accepts zero for it, where the other coordinator timings stay positive.
#[test]
fn runtime_file_config_accepts_a_zero_classic_group_initial_rebalance_delay() {
    let cfg = configured(
        "[runtime]\nclassic_group_initial_rebalance_delay = \"0ms\"\n",
        "parse runtime config",
    )
    .expect("apply runtime config");

    assert!(cfg.classic_group_initial_rebalance_delay == millis(0));
    assert!(cfg.validate().is_ok());
}

/// The five keys of Kafka's `MetadataLogConfig` set the whole of
/// `BrokerConfig::metadata_log` together, and the loader records each Kafka
/// key the table named, so `DescribeConfigs` reports it at
/// `STATIC_BROKER_CONFIG`.
#[test]
fn the_metadata_log_keys_set_the_whole_metadata_log_config() {
    let cfg = configured(
        r#"
[runtime]
metadata_log_segment_bytes = "16MiB"
metadata_log_segment_roll_interval = "1h"
metadata_max_retention_bytes = "0B"
metadata_max_retention = "2d"
metadata_max_idle_interval = "0ms"
"#,
        "parse runtime config",
    )
    .expect("apply runtime config");

    assert!(
        (
            cfg.metadata_log,
            cfg.static_config_origins
                .supplied_kafka_keys
                .into_iter()
                .collect::<Vec<_>>(),
        ) == (
            krabka_raft::MetadataLogConfig {
                segment_size: mebibytes(16),
                segment_roll_interval: hours(1),
                max_retention_size: Some(bytes(0)),
                max_retention: Some(days(2)),
                max_idle_interval: millis(0),
            },
            vec![
                "metadata.log.segment.bytes",
                "metadata.log.segment.ms",
                "metadata.max.idle.interval.ms",
                "metadata.max.retention.bytes",
                "metadata.max.retention.ms",
            ],
        )
    );
}

#[derive(Default)]
struct RuntimeFileApplication {
    sources: Vec<String>,
    applied: Option<Result<BrokerConfig, String>>,
}

#[rstest::fixture]
fn runtime_file_application() -> RuntimeFileApplication {
    // Overlays and provenance are isolated to one broker configuration.
    RuntimeFileApplication::default()
}

#[rstest_bdd_macros::given("a runtime configuration file contains:")]
fn supplied_runtime_configuration(
    runtime_file_application: &mut RuntimeFileApplication,
    docstring: String,
) {
    runtime_file_application.sources.push(docstring);
}

#[rstest_bdd_macros::when("the runtime files are applied in order to a default broker")]
fn apply_runtime_configuration(runtime_file_application: &mut RuntimeFileApplication) {
    assert!(!runtime_file_application.sources.is_empty());
    let mut config = BrokerConfig::default();
    for source in &runtime_file_application.sources {
        let file: FileConfig = toml::from_str(source).expect("parse runtime config");
        if let Err(error) = file.apply_to(&mut config) {
            runtime_file_application.applied = Some(Err(error.to_string()));
            return;
        }
    }
    runtime_file_application.applied = Some(Ok(config));
}

#[rstest_bdd_macros::then(
    "SASL accepts at most {limit} bytes and failed authentication waits {delay} milliseconds"
)]
fn runtime_sasl_limits(runtime_file_application: &RuntimeFileApplication, limit: u64, delay: u32) {
    let config = runtime_file_application
        .applied
        .as_ref()
        .expect("runtime files were applied")
        .as_ref()
        .expect("valid SASL configuration");
    assert!(config.sasl_server_max_receive.bytes_u64() == limit);
    assert!(config.connection_failed_authentication_delay == millis(delay));
    assert!(config.failed_authentication_delay() == Duration::from_millis(u64::from(delay)));
}

#[rstest_bdd_macros::then("runtime application refuses a field named {field}")]
fn runtime_application_refusal(runtime_file_application: &RuntimeFileApplication, field: String) {
    let Err(error) = runtime_file_application
        .applied
        .as_ref()
        .expect("runtime files were applied")
    else {
        panic!("runtime configuration must be refused");
    };
    assert!(error.contains(&field));
}

#[rstest_bdd_macros::then("the supplied Kafka configuration key origins are JSON {keys}")]
fn runtime_configuration_origins(runtime_file_application: &RuntimeFileApplication, keys: String) {
    let config = runtime_file_application
        .applied
        .as_ref()
        .expect("runtime files were applied")
        .as_ref()
        .expect("valid runtime configuration");
    let actual: Vec<_> = config
        .static_config_origins
        .supplied_kafka_keys
        .iter()
        .copied()
        .collect();
    let expected: Vec<String> =
        serde_json::from_str(&keys).expect("explicit ordered Kafka key list");
    assert!(actual == expected);
}

rstest_bdd_macros::scenarios!(
    "tests/features/migrated/src/file_config/runtime_config/tests",
    fixtures = [runtime_file_application: RuntimeFileApplication]
);
