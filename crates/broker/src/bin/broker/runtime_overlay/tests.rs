//! Tests for the overlay of the command line onto a `BrokerConfig`.

use assert2::assert;
use clap::Parser;
use krabka_units::{convert::TimeExt as _, secs};

use super::*;
use crate::test_support::env_guard;

#[test]
fn runtime_policy_cli_rejects_invalid_and_accepts_valid_values() {
    let _guard = env_guard();

    let cases = [
        (vec!["krabka-broker", "--cleaner-interval=0ms"], false),
        (vec!["krabka-broker", "--cleaner-interval=1ms"], true),
        (
            vec![
                "krabka-broker",
                "--streams-internal-topic-replication-factor=0",
            ],
            false,
        ),
        (
            vec![
                "krabka-broker",
                "--streams-internal-topic-replication-factor=1",
            ],
            true,
        ),
        (vec!["krabka-broker", "--replication-fetch-min=0B"], false),
        (vec!["krabka-broker", "--replication-fetch-min=1B"], true),
        (
            vec!["krabka-broker", "--metadata-snapshot-fetch-max=0B"],
            false,
        ),
        (
            vec!["krabka-broker", "--metadata-snapshot-fetch-max=512MiB"],
            true,
        ),
        (
            vec!["krabka-broker", "--record-decompression-max-ratio=0"],
            false,
        ),
        (
            vec!["krabka-broker", "--record-decompression-max-ratio=50"],
            true,
        ),
        (
            vec!["krabka-broker", "--record-decompression-output-floor=0B"],
            false,
        ),
        (
            vec!["krabka-broker", "--client-dispatch-queue-capacity=0"],
            false,
        ),
        (
            vec!["krabka-broker", "--diskless-wal-local-replica-count=0"],
            false,
        ),
        (
            vec!["krabka-broker", "--diskless-wal-local-replica-count=5"],
            true,
        ),
        (
            vec!["krabka-broker", "--diskless-wal-flush-interval=0ms"],
            false,
        ),
        (
            vec!["krabka-broker", "--diskless-wal-flush-max-size=4MiB"],
            true,
        ),
        (
            vec!["krabka-broker", "--diskless-wal-trim-safety-lag=-1"],
            false,
        ),
        (
            vec!["krabka-broker", "--diskless-wal-trim-safety-lag=0"],
            true,
        ),
        (vec!["krabka-broker", "--client-frame-max=101MiB"], false),
        (
            vec![
                "krabka-broker",
                "--client-dispatch-queue-capacity=7",
                "--client-frame-max=32KiB",
            ],
            true,
        ),
        (
            vec![
                "krabka-broker",
                "--record-decompression-output-ceiling=512MiB",
            ],
            true,
        ),
    ];

    for (args, accepted) in cases {
        assert!(Args::try_parse_from(args).is_ok() == accepted);
    }

    let args = Args::try_parse_from(["krabka-broker", "--leader-imbalance-per-broker=101%"])
        .expect("parse ratio");
    assert!(
        args.apply_runtime_to(&mut BrokerConfig::default(), None)
            .is_err()
    );

    let args = Args::try_parse_from(["krabka-broker", "--record-decompression-max-ratio=101"])
        .expect("parse positive ratio");
    assert!(
        args.apply_runtime_to(&mut BrokerConfig::default(), None)
            .is_err()
    );

    let args = Args::try_parse_from(["krabka-broker", "--leader-imbalance-per-broker=100%"])
        .expect("parse ratio");
    assert!(
        args.apply_runtime_to(&mut BrokerConfig::default(), None)
            .is_ok()
    );

    let args = Args::try_parse_from(["krabka-broker", "--metadata-snapshot-fetch-max=1073741825B"])
        .expect("parse dimensioned over-ceiling size");
    assert!(
        args.apply_runtime_to(&mut BrokerConfig::default(), None)
            .is_err()
    );
}

#[test]
fn runtime_policy_cli_reads_krabka_environment() {
    let _guard = env_guard();

    temp_env::with_vars(
        [
            ("KRABKA_CLEANER_INTERVAL", Some("17ms")),
            ("KRABKA_SOCKET_REQUEST_MAX", Some("100MiB")),
            ("KRABKA_LEADER_IMBALANCE_PER_BROKER", Some("10%")),
            ("KRABKA_METADATA_SNAPSHOT_FETCH_MAX", Some("512MiB")),
            ("KRABKA_CONTROLLER_HEARTBEAT_INTERVAL", Some("500ms")),
            ("KRABKA_CONTROLLER_FETCH_MISS_LIMIT", Some("7")),
            ("KRABKA_METADATA_RAFT_COMMAND_QUEUE_CAPACITY", Some("512")),
            ("KRABKA_METADATA_RAFT_FETCH_MAX", Some("4MiB")),
            ("KRABKA_RECORD_DECOMPRESSION_MAX_RATIO", Some("50")),
            ("KRABKA_RECORD_DECOMPRESSION_OUTPUT_FLOOR", Some("8MiB")),
            ("KRABKA_RECORD_DECOMPRESSION_OUTPUT_CEILING", Some("512MiB")),
            ("KRABKA_LOG_READ_BUFFER_CAP", Some("2MiB")),
            ("KRABKA_LOG_TIMESTAMP_SCAN_WINDOW", Some("32KiB")),
            ("KRABKA_TRANSACTION_RECOVERY_READ_MAX", Some("3MiB")),
            ("KRABKA_DISKLESS_WAL_LOCAL_REPLICA_COUNT", Some("5")),
            ("KRABKA_DISKLESS_WAL_FLUSH_INTERVAL", Some("125ms")),
            ("KRABKA_DISKLESS_WAL_FLUSH_MAX_SIZE", Some("4MiB")),
            ("KRABKA_DISKLESS_WAL_TRIM_SAFETY_LAG", Some("0")),
            ("KRABKA_DISKLESS_WAL_INDEX_PROJECTION_TIMEOUT", Some("3s")),
            ("KRABKA_BROKER_CLIENT_DISPATCH_QUEUE_CAPACITY", Some("7")),
            ("KRABKA_BROKER_CLIENT_FRAME_MAX", Some("32KiB")),
        ],
        || {
            let args = Args::try_parse_from(["krabka-broker"]).expect("parse environment");
            assert!(args.runtime.cleaner_interval == Some(Time::from_millis(17)));
            assert!(args.runtime.socket_request_max == Some(krabka_units::mebibytes(100)));
            assert!(args.leader_imbalance_per_broker == Some(krabka_units::fraction(0.1)));
            assert!(args.metadata_snapshot_fetch_max == Some(krabka_units::mebibytes(512)));
            assert!(args.controller_fetch_miss_limit == Some(7));
            assert!(args.metadata_raft_command_queue_capacity == Some(512));
            assert!(args.metadata_raft_fetch_max == Some(krabka_units::mebibytes(4)));
            assert!(
                args.runtime.record_decompression_max_ratio == Some(krabka_units::fraction(50.0))
            );
            let mut config = BrokerConfig::default();
            args.apply_runtime_to(&mut config, None)
                .expect("apply environment runtime");
            assert!(config.metadata_snapshot_fetch_max == krabka_units::mebibytes(512));
            assert!(config.controller_heartbeat_interval_explicit);
            assert!(config.controller_heartbeat_interval == krabka_units::millis(500));
            assert!(config.controller_fetch_miss_limit.get() == 7);
            assert!(config.metadata_raft_command_queue_capacity.get() == 512);
            assert!(config.metadata_raft_fetch_max.bytes() == 4 * 1024 * 1024);
            assert!(
                config.record_decompression_policy().unwrap().output_floor()
                    == krabka_units::mebibytes(8)
            );
            assert!(config.log_config.read_buffer_cap == krabka_units::mebibytes(2));
            assert!(config.log_config.timestamp_scan_window == krabka_units::kibibytes(32));
            assert!(config.transaction_recovery_read_max == krabka_units::mebibytes(3));
            assert!(config.diskless_wal_local_replica_count == 5);
            assert!(config.diskless_wal_flush_interval == krabka_units::millis(125));
            assert!(config.diskless_wal_flush_max_size == krabka_units::mebibytes(4));
            assert!(config.diskless_wal_trim_safety_lag == 0);
            assert!(config.diskless_wal_index_projection_timeout == krabka_units::secs(3));
            assert!(config.client_dispatch_queue_capacity.get() == 7);
            assert!(config.client_frame_max.size() == krabka_units::kibibytes(32));
        },
    );
}

#[test]
fn client_resource_policy_defaults_and_cli_precedence() {
    let _guard = env_guard();

    let defaults = Args::try_parse_from(["krabka-broker"]).expect("parse defaults");
    let mut config = BrokerConfig::default();
    defaults
        .apply_runtime_to(&mut config, None)
        .expect("apply defaults");
    assert!(config.client_dispatch_queue_capacity.get() == 64);
    assert!(config.client_frame_max.size() == krabka_units::mebibytes(100));

    temp_env::with_vars(
        [
            ("KRABKA_BROKER_CLIENT_DISPATCH_QUEUE_CAPACITY", Some("7")),
            ("KRABKA_BROKER_CLIENT_FRAME_MAX", Some("32KiB")),
        ],
        || {
            let args = Args::try_parse_from([
                "krabka-broker",
                "--client-dispatch-queue-capacity=9",
                "--client-frame-max=64KiB",
            ])
            .expect("parse CLI overrides");
            let mut config = BrokerConfig::default();
            args.apply_runtime_to(&mut config, None)
                .expect("apply CLI overrides");
            assert!(config.client_dispatch_queue_capacity.get() == 9);
            assert!(config.client_frame_max.size() == krabka_units::kibibytes(64));
        },
    );
}

#[test]
fn group_member_limits_and_streams_switch_apply_from_cli() {
    let _guard = env_guard();

    let args = Args::try_parse_from([
        "krabka-broker",
        "--share-group-max-size=17",
        "--streams-group-enable=false",
        "--streams-group-max-size=19",
    ])
    .expect("parse group limits");
    let mut config = BrokerConfig::default();

    args.apply_runtime_to(&mut config, None)
        .expect("apply group limits");

    assert!(config.share_group.max_size == 17);
    assert!(!config.streams_group.enable);
    assert!(config.streams_group.max_size == 19);
}

fn file_runtime_with_nondefault_values() -> krabka_broker::file_config::FileConfig {
    toml::from_str(
        r#"
        [runtime]
        cleaner_interval = "7s"
        controlled_shutdown_drain_timeout = "9s"
        auto_join_voter_request_timeout = "9s"
        share_state_replication_factor = 2
        transaction_state_replication_factor = 2
        streams_internal_topic_replication_factor = 2
        "#,
    )
    .expect("parse runtime file config")
}

#[test]
fn explicit_cli_default_runtime_values_override_file() {
    let _guard = env_guard();

    let args = Args::try_parse_from([
        "krabka-broker",
        "--cleaner-interval=30s",
        "--controlled-shutdown-drain-timeout=20s",
        "--auto-join-voter-request-timeout=30s",
        "--share-state-replication-factor=3",
        "--transaction-state-replication-factor=3",
        "--streams-internal-topic-replication-factor=3",
    ])
    .expect("parse explicit CLI defaults");
    let mut config = BrokerConfig::default();
    let file = file_runtime_with_nondefault_values();
    let file_shutdown = file
        .runtime
        .as_ref()
        .and_then(|runtime| runtime.controlled_shutdown_drain_timeout);
    file.apply_to(&mut config).expect("apply file runtime");

    let shutdown = args
        .apply_runtime_to(&mut config, file_shutdown)
        .expect("overlay CLI runtime");

    assert!(
        (
            config.cleaner_interval,
            shutdown,
            config.auto_join_voter_request_timeout,
            config.share_coordinator.state_topic_replication_factor,
            config.transaction_state_replication_factor,
            config.streams_group.internal_topic_replication_factor,
        ) == (secs(30), secs(20), secs(30), 3, 3, 3)
    );
}

#[test]
fn explicit_env_default_runtime_values_override_file() {
    let _guard = env_guard();

    temp_env::with_vars(
        [
            ("KRABKA_CLEANER_INTERVAL", Some("30s")),
            ("KRABKA_CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT", Some("20s")),
            ("KRABKA_AUTO_JOIN_VOTER_REQUEST_TIMEOUT", Some("30s")),
            ("KRABKA_SHARE_STATE_REPLICATION_FACTOR", Some("3")),
            ("KRABKA_TRANSACTION_STATE_REPLICATION_FACTOR", Some("3")),
            (
                "KRABKA_STREAMS_INTERNAL_TOPIC_REPLICATION_FACTOR",
                Some("3"),
            ),
        ],
        || {
            let args = Args::try_parse_from(["krabka-broker"]).expect("parse env defaults");
            let mut config = BrokerConfig::default();
            let file = file_runtime_with_nondefault_values();
            let file_shutdown = file
                .runtime
                .as_ref()
                .and_then(|runtime| runtime.controlled_shutdown_drain_timeout);
            file.apply_to(&mut config).expect("apply file runtime");

            let shutdown = args
                .apply_runtime_to(&mut config, file_shutdown)
                .expect("overlay env runtime");

            assert!(
                (
                    config.cleaner_interval,
                    shutdown,
                    config.auto_join_voter_request_timeout,
                    config.share_coordinator.state_topic_replication_factor,
                    config.transaction_state_replication_factor,
                    config.streams_group.internal_topic_replication_factor,
                ) == (secs(30), secs(20), secs(30), 3, 3, 3)
            );
        },
    );
}
