//! Behaviour of the typed key registry: the rows every surface reads, the
//! wire bytes they carry, and the defaults they must agree with.

use {
    super::*,
    crate::{
        config::{DEFAULT_TXN_ID_EXPIRATION, DEFAULT_TXN_ID_EXPIRATION_CLEANUP_INTERVAL},
        throttle::{
            ALTER_LOG_DIRS_THROTTLED_RATE_KEY, FOLLOWER_THROTTLED_RATE_KEY,
            FOLLOWER_THROTTLED_REPLICAS_KEY, LEADER_THROTTLED_RATE_KEY,
            LEADER_THROTTLED_REPLICAS_KEY,
        },
    },
    assert2::{assert, check},
    krabka_protocol::records::TimestampType,
    krabka_units::convert::{ByteSizeExt as _, RatioExt as _, TimeExt as _},
    std::collections::HashSet,
};

#[cfg(test)]
#[rstest_bdd_macros::then("config keys registry tests: config type wire bytes match kafka")]
fn bdd_verify_config_type_wire_bytes_match_kafka() {
    // `DescribeConfigsResponse.ConfigType`, verified against
    // `apache/kafka:4.3.1` over the wire.
    #[derive(Debug, PartialEq, Eq)]
    struct ConfigTypeBytes {
        label: &'static str,
        wire: i8,
    }
    let observed: Vec<ConfigTypeBytes> = [
        ConfigType::Boolean,
        ConfigType::String,
        ConfigType::Int,
        ConfigType::Long,
        ConfigType::Double,
        ConfigType::List,
    ]
    .into_iter()
    .map(|config_type| ConfigTypeBytes {
        label: config_type.label(),
        wire: config_type.wire(),
    })
    .collect();

    assert!(
        observed
            == [
                ("boolean", 1),
                ("string", 2),
                ("int", 3),
                ("long", 5),
                ("double", 6),
                ("list", 7),
            ]
            .map(|(label, wire)| ConfigTypeBytes { label, wire })
    );
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/config_keys/registry/tests.feature",
    name = "config type wire bytes match kafka"
)]
fn config_type_wire_bytes_match_kafka() {}

#[cfg(test)]
#[rstest_bdd_macros::then(
    "config keys registry tests: topic key types match apache kafka for {example}"
)]
fn bdd_verify_topic_key_types_match_apache_kafka(example: String) {
    type Example<'a> = (&'a str, ConfigType);
    let (name, expected): Example<'_> = match example.as_str() {
        "retention ms" => (RETENTION_MS, ConfigType::Long),
        "retention bytes" => (RETENTION_BYTES, ConfigType::Long),
        "segment bytes" => (SEGMENT_BYTES, ConfigType::Int),
        "cleanup policy" => (CLEANUP_POLICY, ConfigType::List),
        "compression type" => (COMPRESSION_TYPE, ConfigType::String),
        "min insync replicas" => (MIN_INSYNC_REPLICAS, ConfigType::Int),
        "unclean leader election enable" => (UNCLEAN_LEADER_ELECTION_ENABLE, ConfigType::Boolean),
        "remote storage enable" => (REMOTE_STORAGE_ENABLE, ConfigType::Boolean),
        "local retention ms" => (LOCAL_RETENTION_MS, ConfigType::Long),
        "local retention bytes" => (LOCAL_RETENTION_BYTES, ConfigType::Long),
        "delete retention ms" => (DELETE_RETENTION_MS, ConfigType::Long),
        "leader throttled replicas" => (LEADER_THROTTLED_REPLICAS_KEY, ConfigType::List),
        "follower throttled replicas" => (FOLLOWER_THROTTLED_REPLICAS_KEY, ConfigType::List),
        "segment ms" => (SEGMENT_MS, ConfigType::Long),
        "segment index bytes" => (SEGMENT_INDEX_BYTES, ConfigType::Int),
        "segment jitter ms" => (SEGMENT_JITTER_MS, ConfigType::Long),
        "min compaction lag ms" => (MIN_COMPACTION_LAG_MS, ConfigType::Long),
        "max compaction lag ms" => (MAX_COMPACTION_LAG_MS, ConfigType::Long),
        "min cleanable dirty ratio" => (MIN_CLEANABLE_DIRTY_RATIO, ConfigType::Double),
        "file delete delay ms" => (FILE_DELETE_DELAY_MS, ConfigType::Long),
        "flush messages" => (FLUSH_MESSAGES, ConfigType::Long),
        "flush ms" => (FLUSH_MS, ConfigType::Long),
        "index interval bytes" => (INDEX_INTERVAL_BYTES, ConfigType::Int),
        "preallocate" => (PREALLOCATE, ConfigType::Boolean),
        "message timestamp type" => (MESSAGE_TIMESTAMP_TYPE, ConfigType::String),
        "message timestamp after max ms" => (MESSAGE_TIMESTAMP_AFTER_MAX_MS, ConfigType::Long),
        "message timestamp before max ms" => (MESSAGE_TIMESTAMP_BEFORE_MAX_MS, ConfigType::Long),
        _ => panic!("unknown BDD example: {example}"),
    };

    // Read off `apache/kafka:4.3.1` with a DescribeConfigs v3 probe that asked
    // for synonyms and documentation. Every row here is a key Kafka also has,
    // so the byte the JVM AdminClient reads must be the same one.
    let row = lookup(ConfigScope::Topic, name).expect(name);
    check!(row.config_type == expected, "{name}");
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/config_keys/registry/tests.feature",
    name = "topic key types match apache kafka"
)]
fn topic_key_types_match_apache_kafka(example: String) { let _ = example; }

#[cfg(test)]
#[rstest_bdd_macros::then(
    "config keys registry tests: broker key types match apache kafka for {example}"
)]
fn bdd_verify_broker_key_types_match_apache_kafka(example: String) {
    type Example<'a> = (&'a str, ConfigType);
    let (name, expected): Example<'_> = match example.as_str() {
        "leader throttled rate" => (LEADER_THROTTLED_RATE_KEY, ConfigType::Long),
        "follower throttled rate" => (FOLLOWER_THROTTLED_RATE_KEY, ConfigType::Long),
        "alter log dirs throttled rate" => (ALTER_LOG_DIRS_THROTTLED_RATE_KEY, ConfigType::Long),
        "unclean leader election enable" => (UNCLEAN_LEADER_ELECTION_ENABLE, ConfigType::Boolean),
        "remote list offsets request timeout ms" => {
            (REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS, ConfigType::Long)
        }
        "offsets retention minutes" => (OFFSETS_RETENTION_MINUTES, ConfigType::Int),
        "offsets retention check interval ms" => {
            (OFFSETS_RETENTION_CHECK_INTERVAL_MS, ConfigType::Long)
        }
        "connections max idle ms" => (CONNECTIONS_MAX_IDLE_MS, ConfigType::Long),
        "connections max reauth ms" => (CONNECTIONS_MAX_REAUTH_MS, ConfigType::Long),
        "node id" => (NODE_ID, ConfigType::Int),
        "transactional id expiration ms" => (TRANSACTIONAL_ID_EXPIRATION_MS, ConfigType::Int),
        "transaction remove expired cleanup interval ms" => (
            TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
            ConfigType::Int,
        ),
        _ => panic!("unknown BDD example: {example}"),
    };

    let row = lookup(ConfigScope::Broker, name).expect(name);
    check!(row.config_type == expected, "{name}");
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/config_keys/registry/tests.feature",
    name = "broker key types match apache kafka"
)]
fn broker_key_types_match_apache_kafka(example: String) { let _ = example; }

#[cfg(test)]
#[rstest_bdd_macros::then(
    "config keys registry tests: topic key defaults match apache kafka for {example}"
)]
fn bdd_verify_topic_key_defaults_match_apache_kafka(example: String) {
    type Example<'a> = (&'a str, &'a str);
    let (name, expected): Example<'_> = match example.as_str() {
        "retention ms" => (RETENTION_MS, "604800000"),
        "retention bytes" => (RETENTION_BYTES, "-1"),
        "segment bytes" => (SEGMENT_BYTES, "1073741824"),
        "cleanup policy" => (CLEANUP_POLICY, "delete"),
        "compression type" => (COMPRESSION_TYPE, "producer"),
        "min insync replicas" => (MIN_INSYNC_REPLICAS, "1"),
        "unclean leader election enable" => (UNCLEAN_LEADER_ELECTION_ENABLE, "false"),
        "remote storage enable" => (REMOTE_STORAGE_ENABLE, "false"),
        "local retention ms" => (LOCAL_RETENTION_MS, "-2"),
        "local retention bytes" => (LOCAL_RETENTION_BYTES, "-2"),
        "delete retention ms" => (DELETE_RETENTION_MS, "86400000"),
        "leader throttled replicas" => (LEADER_THROTTLED_REPLICAS_KEY, ""),
        "follower throttled replicas" => (FOLLOWER_THROTTLED_REPLICAS_KEY, ""),
        "segment ms" => (SEGMENT_MS, "604800000"),
        "segment index bytes" => (SEGMENT_INDEX_BYTES, "10485760"),
        "segment jitter ms" => (SEGMENT_JITTER_MS, "0"),
        "min compaction lag ms" => (MIN_COMPACTION_LAG_MS, "0"),
        "max compaction lag ms" => (MAX_COMPACTION_LAG_MS, "9223372036854775807"),
        "min cleanable dirty ratio" => (MIN_CLEANABLE_DIRTY_RATIO, "0.5"),
        "file delete delay ms" => (FILE_DELETE_DELAY_MS, "60000"),
        "flush messages" => (FLUSH_MESSAGES, "9223372036854775807"),
        "flush ms" => (FLUSH_MS, "9223372036854775807"),
        "index interval bytes" => (INDEX_INTERVAL_BYTES, "4096"),
        "preallocate" => (PREALLOCATE, "false"),
        "message timestamp type" => (MESSAGE_TIMESTAMP_TYPE, "CreateTime"),
        "message timestamp after max ms" => (MESSAGE_TIMESTAMP_AFTER_MAX_MS, "3600000"),
        "message timestamp before max ms" => {
            (MESSAGE_TIMESTAMP_BEFORE_MAX_MS, "9223372036854775807")
        }
        _ => panic!("unknown BDD example: {example}"),
    };

    // The same probe's `DEFAULT_CONFIG` values on a topic with no overrides.
    let row = lookup(ConfigScope::Topic, name).expect(name);
    check!(row.default == Some(expected), "{name}");
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/config_keys/registry/tests.feature",
    name = "topic key defaults match apache kafka"
)]
fn topic_key_defaults_match_apache_kafka(example: String) { let _ = example; }

#[test]
fn topic_defaults_agree_with_the_log_config_a_partition_runs_on() {
    // The registry states the default a topic reports; `LogConfig::default()`
    // states the value a partition actually runs with. A partition running on
    // a value `DescribeConfigs` does not report is the bug this pins.
    let log = krabka_log::LogConfig::default();
    let default_of = |name| lookup(ConfigScope::Topic, name).expect(name).default;

    check!(
        default_of(RETENTION_MS)
            == Some(
                log.retention
                    .map_or_else(|| "-1".to_owned(), |window| window.millis_i64().to_string())
                    .as_str()
            )
    );
    check!(default_of(SEGMENT_BYTES) == Some(log.segment_size.bytes_u64().to_string().as_str()));
    check!(
        default_of(DELETE_RETENTION_MS)
            == Some(log.delete_retention.millis_i64().to_string().as_str())
    );
    check!(default_of(CLEANUP_POLICY) == Some(log.cleanup_policy.as_str()));
    check!(
        default_of(SEGMENT_MS) == Some(log.segment_roll_interval.millis_i64().to_string().as_str())
    );
    check!(
        default_of(INDEX_INTERVAL_BYTES)
            == Some(log.index_interval.bytes_u64().to_string().as_str())
    );
    check!(
        default_of(MIN_COMPACTION_LAG_MS)
            == Some(log.min_compaction_lag.millis_i64().to_string().as_str())
    );
    // Kafka spells "no bound" as `Long.MAX_VALUE`; the log carries it as
    // `None`, and the registry has to report the number an operator reads.
    check!(
        log.max_compaction_lag == None
            && default_of(MAX_COMPACTION_LAG_MS) == Some("9223372036854775807")
    );
    check!(
        default_of(MIN_CLEANABLE_DIRTY_RATIO)
            == Some(log.min_cleanable_dirty_ratio.as_f64().to_string().as_str())
    );
    check!(
        log.message_timestamp_type == TimestampType::CreateTime
            && default_of(MESSAGE_TIMESTAMP_TYPE) == Some("CreateTime")
    );
    check!(log.compression_type.is_none() && default_of(COMPRESSION_TYPE) == Some("producer"));
    check!(
        default_of(REMOTE_STORAGE_ENABLE)
            == Some(if log.remote_storage_enable {
                "true"
            } else {
                "false"
            })
    );
}

#[test]
fn every_row_is_unique_in_its_scope_and_carries_documentation() {
    let mut seen = HashSet::new();
    for row in CONFIG_KEYS {
        check!(
            seen.insert((row.scope, row.name)),
            "duplicate row for {:?} {}",
            row.scope,
            row.name
        );
        check!(!row.doc.is_empty(), "{} has no documentation", row.name);
    }
}

#[test]
fn value_type_renders_the_type_and_its_note() {
    let retention = lookup(ConfigScope::Topic, RETENTION_MS).expect(RETENTION_MS);
    let policy = lookup(ConfigScope::Topic, CLEANUP_POLICY).expect(CLEANUP_POLICY);

    check!(retention.value_type() == "long (ms)");
    check!(policy.value_type() == "list");
}

#[test]
fn the_synthesised_keys_are_the_only_unstored_rows() {
    let unstored: Vec<&str> = CONFIG_KEYS
        .iter()
        .filter(|row| !row.is_stored())
        .map(|row| row.name)
        .collect();

    assert!(
        unstored
            == vec![
                WRITE_FREEZE,
                OFFSETS_RETENTION_MINUTES,
                OFFSETS_RETENTION_CHECK_INTERVAL_MS,
                CONNECTIONS_MAX_IDLE_MS,
                CONNECTIONS_MAX_REAUTH_MS,
                NUM_PARTITIONS,
                DEFAULT_REPLICATION_FACTOR,
                DELETE_TOPIC_ENABLE,
                AUTO_CREATE_TOPICS_ENABLE,
                TRANSACTIONAL_ID_EXPIRATION_MS,
                TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
                NODE_ID,
            ]
    );
}

#[cfg(test)]
#[rstest_bdd_macros::then(
    "config keys registry tests: transactional id expiry defaults match apache kafka for {example}"
)]
fn bdd_verify_transactional_id_expiry_defaults_match_apache_kafka(example: String) {
    type Example<'a> = (&'a str, &'a str);
    let (name, expected): Example<'_> = match example.as_str() {
        "transactional id expiration" => (TRANSACTIONAL_ID_EXPIRATION_MS, "604800000"),
        "transaction remove expired cleanup interval" => {
            (TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS, "3600000")
        }
        _ => panic!("unknown BDD example: {example}"),
    };

    let row = lookup(ConfigScope::Broker, name).expect(name);
    check!(row.default == Some(expected), "{name}");
    // `kafka-configs --alter` answers `Cannot update these configs
    // dynamically`, so `DescribeConfigs` must report them read-only.
    check!(row.read_only, "{name}");
}

/// The two KIP-98 keys report Kafka's own defaults, so a broker that never
/// touched either one answers `--describe --all` the way a Kafka broker does.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/config_keys/registry/tests.feature",
    name = "transactional id expiry defaults match apache kafka"
)]
fn transactional_id_expiry_defaults_match_apache_kafka(example: String) { let _ = example; }

/// The defaults the registry reports are the values the broker actually runs
/// with. A broker sweeping on a cadence `DescribeConfigs` does not report is
/// the drift this pins.
#[test]
fn transactional_id_expiry_defaults_agree_with_the_broker_config() {
    let default_of = |name| lookup(ConfigScope::Broker, name).expect(name).default;

    check!(
        default_of(TRANSACTIONAL_ID_EXPIRATION_MS)
            == Some(DEFAULT_TXN_ID_EXPIRATION.millis_i64().to_string().as_str())
    );
    check!(
        default_of(TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS)
            == Some(
                DEFAULT_TXN_ID_EXPIRATION_CLEANUP_INTERVAL
                    .millis_i64()
                    .to_string()
                    .as_str()
            )
    );
}

#[test]
fn a_numeric_check_is_as_wide_as_the_type_the_row_advertises() {
    // The width a row's check enforces is the width the JVM `AdminClient`
    // parses the value with. A `ConfigType::Int` row that accepted an `i64`
    // would tell a typed client that a value it cannot represent is valid, so
    // no `Int` row may carry a 64-bit floor and no `Long` row a 32-bit one.
    let mismatched: Vec<&str> = CONFIG_KEYS
        .iter()
        .filter(|row| {
            matches!(
                (row.config_type, row.check),
                (ConfigType::Int, ValueCheck::I64AtLeast(_))
                    | (
                        ConfigType::Long,
                        ValueCheck::I32AtLeast(_) | ValueCheck::I32Between(..)
                    )
            )
        })
        .map(|row| row.name)
        .collect();

    assert!(mismatched == Vec::<&str>::new());
}

#[test]
fn no_key_krabka_has_today_is_sensitive() {
    // The redaction path still has to hold before a secret-valued key exists,
    // which `handlers::describe_configs::entry` tests directly.
    assert!(!CONFIG_KEYS.iter().any(ConfigKey::is_sensitive));
}

#[test]
fn a_scope_reports_only_its_own_rows() {
    // `unclean.leader.election.enable` is both a topic key and a
    // cluster-default broker key, and the two rows differ.
    let topic = lookup(ConfigScope::Topic, UNCLEAN_LEADER_ELECTION_ENABLE)
        .expect("topic unclean.leader.election.enable");
    let broker = lookup(ConfigScope::Broker, UNCLEAN_LEADER_ELECTION_ENABLE)
        .expect("broker unclean.leader.election.enable");

    check!(topic.cluster_default == Some(UNCLEAN_LEADER_ELECTION_ENABLE));
    check!(broker.cluster_default == None);
    check!(lookup(ConfigScope::Group, UNCLEAN_LEADER_ELECTION_ENABLE).is_none());
    check!(keys_in(ConfigScope::ClientMetrics).count() == 3);
}
