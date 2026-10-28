//! Behaviour of the typed key registry: the rows every surface reads, the
//! wire bytes they carry, and the defaults they must agree with.

use {
    super::*,
    crate::config::{DEFAULT_TXN_ID_EXPIRATION, DEFAULT_TXN_ID_EXPIRATION_CLEANUP_INTERVAL},
    assert2::{assert, check},
    krabka_protocol::records::TimestampType,
    krabka_units::convert::{ByteSizeExt as _, RatioExt as _, TimeExt as _},
    rstest_bdd::datatable::Rows,
    std::collections::HashSet,
};

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

#[derive(Default)]
struct RegisteredConfigScenario {
    scope: Option<ConfigScope>,
    key: String,
    row: Option<&'static ConfigKey>,
    primitive_types: Vec<ConfigType>,
    primitive_encoding: Option<Vec<(String, i8)>>,
}

#[derive(rstest_bdd_macros::DataTableRow)]
struct ConfigTypeEncoding {
    label: String,
    wire: i8,
}

#[rstest::fixture]
fn registered_config() -> RegisteredConfigScenario {
    // Config metadata and primitive encodings are observed afresh per scenario.
    RegisteredConfigScenario::default()
}

fn expected_primitive_type(kind: &str) -> ConfigType {
    match kind {
        "boolean" => ConfigType::Boolean,
        "string" => ConfigType::String,
        "int" => ConfigType::Int,
        "long" => ConfigType::Long,
        "double" => ConfigType::Double,
        "list" => ConfigType::List,
        other => panic!("unknown expected config type {other}"),
    }
}

#[rstest_bdd_macros::given(
    "the six Kafka primitive config types Boolean, String, Int, Long, Double, and List"
)]
fn kafka_primitive_config_types(registered_config: &mut RegisteredConfigScenario) {
    registered_config.primitive_types = vec![
        ConfigType::Boolean,
        ConfigType::String,
        ConfigType::Int,
        ConfigType::Long,
        ConfigType::Double,
        ConfigType::List,
    ];
}

#[rstest_bdd_macros::given("the registered {scope} configuration key is {key}")]
fn registered_configuration_key(
    registered_config: &mut RegisteredConfigScenario,
    scope: String,
    key: String,
) {
    registered_config.scope = Some(match scope.as_str() {
        "topic" => ConfigScope::Topic,
        "broker" => ConfigScope::Broker,
        other => panic!("unknown config scope {other}"),
    });
    registered_config.key = key;
}

#[rstest_bdd_macros::when("the primitive config labels and DescribeConfigs wire types are encoded")]
fn encode_config_types(registered_config: &mut RegisteredConfigScenario) {
    registered_config.primitive_encoding = Some(
        registered_config
            .primitive_types
            .iter()
            .map(|kind| (kind.label().to_owned(), kind.wire()))
            .collect(),
    );
}

#[rstest_bdd_macros::when("DescribeConfigs reads the registered key's metadata")]
fn read_registered_configuration(registered_config: &mut RegisteredConfigScenario) {
    let scope = registered_config.scope.expect("the config scope was given");
    registered_config.row =
        Some(lookup(scope, &registered_config.key).expect("the key is registered"));
}

#[rstest_bdd_macros::then("the ordered primitive config labels and wire bytes are:")]
fn encoded_config_types(
    registered_config: &RegisteredConfigScenario,
    #[datatable] rows: Rows<ConfigTypeEncoding>,
) {
    let expected: Vec<_> = rows.into_iter().map(|row| (row.label, row.wire)).collect();
    assert!(registered_config.primitive_encoding.as_ref() == Some(&expected));
}

#[rstest_bdd_macros::then("the registered key's Kafka type is {kind}")]
fn registered_config_type(registered_config: &RegisteredConfigScenario, kind: String) {
    let row = registered_config.row.expect("registered metadata was read");
    check!(
        row.config_type == expected_primitive_type(&kind),
        "{}",
        registered_config.key
    );
}

#[rstest_bdd_macros::then("the registered key's quoted Kafka default is {value}")]
fn registered_config_default(registered_config: &RegisteredConfigScenario, value: String) {
    let expected: String = serde_json::from_str(&value).expect("the default is a quoted string");
    let row = registered_config.row.expect("registered metadata was read");
    check!(
        row.default == Some(expected.as_str()),
        "{}",
        registered_config.key
    );
}

#[rstest_bdd_macros::then("the registered key is read-only to dynamic alterations")]
fn registered_read_only_config(registered_config: &RegisteredConfigScenario) {
    let row = registered_config.row.expect("registered metadata was read");
    check!(row.read_only, "{}", registered_config.key);
}

rstest_bdd_macros::scenarios!(
    "tests/features/migrated/src/config_keys/registry/tests",
    fixtures = [registered_config: RegisteredConfigScenario]
);
