//! Tests for the per-key and whole-map topic-config validators.

use {
    super::{
        super::{
            DELETE_RETENTION_MS, INTERNAL_SEGMENT_BYTES, LOCAL_RETENTION_BYTES, LOCAL_RETENTION_MS,
            MAX_COMPACTION_LAG_MS, MESSAGE_TIMESTAMP_TYPE, MIN_COMPACTION_LAG_MS,
            MIN_INSYNC_REPLICAS, REMOTE_LOG_COPY_DISABLE, REMOTE_LOG_DELETE_ON_DISABLE,
            REMOTE_STORAGE_ENABLE, RETENTION_BYTES, RETENTION_MS, SEGMENT_BYTES,
            delivery::DELIVERY_MODE_IMMEDIATE,
        },
        *,
    },
    crate::throttle::LEADER_THROTTLED_REPLICAS_KEY,
    assert2::check,
    krabka_log::CleanupPolicy,
    krabka_metadata::{
        BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataImage, MetadataRecord,
    },
    rstest_bdd::{Slot, datatable::Rows},
    std::collections::BTreeMap,
    uuid::Uuid,
};

/// A config key and its value.
#[derive(Debug, Clone, Copy)]
struct ConfigPair<'a> {
    key: &'a str,
    value: &'a str,
}

/// The owned map of `pairs`.
fn config_map(pairs: &[ConfigPair<'_>]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|ConfigPair { key, value }| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

/// A name Kafka has no `TopicConfig` entry for, which its own `LogConfig`
/// refuses with `Unknown topic config name`. The probe every unknown-key test
/// uses, so a key krabka later registers cannot quietly turn one of them into
/// a test of nothing.
const UNKNOWN_KEY: &str = "not.a.topic.config";

#[yare::parameterized(
    positive = { "60000", true },
    unlimited = { "-1", true },
    below_unlimited = { "-5", false },
    non_integer = { "abc", false },
)]
fn validate_retention_ms_boundary_cases(value: &str, want_ok: bool) {
    assert2::assert!(
        validate_topic_config(RETENTION_MS, value).is_ok() == want_ok,
        "retention.ms={value}"
    );
}

#[test]
fn internal_segment_bytes_is_not_subject_to_the_topic_floor() {
    // The floor is the topic key's alone. `internal.segment.bytes` is
    // `defineInternal`'d with a null validator, so it still reaches below it.
    assert2::assert!(validate_topic_config(INTERNAL_SEGMENT_BYTES, "1").is_ok());
}

#[test]
fn validate_compression_bogus_rejected() {
    let err = validate_topic_config(COMPRESSION_TYPE, "bzip3").unwrap_err();
    assert2::assert!(err.contains("compression.type"), "got: {err}");
}

#[test]
fn parse_compression_type_maps_producer_to_none() {
    assert2::assert!(parse_compression_type("producer") == Ok(None));
}

#[yare::parameterized(
    gzip = { "gzip", krabka_compression::CompressionType::Gzip },
    snappy = { "snappy", krabka_compression::CompressionType::Snappy },
    lz4 = { "lz4", krabka_compression::CompressionType::Lz4 },
    zstd = { "zstd", krabka_compression::CompressionType::Zstd },
    uncompressed = { "uncompressed", krabka_compression::CompressionType::None },
)]
fn parse_compression_type_maps_codecs(input: &str, want: krabka_compression::CompressionType) {
    assert2::assert!(
        parse_compression_type(input) == Ok(Some(want)),
        "compression.type={input}"
    );
}

#[test]
fn parse_compression_type_rejects_none() {
    assert2::assert!(parse_compression_type("none").is_err());
}

#[test]
fn validate_min_isr_positive_accepted() {
    assert2::assert!(validate_topic_config(MIN_INSYNC_REPLICAS, "2").is_ok());
}

#[test]
fn validate_unknown_key_rejected() {
    assert2::assert!(
        validate_topic_config(UNKNOWN_KEY, "1000")
            == Err(format!("Unknown topic config name: {UNKNOWN_KEY}"))
    );
}

/// Streams applications send both override sets on their internal topics.
#[yare::parameterized(
    repartition_topic = {
        maplit::btreemap! {
            CLEANUP_POLICY.to_owned() => "delete".to_owned(),
            SEGMENT_BYTES.to_owned() => "52428800".to_owned(),
            RETENTION_MS.to_owned() => "-1".to_owned(),
            MESSAGE_TIMESTAMP_TYPE.to_owned() => "CreateTime".to_owned(),
        },
    },
    windowed_changelog = {
        maplit::btreemap! {
            CLEANUP_POLICY.to_owned() => "compact,delete".to_owned(),
            RETENTION_MS.to_owned() => "86400000".to_owned(),
            MIN_COMPACTION_LAG_MS.to_owned() => "0".to_owned(),
            MESSAGE_TIMESTAMP_TYPE.to_owned() => "CreateTime".to_owned(),
        },
    },
)]
fn streams_topic_override_set_is_accepted(overrides: BTreeMap<String, String>) {
    assert2::assert!(validate_topic_config_map(&overrides) == Ok(()));
}

#[yare::parameterized(
    zero = { "0", true },
    midpoint = { "0.5", true },
    one = { "1", true },
    above_one = { "1.0001", false },
    below_zero = { "-0.1", false },
    nan = { "NaN", false },
    non_numeric = { "half", false },
)]
fn validate_dirty_ratio_accepts_the_closed_unit_interval(value: &str, want_ok: bool) {
    check!(
        validate_topic_config(MIN_CLEANABLE_DIRTY_RATIO, value).is_ok() == want_ok,
        "min.cleanable.dirty.ratio={value}"
    );
}

#[test]
fn validate_remote_storage_enable_accepts_bools() {
    assert2::assert!(validate_topic_config(REMOTE_STORAGE_ENABLE, "true").is_ok());
    assert2::assert!(validate_topic_config(REMOTE_STORAGE_ENABLE, "false").is_ok());
}

#[test]
fn validate_remote_storage_enable_rejects_junk() {
    let err = validate_topic_config(REMOTE_STORAGE_ENABLE, "yes").unwrap_err();
    assert2::assert!(err.contains("remote.storage.enable"), "got: {err}");
}

#[test]
fn is_recognized_includes_remote_storage_enable() {
    assert2::assert!(is_recognized(REMOTE_STORAGE_ENABLE));
}

#[yare::parameterized(
    retention_ms = { RETENTION_MS, true },
    retention_bytes = { RETENTION_BYTES, true },
    segment_bytes = { SEGMENT_BYTES, true },
    cleanup_policy = { CLEANUP_POLICY, true },
    compression_type = { COMPRESSION_TYPE, true },
    min_insync_replicas = { MIN_INSYNC_REPLICAS, true },
    unknown = { UNKNOWN_KEY, false },
    empty = { "", false },
)]
fn is_recognized_matches_whitelist(key: &str, want: bool) {
    assert2::assert!(is_recognized(key) == want, "key {key:?}");
}

#[yare::parameterized(
    minus_two = { "-2" },
    minus_one = { "-1" },
    positive = { "60000" },
)]
fn validate_local_retention_ms_accepts_minus_one_minus_two_and_positive(value: &str) {
    assert2::assert!(
        validate_topic_config(LOCAL_RETENTION_MS, value) == Ok(()),
        "local.retention.ms={value}"
    );
}

#[test]
fn validate_local_retention_ms_rejects_below_minus_two() {
    assert2::assert!(validate_topic_config(LOCAL_RETENTION_MS, "-3").is_err());
}

#[test]
fn is_recognized_includes_local_retention_keys() {
    assert2::assert!(is_recognized(LOCAL_RETENTION_MS));
    assert2::assert!(is_recognized(LOCAL_RETENTION_BYTES));
}

#[yare::parameterized(
    zero = { "0", true },
    positive = { "86400000", true },
    negative = { "-1", false },
)]
fn validate_delete_retention_ms_accepts_nonneg_rejects_negative(value: &str, want_ok: bool) {
    assert2::assert!(
        validate_topic_config(DELETE_RETENTION_MS, value).is_ok() == want_ok,
        "delete.retention.ms={value}"
    );
}

#[test]
fn is_recognized_includes_delete_retention_ms() {
    assert2::assert!(is_recognized(DELETE_RETENTION_MS));
}

#[test]
fn compact_and_scheduled_delivery_exclude_each_other() {
    let cases = [
        (Some("compact"), Some(DELIVERY_MODE_SCHEDULED), false),
        (Some("compact"), Some(DELIVERY_MODE_IMMEDIATE), true),
        (Some("compact"), None, true),
        (Some("delete"), Some(DELIVERY_MODE_SCHEDULED), true),
        (None, Some(DELIVERY_MODE_SCHEDULED), true),
        (None, None, true),
    ];
    for (policy, mode, want_ok) in cases {
        let mut overrides = BTreeMap::new();
        if let Some(policy) = policy {
            overrides.insert(CLEANUP_POLICY.to_owned(), policy.to_owned());
        }
        if let Some(mode) = mode {
            overrides.insert(DELIVERY_MODE.to_owned(), mode.to_owned());
        }
        assert2::assert!(
            validate_config_combination(&overrides, &TopicDefaults::default(), true).is_ok()
                == want_ok,
            "overrides {overrides:?}"
        );
    }
}

/// Kafka's `LogConfig.validateValues`: a `min.compaction.lag.ms` above
/// `max.compaction.lag.ms` is refused, and an alter that names only one of the
/// two is checked against the other's stored default, which is what Kafka's
/// fully-defaulted property map carries.
#[test]
fn a_min_compaction_lag_above_the_max_is_refused() {
    let cases = [
        ("both, min below max", Some("1000"), Some("60000"), true),
        ("both, equal", Some("60000"), Some("60000"), true),
        ("both, min above max", Some("60000"), Some("1000"), false),
        (
            "min alone against the unbounded default",
            Some("60000"),
            None,
            true,
        ),
        ("max alone against the zero default", None, Some("1"), true),
        ("neither", None, None, true),
    ];
    for (case, min, max, want_ok) in cases {
        let mut overrides = BTreeMap::new();
        if let Some(min) = min {
            overrides.insert(MIN_COMPACTION_LAG_MS.to_owned(), min.to_owned());
        }
        if let Some(max) = max {
            overrides.insert(MAX_COMPACTION_LAG_MS.to_owned(), max.to_owned());
        }
        assert2::assert!(
            validate_config_combination(&overrides, &TopicDefaults::default(), true).is_ok()
                == want_ok,
            "{case}: {overrides:?}"
        );
    }
}

/// One row of the cross-key table.
struct CrossKeyCase {
    /// The cluster-wide broker configs.
    cluster: Vec<ConfigPair<'static>>,
    /// The topic's overrides.
    topic: Vec<ConfigPair<'static>>,
    want: Result<(), String>,
}

fn cross_key_cases() -> Vec<CrossKeyCase> {
    let local_ms_over = |value: i64, total: i64| {
        Err(format!(
            "Invalid value {value} for configuration local.retention.ms: Value must not be \
             more than retention.ms property value: {total}"
        ))
    };
    let cases: Vec<CrossKeyCase> = vec![
        CrossKeyCase {
            cluster: vec![ConfigPair {
                key: "log.cleanup.policy",
                value: "compact",
            }],
            topic: vec![ConfigPair {
                key: REMOTE_STORAGE_ENABLE,
                value: "true",
            }],
            want: Err(REMOTE_STORAGE_POLICY_MESSAGE.to_owned()),
        },
        CrossKeyCase {
            cluster: vec![ConfigPair {
                key: "log.cleanup.policy",
                value: "compact",
            }],
            topic: vec![
                ConfigPair {
                    key: REMOTE_STORAGE_ENABLE,
                    value: "true",
                },
                ConfigPair {
                    key: CLEANUP_POLICY,
                    value: "delete",
                },
            ],
            want: Ok(()),
        },
        CrossKeyCase {
            cluster: vec![ConfigPair {
                key: "log.cleanup.policy",
                value: "compact",
            }],
            topic: vec![ConfigPair {
                key: REMOTE_STORAGE_ENABLE,
                value: "false",
            }],
            want: Ok(()),
        },
        CrossKeyCase {
            cluster: vec![ConfigPair {
                key: "log.retention.ms",
                value: "1000",
            }],
            topic: vec![
                ConfigPair {
                    key: REMOTE_STORAGE_ENABLE,
                    value: "true",
                },
                ConfigPair {
                    key: LOCAL_RETENTION_MS,
                    value: "2000",
                },
            ],
            want: local_ms_over(2000, 1000),
        },
        CrossKeyCase {
            cluster: vec![ConfigPair {
                key: "log.retention.hours",
                value: "1",
            }],
            topic: vec![
                ConfigPair {
                    key: REMOTE_STORAGE_ENABLE,
                    value: "true",
                },
                ConfigPair {
                    key: LOCAL_RETENTION_MS,
                    value: "3600001",
                },
            ],
            want: local_ms_over(3_600_001, 3_600_000),
        },
        CrossKeyCase {
            cluster: vec![
                ConfigPair {
                    key: "log.retention.hours",
                    value: "1",
                },
                ConfigPair {
                    key: "log.retention.minutes",
                    value: "1",
                },
            ],
            topic: vec![
                ConfigPair {
                    key: REMOTE_STORAGE_ENABLE,
                    value: "true",
                },
                ConfigPair {
                    key: LOCAL_RETENTION_MS,
                    value: "60001",
                },
            ],
            want: local_ms_over(60_001, 60_000),
        },
        CrossKeyCase {
            cluster: vec![ConfigPair {
                key: "log.retention.ms",
                value: "-5",
            }],
            topic: vec![
                ConfigPair {
                    key: REMOTE_STORAGE_ENABLE,
                    value: "true",
                },
                ConfigPair {
                    key: LOCAL_RETENTION_MS,
                    value: "60001",
                },
            ],
            want: Ok(()),
        },
        CrossKeyCase {
            cluster: vec![ConfigPair {
                key: "log.local.retention.ms",
                value: "5000",
            }],
            topic: vec![
                ConfigPair {
                    key: REMOTE_STORAGE_ENABLE,
                    value: "true",
                },
                ConfigPair {
                    key: RETENTION_MS,
                    value: "1000",
                },
            ],
            want: local_ms_over(5000, 1000),
        },
        CrossKeyCase {
            cluster: vec![ConfigPair {
                key: "log.retention.bytes",
                value: "100",
            }],
            topic: vec![
                ConfigPair {
                    key: REMOTE_STORAGE_ENABLE,
                    value: "true",
                },
                ConfigPair {
                    key: LOCAL_RETENTION_BYTES,
                    value: "200",
                },
            ],
            want: Err(
                "Invalid value 200 for configuration local.retention.bytes: Value must not be \
                 more than retention.bytes property value: 100"
                    .to_owned(),
            ),
        },
        CrossKeyCase {
            cluster: vec![ConfigPair {
                key: "log.cleaner.min.compaction.lag.ms",
                value: "100",
            }],
            topic: vec![ConfigPair {
                key: MAX_COMPACTION_LAG_MS,
                value: "50",
            }],
            want: Err(
                "conflict topic config setting min.compaction.lag.ms (100) > \
                 max.compaction.lag.ms (50)"
                    .to_owned(),
            ),
        },
    ];
    cases
}

/// Kafka validates a topic's map merged over the broker's effective defaults
/// (`KafkaConfig.extractLogConfigMap`), so a key the topic leaves unset takes
/// the cluster-wide broker default, in the topic key's unit, and not the
/// registry default.
#[test]
fn cross_key_rules_read_the_cluster_broker_defaults() {
    let cases = cross_key_cases();
    for CrossKeyCase {
        cluster,
        topic,
        want,
    } in cases
    {
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        for ConfigPair { key: name, value } in &cluster {
            image.apply(&krabka_metadata::MetadataRecord::V1BrokerConfig(
                krabka_metadata::BrokerConfigRecord {
                    node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                    config_name: (*name).to_owned(),
                    config_value: Some((*value).to_owned()),
                },
            ));
        }
        let map = config_map(&topic);
        check!(
            canonical_topic_config_map(
                &map,
                &TopicDefaults::from_image(&image, UnstableApiVersions::Enabled),
                true,
                UnstableApiVersions::Enabled,
            )
            .map(drop)
                == want,
            "{cluster:?} {topic:?}"
        );
    }
}

/// A `compact,delete` topic is a compacted topic for every cross-key rule, so
/// KFC-1's exclusion covers it too.
#[test]
fn compact_and_delete_is_compaction_for_the_scheduled_delivery_rule() {
    let overrides = maplit::btreemap! {
    CLEANUP_POLICY.to_owned() => "compact,delete".to_owned(),
    DELIVERY_MODE.to_owned() => DELIVERY_MODE_SCHEDULED.to_owned()};

    assert2::assert!(
        validate_config_combination(&overrides, &TopicDefaults::default(), true).is_err()
    );
}

/// KFC-1's third exclusion: a scheduled topic reads each batch's
/// `max_timestamp` as its activation time, and log-append stamping overwrites
/// exactly that field, so the pair would deliver every record at once.
#[test]
fn log_append_time_and_scheduled_delivery_exclude_each_other() {
    let cases = [
        ("LogAppendTime", Some(DELIVERY_MODE_SCHEDULED), false),
        ("LogAppendTime", Some(DELIVERY_MODE_IMMEDIATE), true),
        ("LogAppendTime", None, true),
        ("CreateTime", Some(DELIVERY_MODE_SCHEDULED), true),
        ("CreateTime", None, true),
    ];
    for (timestamp_type, mode, want_ok) in cases {
        let mut overrides = BTreeMap::new();
        overrides.insert(MESSAGE_TIMESTAMP_TYPE.to_owned(), timestamp_type.to_owned());
        if let Some(mode) = mode {
            overrides.insert(DELIVERY_MODE.to_owned(), mode.to_owned());
        }
        let outcome = validate_config_combination(&overrides, &TopicDefaults::default(), true);
        check!(
            outcome.is_ok() == want_ok,
            "message.timestamp.type={timestamp_type} delivery.mode={mode:?}"
        );
        if !want_ok {
            let error = outcome.unwrap_err();
            check!(error.contains(MESSAGE_TIMESTAMP_TYPE), "got: {error}");
            check!(error.contains(DELIVERY_MODE), "got: {error}");
        }
    }
}

/// The whole-map entry point applies the rule too, which is the path
/// `CreateTopics` takes.
#[test]
fn validate_topic_config_map_refuses_log_append_time_on_a_scheduled_topic() {
    let overrides = maplit::btreemap! {
    MESSAGE_TIMESTAMP_TYPE.to_owned() => "LogAppendTime".to_owned(),
    DELIVERY_MODE.to_owned() => DELIVERY_MODE_SCHEDULED.to_owned()};

    assert2::assert!(validate_topic_config_map(&overrides).is_err());
}

#[test]
fn compact_plus_scheduled_rejection_names_both_keys() {
    let overrides = maplit::btreemap! {
    CLEANUP_POLICY.to_owned() => "compact".to_owned(),
    DELIVERY_MODE.to_owned() => DELIVERY_MODE_SCHEDULED.to_owned()};
    let error =
        validate_config_combination(&overrides, &TopicDefaults::default(), true).unwrap_err();
    assert2::assert!(error.contains(CLEANUP_POLICY), "got: {error}");
    assert2::assert!(error.contains(DELIVERY_MODE), "got: {error}");
}

#[test]
fn validate_topic_config_map_checks_pairs_and_then_combinations() {
    let accepted = maplit::btreemap! {
    RETENTION_MS.to_owned() => "60000".to_owned(),
    DELIVERY_MODE.to_owned() => DELIVERY_MODE_SCHEDULED.to_owned()};
    assert2::assert!(validate_topic_config_map(&accepted) == Ok(()));

    let bad_pair = maplit::btreemap! {DELIVERY_MODE.to_owned() => "later".to_owned()};
    assert2::assert!(validate_topic_config_map(&bad_pair).is_err());

    let unknown_key = maplit::btreemap! {UNKNOWN_KEY.to_owned() => "1000".to_owned()};
    assert2::assert!(validate_topic_config_map(&unknown_key).is_err());

    let bad_combination = maplit::btreemap! {
    CLEANUP_POLICY.to_owned() => "compact".to_owned(),
    DELIVERY_MODE.to_owned() => DELIVERY_MODE_SCHEDULED.to_owned()};
    assert2::assert!(validate_topic_config_map(&bad_combination).is_err());
}

/// KIP-950 `LogConfig.validateRemoteStorageConfigs`: tiered storage cannot be
/// turned off without saying what happens to the segments already in the
/// tier.
#[test]
fn disabling_tiered_storage_needs_the_delete_flag() {
    let stored =
        |enabled: bool| BTreeMap::from([(REMOTE_STORAGE_ENABLE.to_owned(), enabled.to_string())]);
    let cases = [
        (
            "the bare flip is refused",
            Some(stored(true)),
            vec![(REMOTE_STORAGE_ENABLE, "false")],
            false,
        ),
        (
            "the flip with delete-on-disable is accepted",
            Some(stored(true)),
            vec![
                (REMOTE_STORAGE_ENABLE, "false"),
                (REMOTE_LOG_DELETE_ON_DISABLE, "true"),
            ],
            true,
        ),
        (
            "the read-only tier keeps the flag on, so nothing is refused",
            Some(stored(true)),
            vec![
                (REMOTE_STORAGE_ENABLE, "true"),
                (REMOTE_LOG_COPY_DISABLE, "true"),
            ],
            true,
        ),
        (
            "a topic that was never tiered is not flipping anything off",
            Some(stored(false)),
            vec![(REMOTE_STORAGE_ENABLE, "false")],
            true,
        ),
        (
            "a topic with no stored config at all",
            None,
            vec![(REMOTE_STORAGE_ENABLE, "false")],
            true,
        ),
        (
            "turning it on is never refused",
            Some(stored(false)),
            vec![(REMOTE_STORAGE_ENABLE, "true")],
            true,
        ),
    ];
    for (case, current, next, want_ok) in cases {
        let next: BTreeMap<String, String> = next
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect();
        assert2::assert!(
            validate_remote_storage_disable(current.as_ref(), &next).is_ok() == want_ok,
            "{case}: {next:?}"
        );
    }
}

/// The refusal names both of Kafka's ways out, because an operator reads it
/// out of `kafka-configs` and has to pick one.
#[test]
fn the_disable_refusal_names_both_ways_out() {
    let current = BTreeMap::from([(REMOTE_STORAGE_ENABLE.to_owned(), "true".to_owned())]);
    let next = BTreeMap::from([(REMOTE_STORAGE_ENABLE.to_owned(), "false".to_owned())]);
    let message = validate_remote_storage_disable(Some(&current), &next)
        .expect_err("the bare flip is refused");
    check!(
        message
            == "It is invalid to disable remote storage without deleting remote data. If you \
                want to keep the remote data and turn to read only, please set \
                `remote.storage.enable=true,remote.log.copy.disable=true`. If you want to \
                disable remote storage and delete all remote data, please set \
                `remote.storage.enable=false,remote.log.delete.on.disable=true`."
    );
}

/// `apply_to_log_config` reads a stored value with the parser validation uses,
/// so a value validation accepts is never read back as its default.
#[test]
fn the_log_config_reader_parses_what_validation_accepts() {
    let overrides = BTreeMap::from([
        (REMOTE_STORAGE_ENABLE.to_owned(), "TRUE".to_owned()),
        (RETENTION_MS.to_owned(), " 1000 ".to_owned()),
        (CLEANUP_POLICY.to_owned(), String::new()),
    ]);
    let applied = super::super::log_config::apply_to_log_config(
        &overrides,
        &krabka_log::LogConfig::default(),
    );
    check!(applied.remote_storage_enable);
    check!(applied.retention == Some(krabka_units::millis(1000)));
    check!(applied.cleanup_policy == CleanupPolicy::NoCleanup);
}

/// Kafka's `ConfigurationControlManager.validateAlterConfig` refuses a value
/// longer than `Short.MAX_VALUE` with `INVALID_CONFIG` before it validates any
/// key, and counts the length as Java's `String.length()` does, in UTF-16 code
/// units: a value of 32767 units fits, and one of 32768 does not, whatever the
/// bytes it takes.
#[yare::parameterized(
    ascii_fits = { "a".repeat(32_767), true },
    ascii_one_too_long = { "a".repeat(32_768), false },
    two_byte_fits = { "é".repeat(32_767), true },
    two_byte_too_long = { "é".repeat(32_768), false },
    astral_fits = { "😀".repeat(16_383), true },
    astral_too_long = { "😀".repeat(16_384), false },
)]
fn a_config_value_size_matches_java_string_length(value: String, want_ok: bool) {
    const MESSAGE: &str = "The configuration value cannot be added because it exceeds the \
                           maximum value size of 32767 bytes.";
    let expected = if want_ok {
        Ok(())
    } else {
        Err(MESSAGE.to_owned())
    };
    check!(check_value_size(&value) == expected);
}

/// The aggregate config-map path checks a value's size before validating its
/// key, and the per-key path uses the same limit for long list values.
#[test]
fn a_config_value_size_is_checked_before_other_validation() {
    const MESSAGE: &str = "The configuration value cannot be added because it exceeds the \
                           maximum value size of 32767 bytes.";
    // A valid throttled-replicas list: 8000 entries of `0:1` and 7999 commas.
    let fits = vec!["0:1"; 8_000].join(",");
    let too_long = vec!["0:1"; 8_200].join(",");
    // The whole-map check answers the size before a key it would refuse: the
    // key here is unknown, and the other value is not an integer, yet the size
    // wins.
    let mut overrides = BTreeMap::new();
    overrides.insert(UNKNOWN_KEY.to_owned(), "a".repeat(32_768));
    overrides.insert(RETENTION_MS.to_owned(), "abc".to_owned());
    check!(
        canonical_topic_config_map(
            &overrides,
            &TopicDefaults::default(),
            false,
            UnstableApiVersions::Disabled,
        ) == Err(MESSAGE.to_owned())
    );
    // A list that fits validates as its own format says, and one that does not
    // fit is refused for its size on the per-key path too.
    let key = LEADER_THROTTLED_REPLICAS_KEY;
    check!(validate_topic_config(key, &fits) == Ok(()));
    check!(validate_topic_config(key, &too_long) == Err(MESSAGE.to_owned()));
}

struct TopicValueModes {
    single: Result<String, String>,
    map: Result<BTreeMap<String, String>, String>,
}

struct TopicNamedSamples {
    recognized: bool,
    accepted: Result<(), String>,
    refused: Result<(), String>,
}

#[derive(Default)]
struct TopicValueScenario {
    key: String,
    value: String,
    legal_sample: String,
    illegal_sample: String,
    validation: Option<Result<(), String>>,
    canonical: Option<Result<String, String>>,
    cleanup: Option<Result<CleanupPolicy, String>>,
    named_samples: Option<TopicNamedSamples>,
    released: Option<TopicValueModes>,
    trunk: Option<TopicValueModes>,
}

#[rstest::fixture]
fn topic_value() -> TopicValueScenario {
    // Every parser, validator, and version comparison observes fresh inputs.
    TopicValueScenario::default()
}

#[rstest_bdd_macros::given("a topic alteration supplies key {key} and quoted value {value}")]
fn supplied_topic_value(topic_value: &mut TopicValueScenario, key: String, value: String) {
    topic_value.key = key;
    topic_value.value = serde_json::from_str(&value).expect("the topic value is quoted");
}

#[rstest_bdd_macros::given(
    "topic key {key} has quoted legal sample {legal} and quoted illegal sample {illegal}"
)]
fn named_topic_samples(
    topic_value: &mut TopicValueScenario,
    key: String,
    legal: String,
    illegal: String,
) {
    topic_value.key = key;
    topic_value.legal_sample = serde_json::from_str(&legal).expect("the legal sample is quoted");
    topic_value.illegal_sample =
        serde_json::from_str(&illegal).expect("the illegal sample is quoted");
}

#[rstest_bdd_macros::when("the released per-key topic validator reads the supplied value")]
fn validate_supplied_topic_value(topic_value: &mut TopicValueScenario) {
    topic_value.validation = Some(validate_topic_config(&topic_value.key, &topic_value.value));
}

#[rstest_bdd_macros::when("the cleanup policy parser and topic validator read the supplied list")]
fn parse_supplied_cleanup_policy(topic_value: &mut TopicValueScenario) {
    topic_value.cleanup = Some(parse_cleanup_policy(&topic_value.value));
    topic_value.validation = Some(validate_topic_config(CLEANUP_POLICY, &topic_value.value));
}

#[rstest_bdd_macros::when("the broker recognizes the topic key and validates both named samples")]
fn validate_named_topic_samples(topic_value: &mut TopicValueScenario) {
    topic_value.named_samples = Some(TopicNamedSamples {
        recognized: is_recognized(&topic_value.key),
        accepted: validate_topic_config(&topic_value.key, &topic_value.legal_sample),
        refused: validate_topic_config(&topic_value.key, &topic_value.illegal_sample),
    });
}

#[rstest_bdd_macros::when("the supplied topic value is canonicalized in released mode")]
fn canonical_supplied_topic_value(topic_value: &mut TopicValueScenario) {
    topic_value.canonical = Some(canonical_topic_config(
        &topic_value.key,
        &topic_value.value,
        UnstableApiVersions::Disabled,
    ));
}

#[rstest_bdd_macros::when(
    "the supplied trunk topic value is canonicalized with unstable APIs enabled"
)]
fn canonical_supplied_trunk_topic_value(topic_value: &mut TopicValueScenario) {
    topic_value.canonical = Some(canonical_topic_config(
        &topic_value.key,
        &topic_value.value,
        UnstableApiVersions::Enabled,
    ));
}

#[rstest_bdd_macros::when(
    "the supplied key is validated individually and as a map in released and unstable modes"
)]
fn compare_topic_value_modes(topic_value: &mut TopicValueScenario) {
    let map = BTreeMap::from([(topic_value.key.clone(), topic_value.value.clone())]);
    let evaluate = |unstable| TopicValueModes {
        single: canonical_topic_config(&topic_value.key, &topic_value.value, unstable),
        map: canonical_topic_config_map(&map, &TopicDefaults::default(), true, unstable),
    };
    topic_value.released = Some(evaluate(UnstableApiVersions::Disabled));
    topic_value.trunk = Some(evaluate(UnstableApiVersions::Enabled));
}

#[rstest_bdd_macros::then("the supplied topic value is accepted {accepted}")]
fn supplied_topic_acceptance(topic_value: &TopicValueScenario, accepted: bool) {
    let result = topic_value
        .validation
        .as_ref()
        .expect("the topic value was validated");
    check!(
        result.is_ok() == accepted,
        "{}={:?}",
        topic_value.key,
        topic_value.value
    );
}

#[rstest_bdd_macros::then(
    "cleanup resolves as {policy} and the validator agrees with that outcome"
)]
fn cleanup_policy_outcome(topic_value: &TopicValueScenario, policy: String) {
    let expected = match policy.as_str() {
        "delete" => Some(CleanupPolicy::Delete),
        "compact" => Some(CleanupPolicy::Compact),
        "compact-and-delete" => Some(CleanupPolicy::CompactAndDelete),
        "no-cleanup" => Some(CleanupPolicy::NoCleanup),
        "invalid" => None,
        other => panic!("unknown expected cleanup policy {other}"),
    };
    let parsed = topic_value.cleanup.as_ref().expect("cleanup was parsed");
    check!(parsed.as_ref().ok() == expected.as_ref());
    check!(
        topic_value
            .validation
            .as_ref()
            .expect("cleanup was validated")
            .is_ok()
            == expected.is_some()
    );
}

#[rstest_bdd_macros::then(
    "the key is recognized, its legal sample is accepted, and its illegal sample is refused"
)]
fn named_topic_sample_results(topic_value: &TopicValueScenario) {
    let samples = topic_value
        .named_samples
        .as_ref()
        .expect("the key and samples were checked");
    check!(samples.recognized, "{}", topic_value.key);
    check!(samples.accepted == Ok(()));
    check!(samples.refused.is_err());
}

#[rstest_bdd_macros::then("the canonical topic outcome is {expected}")]
fn canonical_topic_outcome(topic_value: &TopicValueScenario, expected: String) {
    let result = topic_value
        .canonical
        .as_ref()
        .expect("canonicalization was attempted");
    if expected == "rejected" {
        check!(result.is_err());
    } else {
        let expected: String =
            serde_json::from_str(&expected).expect("the canonical value is quoted");
        check!(*result == Ok(expected));
    }
}

#[rstest_bdd_macros::then("the trunk topic value is rejected by its validator")]
fn rejected_trunk_topic_value(topic_value: &TopicValueScenario) {
    check!(
        topic_value
            .canonical
            .as_ref()
            .expect("the trunk value was validated")
            .is_err()
    );
}

#[rstest_bdd_macros::then(
    "released mode refuses the unknown key and unstable mode stores quoted value {canonical} exactly"
)]
fn admitted_trunk_topic_value(topic_value: &TopicValueScenario, canonical: String) {
    let canonical: String =
        serde_json::from_str(&canonical).expect("the canonical value is quoted");
    let unknown = format!("Unknown topic config name: {}", topic_value.key);
    let released = topic_value
        .released
        .as_ref()
        .expect("released mode was evaluated");
    let trunk = topic_value
        .trunk
        .as_ref()
        .expect("unstable mode was evaluated");
    check!(released.single == Err(unknown.clone()));
    check!(released.map.as_ref().map(|_| ()) == Err(&unknown));
    check!(trunk.single == Ok(canonical.clone()));
    check!(trunk.map == Ok(BTreeMap::from([(topic_value.key.clone(), canonical)])));
}

#[rstest_bdd_macros::then("the topic refusal carries exact Kafka message {message}")]
fn exact_topic_refusal(topic_value: &TopicValueScenario, message: String) {
    check!(topic_value.validation == Some(Err(message)));
}

rstest_bdd_macros::scenarios!(
    "tests/features/migrated/src/config_keys/validation/tests/values",
    fixtures = [topic_value: TopicValueScenario]
);

#[derive(Default)]
struct TopicMapScenario {
    tiered_storage: bool,
    properties: BTreeMap<String, String>,
    combination: Option<Result<(), String>>,
    image: Option<MetadataImage>,
    topic_key: String,
    released_default: Slot<Option<String>>,
    trunk_default: Slot<Option<String>>,
}

#[derive(rstest_bdd_macros::DataTableRow)]
struct TopicMapProperty {
    key: String,
    value: String,
}

#[rstest::fixture]
fn topic_map() -> TopicMapScenario {
    // Whole-map and inherited-default observations never leak across cases.
    TopicMapScenario::default()
}

#[rstest_bdd_macros::given("the topic alteration contains these properties:")]
fn supplied_topic_map(topic_map: &mut TopicMapScenario, #[datatable] rows: Rows<TopicMapProperty>) {
    topic_map.properties = rows.into_iter().map(|row| (row.key, row.value)).collect();
}

#[rstest_bdd_macros::given(
    "cluster-default broker key {broker_key} is {value} for topic key {topic_key}"
)]
fn trunk_cluster_default(
    topic_map: &mut TopicMapScenario,
    broker_key: String,
    value: String,
    topic_key: String,
) {
    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
        node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
        config_name: broker_key,
        config_value: Some(value),
    }));
    topic_map.image = Some(image);
    topic_map.topic_key = topic_key;
}

#[rstest_bdd_macros::when(
    "the broker validates the resulting topic combination with tiered storage available"
)]
fn validate_supplied_topic_map(topic_map: &mut TopicMapScenario) {
    topic_map.combination = Some(validate_config_combination(
        &topic_map.properties,
        &TopicDefaults::default(),
        true,
    ));
}

#[rstest_bdd_macros::when("topic defaults are inherited in released and unstable API modes")]
fn inherit_trunk_topic_default(topic_map: &mut TopicMapScenario) {
    let image = topic_map
        .image
        .as_ref()
        .expect("cluster-default metadata was given");
    topic_map.released_default.set(
        TopicDefaults::from_image(image, UnstableApiVersions::Disabled)
            .get(&topic_map.topic_key)
            .map(str::to_owned),
    );
    topic_map.trunk_default.set(
        TopicDefaults::from_image(image, UnstableApiVersions::Enabled)
            .get(&topic_map.topic_key)
            .map(str::to_owned),
    );
}

#[rstest_bdd_macros::then("the topic combination is refused with exact Kafka message {message}")]
fn refused_topic_combination(topic_map: &TopicMapScenario, message: String) {
    check!(topic_map.combination == Some(Err(message)));
}

#[rstest_bdd_macros::then(
    "released mode inherits no trunk default and unstable mode inherits {expected}"
)]
fn inherited_trunk_topic_default(topic_map: &TopicMapScenario, expected: String) {
    check!(topic_map.released_default.get() == Some(None));
    check!(topic_map.trunk_default.get() == Some(Some(expected)));
}

rstest_bdd_macros::scenarios!(
    "tests/features/migrated/src/config_keys/validation/tests/maps",
    fixtures = [topic_map: TopicMapScenario]
);

#[rstest_bdd_macros::given("broker tiered storage is {enabled}")]
fn broker_tiered_storage(topic_map: &mut TopicMapScenario, enabled: bool) {
    topic_map.tiered_storage = enabled;
}

#[rstest_bdd_macros::when("the resulting tiered topic map is canonicalized")]
fn canonicalize_tiered_map(topic_map: &mut TopicMapScenario) {
    topic_map.combination = Some(
        canonical_topic_config_map(
            &topic_map.properties,
            &TopicDefaults::default(),
            topic_map.tiered_storage,
            UnstableApiVersions::Enabled,
        )
        .map(drop),
    );
}

#[rstest_bdd_macros::then("the resulting tiered topic map is accepted")]
fn accepted_tiered_map(topic_map: &TopicMapScenario) {
    check!(topic_map.combination == Some(Ok(())));
}
