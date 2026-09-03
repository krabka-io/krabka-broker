//! Tests for the per-key and whole-map topic-config validators.

use assert2::{assert, check};
use krabka_log::CleanupPolicy;

use super::{
    super::{
        DELETE_RETENTION_MS, FILE_DELETE_DELAY_MS, FLUSH_MESSAGES, FLUSH_MS, INDEX_INTERVAL_BYTES,
        LOCAL_RETENTION_BYTES, LOCAL_RETENTION_MS, MAX_COMPACTION_LAG_MS,
        MESSAGE_TIMESTAMP_AFTER_MAX_MS, MESSAGE_TIMESTAMP_BEFORE_MAX_MS, MESSAGE_TIMESTAMP_TYPE,
        MIN_COMPACTION_LAG_MS, MIN_INSYNC_REPLICAS, PREALLOCATE, REMOTE_STORAGE_ENABLE,
        RETENTION_BYTES, RETENTION_MS, SEGMENT_BYTES, SEGMENT_INDEX_BYTES, SEGMENT_JITTER_MS,
        SEGMENT_MS, delivery::DELIVERY_MODE_IMMEDIATE,
    },
    *,
};

/// A name Kafka has no `TopicConfig` entry for, which its own `LogConfig`
/// refuses with `Unknown topic config name`. The probe every unknown-key test
/// uses, so a key krabka later registers cannot quietly turn one of them into
/// a test of nothing.
const UNKNOWN_KEY: &str = "not.a.topic.config";

#[test]
fn validate_retention_ms_boundary_cases() {
    let cases = [
        ("60000", true), // positive accepted
        ("-1", true),    // -1 (unlimited) accepted
        ("-5", false),   // below -1 rejected
        ("abc", false),  // non-integer rejected
    ];
    for (value, want_ok) in cases {
        assert!(
            validate_topic_config(RETENTION_MS, value).is_ok() == want_ok,
            "retention.ms={value}"
        );
    }
}

#[test]
fn validate_segment_bytes_rejects_zero() {
    assert!(validate_topic_config(SEGMENT_BYTES, "0").is_err());
}

#[test]
fn validate_segment_bytes_accepts_minimum_one() {
    assert!(validate_topic_config(SEGMENT_BYTES, "1").is_ok());
}

#[test]
fn validate_cleanup_policy_accepts_every_non_empty_subset_of_the_list() {
    // Kafka types `cleanup.policy` as a LIST and derives `compact` and
    // `delete` from it by membership, so either name alone and both names in
    // either order are all valid. Kafka Streams sends `compact,delete` on
    // every windowed-store changelog topic.
    let cases = [
        ("delete", Some(CleanupPolicy::Delete)),
        ("compact", Some(CleanupPolicy::Compact)),
        ("compact,delete", Some(CleanupPolicy::CompactAndDelete)),
        ("delete,compact", Some(CleanupPolicy::CompactAndDelete)),
        ("compact, delete", Some(CleanupPolicy::CompactAndDelete)),
        ("junk", None),
        ("compact,junk", None),
        ("compact,", None),
        ("", None),
    ];
    for (value, expected) in cases {
        assert!(parse_cleanup_policy(value).ok() == expected, "{value}");
        assert!(
            validate_topic_config(CLEANUP_POLICY, value).is_ok() == expected.is_some(),
            "{value}"
        );
    }
}

#[test]
fn validate_compression_all_supported_values_accepted() {
    for v in [
        "producer",
        "uncompressed",
        "none",
        "gzip",
        "snappy",
        "lz4",
        "zstd",
    ] {
        assert!(
            validate_topic_config(COMPRESSION_TYPE, v).is_ok(),
            "compression.type={v} should be accepted",
        );
    }
}

#[test]
fn validate_compression_bogus_rejected() {
    let err = validate_topic_config(COMPRESSION_TYPE, "bzip3").unwrap_err();
    assert!(err.contains("compression.type"), "got: {err}");
}

#[test]
fn parse_compression_type_maps_producer_to_none() {
    assert!(parse_compression_type("producer") == Ok(None));
}

#[test]
fn parse_compression_type_maps_codecs() {
    use krabka_compression::CompressionType;
    let cases = [
        ("gzip", CompressionType::Gzip),
        ("snappy", CompressionType::Snappy),
        ("lz4", CompressionType::Lz4),
        ("zstd", CompressionType::Zstd),
        ("uncompressed", CompressionType::None),
    ];
    for (input, want) in cases {
        assert!(
            parse_compression_type(input) == Ok(Some(want)),
            "compression.type={input}"
        );
    }
}

#[test]
fn validate_min_isr_positive_accepted() {
    assert!(validate_topic_config(MIN_INSYNC_REPLICAS, "2").is_ok());
}

#[test]
fn an_int_key_refuses_what_kafkas_int_cannot_hold() {
    // `DescribeConfigs` reports both keys as `ConfigType::Int`, and
    // `apache/kafka:4.3.1` refuses a value past `i32::MAX` on both:
    // `Invalid value 2147483648 for configuration segment.bytes: Not a number
    // of type INT`. A value the broker accepts must fit the type it
    // advertises, so the largest accepted value is `i32::MAX`.
    let cases = [
        (SEGMENT_BYTES, "2147483647", true),
        (SEGMENT_BYTES, "2147483648", false),
        (SEGMENT_BYTES, "9223372036854775807", false),
        (MIN_INSYNC_REPLICAS, "2147483647", true),
        (MIN_INSYNC_REPLICAS, "2147483648", false),
        (MIN_INSYNC_REPLICAS, "0", false),
        (MIN_INSYNC_REPLICAS, "-1", false),
    ];
    for (key, value, want_ok) in cases {
        assert!(
            validate_topic_config(key, value).is_ok() == want_ok,
            "{key}={value}"
        );
    }
}

#[test]
fn validate_unknown_key_rejected() {
    let err = validate_topic_config(UNKNOWN_KEY, "1000").unwrap_err();
    assert!(err.contains("unrecognized"));
}

/// The Kafka `TopicConfig` names krabka registers, with the value Kafka's own
/// `ConfigDef` accepts and one it refuses.
#[test]
fn the_remaining_kafka_topic_config_names_are_accepted() {
    let cases = [
        (SEGMENT_MS, "60000", "0"),
        (SEGMENT_INDEX_BYTES, "10485760", "3"),
        (SEGMENT_JITTER_MS, "0", "-1"),
        (MIN_COMPACTION_LAG_MS, "0", "-1"),
        (MAX_COMPACTION_LAG_MS, "9223372036854775807", "0"),
        (MIN_CLEANABLE_DIRTY_RATIO, "0.5", "2.0"),
        (FILE_DELETE_DELAY_MS, "60000", "-1"),
        (FLUSH_MESSAGES, "10000", "0"),
        (FLUSH_MS, "1000", "-1"),
        (INDEX_INTERVAL_BYTES, "4096", "-1"),
        (PREALLOCATE, "true", "yes"),
        (MESSAGE_TIMESTAMP_TYPE, "LogAppendTime", "WallClock"),
        (MESSAGE_TIMESTAMP_AFTER_MAX_MS, "3600000", "-1"),
        (MESSAGE_TIMESTAMP_BEFORE_MAX_MS, "3600000", "-1"),
    ];
    for (key, accepted, refused) in cases {
        check!(is_recognized(key), "{key}");
        check!(
            validate_topic_config(key, accepted) == Ok(()),
            "{key}={accepted}"
        );
        check!(
            validate_topic_config(key, refused).is_err(),
            "{key}={refused}"
        );
    }
}

/// The Streams `RepartitionTopicConfig` override set, which every Streams
/// application sends on its internal repartition topics.
#[test]
fn the_streams_repartition_topic_override_set_is_accepted() {
    let overrides = maplit::btreemap! {
    CLEANUP_POLICY.to_string() => "delete".to_string(),
    SEGMENT_BYTES.to_string() => "52428800".to_string(),
    RETENTION_MS.to_string() => "-1".to_string(),
    MESSAGE_TIMESTAMP_TYPE.to_string() => "CreateTime".to_string()};

    assert!(validate_topic_config_map(&overrides) == Ok(()));
}

/// The Streams `WindowedChangelogTopicConfig` override set, whose
/// `cleanup.policy` is exactly `compact,delete`.
#[test]
fn the_streams_windowed_changelog_override_set_is_accepted() {
    let overrides = maplit::btreemap! {
    CLEANUP_POLICY.to_string() => "compact,delete".to_string(),
    RETENTION_MS.to_string() => "86400000".to_string(),
    MIN_COMPACTION_LAG_MS.to_string() => "0".to_string(),
    MESSAGE_TIMESTAMP_TYPE.to_string() => "CreateTime".to_string()};

    assert!(validate_topic_config_map(&overrides) == Ok(()));
}

#[test]
fn validate_dirty_ratio_accepts_the_closed_unit_interval() {
    let cases = [
        ("0", true),
        ("0.5", true),
        ("1", true),
        ("1.0001", false),
        ("-0.1", false),
        ("NaN", false),
        ("half", false),
    ];
    for (value, want_ok) in cases {
        check!(
            validate_topic_config(MIN_CLEANABLE_DIRTY_RATIO, value).is_ok() == want_ok,
            "min.cleanable.dirty.ratio={value}"
        );
    }
}

#[test]
fn validate_remote_storage_enable_accepts_bools() {
    assert!(validate_topic_config(REMOTE_STORAGE_ENABLE, "true").is_ok());
    assert!(validate_topic_config(REMOTE_STORAGE_ENABLE, "false").is_ok());
}

#[test]
fn validate_remote_storage_enable_rejects_junk() {
    let err = validate_topic_config(REMOTE_STORAGE_ENABLE, "yes").unwrap_err();
    assert!(err.contains("remote.storage.enable"), "got: {err}");
}

#[test]
fn is_recognized_includes_remote_storage_enable() {
    assert!(is_recognized(REMOTE_STORAGE_ENABLE));
}

#[test]
fn is_recognized_matches_whitelist() {
    let cases = [
        (RETENTION_MS, true),
        (RETENTION_BYTES, true),
        (SEGMENT_BYTES, true),
        (CLEANUP_POLICY, true),
        (COMPRESSION_TYPE, true),
        (MIN_INSYNC_REPLICAS, true),
        (UNKNOWN_KEY, false),
        ("", false),
    ];
    for (key, want) in cases {
        assert!(is_recognized(key) == want, "key {key:?}");
    }
}

#[test]
fn validate_local_retention_ms_accepts_minus_one_minus_two_and_positive() {
    for value in ["-2", "-1", "60000"] {
        assert!(
            validate_topic_config(LOCAL_RETENTION_MS, value) == Ok(()),
            "local.retention.ms={value}"
        );
    }
}

#[test]
fn validate_local_retention_ms_rejects_below_minus_two() {
    assert!(validate_topic_config(LOCAL_RETENTION_MS, "-3").is_err());
}

#[test]
fn is_recognized_includes_local_retention_keys() {
    assert!(is_recognized(LOCAL_RETENTION_MS));
    assert!(is_recognized(LOCAL_RETENTION_BYTES));
}

#[test]
fn validate_delete_retention_ms_accepts_nonneg_rejects_negative() {
    let cases = [("0", true), ("86400000", true), ("-1", false)];
    for (value, want_ok) in cases {
        assert!(
            validate_topic_config(DELETE_RETENTION_MS, value).is_ok() == want_ok,
            "delete.retention.ms={value}"
        );
    }
}

#[test]
fn is_recognized_includes_delete_retention_ms() {
    assert!(is_recognized(DELETE_RETENTION_MS));
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
            overrides.insert(CLEANUP_POLICY.to_string(), policy.to_string());
        }
        if let Some(mode) = mode {
            overrides.insert(DELIVERY_MODE.to_string(), mode.to_string());
        }
        assert!(
            validate_config_combination(&overrides).is_ok() == want_ok,
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
            overrides.insert(MIN_COMPACTION_LAG_MS.to_string(), min.to_string());
        }
        if let Some(max) = max {
            overrides.insert(MAX_COMPACTION_LAG_MS.to_string(), max.to_string());
        }
        assert!(
            validate_config_combination(&overrides).is_ok() == want_ok,
            "{case}: {overrides:?}"
        );
    }
}

/// The refusal carries Kafka's wording, which `kafka-configs` prints verbatim.
#[test]
fn the_compaction_lag_conflict_carries_kafkas_message() {
    let overrides = BTreeMap::from([
        (MIN_COMPACTION_LAG_MS.to_string(), "60000".to_string()),
        (MAX_COMPACTION_LAG_MS.to_string(), "1".to_string()),
    ]);
    assert!(
        validate_config_combination(&overrides)
            == Err(
                "conflict topic config setting min.compaction.lag.ms (60000) > \
                 max.compaction.lag.ms (1)"
                    .to_string()
            )
    );
}

/// Kafka's `validateNoRemoteStorageForCompactedTopic`: `remote.storage.enable`
/// and a cleanup policy containing `compact` exclude each other, and because
/// the test is a membership test over the list, `compact,delete` is refused
/// beside `compact`.
#[test]
fn tiered_storage_and_a_compacted_policy_exclude_each_other() {
    let cases = [
        ("compact", "true", false),
        ("compact", "false", true),
        ("compact,delete", "true", false),
        ("compact,delete", "false", true),
        ("delete", "true", true),
        ("delete", "false", true),
    ];
    for (policy, tiered, want_ok) in cases {
        let overrides = maplit::btreemap! {
        CLEANUP_POLICY.to_string() => policy.to_string(),
        REMOTE_STORAGE_ENABLE.to_string() => tiered.to_string()};
        let outcome = validate_config_combination(&overrides);
        check!(
            outcome.is_ok() == want_ok,
            "cleanup.policy={policy} remote.storage.enable={tiered}"
        );
        if !want_ok {
            check!(
                outcome == Err("Tiered storage is not supported for compacted topics".to_owned()),
                "cleanup.policy={policy} remote.storage.enable={tiered}"
            );
        }
    }
}

/// A `compact,delete` topic is a compacted topic for every cross-key rule, so
/// KFC-1's exclusion covers it too.
#[test]
fn compact_and_delete_is_compaction_for_the_scheduled_delivery_rule() {
    let overrides = maplit::btreemap! {
    CLEANUP_POLICY.to_string() => "compact,delete".to_string(),
    DELIVERY_MODE.to_string() => DELIVERY_MODE_SCHEDULED.to_string()};

    assert!(validate_config_combination(&overrides).is_err());
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
        overrides.insert(
            MESSAGE_TIMESTAMP_TYPE.to_string(),
            timestamp_type.to_string(),
        );
        if let Some(mode) = mode {
            overrides.insert(DELIVERY_MODE.to_string(), mode.to_string());
        }
        let outcome = validate_config_combination(&overrides);
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
    MESSAGE_TIMESTAMP_TYPE.to_string() => "LogAppendTime".to_string(),
    DELIVERY_MODE.to_string() => DELIVERY_MODE_SCHEDULED.to_string()};

    assert!(validate_topic_config_map(&overrides).is_err());
}

#[test]
fn compact_plus_scheduled_rejection_names_both_keys() {
    let overrides = maplit::btreemap! {
    CLEANUP_POLICY.to_string() => "compact".to_string(),
    DELIVERY_MODE.to_string() => DELIVERY_MODE_SCHEDULED.to_string()};
    let error = validate_config_combination(&overrides).unwrap_err();
    assert!(error.contains(CLEANUP_POLICY), "got: {error}");
    assert!(error.contains(DELIVERY_MODE), "got: {error}");
}

#[test]
fn validate_topic_config_map_checks_pairs_and_then_combinations() {
    let accepted = maplit::btreemap! {
    RETENTION_MS.to_string() => "60000".to_string(),
    DELIVERY_MODE.to_string() => DELIVERY_MODE_SCHEDULED.to_string()};
    assert!(validate_topic_config_map(&accepted) == Ok(()));

    let bad_pair = maplit::btreemap! {DELIVERY_MODE.to_string() => "later".to_string()};
    assert!(validate_topic_config_map(&bad_pair).is_err());

    let unknown_key = maplit::btreemap! {UNKNOWN_KEY.to_string() => "1000".to_string()};
    assert!(validate_topic_config_map(&unknown_key).is_err());

    let bad_combination = maplit::btreemap! {
    CLEANUP_POLICY.to_string() => "compact".to_string(),
    DELIVERY_MODE.to_string() => DELIVERY_MODE_SCHEDULED.to_string()};
    assert!(validate_topic_config_map(&bad_combination).is_err());
}
