//! Tests for the per-key and whole-map topic-config validators.

use assert2::assert;

use super::*;

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
fn validate_cleanup_policy_accepts_delete_and_compact() {
    assert!(validate_topic_config(CLEANUP_POLICY, "delete").is_ok());
    assert!(validate_topic_config(CLEANUP_POLICY, "compact").is_ok());
}

#[test]
fn validate_cleanup_policy_rejects_unknown() {
    assert!(validate_topic_config(CLEANUP_POLICY, "compact,delete").is_err());
    assert!(validate_topic_config(CLEANUP_POLICY, "junk").is_err());
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
fn validate_unknown_key_rejected() {
    let err = validate_topic_config("flush.ms", "1000").unwrap_err();
    assert!(err.contains("unrecognized"));
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
        ("flush.ms", false),
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

    let unknown_key = maplit::btreemap! {"flush.ms".to_string() => "1000".to_string()};
    assert!(validate_topic_config_map(&unknown_key).is_err());

    let bad_combination = maplit::btreemap! {
    CLEANUP_POLICY.to_string() => "compact".to_string(),
    DELIVERY_MODE.to_string() => DELIVERY_MODE_SCHEDULED.to_string()};
    assert!(validate_topic_config_map(&bad_combination).is_err());
}
