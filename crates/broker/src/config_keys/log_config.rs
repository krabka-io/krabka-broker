//! The applier that merges a topic's validated overrides onto a `LogConfig`
//! for `Log::set_config`.

use std::collections::BTreeMap;

use krabka_log::LogConfig;
use krabka_units::{
    ByteSize, Time,
    convert::{
        ByteSizeExt as _, TimeExt as _,
        wire::{opt_size_from_bytes_i64, opt_time_from_millis_i64},
    },
};

use super::{
    CLEANUP_POLICY, COMPRESSION_TYPE, DELETE_RETENTION_MS, LOCAL_RETENTION_BYTES,
    LOCAL_RETENTION_MS, REMOTE_STORAGE_ENABLE, RETENTION_BYTES, RETENTION_MS, SEGMENT_BYTES,
    delivery::{DELIVERY_MODE, DELIVERY_MODE_SCHEDULED},
    validation::parse_compression_type,
};

/// Merge `overrides` over `base` and return a fresh `LogConfig` to push
/// into `Log::set_config`. This function drops unknown keys silently.
/// `validate_topic_config` should have rejected them at `AlterConfigs` time,
/// before the record reached the metadata image. This function is the
/// applier and treats the input as already-validated.
#[must_use]
pub(crate) fn apply_to_log_config(
    overrides: &BTreeMap<String, String>,
    base: &LogConfig,
) -> LogConfig {
    let mut out = base.clone();
    for (k, v) in overrides {
        match k.as_str() {
            RETENTION_MS => {
                if let Ok(ms) = v.parse::<i64>() {
                    out.retention = opt_time_from_millis_i64(ms);
                }
            }
            RETENTION_BYTES => {
                if let Ok(b) = v.parse::<i64>() {
                    out.retention_size = opt_size_from_bytes_i64(b);
                }
            }
            LOCAL_RETENTION_MS => {
                if let Ok(ms) = v.parse::<i64>() {
                    // -2 (inherit) and -1 (unlimited)
                    // both collapse to `None` — the greenfield simplification noted
                    // in the spec. >=0 maps to `Some(Time)`.
                    out.local_retention = opt_time_from_millis_i64(ms);
                }
            }
            LOCAL_RETENTION_BYTES => {
                if let Ok(b) = v.parse::<i64>() {
                    out.local_retention_size = opt_size_from_bytes_i64(b);
                }
            }
            SEGMENT_BYTES => {
                if let Ok(b) = v.parse::<u64>() {
                    out.segment_size = ByteSize::from_bytes(b);
                }
            }
            CLEANUP_POLICY => {
                out.cleanup_policy = if v == "compact" {
                    krabka_log::CleanupPolicy::Compact
                } else {
                    krabka_log::CleanupPolicy::Delete
                };
            }
            COMPRESSION_TYPE => {
                if let Ok(target) = parse_compression_type(v) {
                    out.compression_type = target;
                }
            }
            REMOTE_STORAGE_ENABLE => {
                out.remote_storage_enable = v == "true";
            }
            DELIVERY_MODE => {
                out.delivery_policy = if v == DELIVERY_MODE_SCHEDULED {
                    krabka_log::DeliveryPolicy::Scheduled
                } else {
                    krabka_log::DeliveryPolicy::Immediate
                };
            }
            DELETE_RETENTION_MS => {
                if let Ok(ms) = v.parse::<i64>()
                    && ms >= 0
                {
                    out.delete_retention = Time::from_millis(ms);
                }
            }
            // The remaining keys are recognized but no broker behavior is
            // wired to them yet (see module docs).
            _ => {}
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::{bytes, mebibytes, millis, minutes};

    use super::*;

    #[test]
    fn apply_compression_type_zstd_propagates() {
        use krabka_compression::CompressionType;
        let mut o = BTreeMap::new();
        o.insert(COMPRESSION_TYPE.into(), "zstd".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.compression_type == Some(CompressionType::Zstd));
    }

    #[test]
    fn apply_compression_type_producer_resets_to_none() {
        use krabka_compression::CompressionType;
        let base = LogConfig {
            compression_type: Some(CompressionType::Lz4),
            ..LogConfig::default()
        };
        let mut o = BTreeMap::new();
        o.insert(COMPRESSION_TYPE.into(), "producer".into());
        let out = apply_to_log_config(&o, &base);
        assert!(out.compression_type == None);
    }

    #[test]
    fn apply_remote_storage_enable_propagates() {
        let mut o = BTreeMap::new();
        o.insert(REMOTE_STORAGE_ENABLE.into(), "true".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.remote_storage_enable);

        let mut off = BTreeMap::new();
        off.insert(REMOTE_STORAGE_ENABLE.into(), "false".into());
        let base = LogConfig {
            remote_storage_enable: true,
            ..LogConfig::default()
        };
        let out = apply_to_log_config(&off, &base);
        assert!(!out.remote_storage_enable);
    }

    #[test]
    fn apply_retention_ms_propagates() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_MS.into(), "60000".into());
        let base = LogConfig::default();
        let out = apply_to_log_config(&o, &base);
        assert!(out.retention == Some(minutes(1)));
    }

    #[test]
    fn apply_retention_ms_minus_one_means_unlimited() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_MS.into(), "-1".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.retention == None);
    }

    #[test]
    fn apply_retention_ms_zero_is_retained() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_MS.into(), "0".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.retention == Some(millis(0)));
    }

    #[test]
    fn apply_retention_bytes_propagates() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_BYTES.into(), "1048576".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.retention_size == Some(mebibytes(1)));
    }

    #[test]
    fn apply_retention_bytes_minus_one_means_unlimited() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_BYTES.into(), "-1".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.retention_size == None);
    }

    #[test]
    fn apply_retention_bytes_zero_is_retained() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_BYTES.into(), "0".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.retention_size == Some(bytes(0)));
    }

    #[test]
    fn apply_segment_bytes_propagates() {
        let mut o = BTreeMap::new();
        o.insert(SEGMENT_BYTES.into(), "1048576".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.segment_size == mebibytes(1));
    }

    #[test]
    fn apply_empty_overrides_preserves_base() {
        let base = LogConfig {
            retention: Some(millis(12_345)),
            ..LogConfig::default()
        };
        let out = apply_to_log_config(&BTreeMap::new(), &base);
        assert!(out.retention == base.retention);
    }

    #[test]
    fn apply_cleanup_policy_compact_propagates() {
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert(CLEANUP_POLICY.to_string(), "compact".to_string());
        let out = apply_to_log_config(&overrides, &krabka_log::LogConfig::default());
        assert!(out.cleanup_policy == krabka_log::CleanupPolicy::Compact);
    }

    #[test]
    fn apply_cleanup_policy_delete_propagates() {
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert(CLEANUP_POLICY.to_string(), "delete".to_string());
        let base = krabka_log::LogConfig {
            cleanup_policy: krabka_log::CleanupPolicy::Compact,
            ..krabka_log::LogConfig::default()
        };
        let out = apply_to_log_config(&overrides, &base);
        assert!(out.cleanup_policy == krabka_log::CleanupPolicy::Delete);
    }

    #[test]
    fn apply_local_retention_ms_minus_two_means_inherit() {
        let mut o = BTreeMap::new();
        o.insert(LOCAL_RETENTION_MS.into(), "-2".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.local_retention == None);

        let mut unlimited = BTreeMap::new();
        unlimited.insert(LOCAL_RETENTION_MS.into(), "-1".into());
        let out = apply_to_log_config(&unlimited, &LogConfig::default());
        assert!(out.local_retention == None);
    }

    #[test]
    fn apply_local_retention_ms_zero_is_retained() {
        let mut o = BTreeMap::new();
        o.insert(LOCAL_RETENTION_MS.into(), "0".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.local_retention == Some(millis(0)));
    }

    #[test]
    fn apply_local_retention_ms_positive_propagates() {
        let mut o = BTreeMap::new();
        o.insert(LOCAL_RETENTION_MS.into(), "60000".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.local_retention == Some(minutes(1)));
    }

    #[test]
    fn apply_local_retention_bytes_propagates() {
        let mut o = BTreeMap::new();
        o.insert(LOCAL_RETENTION_BYTES.into(), "1048576".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.local_retention_size == Some(mebibytes(1)));
    }

    #[test]
    fn apply_local_retention_bytes_minus_two_means_inherit() {
        let mut o = BTreeMap::new();
        o.insert(LOCAL_RETENTION_BYTES.into(), "-2".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.local_retention_size == None);
    }

    #[test]
    fn apply_local_retention_bytes_zero_is_retained() {
        let mut o = BTreeMap::new();
        o.insert(LOCAL_RETENTION_BYTES.into(), "0".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.local_retention_size == Some(bytes(0)));
    }

    #[test]
    fn apply_delete_retention_ms_propagates() {
        let mut o = BTreeMap::new();
        o.insert(DELETE_RETENTION_MS.into(), "12345".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.delete_retention == millis(12_345));
    }
}
