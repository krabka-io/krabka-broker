//! The `AlterConfigs` value checks: the per-key validator, the whole-map
//! validator with the cross-key rules, and the whitelist membership test.

use std::collections::BTreeMap;

use super::{
    CLEANUP_POLICY, COMPRESSION_TYPE, DELETE_RETENTION_MS, LOCAL_RETENTION_BYTES,
    LOCAL_RETENTION_INHERIT, LOCAL_RETENTION_MS, MIN_INSYNC_REPLICAS, REMOTE_STORAGE_ENABLE,
    RETENTION_BYTES, RETENTION_MS, RETENTION_UNLIMITED, SEGMENT_BYTES,
    delivery::{
        DELIVERY_MAX_DELAY_MS, DELIVERY_MAX_DELAY_UNLIMITED, DELIVERY_MODE,
        DELIVERY_MODE_IMMEDIATE, DELIVERY_MODE_SCHEDULED, DELIVERY_SCHEDULE_MONOTONIC,
    },
    qos::{QOS_TIER, validate_qos_tier},
    recovery::{RecoveryStrategy, UNCLEAN_LEADER_ELECTION_ENABLE, UNCLEAN_RECOVERY_STRATEGY},
    schema::{
        SCHEMA_VALIDATION_KEY, SCHEMA_VALIDATION_MODE, SCHEMA_VALIDATION_MODE_FULL,
        SCHEMA_VALIDATION_MODE_ID, SCHEMA_VALIDATION_VALUE,
    },
};

/// Validate a single key/value pair. `Err(reason)` carries an
/// operator-readable explanation that the handler propagates into the
/// `error_message` field of the response.
pub(crate) fn validate_topic_config(key: &str, value: &str) -> Result<(), String> {
    match key {
        RETENTION_MS | RETENTION_BYTES => {
            parse_i64_at_least(RETENTION_UNLIMITED, value).map(|_| ())
        }
        LOCAL_RETENTION_MS | LOCAL_RETENTION_BYTES => {
            parse_i64_at_least(LOCAL_RETENTION_INHERIT, value).map(|_| ())
        }
        DELETE_RETENTION_MS => parse_i64_at_least(0, value).map(|_| ()),
        SEGMENT_BYTES => parse_u64_at_least(1, value).map(|_| ()),
        CLEANUP_POLICY => match value {
            "delete" | "compact" => Ok(()),
            _ => Err(format!(
                "cleanup.policy={value} not supported; expected `delete` or `compact`"
            )),
        },
        COMPRESSION_TYPE => parse_compression_type(value).map(|_| ()),
        MIN_INSYNC_REPLICAS => parse_i64_at_least(1, value).map(|_| ()),
        UNCLEAN_LEADER_ELECTION_ENABLE => match value {
            "true" | "false" => Ok(()),
            _ => Err(format!(
                "unclean.leader.election.enable={value} not supported; expected `true` or `false`"
            )),
        },
        UNCLEAN_RECOVERY_STRATEGY => RecoveryStrategy::parse(value).map(|_| ()).ok_or_else(|| {
            format!(
                "unclean.recovery.strategy={value} not supported; expected `None`, `Balanced`, or `Aggressive`"
            )
        }),
        REMOTE_STORAGE_ENABLE => match value {
            "true" | "false" => Ok(()),
            _ => Err(format!(
                "remote.storage.enable={value} not supported; expected `true` or `false`"
            )),
        },
        QOS_TIER => validate_qos_tier(value),
        DELIVERY_MODE => match value {
            DELIVERY_MODE_IMMEDIATE | DELIVERY_MODE_SCHEDULED => Ok(()),
            _ => Err(format!(
                "delivery.mode={value} not supported; expected \
                 `{DELIVERY_MODE_IMMEDIATE}` or `{DELIVERY_MODE_SCHEDULED}`"
            )),
        },
        DELIVERY_MAX_DELAY_MS => {
            parse_i64_at_least(DELIVERY_MAX_DELAY_UNLIMITED, value).map(|_| ())
        }
        DELIVERY_SCHEDULE_MONOTONIC => match value {
            "true" | "false" => Ok(()),
            _ => Err(format!(
                "delivery.schedule.monotonic={value} not supported; expected `true` or `false`"
            )),
        },
        SCHEMA_VALIDATION_KEY => match value {
            "true" | "false" => Ok(()),
            _ => Err(format!(
                "schema.validation.key={value} not supported; expected `true` or `false`"
            )),
        },
        SCHEMA_VALIDATION_VALUE => match value {
            "true" | "false" => Ok(()),
            _ => Err(format!(
                "schema.validation.value={value} not supported; expected `true` or `false`"
            )),
        },
        SCHEMA_VALIDATION_MODE => match value {
            SCHEMA_VALIDATION_MODE_ID | SCHEMA_VALIDATION_MODE_FULL => Ok(()),
            _ => Err(format!(
                "schema.validation.mode={value} not supported; expected \
                 `{SCHEMA_VALIDATION_MODE_ID}` or `{SCHEMA_VALIDATION_MODE_FULL}`"
            )),
        },
        crate::throttle::LEADER_THROTTLED_REPLICAS_KEY
        | crate::throttle::FOLLOWER_THROTTLED_REPLICAS_KEY => {
            crate::throttle::ThrottledReplicas::parse(value).map(|_| ())
        }
        unknown => Err(format!("unrecognized config key `{unknown}`")),
    }
}

/// Validate a topic's complete override map: every key/value pair through
/// [`validate_topic_config`], then the cross-key rules in
/// [`validate_config_combination`]. `CreateTopics` builds the whole map before
/// it commits anything, so it validates in one call.
pub(crate) fn validate_topic_config_map(
    overrides: &BTreeMap<String, String>,
) -> Result<(), String> {
    for (key, value) in overrides {
        validate_topic_config(key, value)?;
    }
    validate_config_combination(overrides)
}

/// Validate the rules that span two keys, over a topic's complete override
/// map. [`validate_topic_config`] takes one pair and cannot see them.
///
/// KFC-1 states the one such rule: `cleanup.policy=compact` and
/// `delivery.mode=scheduled` exclude each other. Compaction deletes a record
/// once a later record carries the same key, and on a scheduled topic that
/// later record can arrive long before the earlier one comes due. The earlier
/// record would then be deleted without a single delivery, which is the
/// failure scheduled delivery exists to prevent.
pub(crate) fn validate_config_combination(
    overrides: &BTreeMap<String, String>,
) -> Result<(), String> {
    let compacting = overrides
        .get(CLEANUP_POLICY)
        .is_some_and(|policy| policy == "compact");
    let scheduled = overrides
        .get(DELIVERY_MODE)
        .is_some_and(|mode| mode == DELIVERY_MODE_SCHEDULED);
    if compacting && scheduled {
        return Err(format!(
            "{CLEANUP_POLICY}=compact cannot be combined with \
             {DELIVERY_MODE}={DELIVERY_MODE_SCHEDULED}: compaction deletes a record once a \
             later record carries the same key, and on a scheduled topic that later record \
             can arrive long before the earlier one comes due, so the earlier record would \
             be deleted without a single delivery"
        ));
    }
    Ok(())
}

/// Map the wire-side `compression.type` value to the matching
/// [`krabka_log::LogConfig::compression_type`]. This function returns `Ok(None)` for the
/// special `producer` value, which is the Kafka default and does no
/// broker-side re-encoding. It returns `Ok(Some(_))` for any concrete codec.
/// It returns `Err` for an unknown name.
pub(crate) fn parse_compression_type(
    value: &str,
) -> Result<Option<krabka_compression::CompressionType>, String> {
    use krabka_compression::CompressionType;
    match value {
        "producer" => Ok(None),
        "uncompressed" | "none" => Ok(Some(CompressionType::None)),
        "gzip" => Ok(Some(CompressionType::Gzip)),
        "snappy" => Ok(Some(CompressionType::Snappy)),
        "lz4" => Ok(Some(CompressionType::Lz4)),
        "zstd" => Ok(Some(CompressionType::Zstd)),
        other => Err(format!(
            "compression.type=`{other}` not recognized; expected one of \
             producer, uncompressed, gzip, snappy, lz4, zstd"
        )),
    }
}

fn parse_i64_at_least(min: i64, value: &str) -> Result<i64, String> {
    let parsed: i64 = value
        .parse()
        .map_err(|_| format!("expected integer, got `{value}`"))?;
    if parsed < min {
        return Err(format!("value `{value}` must be >= {min}"));
    }
    Ok(parsed)
}

fn parse_u64_at_least(min: u64, value: &str) -> Result<u64, String> {
    let parsed: u64 = value
        .parse()
        .map_err(|_| format!("expected non-negative integer, got `{value}`"))?;
    if parsed < min {
        return Err(format!("value `{value}` must be >= {min}"));
    }
    Ok(parsed)
}

/// Returns `true` if `key` is one of the recognized topic-config keys.
/// This helps `IncrementalAlterConfigs` DELETE-op validation, which then
/// needs no sentinel probe value.
pub(crate) fn is_recognized(key: &str) -> bool {
    matches!(
        key,
        RETENTION_MS
            | RETENTION_BYTES
            | SEGMENT_BYTES
            | CLEANUP_POLICY
            | COMPRESSION_TYPE
            | MIN_INSYNC_REPLICAS
            | UNCLEAN_LEADER_ELECTION_ENABLE
            | UNCLEAN_RECOVERY_STRATEGY
            | REMOTE_STORAGE_ENABLE
            | LOCAL_RETENTION_MS
            | LOCAL_RETENTION_BYTES
            | DELETE_RETENTION_MS
            | QOS_TIER
            | DELIVERY_MODE
            | DELIVERY_MAX_DELAY_MS
            | DELIVERY_SCHEDULE_MONOTONIC
            | SCHEMA_VALIDATION_KEY
            | SCHEMA_VALIDATION_VALUE
            | SCHEMA_VALIDATION_MODE
            | crate::throttle::LEADER_THROTTLED_REPLICAS_KEY
            | crate::throttle::FOLLOWER_THROTTLED_REPLICAS_KEY
    )
}

#[cfg(test)]
mod tests;
