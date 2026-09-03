//! The `AlterConfigs` value checks: the per-key validator, the whole-map
//! validator with the cross-key rules, and the whitelist membership test.

use std::collections::BTreeMap;

use krabka_log::CleanupPolicy;

use super::{
    CLEANUP_POLICY, COMPRESSION_TYPE, MAX_COMPACTION_LAG_MS, MESSAGE_TIMESTAMP_TYPE,
    MESSAGE_TIMESTAMP_TYPE_LOG_APPEND, MIN_CLEANABLE_DIRTY_RATIO, MIN_COMPACTION_LAG_MS,
    REMOTE_LOG_COPY_DISABLE, REMOTE_LOG_DELETE_ON_DISABLE, REMOTE_STORAGE_ENABLE,
    delivery::{DELIVERY_MODE, DELIVERY_MODE_SCHEDULED},
    diskless::validate_diskless_combination,
    qos::{QOS_TIER, validate_qos_tier},
    registry::{self, BOOLEAN_VALUES, CLEANUP_POLICY_VALUES, ConfigScope, ValueCheck},
};

/// Kafka's refusal when a topic asks for tiered storage and compaction at
/// once. `LogConfig.validate` calls `validateNoRemoteStorageForCompactedTopic`
/// whenever `remote.storage.enable` is true and the cleanup policy contains
/// `compact`, and every alter path surfaces the `ConfigException` it throws as
/// `INVALID_CONFIG`.
pub(crate) const REMOTE_STORAGE_COMPACTED_MESSAGE: &str =
    "Tiered storage is not supported for compacted topics";

/// Validate a single key/value pair. `Err(reason)` carries an
/// operator-readable explanation that the handler propagates into the
/// `error_message` field of the response.
///
/// The accepted values come from the key's row in [`super::registry`], so the
/// check an operator meets here is the one the reference page and
/// `DescribeConfigs` describe. Four keys carry a parser their own module
/// owns; the rest are a closed value list or a numeric floor.
pub(crate) fn validate_topic_config(key: &str, value: &str) -> Result<(), String> {
    let Some(row) = registry::lookup(ConfigScope::Topic, key).filter(|row| row.is_alterable())
    else {
        return Err(format!("unrecognized config key `{key}`"));
    };
    match row.check {
        ValueCheck::Bool => expect_one_of(key, value, BOOLEAN_VALUES),
        ValueCheck::OneOf(accepted) => expect_one_of(key, value, accepted),
        ValueCheck::I64AtLeast(min) => parse_i64_at_least(min, value).map(|_| ()),
        ValueCheck::I32AtLeast(min) => parse_i32_at_least(min, value).map(|_| ()),
        ValueCheck::Parsed => match key {
            CLEANUP_POLICY => parse_cleanup_policy(value).map(|_| ()),
            COMPRESSION_TYPE => parse_compression_type(value).map(|_| ()),
            MIN_CLEANABLE_DIRTY_RATIO => parse_dirty_ratio(value).map(|_| ()),
            QOS_TIER => validate_qos_tier(value),
            crate::throttle::LEADER_THROTTLED_REPLICAS_KEY
            | crate::throttle::FOLLOWER_THROTTLED_REPLICAS_KEY => {
                crate::throttle::ThrottledReplicas::parse(value).map(|_| ())
            }
            other => Err(format!("unrecognized config key `{other}`")),
        },
        // The `let ... else` above has already refused every key an alter
        // path may not write, which is every `NotAltered` row.
        ValueCheck::NotAltered => Err(format!("unrecognized config key `{key}`")),
    }
}

/// The refusal a closed value list gives, spelled the way Kafka's own
/// `ConfigDef` refusals read: the offending pair, then the accepted values in
/// the order the registry lists them.
fn expect_one_of(key: &str, value: &str, accepted: &[&str]) -> Result<(), String> {
    if accepted.contains(&value) {
        return Ok(());
    }
    Err(format!(
        "{key}={value} not supported; expected {}",
        list_values(accepted)
    ))
}

/// `` `a` or `b` `` for two values, `` `a`, `b`, or `c` `` for more.
fn list_values(accepted: &[&str]) -> String {
    match accepted {
        [] => String::new(),
        [only] => format!("`{only}`"),
        [first, second] => format!("`{first}` or `{second}`"),
        [rest @ .., last] => {
            let head = rest
                .iter()
                .map(|value| format!("`{value}`"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{head}, or `{last}`")
        }
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
/// KFC-1 states the first such rule: `cleanup.policy=compact` and
/// `delivery.mode=scheduled` exclude each other. Compaction deletes a record
/// once a later record carries the same key, and on a scheduled topic that
/// later record can arrive long before the earlier one comes due. The earlier
/// record would then be deleted without a single delivery, which is the
/// failure scheduled delivery exists to prevent.
///
/// Kafka states the second: `remote.storage.enable=true` and a cleanup policy
/// containing `compact` exclude each other. `LogConfig.validate` refuses the
/// pair in `validateNoRemoteStorageForCompactedTopic`, and because the test is
/// a membership test over the policy list, `compact,delete` is refused beside
/// `compact`.
///
/// KFC-1 states a third: `message.timestamp.type=LogAppendTime` and
/// `delivery.mode=scheduled` exclude each other. A scheduled topic reads each
/// batch's `max_timestamp` as its activation time, and log-append stamping
/// overwrites exactly that field with the broker's clock at append. The pair
/// would silently deliver every record at once, and the schedule the producer
/// wrote would be unrecoverable, because the overwrite destroys it.
///
/// The other two are the data-path rules in [`validate_diskless_combination`]:
/// `krabka.diskless=true` excludes both `remote.storage.enable=true` and
/// `delivery.mode=scheduled`.
pub(crate) fn validate_config_combination(
    overrides: &BTreeMap<String, String>,
) -> Result<(), String> {
    let compacting = overrides.get(CLEANUP_POLICY).is_some_and(|policy| {
        parse_cleanup_policy(policy).is_ok_and(CleanupPolicy::contains_compact)
    });
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
    let log_append_time = overrides
        .get(MESSAGE_TIMESTAMP_TYPE)
        .is_some_and(|value| value == MESSAGE_TIMESTAMP_TYPE_LOG_APPEND);
    if log_append_time && scheduled {
        return Err(format!(
            "{MESSAGE_TIMESTAMP_TYPE}={MESSAGE_TIMESTAMP_TYPE_LOG_APPEND} cannot be combined \
             with {DELIVERY_MODE}={DELIVERY_MODE_SCHEDULED}: a scheduled topic reads a batch's \
             max timestamp as its activation time, and log-append stamping overwrites that \
             field with the broker's clock, so every record would come due at once"
        ));
    }
    let tiered = overrides
        .get(REMOTE_STORAGE_ENABLE)
        .is_some_and(|enabled| enabled == "true");
    if compacting && tiered {
        return Err(REMOTE_STORAGE_COMPACTED_MESSAGE.to_owned());
    }
    validate_compaction_lag_order(overrides)?;
    validate_diskless_combination(overrides)
}

/// Kafka's `LogConfig.validateValues`: the cleaner cannot be told to protect a
/// record for longer than the deadline that forces it to be compacted.
///
/// Each key alone passes its own range check, so the rule belongs here, where
/// the whole map is in hand. A key the request leaves alone is read at its
/// registry default, which is what Kafka's `props` carries for an unset key:
/// `min.compaction.lag.ms` 0 and `max.compaction.lag.ms` `i64::MAX`, so an
/// alter that sets only one of the two is still checked against the other.
fn validate_compaction_lag_order(overrides: &BTreeMap<String, String>) -> Result<(), String> {
    let lag = |name: &str| -> Option<i64> {
        overrides
            .get(name)
            .map(String::as_str)
            .or_else(|| registry::lookup(ConfigScope::Topic, name).and_then(|row| row.default))
            .and_then(|value| value.trim().parse::<i64>().ok())
    };
    let (Some(min), Some(max)) = (lag(MIN_COMPACTION_LAG_MS), lag(MAX_COMPACTION_LAG_MS)) else {
        return Ok(());
    };
    if min > max {
        return Err(format!(
            "conflict topic config setting {MIN_COMPACTION_LAG_MS} ({min}) > \
             {MAX_COMPACTION_LAG_MS} ({max})"
        ));
    }
    Ok(())
}

/// Kafka's KIP-950 `LogConfig.validateRemoteStorageConfigs`: turning tiered
/// storage off is refused unless the operator has said what should happen to
/// the segments already in the tier.
///
/// `remote.storage.enable` going `true -> false` erases the topic's remote
/// copies and raises its log start offset to the local log start, so Kafka
/// makes the operator ask for that explicitly with
/// `remote.log.delete.on.disable=true`. The alternative it names in the same
/// message is the read-only tier: keep `remote.storage.enable=true` and set
/// `remote.log.copy.disable=true`, which stops new copies while the history
/// stays readable.
///
/// `current` is the topic's stored override map and `next` the map the alter
/// installs, so this is the only rule here that reads both: the others decide
/// a map on its own.
///
/// # Errors
/// Returns the refusal message when the alter turns tiered storage off
/// without `remote.log.delete.on.disable=true` in the resulting map.
pub(crate) fn validate_remote_storage_disable(
    current: Option<&BTreeMap<String, String>>,
    next: &BTreeMap<String, String>,
) -> Result<(), String> {
    let enabled = |map: Option<&BTreeMap<String, String>>| {
        map.and_then(|map| map.get(REMOTE_STORAGE_ENABLE))
            .is_some_and(|value| value == "true")
    };
    if !enabled(current) || enabled(Some(next)) {
        return Ok(());
    }
    let deleting = next
        .get(REMOTE_LOG_DELETE_ON_DISABLE)
        .is_some_and(|value| value == "true");
    if deleting {
        return Ok(());
    }
    Err(format!(
        "It is invalid to disable remote storage without deleting remote data. If you want to          keep the remote data, but turn to read only, please set          {REMOTE_STORAGE_ENABLE}=true,{REMOTE_LOG_COPY_DISABLE}=true. If you want to disable          remote storage and delete all remote data, please set          {REMOTE_STORAGE_ENABLE}=false,{REMOTE_LOG_DELETE_ON_DISABLE}=true."
    ))
}

/// Parse Kafka's `cleanup.policy` list into the policy a partition runs under.
///
/// The value is a comma-separated list, and Kafka derives two independent
/// booleans from it: `compact` when the list names `compact`, `delete` when it
/// names `delete`. Either order and either name alone is accepted, and so is
/// `compact,delete`, which Kafka Streams writes on every windowed-store
/// changelog topic. An empty list, an empty element and an unknown name are
/// all refused.
pub(crate) fn parse_cleanup_policy(value: &str) -> Result<CleanupPolicy, String> {
    let mut compact = false;
    let mut delete = false;
    for name in value.split(',') {
        match name.trim() {
            "compact" => compact = true,
            "delete" => delete = true,
            other => {
                return Err(format!(
                    "{CLEANUP_POLICY}={other} not supported; expected {}, or both separated by a comma",
                    list_values(CLEANUP_POLICY_VALUES)
                ));
            }
        }
    }
    match (compact, delete) {
        (true, true) => Ok(CleanupPolicy::CompactAndDelete),
        (true, false) => Ok(CleanupPolicy::Compact),
        (false, true) => Ok(CleanupPolicy::Delete),
        // `split` always yields one element, so this is the empty value alone.
        (false, false) => Err(format!(
            "{CLEANUP_POLICY}= not supported; expected {}, or both separated by a comma",
            list_values(CLEANUP_POLICY_VALUES)
        )),
    }
}

/// Parse Kafka's `min.cleanable.dirty.ratio`, a `DOUBLE` between 0 and 1
/// inclusive. `apache/kafka:4.3.1` refuses anything outside that range with
/// `Invalid value 2.0 for configuration min.cleanable.dirty.ratio: Value must
/// be no more than 1`.
pub(crate) fn parse_dirty_ratio(value: &str) -> Result<krabka_units::Ratio, String> {
    let parsed: f64 = value
        .parse()
        .map_err(|_| format!("expected a number, got `{value}`"))?;
    if !parsed.is_finite() || !(0.0..=1.0).contains(&parsed) {
        return Err(format!("value `{value}` must be between 0 and 1"));
    }
    Ok(krabka_units::fraction(parsed))
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

/// The check an `INT` row carries. Kafka parses the value of a key it types
/// `INT` with `Integer.parseInt`, so `2147483648` is not a value the key can
/// hold however large the broker's own runtime type is: `apache/kafka:4.3.1`
/// answers `Invalid value 2147483648 for configuration min.insync.replicas:
/// Not a number of type INT`. Refusing it here keeps the value an operator
/// may set inside the type `DescribeConfigs` advertises.
fn parse_i32_at_least(min: i32, value: &str) -> Result<i32, String> {
    let parsed: i32 = value
        .parse()
        .map_err(|_| format!("expected a 32-bit integer, got `{value}`"))?;
    if parsed < min {
        return Err(format!("value `{value}` must be >= {min}"));
    }
    Ok(parsed)
}

/// Returns `true` if `key` is one of the recognized topic-config keys.
/// This helps `IncrementalAlterConfigs` DELETE-op validation, which then
/// needs no sentinel probe value. A controller-written key such as
/// [`super::WRITE_FREEZE`] or [`super::ELIGIBLE_LEADER_REPLICAS`] is not
/// recognized: no alter path may write it.
pub(crate) fn is_recognized(key: &str) -> bool {
    registry::lookup(ConfigScope::Topic, key).is_some_and(registry::ConfigKey::is_alterable)
}

#[cfg(test)]
mod tests;
