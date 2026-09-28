//! Kafka's `GroupConfig`: the keys a `GROUP` resource carries, their types,
//! defaults and broker synonyms, and `GroupConfig.validate` for the keys
//! krabka's coordinators apply.
//!
//! Kafka trunk defines 27 group keys. krabka's streams coordinator applies
//! the eight `streams.*` keys it runs with, and its share partitions apply
//! `share.auto.offset.reset`. The streams coordinator reads a group's whole
//! stored override map and ignores it all when one key is foreign to it, so
//! an alter that stored any other group key would silently drop the streams
//! overrides beside it. Those keys are therefore reported by
//! `DescribeConfigs` at the broker's value, and refused by the alter paths.

use std::collections::BTreeMap;

use super::parse::{check_range, int_value, invalid_value, java_trim, long_value};
use crate::coordinator::unified::streams::config::{
    GROUP_CONFIG_KEYS, KEY_ACCEPTABLE_RECOVERY_LAG, KEY_ASSIGNOR_NAME, KEY_HEARTBEAT_INTERVAL_MS,
    KEY_NUM_STANDBY_REPLICAS, KEY_NUM_WARMUP_REPLICAS, KEY_SESSION_TIMEOUT_MS,
    KEY_TASK_OFFSET_INTERVAL_MS, StreamsGroupConfig,
};

/// One key of Kafka's `GroupConfig.CONFIG_DEF`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GroupKey {
    pub(crate) name: &'static str,
    pub(crate) config_type: super::registry::ConfigType,
    /// Kafka's default, `ConfigDef.convertToString` of `defaultValue`.
    pub(crate) default: Option<&'static str>,
    /// `GroupConfig.ALL_GROUP_CONFIG_SYNONYMS`: the broker key that sets the
    /// value for every group that does not override it.
    pub(crate) broker_synonym: Option<&'static str>,
}

const fn group_key(
    name: &'static str,
    config_type: super::registry::ConfigType,
    default: Option<&'static str>,
    broker_synonym: Option<&'static str>,
) -> GroupKey {
    GroupKey {
        name,
        config_type,
        default,
        broker_synonym,
    }
}

/// Every key of Kafka trunk's `GroupConfig.CONFIG_DEF`, in the order
/// `DescribeConfigs` lists them (by name).
pub(crate) const KAFKA_GROUP_KEYS: &[GroupKey] = {
    use super::registry::ConfigType::{Boolean, Int, List, Long, String};
    &[
        group_key(
            "consumer.assignment.interval.ms",
            Int,
            Some("1000"),
            Some("group.consumer.assignment.interval.ms"),
        ),
        group_key(
            "consumer.assignor.offload.enable",
            Boolean,
            Some("true"),
            Some("group.consumer.assignor.offload.enable"),
        ),
        group_key(
            "consumer.heartbeat.interval.ms",
            Int,
            Some("5000"),
            Some("group.consumer.heartbeat.interval.ms"),
        ),
        group_key(
            "consumer.session.timeout.ms",
            Int,
            Some("45000"),
            Some("group.consumer.session.timeout.ms"),
        ),
        group_key(
            "errors.deadletterqueue.copy.record.enable",
            Boolean,
            Some("false"),
            None,
        ),
        group_key("errors.deadletterqueue.topic.name", String, Some(""), None),
        group_key(
            "share.assignment.interval.ms",
            Int,
            Some("1000"),
            Some("group.share.assignment.interval.ms"),
        ),
        group_key(
            "share.assignor.offload.enable",
            Boolean,
            Some("true"),
            Some("group.share.assignor.offload.enable"),
        ),
        group_key("share.auto.offset.reset", String, Some("latest"), None),
        group_key(
            "share.delivery.count.limit",
            Int,
            Some("5"),
            Some("group.share.delivery.count.limit"),
        ),
        group_key(
            "share.heartbeat.interval.ms",
            Int,
            Some("5000"),
            Some("group.share.heartbeat.interval.ms"),
        ),
        group_key(
            "share.isolation.level",
            String,
            Some("read_uncommitted"),
            None,
        ),
        group_key(
            "share.partition.max.record.locks",
            Int,
            Some("2000"),
            Some("group.share.partition.max.record.locks"),
        ),
        group_key(
            "share.record.lock.duration.ms",
            Int,
            Some("30000"),
            Some("group.share.record.lock.duration.ms"),
        ),
        group_key(
            "share.renew.acknowledge.enable",
            Boolean,
            Some("true"),
            None,
        ),
        group_key(
            "share.session.timeout.ms",
            Int,
            Some("45000"),
            Some("group.share.session.timeout.ms"),
        ),
        group_key(
            "streams.acceptable.recovery.lag",
            Long,
            Some("10000"),
            Some("group.streams.acceptable.recovery.lag"),
        ),
        group_key(
            "streams.assignment.interval.ms",
            Int,
            Some("1000"),
            Some("group.streams.assignment.interval.ms"),
        ),
        group_key(
            "streams.assignor.name",
            String,
            None,
            Some("group.streams.assignors"),
        ),
        group_key(
            "streams.assignor.offload.enable",
            Boolean,
            Some("true"),
            Some("group.streams.assignor.offload.enable"),
        ),
        group_key(
            "streams.heartbeat.interval.ms",
            Int,
            Some("5000"),
            Some("group.streams.heartbeat.interval.ms"),
        ),
        group_key(
            "streams.initial.rebalance.delay.ms",
            Int,
            Some("3000"),
            Some("group.streams.initial.rebalance.delay.ms"),
        ),
        group_key(
            "streams.num.standby.replicas",
            Int,
            Some("0"),
            Some("group.streams.num.standby.replicas"),
        ),
        group_key(
            "streams.num.warmup.replicas",
            Int,
            Some("2"),
            Some("group.streams.num.warmup.replicas"),
        ),
        group_key(
            "streams.rack.aware.assignment.tags",
            List,
            Some(""),
            Some("group.streams.rack.aware.assignment.tags"),
        ),
        group_key(
            "streams.session.timeout.ms",
            Int,
            Some("45000"),
            Some("group.streams.session.timeout.ms"),
        ),
        group_key(
            "streams.task.offset.interval.ms",
            Int,
            Some("60000"),
            Some("group.streams.task.offset.interval.ms"),
        ),
    ]
};

/// The row for one Kafka group key.
pub(crate) fn kafka_group_key(name: &str) -> Option<&'static GroupKey> {
    KAFKA_GROUP_KEYS.iter().find(|key| key.name == name)
}

/// A registry-shaped row for a Kafka group key that has none of its own:
/// the keys krabka's coordinators do not apply, which `DescribeConfigs`
/// still reports, typed and disclosed, at the broker's value.
pub(crate) fn group_row(key: &GroupKey) -> super::registry::ConfigKey {
    super::registry::ConfigKey {
        name: key.name,
        scope: super::registry::ConfigScope::Group,
        config_type: key.config_type,
        type_note: None,
        default: key.default,
        doc: "A Kafka group config this broker reports at its Kafka value and does not apply.",
        read_only: false,
        sensitive: false,
        kip: None,
        cluster_default: None,
        check: super::registry::ValueCheck::NotAltered,
    }
}

/// Kafka's `group.streams.max.standby.replicas` default.
const MAX_STANDBY_REPLICAS: i32 = 2;
/// Kafka's `group.streams.max.warmup.replicas` default.
const MAX_WARMUP_REPLICAS: i32 = 20;
/// Kafka's `group.streams.min.task.offset.interval.ms` default.
const MIN_TASK_OFFSET_INTERVAL_MS: i32 = 15_000;
/// The task assignors krabka's streams coordinator registers, which is what
/// `group.streams.assignors` names on a Kafka broker.
const REGISTERED_ASSIGNORS: &[&str] = &["auto", "sticky", "highly_available"];

fn millis(duration: std::time::Duration) -> i32 {
    i32::try_from(duration.as_millis()).unwrap_or(i32::MAX)
}

/// Kafka's `GroupConfig.validate` over a group's resulting override map,
/// with the broker bounds `streams` runs under.
///
/// # Errors
/// Returns Kafka's `InvalidConfigurationException` or `ConfigException`
/// text, which the alter paths answer with `INVALID_CONFIG`.
pub(crate) fn validate_group_configs(
    overrides: &BTreeMap<String, String>,
    streams: &StreamsGroupConfig,
) -> Result<(), String> {
    for name in overrides.keys() {
        if kafka_group_key(name).is_none() {
            return Err(format!("Unknown group config name: {name}"));
        }
        if !GROUP_CONFIG_KEYS.contains(&name.as_str()) {
            return Err(format!(
                "Group config {name} is not supported by this broker: its coordinator does not \
                 apply it"
            ));
        }
    }
    let int = |name: &str, min: i32| -> Result<Option<i32>, String> {
        overrides
            .get(name)
            .map(|value| {
                let parsed = int_value(value)
                    .ok_or_else(|| invalid_value(name, value, "Not a number of type INT"))?;
                check_range(name, parsed, Some(min), None)
            })
            .transpose()
    };
    let session = int(KEY_SESSION_TIMEOUT_MS, 1)?;
    let heartbeat = int(KEY_HEARTBEAT_INTERVAL_MS, 1)?;
    let standby = int(KEY_NUM_STANDBY_REPLICAS, 0)?;
    let warmup = int(KEY_NUM_WARMUP_REPLICAS, 0)?;
    let task_offset = int(KEY_TASK_OFFSET_INTERVAL_MS, 1)?;
    if let Some(value) = overrides.get(KEY_ACCEPTABLE_RECOVERY_LAG) {
        let parsed = long_value(value).ok_or_else(|| {
            invalid_value(
                KEY_ACCEPTABLE_RECOVERY_LAG,
                value,
                "Not a number of type LONG",
            )
        })?;
        check_range(KEY_ACCEPTABLE_RECOVERY_LAG, parsed, Some(0), None)?;
    }

    let in_range = |name: &str, value: Option<i32>, min: i32, max: i32| match value {
        Some(value) if value < min || value > max => Err(format!(
            "{name} must be in the range {min} to {max} inclusive."
        )),
        _ => Ok(()),
    };
    in_range(
        KEY_HEARTBEAT_INTERVAL_MS,
        heartbeat,
        millis(streams.min_heartbeat_interval),
        millis(streams.max_heartbeat_interval),
    )?;
    in_range(
        KEY_SESSION_TIMEOUT_MS,
        session,
        millis(streams.min_session_timeout),
        millis(streams.max_session_timeout),
    )?;
    if let Some(value) = standby
        && value > MAX_STANDBY_REPLICAS
    {
        return Err(format!(
            "{KEY_NUM_STANDBY_REPLICAS} must be less than or equal to {MAX_STANDBY_REPLICAS}"
        ));
    }
    if let Some(value) = task_offset
        && value < MIN_TASK_OFFSET_INTERVAL_MS
    {
        return Err(format!(
            "{KEY_TASK_OFFSET_INTERVAL_MS} must be greater than or equal to \
             {MIN_TASK_OFFSET_INTERVAL_MS}"
        ));
    }
    if let Some(value) = warmup
        && value > MAX_WARMUP_REPLICAS
    {
        return Err(format!(
            "{KEY_NUM_WARMUP_REPLICAS} must be less than or equal to {MAX_WARMUP_REPLICAS}"
        ));
    }
    if let Some(value) = overrides.get(KEY_ASSIGNOR_NAME)
        && !REGISTERED_ASSIGNORS.contains(&java_trim(value))
    {
        return Err(format!(
            "{KEY_ASSIGNOR_NAME} '{}' is not a registered task assignor. Registered assignors \
             are: [{}].",
            java_trim(value),
            REGISTERED_ASSIGNORS.join(", ")
        ));
    }
    if session.is_some() || heartbeat.is_some() {
        let session = session.unwrap_or_else(|| millis(streams.session_timeout));
        let heartbeat = heartbeat.unwrap_or_else(|| millis(streams.heartbeat_interval));
        if session <= heartbeat {
            return Err(format!(
                "{KEY_SESSION_TIMEOUT_MS} must be greater than {KEY_HEARTBEAT_INTERVAL_MS}"
            ));
        }
    }
    // `share.auto.offset.reset`, and whatever else the coordinator's own
    // parser checks, is the applier's to refuse.
    streams.with_group_overrides(overrides).map(drop)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn group_overrides_are_validated_with_kafkas_rules_and_messages() {
        let cases = [
            (
                ("not.a.group.key", "1"),
                Err("Unknown group config name: not.a.group.key".to_owned()),
            ),
            (
                (KEY_NUM_STANDBY_REPLICAS, "3"),
                Err("streams.num.standby.replicas must be less than or equal to 2".to_owned()),
            ),
            (
                (KEY_NUM_WARMUP_REPLICAS, "21"),
                Err("streams.num.warmup.replicas must be less than or equal to 20".to_owned()),
            ),
            (
                (KEY_TASK_OFFSET_INTERVAL_MS, "14999"),
                Err(
                    "streams.task.offset.interval.ms must be greater than or equal to 15000"
                        .to_owned(),
                ),
            ),
            (
                (KEY_SESSION_TIMEOUT_MS, "90000"),
                Err(
                    "streams.session.timeout.ms must be in the range 45000 to 60000 inclusive."
                        .to_owned(),
                ),
            ),
            (
                (KEY_SESSION_TIMEOUT_MS, "abc"),
                Err(
                    "Invalid value abc for configuration streams.session.timeout.ms: Not a \
                     number of type INT"
                        .to_owned(),
                ),
            ),
            (
                (KEY_ASSIGNOR_NAME, "range"),
                Err(
                    "streams.assignor.name 'range' is not a registered task assignor. \
                     Registered assignors are: [auto, sticky, highly_available]."
                        .to_owned(),
                ),
            ),
            ((KEY_NUM_STANDBY_REPLICAS, "2"), Ok(())),
            ((KEY_SESSION_TIMEOUT_MS, "60000"), Ok(())),
        ];
        for ((key, value), want) in cases {
            let overrides = BTreeMap::from([(key.to_owned(), value.to_owned())]);
            check!(
                validate_group_configs(&overrides, &StreamsGroupConfig::default()) == want,
                "{key}={value}"
            );
        }
    }

    #[test]
    fn the_session_timeout_must_exceed_the_effective_heartbeat() {
        let streams = StreamsGroupConfig {
            max_heartbeat_interval: std::time::Duration::from_mins(1),
            ..StreamsGroupConfig::default()
        };
        let overrides =
            BTreeMap::from([(KEY_HEARTBEAT_INTERVAL_MS.to_owned(), "45000".to_owned())]);
        check!(
            validate_group_configs(&overrides, &streams)
                == Err(
                    "streams.session.timeout.ms must be greater than streams.heartbeat.interval.ms"
                        .to_owned()
                )
        );
    }

    #[test]
    fn kafkas_group_key_roster_has_every_trunk_key() {
        check!(KAFKA_GROUP_KEYS.len() == 27);
        let names: Vec<&str> = KAFKA_GROUP_KEYS.iter().map(|key| key.name).collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        check!(names == sorted);
        for key in GROUP_CONFIG_KEYS {
            check!(kafka_group_key(key).is_some(), "{key}");
        }
    }
}
