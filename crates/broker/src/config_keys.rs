//! Topic-config whitelist for `AlterConfigs` / `IncrementalAlterConfigs`.
//!
//! The broker recognizes twenty-one topic keys. Six propagate live to `Log.config`:
//! `retention.ms`, `retention.bytes`, `segment.bytes`, `cleanup.policy`,
//! `compression.type`, and `delivery.mode`. The tiered-storage local-retention pair
//! (`local.retention.ms`, `local.retention.bytes`) and the KIP-534
//! delete-horizon grace window (`delete.retention.ms`) propagate live too.
//!
//! The produce hot path's pre-flight gate reads one key,
//! `min.insync.replicas`, which takes integers >= 1. An `acks=-1` produce
//! against a partition whose ISR is already smaller fails fast with
//! `NOT_ENOUGH_REPLICAS` (19).
//!
//! Two keys are KIP-73 throttle keys:
//! `leader.replication.throttled.replicas` and
//! `follower.replication.throttled.replicas`. `ThrottledReplicas::parse`
//! validates both. One key is the KIP-841 unclean-recovery toggle,
//! `unclean.leader.election.enable`. The controller's automatic failover
//! path reads it on ISR-empty. One key is the KIP-966 offset-aware recovery
//! strategy, `unclean.recovery.strategy`, which supersedes that toggle. Both
//! unclean-recovery settings also accept a cluster-wide default broker config;
//! a topic override takes precedence. One
//! key is krabka's `QoS` routing key, `qos.tier`. Producer quota enforcement
//! uses it to partition runtime buckets by topic tier.
//!
//! Three keys carry KFC-1 scheduled delivery: `delivery.mode`,
//! `delivery.max.delay.ms`, and `delivery.schedule.monotonic`. Only the first
//! propagates live to `Log.config`. The produce path reads the other two,
//! which bound how far ahead of produce time a batch may be scheduled and
//! whether a partition's schedule may run backwards.
//!
//! Three keys carry KFC-7 schema validation: `schema.validation.key`,
//! `schema.validation.value`, and `schema.validation.mode`. None of them
//! reaches `Log.config`. The produce path reads all three. The two booleans
//! turn the check on for record keys and for record values. The mode selects
//! how much of each record the check reads.
//!
//! `cleanup.policy=compact` and `delivery.mode=scheduled` exclude each other.
//! [`validate_topic_config`] sees one pair at a time and cannot see that, so
//! [`validate_config_combination`] checks the rule over a whole override map.
//!
//! One topic key sits outside the whitelist. KFC-9's [`WRITE_FREEZE`] is
//! synthesised for `DescribeConfigs` and is never stored, so
//! [`validate_topic_config`] does not accept it and both alter paths refuse
//! it by name. See [`CONTROLLER_MANAGED_TOPIC_CONFIGS`].
//!
//! The broker rejects unknown keys with `INVALID_CONFIG`.

use std::{collections::BTreeMap, time::Duration};

use krabka_log::LogConfig;
use krabka_units::{
    ByteSize, Time,
    convert::{
        ByteSizeExt as _, TimeExt as _,
        wire::{opt_size_from_bytes_i64, opt_time_from_millis_i64},
    },
};

pub(crate) const RETENTION_MS: &str = "retention.ms";
pub(crate) const RETENTION_BYTES: &str = "retention.bytes";
pub(crate) const SEGMENT_BYTES: &str = "segment.bytes";
pub(crate) const CLEANUP_POLICY: &str = "cleanup.policy";
pub(crate) const COMPRESSION_TYPE: &str = "compression.type";
pub(crate) const MIN_INSYNC_REPLICAS: &str = "min.insync.replicas";
/// KIP-841: gates whether the controller may auto-elect an out-of-ISR
/// replica as leader on ISR-empty failover. Default: `false`, which matches
/// Apache Kafka. The partition then stays unavailable until a former ISR
/// member returns. `true` accepts possible data loss in exchange for
/// availability. `crate::leader_election::on_broker_dead` reads the topic
/// override first and then the cluster-wide default broker config.
pub(crate) const UNCLEAN_LEADER_ELECTION_ENABLE: &str = "unclean.leader.election.enable";
/// KIP-966: topic-level unclean-recovery strategy. It supersedes
/// `unclean.leader.election.enable`. At `Balanced` or `Aggressive` the
/// controller runs offset-aware recovery: it polls surviving replicas for
/// their log offsets and elects the most complete log. Default: `None`,
/// which falls back to the legacy enable-flag behavior.
/// `crate::unclean_recovery` and the failover / `ElectLeaders` paths read the
/// topic override first and then the cluster-wide default broker config.
pub(crate) const UNCLEAN_RECOVERY_STRATEGY: &str = "unclean.recovery.strategy";

/// Marks a node as a data-bearing witness. The broker publishes this key for
/// itself, in the same metadata batch that carries its registration record,
/// so the controller can read the role from the metadata image.
///
/// The key is controller-managed and read-only. `AlterConfigs` and
/// `IncrementalAlterConfigs` reject it with `INVALID_CONFIG`, and
/// `DescribeConfigs` returns it with `read_only` set. An operator reads it
/// with `kafka-configs --entity-type brokers --describe`.
///
/// A witness replicates partition data and votes in `KRaft`, so it counts
/// toward `min.insync.replicas`. It serves no client and leads no partition.
pub(crate) const BROKER_WITNESS: &str = "broker.witness";

/// Names the site that should hold partition leadership in a stretch
/// cluster. The controller leader publishes it as a cluster-default broker
/// config, so every node that later becomes controller reads the same value.
///
/// The value is a `broker.rack` value. Site-aware placement puts a broker
/// from this site first in the replica list. In Kafka the preferred leader
/// is `replicas[0]`, so that ordering is what pins leadership.
///
/// The key is controller-managed and read-only, like [`BROKER_WITNESS`].
pub(crate) const STRETCH_PREFERRED_LEADER_SITE: &str = "stretch.preferred.leader.site";

/// The value krabka writes for [`BROKER_WITNESS`] on a witness node.
pub(crate) const WITNESS_TRUE: &str = "true";

/// Broker-scoped config keys that only the controller writes. `AlterConfigs`
/// and `IncrementalAlterConfigs` must reject every key in this list, and
/// `DescribeConfigs` must report each one as read-only.
pub(crate) const CONTROLLER_MANAGED_BROKER_CONFIGS: [&str; 2] =
    [BROKER_WITNESS, STRETCH_PREFERRED_LEADER_SITE];

/// `true` when `key` is a broker config that only the controller writes.
pub(crate) fn is_controller_managed_broker_config(key: &str) -> bool {
    CONTROLLER_MANAGED_BROKER_CONFIGS.contains(&key)
}

/// KFC-9: the write-freeze state of one topic.
///
/// The broker synthesises this key. It is never stored in `V1TopicConfig`,
/// and it never reaches the topic-config record in the metadata log. The
/// freeze itself lives in the metadata log as
/// [`krabka_metadata::TopicFreezeRecord`], so no snapshot and no restore can
/// bring back a stale freeze through a topic config.
///
/// The key is controller-managed and read-only. `DescribeConfigs` reports it
/// with `read_only` set, and both `AlterConfigs` and
/// `IncrementalAlterConfigs` refuse it with `INVALID_CONFIG`. The
/// krabka-private `SetTopicFreeze` API (key 1015) and the `krabka-guard` CLI
/// are the one path that sets and clears it.
///
/// An operator who holds only the JVM tools reads the freeze with
/// `kafka-configs --entity-type topics --describe`.
pub(crate) const WRITE_FREEZE: &str = "write.freeze";

/// Topic-scoped config keys that only the controller writes. This is the
/// topic-side analogue of [`CONTROLLER_MANAGED_BROKER_CONFIGS`]:
/// `AlterConfigs` and `IncrementalAlterConfigs` must reject every key in this
/// list, and `DescribeConfigs` must report each one as read-only.
pub(crate) const CONTROLLER_MANAGED_TOPIC_CONFIGS: [&str; 1] = [WRITE_FREEZE];

/// `true` when `key` is a topic config that only the controller writes.
pub(crate) fn is_controller_managed_topic_config(key: &str) -> bool {
    CONTROLLER_MANAGED_TOPIC_CONFIGS.contains(&key)
}

/// The refusal both alter paths give for a controller-managed topic config.
///
/// Both handlers build the message here, so an operator reads one wording
/// from `AlterConfigs` and from `IncrementalAlterConfigs`. The message names
/// the commands that change the key. A refusal that names no command leaves
/// the operator with no next step.
pub(crate) fn controller_managed_topic_config_message(key: &str) -> String {
    format!(
        "topic config {key} is controller-managed and read-only; \
         use `krabka-guard freeze set` to set it and `krabka-guard freeze clear` to clear it"
    )
}

/// Resolve [`BROKER_WITNESS`] for one node. A missing or unparseable value
/// resolves to `false`, so a cluster with no witness behaves as it did
/// before the role existed.
pub(crate) fn resolve_broker_witness(
    image: &krabka_metadata::MetadataImage,
    node_id: krabka_metadata::NodeId,
) -> bool {
    image
        .broker_config(node_id)
        .and_then(|configs| configs.get(BROKER_WITNESS))
        .map(String::as_str)
        == Some(WITNESS_TRUE)
}

/// Every registered node that carries the witness role.
///
/// The controller builds this set once for each scan and then excludes its
/// members from leader selection. Building it once keeps the scan a single
/// walk over the image rather than a lookup for each partition replica.
pub(crate) fn witness_node_ids(
    image: &krabka_metadata::MetadataImage,
) -> std::collections::HashSet<krabka_metadata::NodeId> {
    image
        .brokers()
        .filter(|broker| resolve_broker_witness(image, broker.node_id))
        .map(|broker| broker.node_id)
        .collect()
}

/// Resolve [`STRETCH_PREFERRED_LEADER_SITE`] from the cluster defaults.
/// `None` means the cluster pins leadership to no site.
pub(crate) fn resolve_preferred_leader_site(
    image: &krabka_metadata::MetadataImage,
) -> Option<&str> {
    image
        .default_broker_config()?
        .get(STRETCH_PREFERRED_LEADER_SITE)
        .map(String::as_str)
}

/// Resolved value of `unclean.recovery.strategy` for a topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryStrategy {
    /// No offset-aware recovery. Defer to `unclean.leader.election.enable`.
    None,
    /// Wait for all currently-alive replicas, then elect the most complete
    /// log. krabka does not track ELR.
    Balanced,
    /// Elect the most complete log among the replicas that respond within
    /// a short deadline. This optimizes availability.
    Aggressive,
}

impl RecoveryStrategy {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "None" => Some(Self::None),
            "Balanced" => Some(Self::Balanced),
            "Aggressive" => Some(Self::Aggressive),
            _ => None,
        }
    }
}
/// KIP-405: per-topic tiered-storage opt-in.
pub(crate) const REMOTE_STORAGE_ENABLE: &str = "remote.storage.enable";
/// KIP-405: per-topic local-retention time window for tiered partitions.
pub(crate) const LOCAL_RETENTION_MS: &str = "local.retention.ms";
/// KIP-405: per-topic local-retention size budget for tiered partitions.
pub(crate) const LOCAL_RETENTION_BYTES: &str = "local.retention.bytes";
/// KIP-534: how long the broker keeps tombstones and transaction markers
/// after they first become compaction-eligible. This is the delete-horizon
/// grace window.
pub(crate) const DELETE_RETENTION_MS: &str = "delete.retention.ms";
/// Krabka extension: per-topic `QoS` tier used to partition producer quota
/// buckets. Unset topics resolve to [`DEFAULT_QOS_TIER`].
pub(crate) const QOS_TIER: &str = "qos.tier";
pub(crate) const DEFAULT_QOS_TIER: &str = "default";

/// KFC-1: per-topic delivery mode. `immediate`, the default, is Kafka's
/// behavior. `scheduled` makes each batch's `max_timestamp` its delivery time,
/// so a record stays invisible to consumers until it comes due. This is the
/// one delivery key that reaches [`LogConfig::delivery_policy`].
pub(crate) const DELIVERY_MODE: &str = "delivery.mode";
pub(crate) const DELIVERY_MODE_IMMEDIATE: &str = "immediate";
pub(crate) const DELIVERY_MODE_SCHEDULED: &str = "scheduled";

/// KFC-1: the largest delay the produce path accepts, measured forward from
/// produce time. `-1` removes the limit. A batch scheduled further ahead is
/// rejected with `INVALID_TIMESTAMP` (32).
pub(crate) const DELIVERY_MAX_DELAY_MS: &str = "delivery.max.delay.ms";
/// Default `delivery.max.delay.ms`: 7 days.
pub(crate) const DEFAULT_DELIVERY_MAX_DELAY_MS: i64 = 604_800_000;

/// KFC-1: when `true`, the produce path rejects a batch whose delivery time is
/// before the largest delivery time already in the partition. It turns a
/// silently stalled schedule into an `INVALID_TIMESTAMP` (32) at the producer
/// that caused it. Default `false`.
pub(crate) const DELIVERY_SCHEDULE_MONOTONIC: &str = "delivery.schedule.monotonic";

/// KFC-7: when `true`, the produce path validates the schema of every record
/// key on this topic. A record that fails the check is rejected with
/// `INVALID_RECORD` (87). Default `false`. This key does not reach
/// [`LogConfig`].
pub(crate) const SCHEMA_VALIDATION_KEY: &str = "schema.validation.key";

/// KFC-7: when `true`, the produce path validates the schema of every record
/// value on this topic. It is the same check that [`SCHEMA_VALIDATION_KEY`]
/// asks for, on the other half of the record. Default `false`.
pub(crate) const SCHEMA_VALIDATION_VALUE: &str = "schema.validation.value";

/// KFC-7: how much of a record the schema check reads. `id`, the default,
/// reads the five-byte Confluent header alone. `full` also decodes the body
/// against the schema that the header names. This key alone turns nothing on:
/// a topic that sets the mode and leaves both booleans `false` runs no check.
pub(crate) const SCHEMA_VALIDATION_MODE: &str = "schema.validation.mode";
pub(crate) const SCHEMA_VALIDATION_MODE_ID: &str = "id";
pub(crate) const SCHEMA_VALIDATION_MODE_FULL: &str = "full";

/// KIP-1075: server-side deadline for remote `ListOffsets` work when an older
/// request does not carry `timeout_ms`. Kafka exposes this as a dynamic broker
/// config and defaults it to 30 seconds.
pub(crate) const REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS: &str =
    "remote.list.offsets.request.timeout.ms";
pub(crate) const DEFAULT_REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Kafka sentinel for `retention.ms` / `retention.bytes`: `-1` means
/// unlimited retention, and is the lowest legal value.
const RETENTION_UNLIMITED: i64 = -1;

/// KFC-1 sentinel for `delivery.max.delay.ms`: `-1` means no bound on how far
/// ahead a batch may be scheduled, and is the lowest legal value.
const DELIVERY_MAX_DELAY_UNLIMITED: i64 = -1;

/// KIP-405 sentinel for `local.retention.ms` / `local.retention.bytes`:
/// `-2` means "inherit the corresponding non-local retention setting", and
/// is the lowest legal value (`-1` = unlimited also applies).
const LOCAL_RETENTION_INHERIT: i64 = -2;

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
/// [`LogConfig::compression_type`]. This function returns `Ok(None)` for the
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

fn validate_qos_tier(value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err("qos.tier must not be empty".into());
    }
    if value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
    {
        Ok(())
    } else {
        Err(format!(
            "qos.tier={value} not supported; expected non-empty ASCII letters, digits, '.', '_' or '-'"
        ))
    }
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

/// Resolve a topic's `QoS` tier, which partitions producer quota buckets.
/// Missing or corrupt values fall back to `default`. This matches the
/// permissive runtime behavior of other Produce-side topic config reads.
#[must_use]
pub(crate) fn resolve_qos_tier<'a>(
    image: &'a krabka_metadata::MetadataImage,
    topic: &str,
) -> &'a str {
    image
        .topic_config(topic)
        .and_then(|m| m.get(QOS_TIER))
        .filter(|v| validate_qos_tier(v).is_ok())
        .map_or(DEFAULT_QOS_TIER, String::as_str)
}

/// Resolve `delivery.max.delay.ms` for `topic`: the largest delay the produce
/// path accepts, measured forward from produce time. `None` is the `-1`
/// sentinel and removes the bound. A missing or unparseable value resolves to
/// the 7-day default, which matches the permissive runtime behavior of the
/// other Produce-side topic config reads.
#[must_use]
pub(crate) fn resolve_delivery_max_delay(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> Option<Time> {
    let millis = image
        .topic_config(topic)
        .and_then(|configs| configs.get(DELIVERY_MAX_DELAY_MS))
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|millis| *millis >= DELIVERY_MAX_DELAY_UNLIMITED)
        .unwrap_or(DEFAULT_DELIVERY_MAX_DELAY_MS);
    opt_time_from_millis_i64(millis)
}

/// Resolve `delivery.schedule.monotonic` for `topic`. `true` makes the produce
/// path reject a batch whose delivery time is before the largest delivery time
/// already in the partition. A missing or unparseable value resolves to
/// `false`.
#[must_use]
pub(crate) fn resolve_delivery_schedule_monotonic(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> bool {
    image
        .topic_config(topic)
        .and_then(|configs| configs.get(DELIVERY_SCHEDULE_MONOTONIC))
        .map(String::as_str)
        == Some("true")
}

/// Resolve the KFC-7 schema-validation gate for `topic`. `None` means the
/// topic asks for no check, and no schema-validation code then runs on its
/// produce path. A missing or unparseable value resolves to its default:
/// `false` for the two booleans and `id` for the mode. This matches the
/// permissive runtime behavior of the other Produce-side topic config reads.
///
/// `schema.validation.mode` alone does not turn the check on, so a topic that
/// sets only the mode still resolves to `None`.
#[must_use]
pub(crate) fn resolve_schema_validation(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> Option<crate::schema_validation::SchemaGate> {
    use crate::schema_validation::{SchemaGate, ValidationMode};

    let configs = image.topic_config(topic);
    let read = |key: &str| {
        configs
            .and_then(|configs| configs.get(key))
            .map(String::as_str)
    };
    let gate = SchemaGate {
        key: read(SCHEMA_VALIDATION_KEY) == Some("true"),
        value: read(SCHEMA_VALIDATION_VALUE) == Some("true"),
        mode: match read(SCHEMA_VALIDATION_MODE) {
            Some(SCHEMA_VALIDATION_MODE_FULL) => ValidationMode::Full,
            _ => ValidationMode::Id,
        },
    };
    gate.is_active().then_some(gate)
}

fn topic_or_cluster_default<'a>(
    image: &'a krabka_metadata::MetadataImage,
    topic: &str,
    key: &str,
) -> Option<&'a str> {
    image
        .topic_config(topic)
        .and_then(|configs| configs.get(key))
        .or_else(|| image.default_broker_config()?.get(key))
        .map(String::as_str)
}

/// Resolve `unclean.recovery.strategy` for `topic`. A topic override takes
/// precedence over the cluster-wide default broker config. The result is
/// [`RecoveryStrategy::None`] when neither value exists or the selected value
/// is unparseable.
pub(crate) fn resolve_recovery_strategy(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> RecoveryStrategy {
    topic_or_cluster_default(image, topic, UNCLEAN_RECOVERY_STRATEGY)
        .and_then(RecoveryStrategy::parse)
        .unwrap_or(RecoveryStrategy::None)
}

/// Resolve `unclean.leader.election.enable` for `topic`. A topic override
/// takes precedence over the cluster-wide default broker config. Missing or
/// invalid values resolve to `false`.
pub(crate) fn resolve_unclean_leader_election_enabled(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> bool {
    topic_or_cluster_default(image, topic, UNCLEAN_LEADER_ELECTION_ENABLE) == Some("true")
}

/// Parse KIP-1075's dynamic broker timeout.
pub(crate) fn parse_remote_list_offsets_timeout(value: &str) -> Result<Duration, String> {
    let millis = value
        .parse::<i32>()
        .map_err(|_| format!("{REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS} must be a positive int"))?;
    if millis <= 0 {
        return Err(format!(
            "{REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS} must be in 1..={}",
            i32::MAX
        ));
    }
    Ok(Duration::from_millis(
        u64::try_from(millis).expect("positive i32 fits u64"),
    ))
}

/// Resolve the per-broker KIP-1075 timeout over the cluster default.
pub(crate) fn resolve_remote_list_offsets_timeout(
    image: &krabka_metadata::MetadataImage,
    node_id: krabka_metadata::NodeId,
) -> Duration {
    image
        .broker_config(node_id)
        .and_then(|configs| configs.get(REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS))
        .or_else(|| {
            image
                .default_broker_config()
                .and_then(|configs| configs.get(REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS))
        })
        .and_then(|value| parse_remote_list_offsets_timeout(value).ok())
        .unwrap_or(DEFAULT_REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT)
}

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

/// One whitelisted topic-config key, for the generated reference page.
#[derive(Debug, Clone, Copy)]
pub struct TopicConfigDoc {
    pub key: &'static str,
    pub value_type: &'static str,
    pub default: Option<&'static str>,
    pub kip: Option<&'static str>,
    pub description: &'static str,
}

const TOPIC_CONFIG_DOCS: &[TopicConfigDoc] = &[
    TopicConfigDoc {
        key: RETENTION_MS,
        value_type: "long (ms)",
        default: None,
        kip: None,
        description: "Retention time before log segments become eligible for deletion.",
    },
    TopicConfigDoc {
        key: RETENTION_BYTES,
        value_type: "long (bytes)",
        default: None,
        kip: None,
        description: "Maximum partition size before old segments are deleted.",
    },
    TopicConfigDoc {
        key: SEGMENT_BYTES,
        value_type: "int (bytes)",
        default: None,
        kip: None,
        description: "Target size of a single log segment file.",
    },
    TopicConfigDoc {
        key: CLEANUP_POLICY,
        value_type: "string",
        default: Some("delete"),
        kip: None,
        description: "`delete`, `compact`, or `compact,delete`.",
    },
    TopicConfigDoc {
        key: COMPRESSION_TYPE,
        value_type: "string",
        default: Some("producer"),
        kip: None,
        description: "Broker-side compression codec for the topic.",
    },
    TopicConfigDoc {
        key: MIN_INSYNC_REPLICAS,
        value_type: "int (>=1)",
        default: Some("1"),
        kip: None,
        description: "With acks=all, the minimum in-sync replicas required to accept a write; otherwise NOT_ENOUGH_REPLICAS (19).",
    },
    TopicConfigDoc {
        key: UNCLEAN_LEADER_ELECTION_ENABLE,
        value_type: "boolean",
        default: Some("false"),
        kip: Some("KIP-841"),
        description: "Allow electing an out-of-ISR replica as leader on ISR-empty failover (possible data loss).",
    },
    TopicConfigDoc {
        key: UNCLEAN_RECOVERY_STRATEGY,
        value_type: "string",
        default: Some("None"),
        kip: Some("KIP-966"),
        description: "Offset-aware unclean recovery: `None`, `Balanced`, or `Aggressive`. Supersedes unclean.leader.election.enable.",
    },
    TopicConfigDoc {
        key: REMOTE_STORAGE_ENABLE,
        value_type: "boolean",
        default: Some("false"),
        kip: Some("KIP-405"),
        description: "Opt this topic into tiered (remote) storage.",
    },
    TopicConfigDoc {
        key: LOCAL_RETENTION_MS,
        value_type: "long (ms)",
        default: None,
        kip: Some("KIP-405"),
        description: "Local-tier retention time for tiered partitions.",
    },
    TopicConfigDoc {
        key: LOCAL_RETENTION_BYTES,
        value_type: "long (bytes)",
        default: None,
        kip: Some("KIP-405"),
        description: "Local-tier retention size budget for tiered partitions.",
    },
    TopicConfigDoc {
        key: DELETE_RETENTION_MS,
        value_type: "long (ms)",
        default: Some("86400000"),
        kip: Some("KIP-534"),
        description: "How long tombstones and transaction markers are retained after becoming compaction-eligible.",
    },
    TopicConfigDoc {
        key: QOS_TIER,
        value_type: "string",
        default: Some(DEFAULT_QOS_TIER),
        kip: None,
        description: "Krabka QoS tier used to partition producer quota buckets.",
    },
    TopicConfigDoc {
        key: DELIVERY_MODE,
        value_type: "string",
        default: Some(DELIVERY_MODE_IMMEDIATE),
        kip: Some("KFC-1"),
        description: "`immediate` or `scheduled`. Under `scheduled` a batch stays invisible to consumers until its own timestamp comes due.",
    },
    TopicConfigDoc {
        key: DELIVERY_MAX_DELAY_MS,
        value_type: "long (ms)",
        default: Some("604800000"),
        kip: Some("KFC-1"),
        description: "Largest delivery delay accepted at produce time, measured forward from produce time; -1 removes the bound.",
    },
    TopicConfigDoc {
        key: DELIVERY_SCHEDULE_MONOTONIC,
        value_type: "boolean",
        default: Some("false"),
        kip: Some("KFC-1"),
        description: "Reject a batch whose delivery time precedes the largest delivery time already in the partition.",
    },
    TopicConfigDoc {
        key: SCHEMA_VALIDATION_KEY,
        value_type: "boolean",
        default: Some("false"),
        kip: Some("KFC-7"),
        description: "Validate the schema of every record key produced to this topic.",
    },
    TopicConfigDoc {
        key: SCHEMA_VALIDATION_VALUE,
        value_type: "boolean",
        default: Some("false"),
        kip: Some("KFC-7"),
        description: "Validate the schema of every record value produced to this topic.",
    },
    TopicConfigDoc {
        key: SCHEMA_VALIDATION_MODE,
        value_type: "string",
        default: Some(SCHEMA_VALIDATION_MODE_ID),
        kip: Some("KFC-7"),
        description: "`id` checks the Confluent header alone; `full` also decodes the record body against the schema the header names.",
    },
    TopicConfigDoc {
        key: crate::throttle::LEADER_THROTTLED_REPLICAS_KEY,
        value_type: "string",
        default: None,
        kip: Some("KIP-73"),
        description: "Replica list throttled on the leader side during reassignment.",
    },
    TopicConfigDoc {
        key: crate::throttle::FOLLOWER_THROTTLED_REPLICAS_KEY,
        value_type: "string",
        default: None,
        kip: Some("KIP-73"),
        description: "Replica list throttled on the follower side during reassignment.",
    },
];

/// The full whitelist documented on the topic-configs reference page.
#[must_use]
pub fn topic_config_docs() -> Vec<TopicConfigDoc> {
    TOPIC_CONFIG_DOCS.to_vec()
}

#[cfg(test)]
mod doc_tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn topic_config_docs_cover_known_keys() {
        use std::collections::HashSet;
        let docs = topic_config_docs();
        let doc_keys: HashSet<&str> = docs.iter().map(|d| d.key).collect();
        // No duplicate keys in the doc table.
        assert!(
            doc_keys.len() == docs.len(),
            "duplicate key in topic_config_docs"
        );
        // Every documented key is recognized by the validator.
        for k in &doc_keys {
            assert!(
                is_recognized(k),
                "documented key `{k}` not recognized by validator"
            );
        }
        // Every recognized key is documented.
        for k in [
            RETENTION_MS,
            RETENTION_BYTES,
            SEGMENT_BYTES,
            CLEANUP_POLICY,
            COMPRESSION_TYPE,
            MIN_INSYNC_REPLICAS,
            UNCLEAN_LEADER_ELECTION_ENABLE,
            UNCLEAN_RECOVERY_STRATEGY,
            REMOTE_STORAGE_ENABLE,
            LOCAL_RETENTION_MS,
            LOCAL_RETENTION_BYTES,
            DELETE_RETENTION_MS,
            QOS_TIER,
            DELIVERY_MODE,
            DELIVERY_MAX_DELAY_MS,
            DELIVERY_SCHEDULE_MONOTONIC,
            SCHEMA_VALIDATION_KEY,
            SCHEMA_VALIDATION_VALUE,
            SCHEMA_VALIDATION_MODE,
            crate::throttle::LEADER_THROTTLED_REPLICAS_KEY,
            crate::throttle::FOLLOWER_THROTTLED_REPLICAS_KEY,
        ] {
            assert!(
                doc_keys.contains(k),
                "recognized key `{k}` missing from topic_config_docs"
            );
        }
        assert!(docs.iter().all(|d| !d.description.is_empty()));
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::{bytes, mebibytes, millis, minutes};

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
    fn validate_min_isr_positive_accepted() {
        assert!(validate_topic_config(MIN_INSYNC_REPLICAS, "2").is_ok());
    }

    #[test]
    fn validate_unknown_key_rejected() {
        let err = validate_topic_config("flush.ms", "1000").unwrap_err();
        assert!(err.contains("unrecognized"));
    }

    #[test]
    fn validate_qos_tier_accepts_ascii_identifiers() {
        for v in ["default", "gold", "bulk_1", "critical-prod", "tier.2"] {
            assert!(validate_topic_config(QOS_TIER, v).is_ok(), "qos.tier={v}");
        }
    }

    #[test]
    fn validate_qos_tier_rejects_empty_or_unsafe_values() {
        for v in ["", "has space", "../escape", "ümlaut"] {
            assert!(validate_topic_config(QOS_TIER, v).is_err(), "qos.tier={v}");
        }
    }

    #[test]
    fn resolve_qos_tier_defaults_when_unset() {
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        assert!(resolve_qos_tier(&image, "t") == DEFAULT_QOS_TIER);
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
    fn a_controller_managed_topic_config_is_named_as_one() {
        for (label, key, expected) in [
            ("the write-freeze key", WRITE_FREEZE, true),
            ("an ordinary topic key", RETENTION_MS, false),
            (
                "a broker-scoped controller-managed key",
                BROKER_WITNESS,
                false,
            ),
            ("an unknown key", "flush.ms", false),
            ("an empty key", "", false),
        ] {
            check!(
                is_controller_managed_topic_config(key) == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn the_write_freeze_key_stays_outside_the_stored_whitelist() {
        // It is synthesised for `DescribeConfigs` and never stored, so the
        // validator must not accept it as an ordinary override.
        check!(!is_recognized(WRITE_FREEZE));
        check!(validate_topic_config(WRITE_FREEZE, "true").is_err());
    }

    #[test]
    fn the_refusal_names_the_key_and_the_commands_that_change_it() {
        let message = controller_managed_topic_config_message(WRITE_FREEZE);

        check!(message.contains(WRITE_FREEZE), "got: {message}");
        check!(
            message.contains("krabka-guard freeze set"),
            "got: {message}"
        );
        check!(
            message.contains("krabka-guard freeze clear"),
            "got: {message}"
        );
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
    fn validate_unclean_leader_election_enable_accepts_bools() {
        assert!(validate_topic_config(UNCLEAN_LEADER_ELECTION_ENABLE, "true").is_ok());
        assert!(validate_topic_config(UNCLEAN_LEADER_ELECTION_ENABLE, "false").is_ok());
    }

    #[test]
    fn validate_unclean_leader_election_enable_rejects_junk() {
        let err = validate_topic_config(UNCLEAN_LEADER_ELECTION_ENABLE, "yes").unwrap_err();
        assert!(err.contains("unclean.leader.election.enable"), "got: {err}");
    }

    #[test]
    fn is_recognized_includes_unclean_leader_election_enable() {
        assert!(is_recognized(UNCLEAN_LEADER_ELECTION_ENABLE));
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
    fn apply_delete_retention_ms_propagates() {
        let mut o = BTreeMap::new();
        o.insert(DELETE_RETENTION_MS.into(), "12345".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.delete_retention == millis(12_345));
    }

    #[test]
    fn recovery_strategy_accepts_valid_values() {
        for v in ["None", "Balanced", "Aggressive"] {
            assert!(
                validate_topic_config(UNCLEAN_RECOVERY_STRATEGY, v).is_ok(),
                "{v}"
            );
        }
    }

    #[test]
    fn recovery_strategy_rejects_garbage() {
        assert!(validate_topic_config(UNCLEAN_RECOVERY_STRATEGY, "fast").is_err());
    }

    #[test]
    fn recovery_strategy_recognized() {
        assert!(is_recognized(UNCLEAN_RECOVERY_STRATEGY));
    }

    #[test]
    fn parse_recovery_strategy_maps_values() {
        let cases = [
            ("None", Some(RecoveryStrategy::None)),
            ("Balanced", Some(RecoveryStrategy::Balanced)),
            ("Aggressive", Some(RecoveryStrategy::Aggressive)),
            ("bogus", None),
        ];
        for (input, want) in cases {
            assert!(RecoveryStrategy::parse(input) == want, "input {input:?}");
        }
    }

    #[test]
    fn recovery_settings_resolve_topic_over_cluster_default() {
        use std::collections::BTreeMap;

        use krabka_metadata::{
            BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataImage, MetadataRecord,
            TopicConfigRecord,
        };
        use uuid::Uuid;
        let mut img = MetadataImage::new(Uuid::nil());
        assert!(resolve_recovery_strategy(&img, "t") == RecoveryStrategy::None);
        assert!(!resolve_unclean_leader_election_enabled(&img, "t"));

        for (key, value) in [
            (UNCLEAN_RECOVERY_STRATEGY, "Balanced"),
            (UNCLEAN_LEADER_ELECTION_ENABLE, "true"),
        ] {
            img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: key.into(),
                config_value: Some(value.into()),
            }));
        }
        assert!(resolve_recovery_strategy(&img, "t") == RecoveryStrategy::Balanced);
        assert!(resolve_unclean_leader_election_enabled(&img, "t"));

        let mut overrides = BTreeMap::new();
        overrides.insert(UNCLEAN_RECOVERY_STRATEGY.into(), "Aggressive".into());
        overrides.insert(UNCLEAN_LEADER_ELECTION_ENABLE.into(), "false".into());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides,
        }));
        assert!(resolve_recovery_strategy(&img, "t") == RecoveryStrategy::Aggressive);
        assert!(!resolve_unclean_leader_election_enabled(&img, "t"));
    }

    /// Register `node_id` and, when `witness` is set, publish
    /// `broker.witness=true` for it. This is the path the broker takes at
    /// registration.
    fn register_node(
        img: &mut krabka_metadata::MetadataImage,
        node_id: u64,
        witness: Option<&str>,
    ) {
        use krabka_metadata::{
            BrokerConfigRecord, BrokerRegistrationRecord, MetadataRecord, NodeId,
        };
        img.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: NodeId(node_id),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: "127.0.0.1".into(),
                port: 9_092,
                rack: None,
                endpoints: vec![],
                log_dirs: vec![],
                features: BTreeMap::new(),
            },
        ));
        if let Some(value) = witness {
            img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: NodeId(node_id),
                config_name: BROKER_WITNESS.into(),
                config_value: Some(value.into()),
            }));
        }
    }

    #[test]
    fn resolve_broker_witness_reads_only_an_exact_true() {
        use krabka_metadata::NodeId;
        // (published value, expected role)
        let cases = [
            (None, false),
            (Some(WITNESS_TRUE), true),
            (Some("false"), false),
            (Some("TRUE"), false),
            (Some(""), false),
            (Some("yes"), false),
        ];
        for (value, want) in cases {
            let mut img = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
            register_node(&mut img, 1, value);
            assert!(
                resolve_broker_witness(&img, NodeId(1)) == want,
                "broker.witness={value:?}"
            );
        }
    }

    #[test]
    fn resolve_broker_witness_is_false_for_an_unregistered_node() {
        use krabka_metadata::NodeId;
        let img = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        assert!(!resolve_broker_witness(&img, NodeId(7)));
    }

    #[test]
    fn resolve_broker_witness_does_not_read_the_cluster_default() {
        use krabka_metadata::{
            BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataRecord, NodeId,
        };
        // The role is per node. A cluster default must not turn every broker
        // into a witness.
        let mut img = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        register_node(&mut img, 1, None);
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
            config_name: BROKER_WITNESS.into(),
            config_value: Some(WITNESS_TRUE.into()),
        }));
        assert!(!resolve_broker_witness(&img, NodeId(1)));
    }

    #[test]
    fn witness_node_ids_collects_every_marked_node() {
        use std::collections::HashSet;

        use krabka_metadata::NodeId;
        let mut img = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        assert!(witness_node_ids(&img) == HashSet::new());
        register_node(&mut img, 1, None);
        register_node(&mut img, 2, Some(WITNESS_TRUE));
        register_node(&mut img, 3, Some("false"));
        register_node(&mut img, 4, Some(WITNESS_TRUE));
        assert!(witness_node_ids(&img) == HashSet::from([NodeId(2), NodeId(4)]));
    }

    #[test]
    fn broker_witness_is_controller_managed_and_not_a_topic_config() {
        assert!(is_controller_managed_broker_config(BROKER_WITNESS));
        assert!(!is_recognized(BROKER_WITNESS));
        assert!(validate_topic_config(BROKER_WITNESS, WITNESS_TRUE).is_err());
    }

    #[test]
    fn validate_delivery_mode_accepts_the_two_modes_only() {
        let cases = [
            (DELIVERY_MODE_IMMEDIATE, true),
            (DELIVERY_MODE_SCHEDULED, true),
            ("later", false),
            ("", false),
        ];
        for (value, want_ok) in cases {
            assert!(
                validate_topic_config(DELIVERY_MODE, value).is_ok() == want_ok,
                "delivery.mode={value}"
            );
        }
    }

    #[test]
    fn validate_delivery_max_delay_ms_boundary_cases() {
        let cases = [
            ("0", true),         // no delay at all is legal
            ("604800000", true), // the default, 7 days
            ("-1", true),        // -1 (unbounded) accepted
            ("-2", false),       // below -1 rejected
            ("soon", false),     // non-integer rejected
        ];
        for (value, want_ok) in cases {
            assert!(
                validate_topic_config(DELIVERY_MAX_DELAY_MS, value).is_ok() == want_ok,
                "delivery.max.delay.ms={value}"
            );
        }
    }

    #[test]
    fn validate_delivery_schedule_monotonic_accepts_bools_only() {
        let cases = [("true", true), ("false", true), ("yes", false), ("", false)];
        for (value, want_ok) in cases {
            assert!(
                validate_topic_config(DELIVERY_SCHEDULE_MONOTONIC, value).is_ok() == want_ok,
                "delivery.schedule.monotonic={value}"
            );
        }
    }

    #[test]
    fn is_recognized_includes_delivery_keys() {
        assert!(is_recognized(DELIVERY_MODE));
        assert!(is_recognized(DELIVERY_MAX_DELAY_MS));
        assert!(is_recognized(DELIVERY_SCHEDULE_MONOTONIC));
    }

    #[test]
    fn apply_delivery_mode_propagates_both_ways() {
        let mut scheduled = BTreeMap::new();
        scheduled.insert(DELIVERY_MODE.into(), DELIVERY_MODE_SCHEDULED.into());
        assert!(
            apply_to_log_config(&scheduled, &LogConfig::default())
                == LogConfig {
                    delivery_policy: krabka_log::DeliveryPolicy::Scheduled,
                    ..LogConfig::default()
                }
        );

        let base = LogConfig {
            delivery_policy: krabka_log::DeliveryPolicy::Scheduled,
            ..LogConfig::default()
        };
        let mut immediate = BTreeMap::new();
        immediate.insert(DELIVERY_MODE.into(), DELIVERY_MODE_IMMEDIATE.into());
        assert!(apply_to_log_config(&immediate, &base) == LogConfig::default());
    }

    #[test]
    fn apply_leaves_delivery_policy_alone_for_the_produce_side_keys() {
        // Both keys are enforced on the produce path, so neither may move the
        // log's own visibility policy.
        let mut overrides = BTreeMap::new();
        overrides.insert(DELIVERY_MAX_DELAY_MS.into(), "1000".into());
        overrides.insert(DELIVERY_SCHEDULE_MONOTONIC.into(), "true".into());
        assert!(apply_to_log_config(&overrides, &LogConfig::default()) == LogConfig::default());
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
        let overrides = BTreeMap::from([
            (CLEANUP_POLICY.to_string(), "compact".to_string()),
            (
                DELIVERY_MODE.to_string(),
                DELIVERY_MODE_SCHEDULED.to_string(),
            ),
        ]);
        let error = validate_config_combination(&overrides).unwrap_err();
        assert!(error.contains(CLEANUP_POLICY), "got: {error}");
        assert!(error.contains(DELIVERY_MODE), "got: {error}");
    }

    #[test]
    fn validate_topic_config_map_checks_pairs_and_then_combinations() {
        let accepted = BTreeMap::from([
            (RETENTION_MS.to_string(), "60000".to_string()),
            (
                DELIVERY_MODE.to_string(),
                DELIVERY_MODE_SCHEDULED.to_string(),
            ),
        ]);
        assert!(validate_topic_config_map(&accepted) == Ok(()));

        let bad_pair = BTreeMap::from([(DELIVERY_MODE.to_string(), "later".to_string())]);
        assert!(validate_topic_config_map(&bad_pair).is_err());

        let unknown_key = BTreeMap::from([("flush.ms".to_string(), "1000".to_string())]);
        assert!(validate_topic_config_map(&unknown_key).is_err());

        let bad_combination = BTreeMap::from([
            (CLEANUP_POLICY.to_string(), "compact".to_string()),
            (
                DELIVERY_MODE.to_string(),
                DELIVERY_MODE_SCHEDULED.to_string(),
            ),
        ]);
        assert!(validate_topic_config_map(&bad_combination).is_err());
    }

    #[test]
    fn delivery_settings_resolve_topic_overrides_over_defaults() {
        use krabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord};
        use uuid::Uuid;

        let mut image = MetadataImage::new(Uuid::nil());
        assert!(resolve_delivery_max_delay(&image, "t") == Some(millis(604_800_000)));
        assert!(!resolve_delivery_schedule_monotonic(&image, "t"));

        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: BTreeMap::from([
                (DELIVERY_MAX_DELAY_MS.into(), "90000".into()),
                (DELIVERY_SCHEDULE_MONOTONIC.into(), "true".into()),
            ]),
        }));
        assert!(resolve_delivery_max_delay(&image, "t") == Some(millis(90_000)));
        assert!(resolve_delivery_schedule_monotonic(&image, "t"));

        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: BTreeMap::from([(DELIVERY_MAX_DELAY_MS.into(), "-1".into())]),
        }));
        assert!(resolve_delivery_max_delay(&image, "t") == None);
        assert!(!resolve_delivery_schedule_monotonic(&image, "t"));
    }

    #[test]
    fn corrupt_delivery_settings_resolve_to_their_defaults() {
        use krabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord};
        use uuid::Uuid;

        let cases = ["soon", "-5", ""];
        for value in cases {
            let mut image = MetadataImage::new(Uuid::nil());
            image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "t".into(),
                overrides: BTreeMap::from([
                    (DELIVERY_MAX_DELAY_MS.into(), value.into()),
                    (DELIVERY_SCHEDULE_MONOTONIC.into(), value.into()),
                ]),
            }));
            assert!(
                resolve_delivery_max_delay(&image, "t") == Some(millis(604_800_000)),
                "delivery.max.delay.ms={value}"
            );
            assert!(
                !resolve_delivery_schedule_monotonic(&image, "t"),
                "delivery.schedule.monotonic={value}"
            );
        }
    }

    /// A metadata image whose topic `t` carries exactly `overrides`.
    fn image_with_topic_config(overrides: &[(&str, &str)]) -> krabka_metadata::MetadataImage {
        use krabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord};
        use uuid::Uuid;

        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: overrides
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
        }));
        image
    }

    #[test]
    fn validate_schema_validation_booleans_accept_bools_only() {
        let cases = [
            (SCHEMA_VALIDATION_KEY, "true", true),
            (SCHEMA_VALIDATION_KEY, "false", true),
            (SCHEMA_VALIDATION_KEY, "yes", false),
            (SCHEMA_VALIDATION_KEY, "True", false),
            (SCHEMA_VALIDATION_KEY, "", false),
            (SCHEMA_VALIDATION_VALUE, "true", true),
            (SCHEMA_VALIDATION_VALUE, "false", true),
            (SCHEMA_VALIDATION_VALUE, "1", false),
            (SCHEMA_VALIDATION_VALUE, "", false),
        ];
        for (key, value, want_ok) in cases {
            check!(
                validate_topic_config(key, value).is_ok() == want_ok,
                "{key}={value}"
            );
        }
    }

    #[test]
    fn validate_schema_validation_mode_accepts_the_two_modes_only() {
        let cases = [
            (SCHEMA_VALIDATION_MODE_ID, true),
            (SCHEMA_VALIDATION_MODE_FULL, true),
            ("Full", false),
            ("body", false),
            ("", false),
        ];
        for (value, want_ok) in cases {
            check!(
                validate_topic_config(SCHEMA_VALIDATION_MODE, value).is_ok() == want_ok,
                "schema.validation.mode={value}"
            );
        }
    }

    #[test]
    fn schema_validation_mode_rejection_names_both_modes() {
        let error = validate_topic_config(SCHEMA_VALIDATION_MODE, "body").unwrap_err();
        assert!(error == "schema.validation.mode=body not supported; expected `id` or `full`");
    }

    #[test]
    fn is_recognized_includes_schema_validation_keys() {
        assert!(is_recognized(SCHEMA_VALIDATION_KEY));
        assert!(is_recognized(SCHEMA_VALIDATION_VALUE));
        assert!(is_recognized(SCHEMA_VALIDATION_MODE));
    }

    #[test]
    fn apply_leaves_log_config_alone_for_the_schema_validation_keys() {
        // All three keys are enforced on the produce path, so none of them may
        // reach the log's own config.
        let overrides = BTreeMap::from([
            (SCHEMA_VALIDATION_KEY.to_string(), "true".to_string()),
            (SCHEMA_VALIDATION_VALUE.to_string(), "true".to_string()),
            (
                SCHEMA_VALIDATION_MODE.to_string(),
                SCHEMA_VALIDATION_MODE_FULL.to_string(),
            ),
        ]);
        assert!(apply_to_log_config(&overrides, &LogConfig::default()) == LogConfig::default());
    }

    #[test]
    fn resolve_schema_validation_reads_the_three_keys() {
        use crate::schema_validation::{SchemaGate, ValidationMode};

        let cases = [
            // Neither boolean is set, so the topic has no gate.
            (Vec::new(), None),
            (
                vec![
                    (SCHEMA_VALIDATION_KEY, "false"),
                    (SCHEMA_VALIDATION_VALUE, "false"),
                ],
                None,
            ),
            // The mode alone does not turn the check on.
            (
                vec![(SCHEMA_VALIDATION_MODE, SCHEMA_VALIDATION_MODE_FULL)],
                None,
            ),
            (
                vec![
                    (SCHEMA_VALIDATION_KEY, "false"),
                    (SCHEMA_VALIDATION_VALUE, "false"),
                    (SCHEMA_VALIDATION_MODE, SCHEMA_VALIDATION_MODE_FULL),
                ],
                None,
            ),
            // Either boolean alone gives a gate, and the mode defaults to `id`.
            (
                vec![(SCHEMA_VALIDATION_KEY, "true")],
                Some(SchemaGate {
                    key: true,
                    value: false,
                    mode: ValidationMode::Id,
                }),
            ),
            (
                vec![(SCHEMA_VALIDATION_VALUE, "true")],
                Some(SchemaGate {
                    key: false,
                    value: true,
                    mode: ValidationMode::Id,
                }),
            ),
            (
                vec![
                    (SCHEMA_VALIDATION_VALUE, "true"),
                    (SCHEMA_VALIDATION_MODE, SCHEMA_VALIDATION_MODE_ID),
                ],
                Some(SchemaGate {
                    key: false,
                    value: true,
                    mode: ValidationMode::Id,
                }),
            ),
            (
                vec![
                    (SCHEMA_VALIDATION_KEY, "true"),
                    (SCHEMA_VALIDATION_MODE, SCHEMA_VALIDATION_MODE_FULL),
                ],
                Some(SchemaGate {
                    key: true,
                    value: false,
                    mode: ValidationMode::Full,
                }),
            ),
            (
                vec![
                    (SCHEMA_VALIDATION_KEY, "true"),
                    (SCHEMA_VALIDATION_VALUE, "true"),
                    (SCHEMA_VALIDATION_MODE, SCHEMA_VALIDATION_MODE_FULL),
                ],
                Some(SchemaGate {
                    key: true,
                    value: true,
                    mode: ValidationMode::Full,
                }),
            ),
        ];
        for (overrides, want) in cases {
            let image = image_with_topic_config(&overrides);
            check!(
                resolve_schema_validation(&image, "t") == want,
                "{overrides:?}"
            );
        }
    }

    #[test]
    fn a_topic_with_no_config_at_all_has_no_schema_validation_gate() {
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        assert!(resolve_schema_validation(&image, "t").is_none());
    }

    #[test]
    fn corrupt_schema_validation_settings_resolve_to_their_defaults() {
        use crate::schema_validation::{SchemaGate, ValidationMode};

        // A corrupt boolean resolves to `false`, which leaves no gate.
        for value in ["yes", "TRUE", "1", ""] {
            let image = image_with_topic_config(&[
                (SCHEMA_VALIDATION_KEY, value),
                (SCHEMA_VALIDATION_VALUE, value),
            ]);
            check!(
                resolve_schema_validation(&image, "t").is_none(),
                "schema.validation.key=schema.validation.value={value}"
            );
        }

        // A corrupt mode resolves to `id`, and the gate stays on.
        for value in ["Full", "body", ""] {
            let image = image_with_topic_config(&[
                (SCHEMA_VALIDATION_VALUE, "true"),
                (SCHEMA_VALIDATION_MODE, value),
            ]);
            check!(
                resolve_schema_validation(&image, "t")
                    == Some(SchemaGate {
                        key: false,
                        value: true,
                        mode: ValidationMode::Id,
                    }),
                "schema.validation.mode={value}"
            );
        }
    }

    #[test]
    fn invalid_topic_recovery_setting_does_not_expose_cluster_default() {
        use std::collections::BTreeMap;

        use krabka_metadata::{
            BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataImage, MetadataRecord,
            TopicConfigRecord,
        };
        use uuid::Uuid;

        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
            config_name: UNCLEAN_RECOVERY_STRATEGY.into(),
            config_value: Some("Balanced".into()),
        }));
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: BTreeMap::from([(UNCLEAN_RECOVERY_STRATEGY.into(), "invalid".into())]),
        }));

        assert!(resolve_recovery_strategy(&img, "t") == RecoveryStrategy::None);
    }
}
