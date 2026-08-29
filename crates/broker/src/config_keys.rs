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
//! The broker rejects unknown keys with `INVALID_CONFIG`.

mod broker_scope;
mod delivery;
mod docs;
mod log_config;
mod qos;
mod recovery;
mod schema;
mod validation;

pub use self::docs::{TopicConfigDoc, topic_config_docs};
pub(crate) use self::{
    broker_scope::{
        BROKER_WITNESS, CONTROLLER_MANAGED_BROKER_CONFIGS, REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS,
        STRETCH_PREFERRED_LEADER_SITE, WITNESS_TRUE, is_controller_managed_broker_config,
        parse_remote_list_offsets_timeout, resolve_broker_witness, resolve_preferred_leader_site,
        resolve_remote_list_offsets_timeout, witness_node_ids,
    },
    delivery::{
        DELIVERY_MAX_DELAY_MS, DELIVERY_MODE, DELIVERY_MODE_IMMEDIATE, DELIVERY_MODE_SCHEDULED,
        DELIVERY_SCHEDULE_MONOTONIC, resolve_delivery_max_delay,
        resolve_delivery_schedule_monotonic,
    },
    log_config::apply_to_log_config,
    qos::{DEFAULT_QOS_TIER, QOS_TIER, resolve_qos_tier},
    recovery::{
        RecoveryStrategy, UNCLEAN_LEADER_ELECTION_ENABLE, UNCLEAN_RECOVERY_STRATEGY,
        resolve_recovery_strategy, resolve_unclean_leader_election_enabled,
    },
    schema::resolve_schema_validation,
    validation::{
        is_recognized, parse_compression_type, validate_config_combination, validate_topic_config,
        validate_topic_config_map,
    },
};

pub(crate) const RETENTION_MS: &str = "retention.ms";
pub(crate) const RETENTION_BYTES: &str = "retention.bytes";
pub(crate) const SEGMENT_BYTES: &str = "segment.bytes";
pub(crate) const CLEANUP_POLICY: &str = "cleanup.policy";
pub(crate) const COMPRESSION_TYPE: &str = "compression.type";
pub(crate) const MIN_INSYNC_REPLICAS: &str = "min.insync.replicas";

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

/// Kafka sentinel for `retention.ms` / `retention.bytes`: `-1` means
/// unlimited retention, and is the lowest legal value.
const RETENTION_UNLIMITED: i64 = -1;

/// KIP-405 sentinel for `local.retention.ms` / `local.retention.bytes`:
/// `-2` means "inherit the corresponding non-local retention setting", and
/// is the lowest legal value (`-1` = unlimited also applies).
const LOCAL_RETENTION_INHERIT: i64 = -2;
