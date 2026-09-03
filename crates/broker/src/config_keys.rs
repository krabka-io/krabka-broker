//! Topic-config whitelist for `AlterConfigs` / `IncrementalAlterConfigs`.
//!
//! The broker recognizes every key Kafka's `TopicConfig` carries, plus the
//! krabka-private ones. Six of Kafka's propagate live to `Log.config`:
//! `retention.ms`, `retention.bytes`, `segment.bytes`, `cleanup.policy`,
//! `compression.type`, and `delivery.mode`. The tiered-storage local-retention pair
//! (`local.retention.ms`, `local.retention.bytes`), the KIP-534
//! delete-horizon grace window (`delete.retention.ms`), and the batch-size cap
//! (`max.message.bytes`) propagate live too, as do the roll interval
//! (`segment.ms`), the sparse-index spacing (`index.interval.bytes`), the
//! cleaner's three selection keys (`min.compaction.lag.ms`,
//! `max.compaction.lag.ms`, `min.cleanable.dirty.ratio`) and
//! `message.timestamp.type`.
//!
//! `cleanup.policy` is a list, as it is on Kafka: `delete`, `compact`, or
//! `compact,delete`, which both compacts the log and applies retention to it.
//! Kafka Streams writes the pair on every windowed-store changelog topic.
//!
//! Six keys are accepted and stored with no krabka behaviour behind them:
//! `segment.index.bytes`, `segment.jitter.ms`, `file.delete.delay.ms`,
//! `flush.messages`, `flush.ms` and `preallocate`. Kafka accepts them, so a
//! topic manifest that carries one creates the topic here too, and
//! `DescribeConfigs` reports back what was set.
//!
//! One key bounds a single write: `max.message.bytes`. The produce path reads
//! it per topic, falls back to the broker's `message.max.bytes` when the topic
//! sets none, and refuses a larger record batch with `MESSAGE_TOO_LARGE` (10)
//! before it verifies a CRC or decompresses a body.
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
//! One key is krabka's data-path opt-in, `krabka.diskless`. It takes
//! `true`/`false` and it is the only key a partition reads once, when it is
//! opened, so [`validate_diskless_unchanged`] pins it for the life of the
//! topic and `DescribeConfigs` reports it read-only.
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
//! Four pairs of keys exclude each other: a `cleanup.policy` containing
//! `compact` with each of `delivery.mode=scheduled` and
//! `remote.storage.enable=true` (Kafka's
//! `validateNoRemoteStorageForCompactedTopic`), and `krabka.diskless=true`
//! with each of `remote.storage.enable=true` and `delivery.mode=scheduled`.
//! [`validate_topic_config`] sees one pair at a time and cannot see any of
//! them, so [`validate_config_combination`] checks all four rules over a whole
//! override map.
//!
//! Two topic keys sit outside the whitelist, because the controller is their
//! only writer. KFC-9's [`WRITE_FREEZE`] is synthesised for `DescribeConfigs`
//! and is never stored. KIP-966's [`ELIGIBLE_LEADER_REPLICAS`] is stored, but
//! only the controller's ISR transitions write it and only
//! `DescribeTopicPartitions` reads it. [`validate_topic_config`] accepts
//! neither and both alter paths refuse them by name. See
//! [`topic_scope::CONTROLLER_MANAGED_TOPIC_CONFIGS`].
//!
//! The broker rejects unknown keys with `INVALID_CONFIG`.
//!
//! [`registry`] is the one table all of that reads from. A row states a key's
//! name, the `ConfigDef` type byte the JVM `AdminClient` parses its value
//! with, its default, its documentation, whether the broker may disclose the
//! value, and the value check [`validate_topic_config`] applies. The generated
//! reference page in [`docs`] and the typed metadata `DescribeConfigs` reports
//! are both projections of it, so an operator cannot be told one thing by
//! `kafka-configs --describe` and another by an alter refusal.

mod broker_scope;
mod delivery;
mod diskless;
mod docs;
mod log_config;
mod lookup;
mod message_size;
mod min_isr;
mod qos;
mod recovery;
pub(crate) mod registry;
mod schema;
mod topic_scope;
mod validation;

pub use self::docs::{TopicConfigDoc, topic_config_docs};
// Reached only from #[cfg(test)] code -- the produce delivery/throttle tests and
// the alter_configs tests -- so an ungated re-export is dead in a normal build.
#[cfg(test)]
pub(crate) use self::{
    broker_scope::CONTROLLER_MANAGED_BROKER_CONFIGS,
    delivery::{DELIVERY_MAX_DELAY_MS, DELIVERY_MODE_IMMEDIATE, DELIVERY_SCHEDULE_MONOTONIC},
    qos::{DEFAULT_QOS_TIER, QOS_TIER},
    topic_scope::CONTROLLER_MANAGED_TOPIC_CONFIGS,
};
pub(crate) use self::{
    broker_scope::{
        BROKER_FENCED, BROKER_WITNESS, CONNECTIONS_MAX_IDLE_MS, FENCED_TRUE,
        OFFSETS_RETENTION_CHECK_INTERVAL_MS, OFFSETS_RETENTION_MINUTES,
        REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS, STRETCH_PREFERRED_LEADER_SITE,
        TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS, TRANSACTIONAL_ID_EXPIRATION_MS,
        WITNESS_TRUE, fenced_node_ids, is_controller_managed_broker_config,
        parse_remote_list_offsets_timeout, resolve_broker_fenced, resolve_broker_witness,
        resolve_preferred_leader_site, resolve_remote_list_offsets_timeout, witness_node_ids,
    },
    delivery::{DELIVERY_MODE, DELIVERY_MODE_SCHEDULED, resolve_delivery_max_delay},
    diskless::{DISKLESS, resolve_diskless, validate_diskless_unchanged},
    log_config::apply_to_log_config,
    message_size::resolve_max_message_bytes,
    min_isr::{configured_min_insync_replicas, effective_min_insync_replicas},
    qos::resolve_qos_tier,
    recovery::{
        RecoveryStrategy, UNCLEAN_LEADER_ELECTION_ENABLE, UNCLEAN_RECOVERY_STRATEGY,
        resolve_recovery_strategy, resolve_unclean_leader_election_enabled,
    },
    schema::resolve_schema_validation,
    topic_scope::{
        ELIGIBLE_LEADER_REPLICAS, WRITE_FREEZE, controller_managed_topic_config_message,
        is_controller_managed_topic_config,
    },
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
/// The largest record batch a topic accepts, measured over the batch's whole
/// wire encoding. An oversized batch earns `MESSAGE_TOO_LARGE` (10).
pub(crate) const MAX_MESSAGE_BYTES: &str = "max.message.bytes";

/// Kafka's `segment.ms`: the age at which the active segment rolls.
pub(crate) const SEGMENT_MS: &str = "segment.ms";
/// Kafka's `segment.index.bytes`: the size cap on a segment's offset index.
pub(crate) const SEGMENT_INDEX_BYTES: &str = "segment.index.bytes";
/// Kafka's `segment.jitter.ms`: random subtraction from the roll interval.
pub(crate) const SEGMENT_JITTER_MS: &str = "segment.jitter.ms";
/// Kafka's `min.compaction.lag.ms`: how long a record is safe from the
/// cleaner after it is written.
pub(crate) const MIN_COMPACTION_LAG_MS: &str = "min.compaction.lag.ms";
/// Kafka's `max.compaction.lag.ms`: how long a dirty record may go
/// uncompacted before the cleaner runs regardless of the dirty ratio.
pub(crate) const MAX_COMPACTION_LAG_MS: &str = "max.compaction.lag.ms";
/// Kafka's `min.cleanable.dirty.ratio`: the share of the log that must be
/// uncleaned before a compaction pass is worth running.
pub(crate) const MIN_CLEANABLE_DIRTY_RATIO: &str = "min.cleanable.dirty.ratio";
/// Kafka's `file.delete.delay.ms`: the delay before a removed segment file is
/// unlinked.
pub(crate) const FILE_DELETE_DELAY_MS: &str = "file.delete.delay.ms";
/// Kafka's `flush.messages`: records between forced fsyncs.
pub(crate) const FLUSH_MESSAGES: &str = "flush.messages";
/// Kafka's `flush.ms`: milliseconds between forced fsyncs.
pub(crate) const FLUSH_MS: &str = "flush.ms";
/// Kafka's `index.interval.bytes`: bytes of `.log` between sparse index
/// entries.
pub(crate) const INDEX_INTERVAL_BYTES: &str = "index.interval.bytes";
/// Kafka's `preallocate`: whether a new segment file is preallocated.
pub(crate) const PREALLOCATE: &str = "preallocate";
/// Kafka's `message.timestamp.type`: whose clock the stored records carry.
pub(crate) const MESSAGE_TIMESTAMP_TYPE: &str = "message.timestamp.type";
/// Kafka's `message.timestamp.after.max.ms`: how far into the future a
/// producer timestamp may sit.
pub(crate) const MESSAGE_TIMESTAMP_AFTER_MAX_MS: &str = "message.timestamp.after.max.ms";
/// Kafka's `message.timestamp.before.max.ms`: how far into the past a
/// producer timestamp may sit.
pub(crate) const MESSAGE_TIMESTAMP_BEFORE_MAX_MS: &str = "message.timestamp.before.max.ms";
/// `message.timestamp.type=CreateTime`: the producer's own timestamps.
pub(crate) const MESSAGE_TIMESTAMP_TYPE_CREATE: &str = "CreateTime";
/// `message.timestamp.type=LogAppendTime`: the broker's clock at append time.
pub(crate) const MESSAGE_TIMESTAMP_TYPE_LOG_APPEND: &str = "LogAppendTime";

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
