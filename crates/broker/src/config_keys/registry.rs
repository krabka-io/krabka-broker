//! The one typed table every config surface reads.
//!
//! `AlterConfigs` validation, the generated topic-config reference page, and
//! `DescribeConfigs` all used to carry their own idea of what a config key is.
//! The validator knew the accepted values, the reference page knew the type
//! and the default, and `DescribeConfigs` knew neither, so every key it
//! reported came back at `ConfigDef.Type::UNKNOWN` with no default chain and
//! `is_sensitive` hard-wired to `false`. This module holds that knowledge once
//! and the three surfaces read it.
//!
//! A row carries the name, the [`ConfigType`] the JVM `AdminClient` parses the
//! value with, the default an unset key reports, the documentation
//! `include_documentation` returns, whether the value is sensitive, and
//! whether `DescribeConfigs` must mark the key read-only. It also carries the
//! value check `validate_topic_config` applies, so the accepted values are
//! stated beside the type rather than in a second `match`.
//!
//! Rows are keyed by [`ConfigScope`] and name together: a key such as
//! `unclean.leader.election.enable` is both a topic config and a
//! cluster-default broker config, and the two rows differ.

use super::{
    CLEANUP_POLICY, COMPRESSION_TYPE, DELETE_RETENTION_MS, FILE_DELETE_DELAY_MS, FLUSH_MESSAGES,
    FLUSH_MS, INDEX_INTERVAL_BYTES, LOCAL_RETENTION_BYTES, LOCAL_RETENTION_INHERIT,
    LOCAL_RETENTION_MS, MAX_COMPACTION_LAG_MS, MAX_MESSAGE_BYTES, MESSAGE_TIMESTAMP_AFTER_MAX_MS,
    MESSAGE_TIMESTAMP_BEFORE_MAX_MS, MESSAGE_TIMESTAMP_TYPE, MESSAGE_TIMESTAMP_TYPE_CREATE,
    MESSAGE_TIMESTAMP_TYPE_LOG_APPEND, MIN_CLEANABLE_DIRTY_RATIO, MIN_COMPACTION_LAG_MS,
    MIN_INSYNC_REPLICAS, PREALLOCATE, REMOTE_LOG_COPY_DISABLE, REMOTE_LOG_DELETE_ON_DISABLE,
    REMOTE_STORAGE_ENABLE, RETENTION_BYTES, RETENTION_MS, RETENTION_UNLIMITED, SEGMENT_BYTES,
    SEGMENT_INDEX_BYTES, SEGMENT_JITTER_MS, SEGMENT_MS,
    broker_scope::{
        BROKER_FENCED, BROKER_WITNESS, CONNECTIONS_MAX_IDLE_MS, CONNECTIONS_MAX_REAUTH_MS,
        OFFSETS_RETENTION_CHECK_INTERVAL_MS, OFFSETS_RETENTION_MINUTES,
        REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS, STRETCH_PREFERRED_LEADER_SITE,
        TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS, TRANSACTIONAL_ID_EXPIRATION_MS,
    },
    delivery::{
        DELIVERY_MAX_DELAY_MS, DELIVERY_MAX_DELAY_UNLIMITED, DELIVERY_MODE,
        DELIVERY_MODE_IMMEDIATE, DELIVERY_MODE_SCHEDULED, DELIVERY_SCHEDULE_MONOTONIC,
    },
    diskless::DISKLESS,
    qos::{DEFAULT_QOS_TIER, QOS_TIER},
    recovery::{UNCLEAN_LEADER_ELECTION_ENABLE, UNCLEAN_RECOVERY_STRATEGY},
    schema::{
        SCHEMA_VALIDATION_KEY, SCHEMA_VALIDATION_MODE, SCHEMA_VALIDATION_MODE_FULL,
        SCHEMA_VALIDATION_MODE_ID, SCHEMA_VALIDATION_VALUE,
    },
    topic_scope::{ELIGIBLE_LEADER_REPLICAS, WRITE_FREEZE},
};

/// The `node.id` a broker resource reports beside its dynamic overrides. The
/// broker reads it from its own static configuration, never from the metadata
/// image, so it is the one broker key with no stored form.
pub(crate) const NODE_ID: &str = "node.id";

/// `DescribeConfigsResponse.ConfigType`, the byte the JVM `AdminClient` reads
/// out of `ConfigEntry.type()`.
///
/// The values are
/// `org.apache.kafka.common.requests.DescribeConfigsResponse.ConfigType`,
/// which mirrors `ConfigDef.Type` one-for-one: `UNKNOWN = 0`, `BOOLEAN = 1`,
/// `STRING = 2`, `INT = 3`, `SHORT = 4`, `LONG = 5`, `DOUBLE = 6`, `LIST = 7`,
/// `CLASS = 8`, `PASSWORD = 9`. The variants here are the ones krabka's keys
/// carry; add the rest when a key needs one.
///
/// `UNKNOWN` is deliberately absent. A key krabka reports is a key krabka has
/// a row for, and the `DescribeConfigs` handler treats a missing row as a
/// config it may not disclose rather than as an untyped one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConfigType {
    Boolean,
    String,
    Int,
    Long,
    Double,
    List,
}

impl ConfigType {
    /// The wire byte `DescribeConfigsResourceResult.config_type` carries.
    pub(crate) const fn wire(self) -> i8 {
        match self {
            Self::Boolean => 1,
            Self::String => 2,
            Self::Int => 3,
            Self::Long => 5,
            Self::Double => 6,
            Self::List => 7,
        }
    }

    /// The name the generated reference page prints in its type column.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::String => "string",
            Self::Int => "int",
            Self::Long => "long",
            Self::Double => "double",
            Self::List => "list",
        }
    }
}

/// The resource type a row belongs to. A name alone does not identify a row:
/// `unclean.leader.election.enable` is both a topic key and a cluster-default
/// broker key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ConfigScope {
    Topic,
    Broker,
    ClientMetrics,
    Group,
}

/// The value check `validate_topic_config` applies to a topic key.
///
/// The four mechanical checks cover every key whose accepted values are a
/// closed set or a numeric floor. [`ValueCheck::Parsed`] names the keys whose
/// check is a parser their own module owns, and
/// [`ValueCheck::NotAltered`] names the keys no alter path accepts at all.
///
/// The width of a numeric check follows the row's [`ConfigType`], because the
/// width is what the JVM `AdminClient` parses the value with: a key krabka
/// reports as [`ConfigType::Int`] must refuse what Kafka's `INT` cannot hold.
/// `apache/kafka:4.3.1` refuses `segment.bytes=2147483648` with
/// `Invalid value 2147483648 for configuration segment.bytes: Not a number of
/// type INT`, so krabka refuses it too.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValueCheck {
    /// `true` or `false`, exactly.
    Bool,
    /// One of a closed list, in the order the refusal names them.
    OneOf(&'static [&'static str]),
    /// An `i64` no smaller than the bound. Only a [`ConfigType::Long`] row
    /// may carry it.
    I64AtLeast(i64),
    /// An `i32` no smaller than the bound, which is the width Kafka's `INT`
    /// carries on the wire.
    I32AtLeast(i32),
    /// Checked by a parser the key's own module owns.
    Parsed,
    /// Never accepted by an alter path: the broker synthesises the key, or
    /// only the controller writes it.
    NotAltered,
}

/// One config key, as every surface that reports it needs to know it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ConfigKey {
    pub(crate) name: &'static str,
    pub(crate) scope: ConfigScope,
    /// The type the JVM `AdminClient` parses the value with.
    pub(crate) config_type: ConfigType,
    /// A unit or range the reference page appends to the type name, such as
    /// `ms` or `>=1`. It is documentation, not a second type.
    pub(crate) type_note: Option<&'static str>,
    /// The value an unset key reports, at `DEFAULT_CONFIG`. `None` means the
    /// broker computes the default at request time, or that the key has none.
    pub(crate) default: Option<&'static str>,
    /// What `include_documentation` returns for the key.
    pub(crate) doc: &'static str,
    /// `true` when no alter path may change the key, which
    /// `DescribeConfigs` reports as `read_only`.
    pub(crate) read_only: bool,
    /// `true` when the broker must not disclose the value. Kafka reads this
    /// off `ConfigDef.Type::PASSWORD`; krabka states it, because a key can be
    /// secret without being a password. `DescribeConfigs` reports a sensitive
    /// key with a null value, in the entry and in every synonym.
    pub(crate) sensitive: bool,
    /// The KIP or KFC the key comes from, for the reference page.
    pub(crate) kip: Option<&'static str>,
    /// The cluster-default broker config the broker falls back to when the
    /// resource sets no value of its own. `None` means the broker consults
    /// none, so the key reports no `DYNAMIC_DEFAULT_BROKER_CONFIG` synonym.
    pub(crate) cluster_default: Option<&'static str>,
    pub(crate) check: ValueCheck,
}

impl ConfigKey {
    /// `true` when the resource's stored override map can hold the key.
    ///
    /// The broker synthesises the rest: `write.freeze` comes from the freeze
    /// registry, and `node.id`, the two KIP-211 retention keys, the two
    /// KIP-98 transactional-id expiry keys and the idle window come from the
    /// broker's own static configuration, which no metadata record holds.
    pub(crate) fn is_stored(&self) -> bool {
        !matches!(
            self.name,
            WRITE_FREEZE
                | NODE_ID
                | OFFSETS_RETENTION_MINUTES
                | OFFSETS_RETENTION_CHECK_INTERVAL_MS
                | CONNECTIONS_MAX_IDLE_MS
                | CONNECTIONS_MAX_REAUTH_MS
                | TRANSACTIONAL_ID_EXPIRATION_MS
                | TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS
        )
    }

    /// `true` when an alter path may write the key, which is what the
    /// reference page documents and what `validate_topic_config` accepts.
    ///
    /// Two things put a key outside that set. A key the resource's override
    /// map cannot hold is synthesised, so no alter has anywhere to store it.
    /// A key whose check is [`ValueCheck::NotAltered`] is stored but
    /// controller-written: KIP-966's `krabka.elr` and the broker fencing and
    /// stretch-site keys are published by the controller as cluster state
    /// changes, and an operator who set one by hand would be overwritten.
    pub(crate) fn is_alterable(&self) -> bool {
        self.is_stored() && !matches!(self.check, ValueCheck::NotAltered)
    }

    /// `true` when the value must not be disclosed. `DescribeConfigs` reports
    /// a sensitive key with a null value, as Kafka does.
    pub(crate) const fn is_sensitive(&self) -> bool {
        self.sensitive
    }

    /// The type column of the generated reference page.
    pub(crate) fn value_type(&self) -> String {
        match self.type_note {
            Some(note) => format!("{} ({note})", self.config_type.label()),
            None => self.config_type.label().to_owned(),
        }
    }
}

/// Shorthand for a row that needs none of the uncommon fields.
const fn key(
    name: &'static str,
    scope: ConfigScope,
    config_type: ConfigType,
    default: Option<&'static str>,
    doc: &'static str,
    check: ValueCheck,
) -> ConfigKey {
    ConfigKey {
        name,
        scope,
        config_type,
        type_note: None,
        default,
        doc,
        read_only: false,
        sensitive: false,
        kip: None,
        cluster_default: None,
        check,
    }
}

/// The two values every boolean key accepts, in the order a refusal names
/// them.
pub(super) const BOOLEAN_VALUES: &[&str] = &["true", "false"];
/// The values `cleanup.policy` accepts, in any order and in any non-empty
/// combination: Kafka types the key as a LIST and `LogConfig` derives its
/// `compact` and `delete` booleans by membership, so `compact,delete` is as
/// valid as either name alone.
pub(super) const CLEANUP_POLICY_VALUES: &[&str] = &["delete", "compact"];
const MESSAGE_TIMESTAMP_TYPE_VALUES: &[&str] = &[
    MESSAGE_TIMESTAMP_TYPE_CREATE,
    MESSAGE_TIMESTAMP_TYPE_LOG_APPEND,
];
const RECOVERY_STRATEGY_VALUES: &[&str] = &["None", "Balanced", "Aggressive"];
const DELIVERY_MODE_VALUES: &[&str] = &[DELIVERY_MODE_IMMEDIATE, DELIVERY_MODE_SCHEDULED];
const SCHEMA_VALIDATION_MODE_VALUES: &[&str] =
    &[SCHEMA_VALIDATION_MODE_ID, SCHEMA_VALIDATION_MODE_FULL];

/// Every config key the broker validates, documents, or reports.
pub(crate) const CONFIG_KEYS: &[ConfigKey] = &[
    // ── Topic scope ─────────────────────────────────────────────
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            RETENTION_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("604800000"),
            "Retention time before log segments become eligible for deletion.",
            ValueCheck::I64AtLeast(RETENTION_UNLIMITED),
        )
    },
    ConfigKey {
        type_note: Some("bytes"),
        ..key(
            RETENTION_BYTES,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("-1"),
            "Maximum partition size before old segments are deleted.",
            ValueCheck::I64AtLeast(RETENTION_UNLIMITED),
        )
    },
    ConfigKey {
        type_note: Some("bytes"),
        ..key(
            SEGMENT_BYTES,
            ConfigScope::Topic,
            ConfigType::Int,
            Some("1073741824"),
            "Target size of a single log segment file.",
            ValueCheck::I32AtLeast(1),
        )
    },
    key(
        CLEANUP_POLICY,
        ConfigScope::Topic,
        ConfigType::List,
        Some("delete"),
        "Any non-empty combination of `delete` and `compact`, comma-separated: `delete`, `compact`, or `compact,delete`, which both compacts the log and applies retention to it. A policy containing `compact` cannot be combined with remote.storage.enable=true or with delivery.mode=scheduled.",
        ValueCheck::Parsed,
    ),
    key(
        COMPRESSION_TYPE,
        ConfigScope::Topic,
        ConfigType::String,
        Some("producer"),
        "Broker-side compression codec for the topic.",
        ValueCheck::Parsed,
    ),
    ConfigKey {
        type_note: Some(">=1"),
        ..key(
            MIN_INSYNC_REPLICAS,
            ConfigScope::Topic,
            ConfigType::Int,
            Some("1"),
            "With acks=all, the minimum in-sync replicas required to accept a write; otherwise NOT_ENOUGH_REPLICAS (19).",
            ValueCheck::I32AtLeast(1),
        )
    },
    ConfigKey {
        type_note: Some("bytes, >=0"),
        ..key(
            MAX_MESSAGE_BYTES,
            ConfigScope::Topic,
            ConfigType::Int,
            Some("1048588"),
            "Largest record batch accepted for this topic, measured over the batch's whole wire encoding; a larger one is refused with MESSAGE_TOO_LARGE (10). Unset topics inherit the broker's message.max.bytes.",
            ValueCheck::I32AtLeast(0),
        )
    },
    ConfigKey {
        kip: Some("KIP-841"),
        cluster_default: Some(UNCLEAN_LEADER_ELECTION_ENABLE),
        ..key(
            UNCLEAN_LEADER_ELECTION_ENABLE,
            ConfigScope::Topic,
            ConfigType::Boolean,
            Some("false"),
            "Allow electing an out-of-ISR replica as leader on ISR-empty failover (possible data loss).",
            ValueCheck::Bool,
        )
    },
    ConfigKey {
        kip: Some("KIP-966"),
        cluster_default: Some(UNCLEAN_RECOVERY_STRATEGY),
        ..key(
            UNCLEAN_RECOVERY_STRATEGY,
            ConfigScope::Topic,
            ConfigType::String,
            Some("None"),
            "Offset-aware unclean recovery: `None`, `Balanced`, or `Aggressive`. Supersedes unclean.leader.election.enable.",
            ValueCheck::OneOf(RECOVERY_STRATEGY_VALUES),
        )
    },
    ConfigKey {
        kip: Some("KIP-405"),
        ..key(
            REMOTE_STORAGE_ENABLE,
            ConfigScope::Topic,
            ConfigType::Boolean,
            Some("false"),
            "Opt this topic into tiered (remote) storage. Refused on a topic whose cleanup.policy contains `compact`: tiered storage is not supported for compacted topics.",
            ValueCheck::Bool,
        )
    },
    ConfigKey {
        kip: Some("KIP-950"),
        ..key(
            REMOTE_LOG_COPY_DISABLE,
            ConfigScope::Topic,
            ConfigType::Boolean,
            Some("false"),
            "Stop copying this topic's sealed segments to the remote tier while remote.storage.enable stays true. Reads are still served from the tier, so the topic becomes read-only there rather than losing its history.",
            ValueCheck::Bool,
        )
    },
    ConfigKey {
        kip: Some("KIP-950"),
        ..key(
            REMOTE_LOG_DELETE_ON_DISABLE,
            ConfigScope::Topic,
            ConfigType::Boolean,
            Some("false"),
            "Permit turning remote.storage.enable off. The flip erases this topic's remote segments and raises its log start offset to the local log start, so it is refused while this is false.",
            ValueCheck::Bool,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-405"),
        ..key(
            LOCAL_RETENTION_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("-2"),
            "Local-tier retention time for tiered partitions.",
            ValueCheck::I64AtLeast(LOCAL_RETENTION_INHERIT),
        )
    },
    ConfigKey {
        type_note: Some("bytes"),
        kip: Some("KIP-405"),
        ..key(
            LOCAL_RETENTION_BYTES,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("-2"),
            "Local-tier retention size budget for tiered partitions.",
            ValueCheck::I64AtLeast(LOCAL_RETENTION_INHERIT),
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-534"),
        ..key(
            DELETE_RETENTION_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("86400000"),
            "How long tombstones and transaction markers are retained after becoming compaction-eligible.",
            ValueCheck::I64AtLeast(0),
        )
    },
    key(
        QOS_TIER,
        ConfigScope::Topic,
        ConfigType::String,
        Some(DEFAULT_QOS_TIER),
        "Krabka QoS tier used to partition producer quota buckets.",
        ValueCheck::Parsed,
    ),
    ConfigKey {
        read_only: true,
        ..key(
            DISKLESS,
            ConfigScope::Topic,
            ConfigType::Boolean,
            Some("false"),
            "Route this topic through the diskless WAL data path instead of the local log. Fixed when the topic is created, and exclusive with both remote.storage.enable and delivery.mode=scheduled.",
            ValueCheck::Bool,
        )
    },
    ConfigKey {
        kip: Some("KFC-1"),
        ..key(
            DELIVERY_MODE,
            ConfigScope::Topic,
            ConfigType::String,
            Some(DELIVERY_MODE_IMMEDIATE),
            "`immediate` or `scheduled`. Under `scheduled` a batch stays invisible to consumers until its own timestamp comes due.",
            ValueCheck::OneOf(DELIVERY_MODE_VALUES),
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KFC-1"),
        ..key(
            DELIVERY_MAX_DELAY_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("604800000"),
            "Largest delivery delay accepted at produce time, measured forward from produce time; -1 removes the bound.",
            ValueCheck::I64AtLeast(DELIVERY_MAX_DELAY_UNLIMITED),
        )
    },
    ConfigKey {
        kip: Some("KFC-1"),
        ..key(
            DELIVERY_SCHEDULE_MONOTONIC,
            ConfigScope::Topic,
            ConfigType::Boolean,
            Some("false"),
            "Reject a batch whose delivery time precedes the largest delivery time already in the partition.",
            ValueCheck::Bool,
        )
    },
    ConfigKey {
        kip: Some("KFC-7"),
        ..key(
            SCHEMA_VALIDATION_KEY,
            ConfigScope::Topic,
            ConfigType::Boolean,
            Some("false"),
            "Validate the schema of every record key produced to this topic.",
            ValueCheck::Bool,
        )
    },
    ConfigKey {
        kip: Some("KFC-7"),
        ..key(
            SCHEMA_VALIDATION_VALUE,
            ConfigScope::Topic,
            ConfigType::Boolean,
            Some("false"),
            "Validate the schema of every record value produced to this topic.",
            ValueCheck::Bool,
        )
    },
    ConfigKey {
        kip: Some("KFC-7"),
        ..key(
            SCHEMA_VALIDATION_MODE,
            ConfigScope::Topic,
            ConfigType::String,
            Some(SCHEMA_VALIDATION_MODE_ID),
            "`id` checks the Confluent header alone; `full` also decodes the record body against the schema the header names.",
            ValueCheck::OneOf(SCHEMA_VALIDATION_MODE_VALUES),
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            SEGMENT_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("604800000"),
            "Roll the active segment once its first record is older than this, even when it has not reached segment.bytes.",
            ValueCheck::I64AtLeast(1),
        )
    },
    ConfigKey {
        type_note: Some("bytes"),
        ..key(
            SEGMENT_INDEX_BYTES,
            ConfigScope::Topic,
            ConfigType::Int,
            Some("10485760"),
            "Size cap on a segment's offset index. krabka sizes its sparse indexes from index.interval.bytes, so this value is stored and reported only.",
            ValueCheck::I32AtLeast(4),
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            SEGMENT_JITTER_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("0"),
            "Random subtraction from segment.ms, which staggers the roll of many partitions. Stored and reported only: krabka rolls on the interval itself.",
            ValueCheck::I64AtLeast(0),
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            MIN_COMPACTION_LAG_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("0"),
            "How long a record is safe from the log cleaner after it is written. A pass stops at the first uncompacted segment younger than this and compacts what lies before it. Must not exceed max.compaction.lag.ms.",
            ValueCheck::I64AtLeast(0),
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            MAX_COMPACTION_LAG_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("9223372036854775807"),
            "How long a record may stay uncompacted before the cleaner runs whatever the dirty ratio says. Must not be below min.compaction.lag.ms.",
            ValueCheck::I64AtLeast(1),
        )
    },
    ConfigKey {
        type_note: Some("0..1"),
        ..key(
            MIN_CLEANABLE_DIRTY_RATIO,
            ConfigScope::Topic,
            ConfigType::Double,
            Some("0.5"),
            "The share of a compacted partition's log that must be uncleaned before the cleaner spends a pass on it.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            FILE_DELETE_DELAY_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("60000"),
            "Delay before a segment file removed by retention is unlinked. Stored and reported only: krabka unlinks a segment as it evicts it.",
            ValueCheck::I64AtLeast(0),
        )
    },
    ConfigKey {
        type_note: Some("records"),
        ..key(
            FLUSH_MESSAGES,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("9223372036854775807"),
            "Records between forced fsyncs. Stored and reported only: krabka manages fsync from its own durability settings.",
            ValueCheck::I64AtLeast(1),
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            FLUSH_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("9223372036854775807"),
            "Milliseconds between forced fsyncs. Stored and reported only: krabka manages fsync from its own durability settings.",
            ValueCheck::I64AtLeast(0),
        )
    },
    ConfigKey {
        type_note: Some("bytes"),
        ..key(
            INDEX_INTERVAL_BYTES,
            ConfigScope::Topic,
            ConfigType::Int,
            Some("4096"),
            "Bytes of .log between entries in the sparse offset and timestamp indexes.",
            ValueCheck::I32AtLeast(0),
        )
    },
    key(
        PREALLOCATE,
        ConfigScope::Topic,
        ConfigType::Boolean,
        Some("false"),
        "Preallocate a new segment file to segment.bytes. Stored and reported only: krabka grows a segment as it writes it.",
        ValueCheck::Bool,
    ),
    key(
        MESSAGE_TIMESTAMP_TYPE,
        ConfigScope::Topic,
        ConfigType::String,
        Some(MESSAGE_TIMESTAMP_TYPE_CREATE),
        "`CreateTime` stores the producer's own timestamps; `LogAppendTime` makes the broker stamp its own clock into every batch at append and report it as the produce response's logAppendTimeMs. It cannot be combined with delivery.mode=scheduled, whose activation time is the field the stamp overwrites.",
        ValueCheck::OneOf(MESSAGE_TIMESTAMP_TYPE_VALUES),
    ),
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            MESSAGE_TIMESTAMP_AFTER_MAX_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("9223372036854775807"),
            "How far ahead of the broker's clock a producer timestamp may sit. A batch holding a record past the window is refused with INVALID_TIMESTAMP. The default of Long.MAX_VALUE removes the bound, and a LogAppendTime topic ignores it, as in Kafka.",
            ValueCheck::I64AtLeast(0),
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            MESSAGE_TIMESTAMP_BEFORE_MAX_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("9223372036854775807"),
            "How far behind the broker's clock a producer timestamp may sit. A batch holding a record before the window is refused with INVALID_TIMESTAMP. The default of Long.MAX_VALUE removes the bound, and a LogAppendTime topic ignores it, as in Kafka.",
            ValueCheck::I64AtLeast(0),
        )
    },
    ConfigKey {
        kip: Some("KIP-73"),
        ..key(
            crate::throttle::LEADER_THROTTLED_REPLICAS_KEY,
            ConfigScope::Topic,
            ConfigType::List,
            Some(""),
            "Replica list throttled on the leader side during reassignment.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        kip: Some("KIP-73"),
        ..key(
            crate::throttle::FOLLOWER_THROTTLED_REPLICAS_KEY,
            ConfigScope::Topic,
            ConfigType::List,
            Some(""),
            "Replica list throttled on the follower side during reassignment.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        read_only: true,
        kip: Some("KFC-9"),
        ..key(
            WRITE_FREEZE,
            ConfigScope::Topic,
            ConfigType::String,
            Some("false"),
            "Write-freeze state of the topic: `false`, or `frozen:` and the registry scope that matched. Set and cleared with `krabka-guard freeze`.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        read_only: true,
        kip: Some("KIP-966"),
        ..key(
            ELIGIBLE_LEADER_REPLICAS,
            ConfigScope::Topic,
            ConfigType::String,
            None,
            "Eligible-leader-replica state of the topic's partitions, one `partition:elr:last-known-elr` group per partition that has any. Only the controller's ISR transitions write it, and only while `eligible.leader.replicas.version` is finalized at 1; `kafka-topics --describe` reads it, and a downgrade of that feature to 0 drops it.",
            ValueCheck::NotAltered,
        )
    },
    // ── Broker scope ────────────────────────────────────────────
    ConfigKey {
        type_note: Some("bytes/s"),
        kip: Some("KIP-73"),
        ..key(
            crate::throttle::LEADER_THROTTLED_RATE_KEY,
            ConfigScope::Broker,
            ConfigType::Long,
            None,
            "Byte rate ceiling this broker applies to leader-side replication of throttled replicas.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        type_note: Some("bytes/s"),
        kip: Some("KIP-73"),
        ..key(
            crate::throttle::FOLLOWER_THROTTLED_RATE_KEY,
            ConfigScope::Broker,
            ConfigType::Long,
            None,
            "Byte rate ceiling this broker applies to follower-side replication of throttled replicas.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        type_note: Some("bytes/s"),
        kip: Some("KIP-73"),
        ..key(
            crate::throttle::ALTER_LOG_DIRS_THROTTLED_RATE_KEY,
            ConfigScope::Broker,
            ConfigType::Long,
            None,
            "Byte rate ceiling this broker applies to inter-log-dir replica movement.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        kip: Some("KIP-841"),
        ..key(
            UNCLEAN_LEADER_ELECTION_ENABLE,
            ConfigScope::Broker,
            ConfigType::Boolean,
            Some("false"),
            "Cluster-wide default for the topic key of the same name. Valid only on the cluster-default broker resource.",
            ValueCheck::Bool,
        )
    },
    ConfigKey {
        kip: Some("KIP-966"),
        ..key(
            UNCLEAN_RECOVERY_STRATEGY,
            ConfigScope::Broker,
            ConfigType::String,
            Some("None"),
            "Cluster-wide default for the topic key of the same name. Valid only on the cluster-default broker resource.",
            ValueCheck::OneOf(RECOVERY_STRATEGY_VALUES),
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-1075"),
        ..key(
            REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS,
            ConfigScope::Broker,
            ConfigType::Long,
            Some("30000"),
            "Server-side deadline for remote ListOffsets work when the request carries no timeout of its own.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        read_only: true,
        ..key(
            BROKER_WITNESS,
            ConfigScope::Broker,
            ConfigType::Boolean,
            Some("false"),
            "Marks this node as a data-bearing witness: it replicates and votes but serves no client and leads no partition. Only the controller writes it.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        kip: Some("KIP-500"),
        read_only: true,
        ..key(
            BROKER_FENCED,
            ConfigScope::Broker,
            ConfigType::Boolean,
            Some("false"),
            "Marks this node as fenced: it is past its heartbeat deadline, or has not yet proved metadata catch-up, so every node reports its replicas offline. Only the controller writes it.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        read_only: true,
        ..key(
            STRETCH_PREFERRED_LEADER_SITE,
            ConfigScope::Broker,
            ConfigType::String,
            None,
            "The `broker.rack` value that should hold partition leadership in a stretch cluster. Only the controller writes it.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        type_note: Some("minutes"),
        kip: Some("KIP-211"),
        read_only: true,
        ..key(
            OFFSETS_RETENTION_MINUTES,
            ConfigScope::Broker,
            ConfigType::Int,
            Some("10080"),
            "How long a committed consumer-group offset outlives the group that owns it, once that group loses its last member. The process reads it at startup, so no alter path can change it.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-211"),
        read_only: true,
        ..key(
            OFFSETS_RETENTION_CHECK_INTERVAL_MS,
            ConfigScope::Broker,
            ConfigType::Long,
            Some("600000"),
            "Cadence of the background sweep that tombstones expired committed offsets. The process reads it at startup, so no alter path can change it.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        read_only: true,
        ..key(
            CONNECTIONS_MAX_IDLE_MS,
            ConfigScope::Broker,
            ConfigType::Long,
            Some("600000"),
            "How long a client connection may go without a complete request frame before the broker closes it. The process reads it at startup, so no alter path can change it.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-368"),
        read_only: true,
        ..key(
            CONNECTIONS_MAX_REAUTH_MS,
            ConfigScope::Broker,
            ConfigType::Long,
            Some("0"),
            "How long an authenticated SASL session may live before the client must re-authenticate in band. Zero disables re-authentication. The process reads it at startup, so no alter path can change it.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-98"),
        read_only: true,
        ..key(
            TRANSACTIONAL_ID_EXPIRATION_MS,
            ConfigScope::Broker,
            ConfigType::Int,
            Some("604800000"),
            "How long a transactional id may sit in a terminal or idle state before the transaction coordinator tombstones it out of `__transaction_state`. Read from this node's own configuration; Kafka refuses to alter it dynamically.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-98"),
        read_only: true,
        ..key(
            TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
            ConfigScope::Broker,
            ConfigType::Int,
            Some("3600000"),
            "How often the transactional-id expiry sweep scans the `__transaction_state` partitions this node leads. Read from this node's own configuration; Kafka refuses to alter it dynamically.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        read_only: true,
        ..key(
            NODE_ID,
            ConfigScope::Broker,
            ConfigType::Int,
            None,
            "The node id this process runs under, from its static configuration.",
            ValueCheck::NotAltered,
        )
    },
    // ── Client-metrics scope (KIP-714) ──────────────────────────
    ConfigKey {
        kip: Some("KIP-714"),
        ..key(
            crate::client_metrics::config::KEY_METRICS,
            ConfigScope::ClientMetrics,
            ConfigType::List,
            Some(""),
            "Metric name prefixes this subscription collects. `*` collects every metric; an empty list collects none, as it does on Kafka, where an empty subscription contributes no metric name to the client's set.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-714"),
        ..key(
            crate::client_metrics::config::KEY_INTERVAL_MS,
            ConfigScope::ClientMetrics,
            ConfigType::Int,
            None,
            "How often a matching client pushes metrics. Unset falls back to the broker's default push interval.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        kip: Some("KIP-714"),
        ..key(
            crate::client_metrics::config::KEY_MATCH,
            ConfigScope::ClientMetrics,
            ConfigType::List,
            Some(""),
            "Client selectors this subscription matches on. Empty matches every client.",
            ValueCheck::Parsed,
        )
    },
    // ── Group scope (KIP-1071) ──────────────────────────────────
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-1071"),
        ..key(
            crate::coordinator::unified::streams::config::KEY_SESSION_TIMEOUT_MS,
            ConfigScope::Group,
            ConfigType::Int,
            None,
            "How long the coordinator waits for a heartbeat before it removes a member of this streams group.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-1071"),
        ..key(
            crate::coordinator::unified::streams::config::KEY_HEARTBEAT_INTERVAL_MS,
            ConfigScope::Group,
            ConfigType::Int,
            None,
            "How often a member of this streams group heartbeats.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        type_note: Some("records"),
        kip: Some("KIP-1071"),
        ..key(
            crate::coordinator::unified::streams::config::KEY_ACCEPTABLE_RECOVERY_LAG,
            ConfigScope::Group,
            ConfigType::Long,
            None,
            "Changelog lag in records at which a standby task counts as caught up enough to take an active task.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        kip: Some("KIP-1071"),
        ..key(
            crate::coordinator::unified::streams::config::KEY_NUM_WARMUP_REPLICAS,
            ConfigScope::Group,
            ConfigType::Int,
            None,
            "Cap on the warmup tasks that may migrate state at once.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        kip: Some("KIP-1071"),
        ..key(
            crate::coordinator::unified::streams::config::KEY_NUM_STANDBY_REPLICAS,
            ConfigScope::Group,
            ConfigType::Int,
            None,
            "Standby copies the assignor places for each stateful task.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-1071"),
        ..key(
            crate::coordinator::unified::streams::config::KEY_TASK_OFFSET_INTERVAL_MS,
            ConfigScope::Group,
            ConfigType::Long,
            None,
            "How often a member reports its task offsets to the coordinator.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        kip: Some("KIP-1071"),
        ..key(
            crate::coordinator::unified::streams::config::KEY_ASSIGNOR_NAME,
            ConfigScope::Group,
            ConfigType::String,
            None,
            "Server-side task assignor: `auto`, `sticky`, or `highly_available`.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        kip: Some("KIP-932"),
        ..key(
            crate::coordinator::unified::streams::config::KEY_SHARE_AUTO_OFFSET_RESET,
            ConfigScope::Group,
            ConfigType::String,
            None,
            "Where a share partition starts when the share coordinator holds no state for it: `latest` (the default, the high watermark), `earliest` (the log start offset), or `by_duration:<PnDTnHnMn.nS>` (the first record at or after `now - duration`, and the high watermark when no record qualifies). A negative duration is refused with INVALID_CONFIG.",
            ValueCheck::Parsed,
        )
    },
];

/// The row for one key, or `None` when the scope reports no such key.
pub(crate) fn lookup(scope: ConfigScope, name: &str) -> Option<&'static ConfigKey> {
    CONFIG_KEYS
        .iter()
        .find(|entry| entry.scope == scope && entry.name == name)
}

/// Every row in one scope, in the table's own order.
pub(crate) fn keys_in(scope: ConfigScope) -> impl Iterator<Item = &'static ConfigKey> {
    CONFIG_KEYS.iter().filter(move |entry| entry.scope == scope)
}

#[cfg(test)]
mod tests;
