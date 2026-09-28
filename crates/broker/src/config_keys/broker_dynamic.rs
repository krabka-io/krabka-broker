//! Kafka's dynamic broker config rules, `DynamicBrokerConfig.validateConfigs`,
//! for the `BROKER` resource both alter APIs write.
//!
//! Kafka lets an alter write a key it does not define, and stores it. It
//! refuses a Kafka key that is not dynamic, an SSL key without a listener
//! prefix, a per-broker key on the cluster-default resource, and a value its
//! own validator refuses, each with `INVALID_REQUEST` (KAFKA-13609 keeps the
//! code at 42 even for a bad value). The rules here are that check, over the
//! broker keys krabka knows: the dynamic keys it runs with, the broker
//! synonyms of its topic keys, and the static keys it reads at startup.

use std::collections::BTreeMap;

use krabka_metadata::NodeId;

use super::{
    MIN_INSYNC_REPLICAS,
    broker_scope::{
        AUTO_CREATE_TOPICS_ENABLE, CONNECTIONS_MAX_IDLE_MS, CONNECTIONS_MAX_REAUTH_MS,
        DEFAULT_REPLICATION_FACTOR, DELETE_TOPIC_ENABLE, NUM_PARTITIONS,
        OFFSETS_RETENTION_CHECK_INTERVAL_MS, OFFSETS_RETENTION_MINUTES,
        REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS, TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
        TRANSACTIONAL_ID_EXPIRATION_MS, parse_remote_list_offsets_timeout,
    },
    parse::{check_range, parse_long},
    recovery::UNCLEAN_RECOVERY_STRATEGY,
    registry::{self, ConfigScope},
    validation::canonical_value,
};
use crate::codes;

/// Kafka's `ServerTopicConfigSynonyms.TOPIC_CONFIG_SYNONYMS`: the broker key
/// that sets the cluster-wide default of each topic key, for the topic keys
/// krabka carries. `DynamicLogConfig.RECONFIGURABLE_CONFIGS` is exactly this
/// set, so each of them is dynamic.
pub(crate) const TOPIC_DEFAULT_SYNONYMS: &[(&str, &str)] = &[
    ("log.segment.bytes", "segment.bytes"),
    ("log.roll.ms", "segment.ms"),
    ("log.roll.jitter.ms", "segment.jitter.ms"),
    ("log.index.size.max.bytes", "segment.index.bytes"),
    ("log.flush.interval.messages", "flush.messages"),
    ("log.flush.interval.ms", "flush.ms"),
    ("log.retention.bytes", "retention.bytes"),
    ("log.retention.ms", "retention.ms"),
    ("message.max.bytes", "max.message.bytes"),
    (
        "max.decompressed.message.bytes",
        "max.decompressed.message.bytes",
    ),
    ("log.index.interval.bytes", "index.interval.bytes"),
    ("log.cleaner.delete.retention.ms", "delete.retention.ms"),
    ("log.cleaner.min.compaction.lag.ms", "min.compaction.lag.ms"),
    ("log.cleaner.max.compaction.lag.ms", "max.compaction.lag.ms"),
    ("log.segment.delete.delay.ms", "file.delete.delay.ms"),
    (
        "log.cleaner.min.cleanable.ratio",
        "min.cleanable.dirty.ratio",
    ),
    ("log.cleanup.policy", "cleanup.policy"),
    (
        "unclean.leader.election.enable",
        "unclean.leader.election.enable",
    ),
    ("min.insync.replicas", "min.insync.replicas"),
    ("compression.type", "compression.type"),
    ("compression.gzip.level", "compression.gzip.level"),
    ("compression.lz4.level", "compression.lz4.level"),
    ("compression.zstd.level", "compression.zstd.level"),
    ("log.preallocate", "preallocate"),
    ("log.message.timestamp.type", "message.timestamp.type"),
    (
        "log.message.timestamp.before.max.ms",
        "message.timestamp.before.max.ms",
    ),
    (
        "log.message.timestamp.after.max.ms",
        "message.timestamp.after.max.ms",
    ),
    ("log.local.retention.ms", "local.retention.ms"),
    ("log.local.retention.bytes", "local.retention.bytes"),
    ("log.remote.copy.lag.ms", "remote.copy.lag.ms"),
    ("log.remote.copy.lag.bytes", "remote.copy.lag.bytes"),
];

/// One broker synonym of a topic key, with Kafka's broker default for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BrokerSynonym {
    pub(crate) name: &'static str,
    pub(crate) default: Option<&'static str>,
}

const fn synonym(name: &'static str, default: Option<&'static str>) -> BrokerSynonym {
    BrokerSynonym { name, default }
}

/// Kafka's `DynamicBrokerConfig.brokerConfigSynonyms` for the broker key of a
/// topic key, most preferred first, each with its `KafkaConfig` default. Four
/// topic keys have a family of broker keys, and only the least preferred of
/// each family carries a default; every other topic key has one broker key,
/// whose default is the topic key's own.
pub(crate) fn topic_broker_synonyms(topic_key: &str) -> Vec<BrokerSynonym> {
    match topic_key {
        "retention.ms" => vec![
            synonym("log.retention.ms", None),
            synonym("log.retention.minutes", None),
            synonym("log.retention.hours", Some("168")),
        ],
        "segment.ms" => vec![
            synonym("log.roll.ms", None),
            synonym("log.roll.hours", Some("168")),
        ],
        "segment.jitter.ms" => vec![
            synonym("log.roll.jitter.ms", None),
            synonym("log.roll.jitter.hours", Some("0")),
        ],
        "flush.ms" => vec![
            synonym("log.flush.interval.ms", None),
            synonym(
                "log.flush.scheduler.interval.ms",
                Some("9223372036854775807"),
            ),
        ],
        _ => TOPIC_DEFAULT_SYNONYMS
            .iter()
            .find(|(_, topic)| *topic == topic_key)
            .map(|(broker, topic)| {
                synonym(
                    broker,
                    registry::lookup(ConfigScope::Topic, topic).and_then(|row| row.default),
                )
            })
            .into_iter()
            .collect(),
    }
}

/// The registry row that types a broker key: its own broker row, or the row
/// of the topic key it sets the default of.
pub(crate) fn broker_key_row(name: &str) -> Option<&'static registry::ConfigKey> {
    registry::lookup(ConfigScope::Broker, name).or_else(|| {
        TOPIC_DEFAULT_SYNONYMS
            .iter()
            .find(|(broker, _)| *broker == name)
            .and_then(|(_, topic)| registry::lookup(ConfigScope::Topic, topic))
    })
}

/// The three KIP-73 replication quotas, which Kafka's
/// `QuotaConfig.brokerQuotaConfigs` defines as `LONG` with `atLeast(0)`.
const THROTTLE_RATES: [&str; 3] = [
    crate::throttle::LEADER_THROTTLED_RATE_KEY,
    crate::throttle::FOLLOWER_THROTTLED_RATE_KEY,
    crate::throttle::ALTER_LOG_DIRS_THROTTLED_RATE_KEY,
];

/// Kafka broker keys that `DynamicConfig.Broker.nonDynamicProps` names and
/// that krabka knows of: the process reads them at startup, so an alter of
/// one is refused rather than stored and ignored.
const NON_DYNAMIC_KEYS: &[&str] = &[
    "node.id",
    "broker.id",
    "process.roles",
    "log.dirs",
    "log.dir",
    "listeners",
    "advertised.listeners",
    "controller.listener.names",
    "controller.quorum.voters",
    "controller.quorum.bootstrap.servers",
    "inter.broker.listener.name",
    "broker.rack",
    "log.retention.hours",
    "log.retention.minutes",
    "log.roll.hours",
    "log.roll.jitter.hours",
    "log.flush.scheduler.interval.ms",
    NUM_PARTITIONS,
    DEFAULT_REPLICATION_FACTOR,
    DELETE_TOPIC_ENABLE,
    AUTO_CREATE_TOPICS_ENABLE,
    OFFSETS_RETENTION_MINUTES,
    OFFSETS_RETENTION_CHECK_INTERVAL_MS,
    CONNECTIONS_MAX_IDLE_MS,
    CONNECTIONS_MAX_REAUTH_MS,
    TRANSACTIONAL_ID_EXPIRATION_MS,
    TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
];

/// Kafka's `SslConfigs.RECONFIGURABLE_CONFIGS`: dynamic only per listener,
/// under a `listener.name.<listener>.` prefix.
const DYNAMIC_SECURITY_CONFIGS: &[&str] = &[
    "ssl.keystore.type",
    "ssl.keystore.location",
    "ssl.keystore.password",
    "ssl.key.password",
    "ssl.keystore.key",
    "ssl.keystore.certificate.chain",
    "ssl.truststore.type",
    "ssl.truststore.location",
    "ssl.truststore.password",
    "ssl.truststore.certificates",
];

/// Kafka's `CLUSTER_LEVEL_LISTENER_CONFIGS`: listener-prefixed keys that the
/// cluster-default resource may still hold.
const CLUSTER_LEVEL_LISTENER_CONFIGS: &[&str] = &[
    "max.connections",
    "max.connection.creation.rate",
    "num.network.threads",
];

/// The base key of a `listener.name.<listener>.<key>` override.
fn listener_base(name: &str) -> Option<&str> {
    let rest = name.strip_prefix("listener.name.")?;
    let (_, base) = rest.split_once('.')?;
    Some(base)
}

/// Whether a key may be set only on a named broker: an SSL key,
/// `cordoned.log.dirs` (`DynamicBrokerConfig.PER_BROKER_CONFIGS`), or a
/// listener override of anything but the cluster-level listener keys.
fn is_per_broker(name: &str) -> bool {
    DYNAMIC_SECURITY_CONFIGS.contains(&name)
        || name == crate::cordoned_log_dirs::CORDONED_LOG_DIRS
        || listener_base(name).is_some_and(|base| !CLUSTER_LEVEL_LISTENER_CONFIGS.contains(&base))
}

/// What krabka knows of a broker key, for `IncrementalAlterConfigs`'
/// APPEND and SUBTRACT, which Kafka allows on a `LIST` key alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrokerKeyKind {
    /// A key Kafka types `LIST`.
    List,
    /// A key Kafka defines with another type.
    Scalar,
    /// A key Kafka does not define.
    Unknown,
}

/// The kind of a broker key, for the APPEND and SUBTRACT checks.
pub(crate) fn broker_key_kind(name: &str) -> BrokerKeyKind {
    if let Some(row) = registry::lookup(ConfigScope::Broker, name)
        && row.config_type == registry::ConfigType::List
    {
        return BrokerKeyKind::List;
    }
    let topic_key = TOPIC_DEFAULT_SYNONYMS
        .iter()
        .find(|(broker, _)| *broker == name)
        .map(|(_, topic)| *topic);
    if let Some(row) = topic_key.and_then(|key| registry::lookup(ConfigScope::Topic, key)) {
        return if row.config_type == registry::ConfigType::List {
            BrokerKeyKind::List
        } else {
            BrokerKeyKind::Scalar
        };
    }
    if THROTTLE_RATES.contains(&name)
        || NON_DYNAMIC_KEYS.contains(&name)
        || DYNAMIC_SECURITY_CONFIGS.contains(&name)
        || name == REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS
        || registry::lookup(ConfigScope::Broker, name).is_some()
    {
        return BrokerKeyKind::Scalar;
    }
    BrokerKeyKind::Unknown
}

/// The node a `BROKER` resource names: the cluster default for the empty
/// name, else this node, which Kafka's `validateResourceNameIsCurrentNodeId`
/// requires.
///
/// # Errors
/// Returns `INVALID_REQUEST` with Kafka's message for a name that is not an
/// integer, or that names another node.
pub(crate) fn broker_resource_node(
    resource_name: &str,
    serving: NodeId,
) -> Result<NodeId, (i16, String)> {
    if resource_name.is_empty() {
        return Ok(krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID);
    }
    let id = resource_name.parse::<i32>().map_err(|_| {
        (
            codes::INVALID_REQUEST,
            format!("Node id must be an integer, but it is: {resource_name}"),
        )
    })?;
    u64::try_from(id)
        .ok()
        .map(NodeId)
        .filter(|node| *node == serving)
        .ok_or_else(|| {
            (
                codes::INVALID_REQUEST,
                format!(
                    "Unexpected broker id, expected {}, but received {resource_name}",
                    serving.0
                ),
            )
        })
}

/// Kafka's `checkInvalidProps` text: the message and the names, the way a
/// Java `Set` prints them.
fn invalid_props(message: &str, names: &[&str]) -> (i16, String) {
    (
        codes::INVALID_REQUEST,
        format!("{message}: [{}]", names.join(", ")),
    )
}

/// Validate one value of a dynamic broker key krabka knows, and return it in
/// its canonical form. A key krabka does not know passes unchanged, as Kafka's
/// `DynamicConfig.Broker.validate` passes a key it does not define.
fn canonical_broker_value(name: &str, value: &str) -> Result<String, String> {
    if THROTTLE_RATES.contains(&name) {
        return check_range(name, parse_long(name, value)?, Some(0), None).map(|v| v.to_string());
    }
    if name == REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS {
        return parse_remote_list_offsets_timeout(value).map(|_| value.trim().to_owned());
    }
    if name == crate::cordoned_log_dirs::CORDONED_LOG_DIRS {
        return crate::cordoned_log_dirs::validate_value_type(value).map(|()| value.to_owned());
    }
    if name == UNCLEAN_RECOVERY_STRATEGY {
        return registry::lookup(ConfigScope::Broker, name)
            .map_or_else(|| Ok(value.to_owned()), |row| canonical_value(row, value));
    }
    let topic_key = TOPIC_DEFAULT_SYNONYMS
        .iter()
        .find(|(broker, _)| *broker == name)
        .map(|(_, topic)| *topic);
    match topic_key.and_then(|key| registry::lookup(ConfigScope::Topic, key)) {
        // The topic key's validator, under the broker key's name.
        Some(row) => {
            canonical_value(row, value).map_err(|message| message.replacen(row.name, name, 2))
        }
        None => Ok(value.to_owned()),
    }
}

/// Kafka's `DynamicBrokerConfig.validateConfigs`, over the whole set of
/// dynamic configs the resource ends up with, returning that set with each
/// value in its canonical form. `per_broker` is `true` for a named broker.
///
/// # Errors
/// Returns `INVALID_REQUEST` with Kafka's message, in Kafka's order: a
/// non-dynamic key, then an unprefixed SSL key, then a bad value, then a
/// per-broker key on the cluster-default resource.
pub(crate) fn canonical_dynamic_broker_configs(
    props: &BTreeMap<String, String>,
    per_broker: bool,
) -> Result<BTreeMap<String, String>, (i16, String)> {
    let names = |test: &dyn Fn(&str) -> bool| -> Vec<&str> {
        props
            .keys()
            .map(String::as_str)
            .filter(|name| test(name))
            .collect()
    };
    let non_dynamic = names(&|name| NON_DYNAMIC_KEYS.contains(&name));
    if !non_dynamic.is_empty() {
        return Err(invalid_props(
            "Cannot update these configs dynamically",
            &non_dynamic,
        ));
    }
    let unprefixed = names(&|name| DYNAMIC_SECURITY_CONFIGS.contains(&name));
    if !unprefixed.is_empty() {
        return Err(invalid_props(
            "These security configs can be dynamically updated only per-listener using the \
             listener prefix",
            &unprefixed,
        ));
    }
    let canonical = props
        .iter()
        .map(|(name, value)| {
            canonical_broker_value(name, value)
                .map(|value| (name.clone(), value))
                .map_err(|message| (codes::INVALID_REQUEST, message))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    if !per_broker {
        let per_broker_keys = names(&is_per_broker);
        if !per_broker_keys.is_empty() {
            return Err(invalid_props(
                "Cannot update these configs at default cluster level, broker id must be \
                 specified",
                &per_broker_keys,
            ));
        }
    }
    Ok(canonical)
}

/// Kafka's `ConfigurationControlManager` ELR rules for one broker config
/// write: while ELR is on, no named broker may carry `min.insync.replicas`,
/// and the cluster-level value may not be removed.
pub(crate) fn elr_min_isr_error(
    image: &krabka_metadata::MetadataImage,
    node: NodeId,
    name: &str,
    value: Option<&str>,
) -> Option<(i16, String)> {
    let elr_enabled = image
        .finalized_feature(crate::features::ELR_VERSION)
        .is_some_and(|level| level >= 1);
    if name != MIN_INSYNC_REPLICAS || !elr_enabled {
        return None;
    }
    if node != krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID {
        return Some((
            codes::INVALID_CONFIG,
            "Broker-level min.insync.replicas cannot be altered while ELR is enabled.".into(),
        ));
    }
    value.is_none().then(|| {
        (
            codes::INVALID_CONFIG,
            "Cluster-level min.insync.replicas cannot be removed while ELR is enabled.".into(),
        )
    })
}

/// The broker's own check of the `cordoned.log.dirs` a named-broker resource
/// ends up with, against this node's `log_dirs`.
///
/// Kafka's `ConfigAdminManager.validateBrokerConfigChange` builds a whole
/// `KafkaConfig` from the resource's dynamic configs, and its
/// `validateCordonedLogDirs` refuses a value that is not `*` alone or a subset
/// of `log.dirs`. The `IllegalArgumentException` it throws reaches the client
/// as `INVALID_REQUEST`. `log_dirs` is empty on a node without the broker role,
/// which checks nothing here, as a Kafka controller does not.
///
/// # Errors
/// Returns `INVALID_REQUEST` with Kafka's message.
pub(crate) fn cordoned_log_dirs_error(
    canonical: &BTreeMap<String, String>,
    log_dirs: &[std::path::PathBuf],
) -> Result<(), (i16, String)> {
    match canonical.get(crate::cordoned_log_dirs::CORDONED_LOG_DIRS) {
        Some(value) if !log_dirs.is_empty() => crate::cordoned_log_dirs::resolve(value, log_dirs)
            .map(drop)
            .map_err(|message| (codes::INVALID_REQUEST, message)),
        _ => Ok(()),
    }
}

/// Kafka's `ConfigurationControlManager.isCordonedLogDirsDisabled`: below
/// `metadata.version` `4.3-IV0` the controller refuses every write of
/// `cordoned.log.dirs` to a broker resource, a deletion included.
pub(crate) fn cordoned_log_dirs_disabled_error(
    image: &krabka_metadata::MetadataImage,
    name: &str,
) -> Option<(i16, String)> {
    let supported = image.finalized_metadata_version().is_some_and(|level| {
        level >= krabka_metadata::metadata_version::CORDONED_LOG_DIRS_MIN_LEVEL
    });
    (name == crate::cordoned_log_dirs::CORDONED_LOG_DIRS && !supported).then(|| {
        (
            codes::INVALID_CONFIG,
            format!(
                "The {name} configuration value cannot be set because it requires \
                 metadata.version >= 4.3-IV0"
            ),
        )
    })
}

/// krabka's own KIP-966 strategy key, which the controller reads from the
/// cluster-default resource alone, so a named broker may not hold it.
pub(crate) const CLUSTER_DEFAULT_ONLY: &[&str] = &[UNCLEAN_RECOVERY_STRATEGY];

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn broker_resource_names_follow_kafkas_node_id_rule() {
        let serving = NodeId(1);
        let cases = [
            ("", Ok(krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID)),
            ("1", Ok(NodeId(1))),
            (
                "2",
                Err((
                    codes::INVALID_REQUEST,
                    "Unexpected broker id, expected 1, but received 2".to_owned(),
                )),
            ),
            (
                "-1",
                Err((
                    codes::INVALID_REQUEST,
                    "Unexpected broker id, expected 1, but received -1".to_owned(),
                )),
            ),
            (
                "abc",
                Err((
                    codes::INVALID_REQUEST,
                    "Node id must be an integer, but it is: abc".to_owned(),
                )),
            ),
        ];
        for (name, want) in cases {
            check!(broker_resource_node(name, serving) == want, "{name:?}");
        }
    }

    #[test]
    fn dynamic_broker_configs_follow_kafkas_validate_configs() {
        let cases = [
            (
                map(&[("plugin.custom.key", "x")]),
                false,
                Ok(map(&[("plugin.custom.key", "x")])),
            ),
            (
                map(&[("log.dirs", "/tmp")]),
                false,
                Err("Cannot update these configs dynamically: [log.dirs]"),
            ),
            (
                map(&[("ssl.keystore.location", "/k")]),
                true,
                Err(
                    "These security configs can be dynamically updated only per-listener using \
                     the listener prefix: [ssl.keystore.location]",
                ),
            ),
            (
                map(&[("listener.name.client.ssl.keystore.location", "/k")]),
                false,
                Err(
                    "Cannot update these configs at default cluster level, broker id must be \
                     specified: [listener.name.client.ssl.keystore.location]",
                ),
            ),
            (
                map(&[("listener.name.client.ssl.keystore.location", "/k")]),
                true,
                Ok(map(&[("listener.name.client.ssl.keystore.location", "/k")])),
            ),
            (
                map(&[("log.retention.ms", " 86400000 ")]),
                false,
                Ok(map(&[("log.retention.ms", "86400000")])),
            ),
            (
                map(&[("num.io.threads", "16")]),
                false,
                Ok(map(&[("num.io.threads", "16")])),
            ),
            (
                map(&[(crate::throttle::LEADER_THROTTLED_RATE_KEY, "-1")]),
                true,
                Err(
                    "Invalid value -1 for configuration leader.replication.throttled.rate: Value \
                     must be at least 0",
                ),
            ),
            (
                map(&[("min.insync.replicas", "0")]),
                true,
                Err(
                    "Invalid value 0 for configuration min.insync.replicas: Value must be at \
                     least 1",
                ),
            ),
        ];
        for (props, per_broker, want) in cases {
            let want = want.map_err(|message| (codes::INVALID_REQUEST, message.to_owned()));
            check!(
                canonical_dynamic_broker_configs(&props, per_broker) == want,
                "{props:?} per_broker={per_broker}"
            );
        }
    }

    #[test]
    fn broker_key_kinds_decide_append_and_subtract() {
        for (name, kind) in [
            ("log.cleanup.policy", BrokerKeyKind::List),
            ("log.retention.ms", BrokerKeyKind::Scalar),
            (
                crate::throttle::LEADER_THROTTLED_RATE_KEY,
                BrokerKeyKind::Scalar,
            ),
            ("not.a.kafka.key", BrokerKeyKind::Unknown),
        ] {
            check!(broker_key_kind(name) == kind, "{name}");
        }
    }
}
