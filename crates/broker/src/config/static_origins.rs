//! Which static broker settings the operator supplied, as opposed to
//! inheriting.
//!
//! `DescribeConfigs` reports a config's *source*, and a source is provenance:
//! where a value came from, not whether it happens to differ from the built-in
//! default. Kafka reads that from `KafkaConfig.originals`, the properties the
//! operator actually wrote. Starting `apache/kafka:4.3.1` with
//! `transactional.id.expiration.ms` set to Kafka's own default and asking
//! `kafka-configs --entity-type brokers --entity-name 1 --describe --all`
//! answers
//!
//! ```text
//! transactional.id.expiration.ms=604800000 sensitive=false
//!   synonyms={STATIC_BROKER_CONFIG:transactional.id.expiration.ms=604800000,
//!             DEFAULT_CONFIG:transactional.id.expiration.ms=604800000}
//! ```
//!
//! -- the supplied value at the head of the chain, with the identical built-in
//! default beneath it. A handler that compared values would report only
//! `DEFAULT_CONFIG` and tell the operator their setting had not been read.
//!
//! The loader records the provenance here as it applies each override, which
//! is the only place that knows it.

/// The static broker keys this node's configuration named explicitly.
///
/// One flag per key that `crate::handlers`' `DescribeConfigs` reports as a
/// static broker config. A key is flagged when the CLI, the environment or the
/// `[runtime]` file config supplied it -- all three arrive through
/// [`crate::file_config::RuntimeFileConfig`] -- and left clear when the broker
/// runs the built-in default.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StaticConfigOrigins {
    /// The Kafka keys that the broker's own key table states a running value
    /// of (`consumer_group_session_timeout` for
    /// `group.consumer.session.timeout.ms`, and so on) and that a `[runtime]`
    /// field supplied. A key here reports at `STATIC_BROKER_CONFIG` even at
    /// Kafka's default value; a key that is not here does so only when the
    /// node runs another value. The keys the broker reads under their Kafka
    /// names from `server_properties`, such as `log.roll.ms`, are here when
    /// the operator named them.
    pub supplied_kafka_keys: std::collections::BTreeSet<&'static str>,
    /// `transactional.id.expiration.ms` was supplied.
    pub txn_id_expiration: bool,
    /// `transaction.remove.expired.transaction.cleanup.interval.ms` was
    /// supplied.
    pub txn_id_expiration_cleanup_interval: bool,
    /// Which of the KIP-464 topic-creation defaults were supplied.
    pub topic_creation: TopicCreationOrigins,
    /// Which of the static settings that back a topic key's broker synonym
    /// were supplied.
    pub log: LogOrigins,
    /// Which of the static topic-administration switches were supplied.
    pub topic_admin: TopicAdminOrigins,
    /// Which of the static socket authentication limits were supplied.
    pub authentication: AuthenticationOrigins,
}

/// Which of the two static limits on a connection that has not finished
/// authenticating this node's configuration named explicitly. Neither is
/// dynamically reconfigurable in Kafka, so `DescribeConfigs` reports the value
/// a broker runs with at `STATIC_BROKER_CONFIG` when `server.properties` names
/// the key, and the built-in default alone otherwise.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AuthenticationOrigins {
    /// `sasl.server.max.receive.size` was supplied.
    pub sasl_server_max_receive: bool,
    /// `connection.failed.authentication.delay.ms` was supplied.
    pub connection_failed_authentication_delay: bool,
}

/// Which of the static log defaults, each the broker synonym of a topic key,
/// this node's configuration named explicitly. A topic that overrides none of
/// them reports the supplied one at `STATIC_BROKER_CONFIG`, Kafka's
/// `staticNodeConfig.containsKey(synonym)` rule, and reports the built-in
/// default alone otherwise.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogOrigins {
    /// `message.max.bytes`, the default of `max.message.bytes`, was supplied.
    pub message_max_bytes: bool,
    /// `log.segment.bytes`, the default of `segment.bytes`, was supplied.
    pub log_segment_bytes: bool,
    /// `min.insync.replicas` was supplied.
    pub min_insync_replicas: bool,
}

/// Which of the static switches that gate topic deletion and auto-creation
/// this node's configuration named explicitly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TopicAdminOrigins {
    /// `delete.topic.enable` was supplied.
    pub delete_topic_enable: bool,
    /// `auto.create.topics.enable` was supplied.
    pub auto_create_topics_enable: bool,
}

/// Which of the two KIP-464 topic-creation defaults this node's configuration
/// named explicitly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TopicCreationOrigins {
    /// `num.partitions` was supplied.
    pub num_partitions: bool,
    /// `default.replication.factor` was supplied.
    pub default_replication_factor: bool,
}
