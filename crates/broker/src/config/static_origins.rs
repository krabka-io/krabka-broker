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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StaticConfigOrigins {
    /// `transactional.id.expiration.ms` was supplied.
    pub txn_id_expiration: bool,
    /// `transaction.remove.expired.transaction.cleanup.interval.ms` was
    /// supplied.
    pub txn_id_expiration_cleanup_interval: bool,
}
