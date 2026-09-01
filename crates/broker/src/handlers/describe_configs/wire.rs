//! The `DescribeConfigs` wire vocabulary: the `ConfigSource` and
//! `ResourceType` bytes Kafka defines, and the one constructor that shapes a
//! `DescribeConfigsResourceResult` from a `(key, value)` pair.
//!
//! These values are the response's contract with the JVM `AdminClient`, so
//! they sit apart from the code that decides which configs to report.

use krabka_protocol::owned::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsSynonym,
};

/// `ConfigSource::DYNAMIC_TOPIC_CONFIG`, the value Kafka uses for per-topic
/// overrides held in `ZooKeeper` or `KRaft` metadata.
///
/// From `org.apache.kafka.clients.admin.ConfigEntry.ConfigSource`:
/// `DYNAMIC_TOPIC_CONFIG = 1`, `DYNAMIC_BROKER_CONFIG = 2`,
/// `DYNAMIC_DEFAULT_BROKER_CONFIG = 3`, `STATIC_BROKER_CONFIG = 4`,
/// `DEFAULT_CONFIG = 5`, `DYNAMIC_BROKER_LOGGER_CONFIG = 6`.
pub(super) const CONFIG_SOURCE_DYNAMIC_TOPIC: i8 = 1;
pub(super) const CONFIG_SOURCE_DYNAMIC_BROKER: i8 = 2;
pub(super) const CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER: i8 = 3;
pub(super) const CONFIG_SOURCE_STATIC_BROKER: i8 = 4;
/// `ConfigSource::DEFAULT_CONFIG`, for keys reported at their default.
pub(super) const CONFIG_SOURCE_DEFAULT: i8 = 5;
/// `DescribeConfigsResponse.ConfigSource::CLIENT_METRICS_CONFIG` wire byte.
pub(super) const CONFIG_SOURCE_CLIENT_METRICS: i8 = 7;
/// `ConfigSource::DYNAMIC_GROUP_CONFIG`.
pub(super) const CONFIG_SOURCE_DYNAMIC_GROUP: i8 = 8;

pub(super) const RESOURCE_TYPE_TOPIC: i8 = 2;
pub(super) const RESOURCE_TYPE_BROKER: i8 = 4;
pub(super) const RESOURCE_TYPE_CLIENT_METRICS: i8 = 16;
pub(super) const RESOURCE_TYPE_GROUP: i8 = 32;

/// `ConfigDef.Type::UNKNOWN` wire byte. Krabka reports no typed config
/// metadata, which matches brokers from before KIP-226's typed responses.
const CONFIG_TYPE_UNKNOWN: i8 = 0;

/// Produces a `DescribeConfigsResourceResult` for one `(key, value)` pair.
pub(super) fn make_entry(
    key: &str,
    value: &str,
    config_source: i8,
) -> DescribeConfigsResourceResult {
    DescribeConfigsResourceResult {
        name: key.to_owned(),
        value: Some(value.to_owned()),
        read_only: false,
        config_source,
        is_sensitive: false,
        synonyms: Vec::new(),
        config_type: CONFIG_TYPE_UNKNOWN,
        documentation: None,
        ..Default::default()
    }
}

/// Produces one entry in a config's synonym chain: the key the value was
/// written under, that value, and the source it came from.
///
/// Kafka fills the chain only when the request asks for it
/// (`include_synonyms`), and an entry's own `config_source` is the source of
/// the chain's head. A request that leaves the flag unset gets an empty list
/// on every entry, so a caller that was not asked for synonyms must not
/// synthesise any.
pub(super) fn synonym(key: &str, value: &str, source: i8) -> DescribeConfigsSynonym {
    DescribeConfigsSynonym {
        name: key.to_owned(),
        value: Some(value.to_owned()),
        source,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::UnknownTaggedFields;

    use super::*;

    #[test]
    fn make_entry_preserves_wire_metadata_fields() {
        let entry = super::make_entry(
            "leader.replication.throttled.rate",
            "1024",
            super::CONFIG_SOURCE_DYNAMIC_BROKER,
        );

        let expected = DescribeConfigsResourceResult {
            name: "leader.replication.throttled.rate".to_string(),
            value: Some("1024".to_string()),
            read_only: false,
            config_source: super::CONFIG_SOURCE_DYNAMIC_BROKER,
            is_sensitive: false,
            synonyms: Vec::new(),
            config_type: 0,
            documentation: None,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(entry == expected);
    }

    #[test]
    fn config_source_dynamic_broker_is_2() {
        assert!(super::CONFIG_SOURCE_DYNAMIC_BROKER == 2i8);
    }
}
