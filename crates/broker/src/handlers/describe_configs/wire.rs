//! The `DescribeConfigs` wire vocabulary: the `ConfigSource` and
//! `ResourceType` bytes Kafka defines.
//!
//! These values are the response's contract with the JVM `AdminClient`, so
//! they sit apart from the code that decides which configs to report.

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
/// `ConfigSource::DYNAMIC_BROKER_LOGGER_CONFIG`, the source every logger
/// level reports. A JVM broker stamps it on every entry it reads out of the
/// live log4j2 context.
pub(super) const CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER: i8 = 6;
/// `ConfigSource::DEFAULT_CONFIG`, for keys reported at their default.
pub(super) const CONFIG_SOURCE_DEFAULT: i8 = 5;
/// `DescribeConfigsResponse.ConfigSource::CLIENT_METRICS_CONFIG` wire byte.
pub(super) const CONFIG_SOURCE_CLIENT_METRICS: i8 = 7;
/// `ConfigSource::DYNAMIC_GROUP_CONFIG`.
pub(super) const CONFIG_SOURCE_DYNAMIC_GROUP: i8 = 8;

pub(super) const RESOURCE_TYPE_TOPIC: i8 = 2;
pub(super) const RESOURCE_TYPE_BROKER: i8 = 4;
pub(super) const RESOURCE_TYPE_BROKER_LOGGER: i8 = 8;
pub(super) const RESOURCE_TYPE_CLIENT_METRICS: i8 = 16;
pub(super) const RESOURCE_TYPE_GROUP: i8 = 32;

#[cfg(test)]
mod tests {
    use assert2::check;

    #[test]
    fn config_source_bytes_match_the_admin_client_enum() {
        // `ConfigEntry.ConfigSource` ordinals, which the JVM AdminClient maps
        // straight onto `ConfigEntry.source()`.
        check!(super::CONFIG_SOURCE_DYNAMIC_TOPIC == 1i8);
        check!(super::CONFIG_SOURCE_DYNAMIC_BROKER == 2i8);
        check!(super::CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER == 3i8);
        check!(super::CONFIG_SOURCE_STATIC_BROKER == 4i8);
        check!(super::CONFIG_SOURCE_DEFAULT == 5i8);
        check!(super::CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER == 6i8);
        check!(super::CONFIG_SOURCE_CLIENT_METRICS == 7i8);
        check!(super::CONFIG_SOURCE_DYNAMIC_GROUP == 8i8);
    }
}
