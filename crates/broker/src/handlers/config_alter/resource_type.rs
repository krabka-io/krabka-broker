//! The config resource types: the `resource_type` byte of `DescribeConfigs`,
//! `AlterConfigs`, `IncrementalAlterConfigs` and `ListConfigResources`.
//!
//! The bytes are the JVM `org.apache.kafka.common.config.ConfigResource.Type`
//! ids. They are a different byte space from the ACL `ResourceType` that
//! [`crate::handlers::acl_wire`] maps, where `4` is `CLUSTER`, not `BROKER`.

use {
    crate::{codes, handlers::acl_wire::CLUSTER_RESOURCE_NAME},
    krabka_metadata::ResourceType,
};

/// A config resource type Kafka knows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigResourceType {
    /// `TOPIC` (2).
    Topic,
    /// `BROKER` (4).
    Broker,
    /// `BROKER_LOGGER` (8).
    BrokerLogger,
    /// `CLIENT_METRICS` (16), KIP-714.
    ClientMetrics,
    /// `GROUP` (32), KIP-848.
    Group,
}

/// The ACL a config resource is authorized on, and what a denial answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConfigResourceAcl {
    /// The ACL resource type the check runs on.
    pub resource_type: ResourceType,
    /// Whether the check runs on `Cluster("kafka-cluster")` rather than on
    /// the resource's own name.
    pub cluster_scoped: bool,
    /// The error code of a denial.
    pub failure_code: i16,
    /// Kafka's message for a denied `AlterConfigs` or
    /// `IncrementalAlterConfigs` resource.
    pub failure_message: &'static str,
}

impl ConfigResourceAcl {
    /// The ACL resource name the check runs on for a resource named `name`.
    pub const fn resource_name<'a>(&self, name: &'a str) -> &'a str {
        if self.cluster_scoped {
            CLUSTER_RESOURCE_NAME
        } else {
            name
        }
    }
}

impl ConfigResourceType {
    /// The type of a wire `resource_type` byte, or `None` for a byte Kafka's
    /// `ConfigResource.Type.forId` reads as `UNKNOWN`.
    pub const fn from_wire(byte: i8) -> Option<Self> {
        match byte {
            2 => Some(Self::Topic),
            4 => Some(Self::Broker),
            8 => Some(Self::BrokerLogger),
            16 => Some(Self::ClientMetrics),
            32 => Some(Self::Group),
            _ => None,
        }
    }

    /// The wire `resource_type` byte.
    pub const fn to_wire(self) -> i8 {
        match self {
            Self::Topic => 2,
            Self::Broker => 4,
            Self::BrokerLogger => 8,
            Self::ClientMetrics => 16,
            Self::Group => 32,
        }
    }

    /// The audit record's `resource_type` for this type.
    pub const fn audit_name(self) -> &'static str {
        match self {
            Self::Topic => "Topic",
            Self::Broker => "Broker",
            Self::BrokerLogger => "BrokerLogger",
            Self::ClientMetrics => "ClientMetrics",
            Self::Group => "Group",
        }
    }

    /// The ACL a resource of this type is authorized on. A topic or group is
    /// checked on its own name, and the node-wide types on the cluster, as
    /// Kafka's `ConfigHelper` and `ControllerApis.authorizeAlterResource`
    /// check them.
    pub const fn acl(self) -> ConfigResourceAcl {
        match self {
            Self::Topic => ConfigResourceAcl {
                resource_type: ResourceType::Topic,
                cluster_scoped: false,
                failure_code: codes::TOPIC_AUTHORIZATION_FAILED,
                failure_message: codes::default_message(codes::TOPIC_AUTHORIZATION_FAILED),
            },
            Self::Group => ConfigResourceAcl {
                resource_type: ResourceType::Group,
                cluster_scoped: false,
                failure_code: codes::GROUP_AUTHORIZATION_FAILED,
                failure_message: codes::default_message(codes::GROUP_AUTHORIZATION_FAILED),
            },
            Self::Broker | Self::BrokerLogger | Self::ClientMetrics => ConfigResourceAcl {
                resource_type: ResourceType::Cluster,
                cluster_scoped: true,
                failure_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                failure_message: codes::default_message(codes::CLUSTER_AUTHORIZATION_FAILED),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::ConfigResourceType, assert2::check};

    /// Every byte maps to Kafka's `ConfigResource.Type` id, and back.
    #[yare::parameterized(
        unknown = { 0, None },
        negative = { -1, None },
        topic = { 2, Some(ConfigResourceType::Topic) },
        between_topic_and_broker = { 3, None },
        broker = { 4, Some(ConfigResourceType::Broker) },
        broker_logger = { 8, Some(ConfigResourceType::BrokerLogger) },
        client_metrics = { 16, Some(ConfigResourceType::ClientMetrics) },
        group = { 32, Some(ConfigResourceType::Group) },
        past_group = { 64, None },
    )]
    fn a_wire_byte_round_trips(byte: i8, expected: Option<ConfigResourceType>) {
        let parsed = ConfigResourceType::from_wire(byte);

        check!(parsed == expected);
        check!(parsed.map_or(byte, ConfigResourceType::to_wire) == byte);
    }
}
