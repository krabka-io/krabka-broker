//! The five client quotas the broker charges and throttles on.

/// The client quota that caused a throttle the broker applied.
///
/// Kafka splits its quotas the same way, and names them the same way in
/// `kafka.server:type=*QuotaManager`. The five variants are the quotas whose
/// delay this broker *sleeps* on: `producer_byte_rate` (KIP-13),
/// `consumer_byte_rate` (KIP-13), `request_percentage` (KIP-124),
/// `controller_mutation_rate` (KIP-599), which `CreateTopics`,
/// `CreatePartitions` and `DeleteTopics` apply inline once they have assembled
/// their response, and `connection_creation_rate` (KIP-612) on the accept path.
/// Kafka's `LeaderReplication` and `FollowerReplication` quotas are absent
/// because KIP-73 throttles a follower fetch by dropping partitions out of the
/// response rather than by delaying it, so there is no sleep to attribute. The
/// replication throttle is published as its own byte-rate instead:
/// `replication_throttled_bytes_out` and `replication_throttled_bytes_in`,
/// which stand for Kafka's `LeaderReplication` and `FollowerReplication`
/// `byte-rate` sensors, with `replication_throttle_sleeps` for the rounds it
/// held back entirely.
///
/// The `quota_type` label value, `as_str`, is Kafka's own `QuotaType` name, so
/// one dashboard query reads the same against either broker. `ALL` lists
/// every quota the broker applies a throttle for.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, krabka_macros::EnumStr)]
#[enum_str(all, label_value)]
pub enum QuotaType {
    /// KIP-13 `producer_byte_rate`, charged on the Produce path.
    Produce,
    /// KIP-13 `consumer_byte_rate`, charged on the Fetch path.
    Fetch,
    /// KIP-124 `request_percentage`, charged on every api by handler time.
    Request,
    /// KIP-599 `controller_mutation_rate`, charged on the topic-mutating
    /// admin apis by the number of partitions the request moves.
    ControllerMutation,
    /// KIP-612 `connection_creation_rate`, charged on the accept path.
    #[enum_str(name = "connection_creation_rate")]
    ConnectionCreation,
}

impl QuotaType {
    /// The dynamic-config key this quota is configured under, as
    /// `AlterClientQuotas` spells it.
    ///
    /// It is the inverse of the key each bucket is created under, and it is
    /// what lets the quota-expiry sweep find the series an expired bucket
    /// published. `None` for a key no quota is charged against.
    #[must_use]
    pub fn from_config_key(key: &str) -> Option<Self> {
        match key {
            "producer_byte_rate" => Some(Self::Produce),
            "consumer_byte_rate" => Some(Self::Fetch),
            "request_percentage" => Some(Self::Request),
            "controller_mutation_rate" => Some(Self::ControllerMutation),
            "connection_creation_rate" => Some(Self::ConnectionCreation),
            _ => None,
        }
    }
}
