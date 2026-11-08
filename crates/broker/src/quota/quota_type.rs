//! The five client quotas the broker charges and throttles on.

use super::request::request_percentage_bound;

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
    /// `AlterClientQuotas` spells it and the image stores it.
    #[must_use]
    pub const fn config_key(self) -> &'static str {
        match self {
            Self::Produce => "producer_byte_rate",
            Self::Fetch => "consumer_byte_rate",
            Self::Request => "request_percentage",
            Self::ControllerMutation => "controller_mutation_rate",
            Self::ConnectionCreation => "connection_creation_rate",
        }
    }

    /// The quota a dynamic-config key names: the inverse of
    /// [`Self::config_key`]. `None` for a key no quota is charged against.
    #[must_use]
    pub fn from_config_key(key: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|quota_type| quota_type.config_key() == key)
    }

    /// The configured rate in the unit this quota's meter records, per
    /// second: the same conversion the consumer used when it created the
    /// meter.
    ///
    /// `request_percentage` records microseconds of handler time. Every other
    /// quota records its own unit, and keeps its fractional part: Kafka holds
    /// each quota as a double, and enforces a fractional rate as configured.
    pub(super) fn metered_rate(self, rate: f64) -> f64 {
        match self {
            Self::Request => request_percentage_bound(rate),
            Self::Produce | Self::Fetch | Self::ControllerMutation | Self::ConnectionCreation => {
                rate
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::QuotaType, assert2::check};

    /// Each quota keeps Kafka's dynamic-config key, which the image is keyed
    /// by, and Kafka's `QuotaType` name, which the `quota_type` label carries.
    #[test]
    fn every_quota_keeps_its_config_key_and_label_value() {
        let actual = QuotaType::ALL.map(|quota_type| {
            (
                quota_type,
                quota_type.config_key(),
                quota_type.as_str(),
                QuotaType::from_config_key(quota_type.config_key()),
            )
        });
        check!(
            actual
                == [
                    (
                        QuotaType::Produce,
                        "producer_byte_rate",
                        "Produce",
                        Some(QuotaType::Produce),
                    ),
                    (
                        QuotaType::Fetch,
                        "consumer_byte_rate",
                        "Fetch",
                        Some(QuotaType::Fetch),
                    ),
                    (
                        QuotaType::Request,
                        "request_percentage",
                        "Request",
                        Some(QuotaType::Request),
                    ),
                    (
                        QuotaType::ControllerMutation,
                        "controller_mutation_rate",
                        "ControllerMutation",
                        Some(QuotaType::ControllerMutation),
                    ),
                    (
                        QuotaType::ConnectionCreation,
                        "connection_creation_rate",
                        "connection_creation_rate",
                        Some(QuotaType::ConnectionCreation),
                    ),
                ]
        );
    }

    #[test]
    fn an_unknown_quota_config_key_has_no_type() {
        check!(QuotaType::from_config_key("not_a_quota") == None);
    }
}
