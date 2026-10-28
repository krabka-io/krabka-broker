//! KIP-211 offset retention: the effective values the broker runs with.
//!
//! The two knobs are stored as the operator's own answer — `None` where the
//! operator named nothing — because `DescribeConfigs` reports where a value
//! came from, not only what it is. Kafka's `ConfigHelper.brokerSynonyms` asks
//! whether the key appears in `KafkaConfig.originals`, so a key set explicitly
//! to its own default still reports `STATIC_BROKER_CONFIG`. Verified against
//! `apache/kafka:4.3.1`: a broker whose properties carry
//! `offsets.retention.minutes=10080` answers `kafka-configs --describe --all`
//! with `synonyms={STATIC_BROKER_CONFIG:offsets.retention.minutes=10080,
//! DEFAULT_CONFIG:offsets.retention.minutes=10080}`.
//!
//! Everything that runs on the value reads it through these two accessors, so
//! the fallback lives in one place.

use {
    crate::config::{
        BrokerConfig, DEFAULT_OFFSETS_RETENTION, DEFAULT_OFFSETS_RETENTION_CHECK_INTERVAL,
    },
    krabka_units::Time,
};

impl BrokerConfig {
    /// How long a committed offset outlives the group that owns it.
    #[must_use]
    pub fn offsets_retention(&self) -> Time {
        self.offsets_retention_override
            .unwrap_or(DEFAULT_OFFSETS_RETENTION)
    }

    /// How often the offset-retention sweep runs.
    #[must_use]
    pub fn offsets_retention_check_interval(&self) -> Time {
        self.offsets_retention_check_interval_override
            .unwrap_or(DEFAULT_OFFSETS_RETENTION_CHECK_INTERVAL)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        krabka_units::{convert::TimeExt as _, minutes},
    };

    #[derive(Default)]
    struct OffsetRetentionScenario {
        config: Option<BrokerConfig>,
        observed: Option<(Time, Time)>,
    }

    #[rstest::fixture]
    fn retention() -> OffsetRetentionScenario {
        // Keep retention inputs and observations fresh for each scenario.
        OffsetRetentionScenario::default()
    }

    #[rstest_bdd_macros::given(
        "the operator has not overridden offset retention or its sweep interval"
    )]
    fn untouched_retention(retention: &mut OffsetRetentionScenario) {
        retention.config = Some(BrokerConfig::default());
    }

    #[rstest_bdd_macros::when("the broker resolves its offset retention policy")]
    fn resolve_retention(retention: &mut OffsetRetentionScenario) {
        let config = retention
            .config
            .as_ref()
            .expect("a broker config was given");
        retention.observed = Some((
            config.offsets_retention(),
            config.offsets_retention_check_interval(),
        ));
    }

    #[rstest_bdd_macros::then(
        "offsets are retained for 10080 minutes with a 600000 millisecond sweep interval"
    )]
    fn kafka_retention_defaults(retention: &OffsetRetentionScenario) {
        let (duration, interval) = retention
            .observed
            .expect("the retention policy was resolved");
        check!(duration == DEFAULT_OFFSETS_RETENTION);
        check!(duration.millis_i64() / 60_000 == 10_080);
        check!(interval.millis_i64() == 600_000);
    }

    #[test]
    fn an_operator_value_wins_even_when_it_equals_the_default() {
        let config = BrokerConfig {
            offsets_retention_override: Some(DEFAULT_OFFSETS_RETENTION),
            offsets_retention_check_interval_override: Some(minutes(1)),
            ..BrokerConfig::default()
        };

        // The override survives even where it matches the default, because
        // `DescribeConfigs` reports it as `STATIC_BROKER_CONFIG` on that basis.
        check!(config.offsets_retention_override == Some(DEFAULT_OFFSETS_RETENTION));
        check!(config.offsets_retention() == DEFAULT_OFFSETS_RETENTION);
        check!(config.offsets_retention_check_interval() == minutes(1));
    }

    rstest_bdd_macros::scenarios!("tests/features/migrated/src/config/offset_retention.feature", fixtures = [retention: OffsetRetentionScenario]);
}
