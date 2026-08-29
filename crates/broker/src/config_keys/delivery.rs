//! The three KFC-1 scheduled-delivery keys and the produce-path resolvers for
//! the two of them that never reach `Log.config`.

use krabka_units::{Time, convert::wire::opt_time_from_millis_i64};

/// KFC-1: per-topic delivery mode. `immediate`, the default, is Kafka's
/// behavior. `scheduled` makes each batch's `max_timestamp` its delivery time,
/// so a record stays invisible to consumers until it comes due. This is the
/// one delivery key that reaches [`krabka_log::LogConfig::delivery_policy`].
pub(crate) const DELIVERY_MODE: &str = "delivery.mode";
pub(crate) const DELIVERY_MODE_IMMEDIATE: &str = "immediate";
pub(crate) const DELIVERY_MODE_SCHEDULED: &str = "scheduled";

/// KFC-1: the largest delay the produce path accepts, measured forward from
/// produce time. `-1` removes the limit. A batch scheduled further ahead is
/// rejected with `INVALID_TIMESTAMP` (32).
pub(crate) const DELIVERY_MAX_DELAY_MS: &str = "delivery.max.delay.ms";
/// Default `delivery.max.delay.ms`: 7 days.
pub(crate) const DEFAULT_DELIVERY_MAX_DELAY_MS: i64 = 604_800_000;

/// KFC-1: when `true`, the produce path rejects a batch whose delivery time is
/// before the largest delivery time already in the partition. It turns a
/// silently stalled schedule into an `INVALID_TIMESTAMP` (32) at the producer
/// that caused it. Default `false`.
pub(crate) const DELIVERY_SCHEDULE_MONOTONIC: &str = "delivery.schedule.monotonic";

/// KFC-1 sentinel for `delivery.max.delay.ms`: `-1` means no bound on how far
/// ahead a batch may be scheduled, and is the lowest legal value.
pub(super) const DELIVERY_MAX_DELAY_UNLIMITED: i64 = -1;

/// Resolve `delivery.max.delay.ms` for `topic`: the largest delay the produce
/// path accepts, measured forward from produce time. `None` is the `-1`
/// sentinel and removes the bound. A missing or unparseable value resolves to
/// the 7-day default, which matches the permissive runtime behavior of the
/// other Produce-side topic config reads.
#[must_use]
pub(crate) fn resolve_delivery_max_delay(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> Option<Time> {
    let millis = image
        .topic_config(topic)
        .and_then(|configs| configs.get(DELIVERY_MAX_DELAY_MS))
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|millis| *millis >= DELIVERY_MAX_DELAY_UNLIMITED)
        .unwrap_or(DEFAULT_DELIVERY_MAX_DELAY_MS);
    opt_time_from_millis_i64(millis)
}

/// Resolve `delivery.schedule.monotonic` for `topic`. `true` makes the produce
/// path reject a batch whose delivery time is before the largest delivery time
/// already in the partition. A missing or unparseable value resolves to
/// `false`.
#[must_use]
pub(crate) fn resolve_delivery_schedule_monotonic(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> bool {
    image
        .topic_config(topic)
        .and_then(|configs| configs.get(DELIVERY_SCHEDULE_MONOTONIC))
        .map(String::as_str)
        == Some("true")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;
    use krabka_log::LogConfig;
    use krabka_units::millis;

    use super::{
        super::{
            log_config::apply_to_log_config,
            validation::{is_recognized, validate_topic_config},
        },
        *,
    };

    #[test]
    fn validate_delivery_mode_accepts_the_two_modes_only() {
        let cases = [
            (DELIVERY_MODE_IMMEDIATE, true),
            (DELIVERY_MODE_SCHEDULED, true),
            ("later", false),
            ("", false),
        ];
        for (value, want_ok) in cases {
            assert!(
                validate_topic_config(DELIVERY_MODE, value).is_ok() == want_ok,
                "delivery.mode={value}"
            );
        }
    }

    #[test]
    fn validate_delivery_max_delay_ms_boundary_cases() {
        let cases = [
            ("0", true),         // no delay at all is legal
            ("604800000", true), // the default, 7 days
            ("-1", true),        // -1 (unbounded) accepted
            ("-2", false),       // below -1 rejected
            ("soon", false),     // non-integer rejected
        ];
        for (value, want_ok) in cases {
            assert!(
                validate_topic_config(DELIVERY_MAX_DELAY_MS, value).is_ok() == want_ok,
                "delivery.max.delay.ms={value}"
            );
        }
    }

    #[test]
    fn validate_delivery_schedule_monotonic_accepts_bools_only() {
        let cases = [("true", true), ("false", true), ("yes", false), ("", false)];
        for (value, want_ok) in cases {
            assert!(
                validate_topic_config(DELIVERY_SCHEDULE_MONOTONIC, value).is_ok() == want_ok,
                "delivery.schedule.monotonic={value}"
            );
        }
    }

    #[test]
    fn is_recognized_includes_delivery_keys() {
        assert!(is_recognized(DELIVERY_MODE));
        assert!(is_recognized(DELIVERY_MAX_DELAY_MS));
        assert!(is_recognized(DELIVERY_SCHEDULE_MONOTONIC));
    }

    #[test]
    fn apply_delivery_mode_propagates_both_ways() {
        let mut scheduled = BTreeMap::new();
        scheduled.insert(DELIVERY_MODE.into(), DELIVERY_MODE_SCHEDULED.into());
        assert!(
            apply_to_log_config(&scheduled, &LogConfig::default())
                == LogConfig {
                    delivery_policy: krabka_log::DeliveryPolicy::Scheduled,
                    ..LogConfig::default()
                }
        );

        let base = LogConfig {
            delivery_policy: krabka_log::DeliveryPolicy::Scheduled,
            ..LogConfig::default()
        };
        let mut immediate = BTreeMap::new();
        immediate.insert(DELIVERY_MODE.into(), DELIVERY_MODE_IMMEDIATE.into());
        assert!(apply_to_log_config(&immediate, &base) == LogConfig::default());
    }

    #[test]
    fn apply_leaves_delivery_policy_alone_for_the_produce_side_keys() {
        // Both keys are enforced on the produce path, so neither may move the
        // log's own visibility policy.
        let mut overrides = BTreeMap::new();
        overrides.insert(DELIVERY_MAX_DELAY_MS.into(), "1000".into());
        overrides.insert(DELIVERY_SCHEDULE_MONOTONIC.into(), "true".into());
        assert!(apply_to_log_config(&overrides, &LogConfig::default()) == LogConfig::default());
    }

    #[test]
    fn delivery_settings_resolve_topic_overrides_over_defaults() {
        use krabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord};
        use uuid::Uuid;

        let mut image = MetadataImage::new(Uuid::nil());
        assert!(resolve_delivery_max_delay(&image, "t") == Some(millis(604_800_000)));
        assert!(!resolve_delivery_schedule_monotonic(&image, "t"));

        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: maplit::btreemap! {
            DELIVERY_MAX_DELAY_MS.into() => "90000".into(),
            DELIVERY_SCHEDULE_MONOTONIC.into() => "true".into()},
        }));
        assert!(resolve_delivery_max_delay(&image, "t") == Some(millis(90_000)));
        assert!(resolve_delivery_schedule_monotonic(&image, "t"));

        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: maplit::btreemap! {DELIVERY_MAX_DELAY_MS.into() => "-1".into()},
        }));
        assert!(resolve_delivery_max_delay(&image, "t") == None);
        assert!(!resolve_delivery_schedule_monotonic(&image, "t"));
    }

    #[test]
    fn corrupt_delivery_settings_resolve_to_their_defaults() {
        use krabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord};
        use uuid::Uuid;

        let cases = ["soon", "-5", ""];
        for value in cases {
            let mut image = MetadataImage::new(Uuid::nil());
            image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "t".into(),
                overrides: maplit::btreemap! {
                DELIVERY_MAX_DELAY_MS.into() => value.into(),
                DELIVERY_SCHEDULE_MONOTONIC.into() => value.into()},
            }));
            assert!(
                resolve_delivery_max_delay(&image, "t") == Some(millis(604_800_000)),
                "delivery.max.delay.ms={value}"
            );
            assert!(
                !resolve_delivery_schedule_monotonic(&image, "t"),
                "delivery.schedule.monotonic={value}"
            );
        }
    }
}
