//! The per-topic produce settings that the handler resolves once per topic
//! out of the metadata image, before it walks that topic's partitions.

use krabka_protocol::records::TimestampType;

use crate::config_keys::{
    COMPRESSION_TYPE, MESSAGE_TIMESTAMP_AFTER_MAX_MS, MESSAGE_TIMESTAMP_BEFORE_MAX_MS,
    MESSAGE_TIMESTAMP_TYPE, MESSAGE_TIMESTAMP_TYPE_LOG_APPEND, configured_min_insync_replicas,
    parse_compression_type,
};

/// Resolve `min.insync.replicas` for a topic from the metadata image.
///
/// The lookup is [`configured_min_insync_replicas`], the one the controller
/// resolves KIP-966's ELR threshold through: the topic override, then the
/// cluster-wide dynamic broker default. Reading only the topic override here
/// would let a cluster-wide default that the controller honours govern the
/// ELR while this gate ignored it, and the ELR would then name replicas that
/// accepted writes had moved past.
///
/// `default_min_insync_replicas` is this broker's command-line value, the one
/// layer the controller cannot see. It applies only when the image names
/// nothing, where the controller resolves Kafka's default of 1, so this gate
/// is never the more permissive of the two.
///
/// On a malformed value the function falls back to the broker default without
/// a message. The `AlterConfigs` validator already rejected the invalid
/// values, so any string here that does not parse means a corrupt metadata
/// image.
pub(super) fn topic_min_insync_replicas(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    default_min_insync_replicas: i32,
) -> i32 {
    configured_min_insync_replicas(image, topic).unwrap_or(default_min_insync_replicas)
}

/// Resolve a topic's broker-side `compression.type` from the metadata image.
///
/// `None` means Kafka's `producer` pass-through, with no recompression.
/// `Some(codec)` forces recompression of the batches whose codec differs. The
/// result matches the resolution that the partition writer applies through its
/// `LogConfig::compression_type`.
pub(super) fn resolve_topic_compression(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> Option<krabka_compression::CompressionType> {
    image
        .topic_config(topic)
        .and_then(|m| m.get(COMPRESSION_TYPE))
        .and_then(|v| parse_compression_type(v).ok())
        .flatten()
}

/// A topic's KIP-32 timestamp policy at produce time: whose clock the stored
/// records carry, and how far a producer's own timestamp may sit from the
/// broker's clock.
///
/// Kafka keeps the three settings together in `LogValidator`, and so does
/// this: `message.timestamp.type` decides whether the window applies at all,
/// because `validateTimestamp` tests a record's timestamp only under
/// `CreateTime`. A `LogAppendTime` topic overwrites every timestamp with the
/// broker's clock at append, so there is nothing left for the window to
/// refuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampPolicy {
    /// The topic's `message.timestamp.type`.
    timestamp_type: TimestampType,
    /// `message.timestamp.before.max.ms`: how far behind the broker's clock a
    /// producer timestamp may sit. `None` is Kafka's `Long.MAX_VALUE` default,
    /// which removes the bound.
    before_max_ms: Option<i64>,
    /// `message.timestamp.after.max.ms`: how far ahead of the broker's clock a
    /// producer timestamp may sit. `None` is the same default and the same
    /// meaning.
    after_max_ms: Option<i64>,
}

impl Default for TimestampPolicy {
    /// Kafka's defaults: the producer's own timestamps, and neither window
    /// bounded. Every topic that configured none of the three keys resolves to
    /// this, and so does the benchmark seam.
    fn default() -> Self {
        Self {
            timestamp_type: TimestampType::CreateTime,
            before_max_ms: None,
            after_max_ms: None,
        }
    }
}

impl TimestampPolicy {
    /// Whether any record timestamp in a batch has to be looked at.
    ///
    /// False on every topic that left both windows at their defaults, which is
    /// every topic that did not ask for the check, and on a `LogAppendTime`
    /// topic whatever the windows say. The produce path then reads no clock
    /// and walks no records.
    pub(super) fn bounds_records(self) -> bool {
        self.timestamp_type == TimestampType::CreateTime
            && (self.before_max_ms.is_some() || self.after_max_ms.is_some())
    }

    /// Whether `timestamp_ms` is outside the window `now_ms` puts it in, which
    /// Kafka answers with `INVALID_TIMESTAMP` for the whole batch.
    ///
    /// This is `LogValidator.recordHasInvalidTimestamp`: the record is refused
    /// when it is more than `before.max.ms` older than the broker's clock or
    /// more than `after.max.ms` newer than it. `NO_TIMESTAMP` (-1) is exempt,
    /// as it is in Kafka, because such a record carries no timestamp to judge.
    /// The arithmetic saturates rather than wrapping: a producer that sends
    /// `i64::MIN` must be refused, not admitted by an overflow.
    pub(super) fn rejects_record(self, timestamp_ms: i64, now_ms: i64) -> bool {
        if !self.bounds_records() || timestamp_ms == NO_TIMESTAMP {
            return false;
        }
        let difference = now_ms.saturating_sub(timestamp_ms);
        self.before_max_ms.is_some_and(|before| difference > before)
            || self
                .after_max_ms
                .is_some_and(|after| difference.saturating_neg() > after)
    }
}

/// Kafka's `RecordBatch.NO_TIMESTAMP`: a record that carries no create time.
const NO_TIMESTAMP: i64 = -1;

/// Resolve a topic's produce-time timestamp policy from the metadata image.
///
/// Every topic has one, so this returns a value rather than an `Option`: the
/// default is `CreateTime` with both windows open, which is Kafka's default and
/// costs the produce path one boolean test per batch.
///
/// An unparseable window falls back to "no bound", the same direction the other
/// produce-side config reads fall back in. `AlterConfigs` already refused the
/// value, so a string here that does not parse means a corrupt metadata image,
/// and refusing every write to the topic over one is the worse answer.
pub(super) fn resolve_timestamp_policy(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> TimestampPolicy {
    let configs = image.topic_config(topic);
    let value = |key: &str| configs.and_then(|c| c.get(key)).map(String::as_str);
    let timestamp_type = if value(MESSAGE_TIMESTAMP_TYPE) == Some(MESSAGE_TIMESTAMP_TYPE_LOG_APPEND)
    {
        TimestampType::LogAppendTime
    } else {
        TimestampType::CreateTime
    };
    TimestampPolicy {
        timestamp_type,
        before_max_ms: parse_timestamp_window(value(MESSAGE_TIMESTAMP_BEFORE_MAX_MS)),
        after_max_ms: parse_timestamp_window(value(MESSAGE_TIMESTAMP_AFTER_MAX_MS)),
    }
}

/// One `message.timestamp.{before,after}.max.ms` value as a bound.
///
/// `None` is "no bound": the key is unset, it carries Kafka's `Long.MAX_VALUE`
/// default, or it carries a value that does not parse. A negative value is a
/// bound of its own in Kafka -- the config's minimum is 0 -- so nothing here
/// clamps one.
fn parse_timestamp_window(value: Option<&str>) -> Option<i64> {
    value
        .and_then(|raw| raw.parse::<i64>().ok())
        .filter(|ms| *ms != i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::{assert, check};
    use krabka_compression::CompressionType;
    use krabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord};
    use uuid::Uuid;

    use super::*;
    use crate::{
        config_keys::MIN_INSYNC_REPLICAS,
        handlers::produce::test_support::{image_with_topic, set_min_isr},
    };

    /// Seed a cluster-wide dynamic `min.insync.replicas` default, the layer
    /// Kafka resolves between the topic override and each node's static
    /// config, and the one the controller's ELR resolver reads.
    fn set_cluster_default_min_isr(img: &mut MetadataImage, n: i32) {
        img.apply(&MetadataRecord::V1BrokerConfig(
            krabka_metadata::BrokerConfigRecord {
                node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: MIN_INSYNC_REPLICAS.to_string(),
                config_value: Some(n.to_string()),
            },
        ));
    }

    /// KIP-966 keys the whole ELR decision on `min.insync.replicas`: the
    /// controller records the replicas still known to hold every committed
    /// record, and what is committed is what this gate accepted. So the
    /// threshold the two resolve has to be the same one, and the gate must
    /// never be the more permissive of the two.
    ///
    /// A cluster-wide default alone is the case that used to break it. The
    /// controller honoured it and this gate did not, so an `acks=all` write
    /// was accepted at an ISR the controller still called below-min -- and
    /// the replicas the controller was holding in the ELR fell behind that
    /// write while it went on calling them eligible to lead.
    #[test]
    fn the_gate_resolves_the_threshold_the_controller_maintains_the_elr_against() {
        for (label, topic_override, cluster_default, broker_default, expected) in [
            (
                "nothing published, so the broker's own default stands alone",
                None,
                None,
                2,
                2,
            ),
            (
                "a cluster-wide default outranks the broker's own",
                None,
                Some(3),
                1,
                3,
            ),
            ("a topic override outranks both", Some(3), Some(2), 1, 3),
        ] {
            let mut img = image_with_topic("t", &[1, 2, 3]);
            if let Some(value) = topic_override {
                set_min_isr(&mut img, "t", value);
            }
            if let Some(value) = cluster_default {
                set_cluster_default_min_isr(&mut img, value);
            }

            let gate = topic_min_insync_replicas(&img, "t", broker_default);
            let controller = crate::config_keys::effective_min_insync_replicas(&img, "t", 3);

            assert!(gate == expected, "{label}: got {gate}");
            assert!(
                i32::try_from(controller).expect("a min ISR fits in i32") <= gate,
                "{label}: the controller resolved {controller} against the gate's {gate}"
            );
        }
    }

    #[test]
    fn topic_min_isr_defaults_to_one_when_unset() {
        let img = image_with_topic("t", &[1, 2, 3]);
        assert!(topic_min_insync_replicas(&img, "t", 1) == 1);
    }

    #[test]
    fn topic_min_isr_reads_override_when_set() {
        let mut img = image_with_topic("t", &[1, 2, 3]);
        set_min_isr(&mut img, "t", 3);
        assert!(topic_min_insync_replicas(&img, "t", 1) == 3);
    }

    #[test]
    fn topic_min_isr_uses_broker_fallback_unless_valid_override_exists() {
        let cases = [(None, 2), (Some(3), 3)];

        for (override_value, expected) in cases {
            let mut img = image_with_topic("t", &[1, 2, 3]);
            if let Some(value) = override_value {
                set_min_isr(&mut img, "t", value);
            }

            assert!(topic_min_insync_replicas(&img, "t", 2) == expected);
        }
    }

    #[test]
    fn topic_min_isr_default_one_on_unknown_topic() {
        let img = MetadataImage::new(Uuid::nil());
        assert!(
            topic_min_insync_replicas(&img, "ghost", 1) == 1,
            "missing topic_config must default to 1, not crash"
        );
    }

    #[test]
    fn topic_min_isr_default_one_on_malformed_value() {
        let mut img = image_with_topic("t", &[1, 2, 3]);
        let mut o = BTreeMap::new();
        o.insert(MIN_INSYNC_REPLICAS.into(), "not-a-number".into());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: o,
        }));
        assert!(
            topic_min_insync_replicas(&img, "t", 1) == 1,
            "unparseable value must fall back to permissive default 1"
        );
    }

    #[test]
    fn topic_min_isr_handles_topic_config_without_min_isr_key() {
        // Topic has *some* override (e.g. retention.ms) but no
        // min.insync.replicas — still defaults to 1.
        let mut img = image_with_topic("t", &[1, 2, 3]);
        let mut o = BTreeMap::new();
        o.insert("retention.ms".into(), "60000".into());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: o,
        }));
        assert!(topic_min_insync_replicas(&img, "t", 1) == 1);
    }

    #[test]
    fn resolve_topic_compression_distinguishes_producer_and_forced_codecs() {
        let cases = [
            // "producer" keeps the producer's codec → no forced compression.
            ("producer", None),
            // A concrete codec forces recompression to that codec.
            ("zstd", Some(CompressionType::Zstd)),
        ];
        for (config_value, want) in cases {
            let mut img = image_with_topic("t", &[1]);
            let mut overrides = BTreeMap::new();
            overrides.insert(
                crate::config_keys::COMPRESSION_TYPE.into(),
                config_value.into(),
            );
            img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "t".into(),
                overrides,
            }));
            assert!(
                resolve_topic_compression(&img, "t") == want,
                "compression.type {config_value:?}"
            );
        }
    }

    /// Kafka's `LogValidator.recordHasInvalidTimestamp`, which is
    /// `now - timestamp > before.max.ms || timestamp - now > after.max.ms`,
    /// applied only under `CreateTime` and only to a record that carries a
    /// timestamp at all.
    #[test]
    fn the_window_is_kafkas_record_timestamp_test() {
        const NOW: i64 = 1_000_000;
        let policy = |timestamp_type, before, after| TimestampPolicy {
            timestamp_type,
            before_max_ms: before,
            after_max_ms: after,
        };
        let cases = [
            (
                "a topic that configured neither window admits any timestamp",
                policy(TimestampType::CreateTime, None, None),
                0,
                false,
            ),
            (
                "exactly at the past bound is still inside the window",
                policy(TimestampType::CreateTime, Some(100), None),
                NOW - 100,
                false,
            ),
            (
                "one millisecond past the past bound is outside it",
                policy(TimestampType::CreateTime, Some(100), None),
                NOW - 101,
                true,
            ),
            (
                "exactly at the future bound is still inside the window",
                policy(TimestampType::CreateTime, None, Some(100)),
                NOW + 100,
                false,
            ),
            (
                "one millisecond past the future bound is outside it",
                policy(TimestampType::CreateTime, None, Some(100)),
                NOW + 101,
                true,
            ),
            (
                "the past bound says nothing about a future timestamp",
                policy(TimestampType::CreateTime, Some(100), None),
                NOW + 100_000,
                false,
            ),
            (
                "NO_TIMESTAMP carries no time to judge, so Kafka exempts it",
                policy(TimestampType::CreateTime, Some(100), Some(100)),
                -1,
                false,
            ),
            (
                "a LogAppendTime topic overwrites the timestamp, so it ignores the window",
                policy(TimestampType::LogAppendTime, Some(100), Some(100)),
                0,
                false,
            ),
            (
                "an absurd timestamp saturates rather than wrapping into the window",
                policy(TimestampType::CreateTime, Some(100), Some(100)),
                i64::MIN,
                true,
            ),
        ];
        for (label, policy, timestamp_ms, want_rejected) in cases {
            check!(
                policy.rejects_record(timestamp_ms, NOW) == want_rejected,
                "{label}"
            );
        }
    }

    /// The three keys the produce path resolves per topic, and the defaults it
    /// resolves for a topic that set none of them.
    #[test]
    fn resolve_timestamp_policy_reads_the_three_keys() {
        let cases = [
            (
                "an unconfigured topic is CreateTime with both windows open",
                vec![],
                TimestampPolicy::default(),
            ),
            (
                "LogAppendTime alone",
                vec![(MESSAGE_TIMESTAMP_TYPE, "LogAppendTime")],
                TimestampPolicy {
                    timestamp_type: TimestampType::LogAppendTime,
                    before_max_ms: None,
                    after_max_ms: None,
                },
            ),
            (
                "both windows",
                vec![
                    (MESSAGE_TIMESTAMP_BEFORE_MAX_MS, "1000"),
                    (MESSAGE_TIMESTAMP_AFTER_MAX_MS, "2000"),
                ],
                TimestampPolicy {
                    timestamp_type: TimestampType::CreateTime,
                    before_max_ms: Some(1_000),
                    after_max_ms: Some(2_000),
                },
            ),
            (
                "Kafka's Long.MAX_VALUE default spells `no bound`",
                vec![(MESSAGE_TIMESTAMP_BEFORE_MAX_MS, "9223372036854775807")],
                TimestampPolicy::default(),
            ),
            (
                "an unparseable window falls back to no bound",
                vec![(MESSAGE_TIMESTAMP_AFTER_MAX_MS, "soon")],
                TimestampPolicy::default(),
            ),
            (
                "an unknown timestamp type is CreateTime, the default",
                vec![(MESSAGE_TIMESTAMP_TYPE, "WallClock")],
                TimestampPolicy::default(),
            ),
        ];
        for (label, overrides, want) in cases {
            let mut img = image_with_topic("t", &[1]);
            let mut map = BTreeMap::new();
            for (key, value) in overrides {
                map.insert(key.to_string(), value.to_string());
            }
            img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "t".into(),
                overrides: map,
            }));
            check!(resolve_timestamp_policy(&img, "t") == want, "{label}");
        }
    }

    /// A topic the image does not know resolves to the defaults rather than
    /// refusing every write to it.
    #[test]
    fn resolve_timestamp_policy_defaults_on_an_unknown_topic() {
        let img = MetadataImage::new(Uuid::nil());

        check!(resolve_timestamp_policy(&img, "ghost") == TimestampPolicy::default());
    }
}
