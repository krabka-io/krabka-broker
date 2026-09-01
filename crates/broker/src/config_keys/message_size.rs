//! Kafka's `max.message.bytes` topic key: the value check, and the per-topic
//! lookup the produce path reads with the broker-wide `message.max.bytes`
//! behind it.

use krabka_units::{ByteSize, convert::ByteSizeExt as _};

use super::MAX_MESSAGE_BYTES;

/// Validate a `max.message.bytes` value.
///
/// Kafka declares the key as `INT` with `atLeast(0)`, so `0` is legal, `-1` is
/// not, and a value past `i32::MAX` is "Not a number of type INT" rather than
/// an out-of-range one. Both rejections are reproduced here, because
/// `kafka-configs --alter` is where an operator meets them.
pub(super) fn validate_max_message_bytes(value: &str) -> Result<(), String> {
    let parsed: i32 = value
        .parse()
        .map_err(|_| format!("{MAX_MESSAGE_BYTES}={value} is not a 32-bit integer"))?;
    if parsed < 0 {
        return Err(format!("{MAX_MESSAGE_BYTES}={value} must be >= 0"));
    }
    Ok(())
}

/// Resolve the largest record batch a topic accepts.
///
/// `broker_default` is the broker's `message.max.bytes`, which is what Kafka
/// reports as the `DEFAULT_CONFIG` synonym of an unset `max.message.bytes`.
/// A corrupt stored value falls back to it too, matching the other
/// produce-side config reads: `AlterConfigs` already refused the value, so a
/// string here that does not parse means a damaged metadata image and not an
/// operator's intent.
#[must_use]
pub(crate) fn resolve_max_message_bytes(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    broker_default: ByteSize,
) -> ByteSize {
    image
        .topic_config(topic)
        .and_then(|configs| configs.get(MAX_MESSAGE_BYTES))
        .and_then(|value| value.parse::<i32>().ok())
        .and_then(|value| u64::try_from(value).ok())
        .map_or(broker_default, ByteSize::from_bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;
    use krabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord};
    use krabka_units::bytes;
    use uuid::Uuid;

    use super::{super::validation::validate_topic_config, *};

    #[test]
    fn validate_max_message_bytes_matches_kafkas_int_at_least_zero() {
        let cases = [
            ("0", true),
            ("1048588", true),
            ("2147483647", true),
            ("-1", false),
            ("2147483648", false),
            ("abc", false),
            ("", false),
        ];
        for (value, accepted) in cases {
            assert!(
                validate_topic_config(MAX_MESSAGE_BYTES, value).is_ok() == accepted,
                "max.message.bytes={value}"
            );
        }
    }

    fn image_with(value: Option<&str>) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        if let Some(value) = value {
            let mut overrides = BTreeMap::new();
            overrides.insert(MAX_MESSAGE_BYTES.to_owned(), value.to_owned());
            image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "t".to_owned(),
                overrides,
            }));
        }
        image
    }

    #[test]
    fn resolve_max_message_bytes_prefers_the_override_and_falls_back_otherwise() {
        let default = bytes(1_048_588);
        let cases = [
            (None, default),
            (Some("2048"), bytes(2048)),
            (Some("0"), bytes(0)),
            // A corrupt stored value is not an operator's intent.
            (Some("not-a-number"), default),
            (Some("-5"), default),
        ];
        for (stored, want) in cases {
            assert!(
                resolve_max_message_bytes(&image_with(stored), "t", default) == want,
                "stored {stored:?}"
            );
        }
    }

    #[test]
    fn resolve_max_message_bytes_falls_back_on_a_topic_with_no_config_at_all() {
        let default = bytes(4096);
        assert!(resolve_max_message_bytes(&image_with(None), "ghost", default) == default);
    }
}
