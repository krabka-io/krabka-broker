//! Ordered tiered-topic configuration shared by the tiered-storage suites.
use krabka_protocol::owned::create_topics_request::CreatableTopicConfig;

pub(crate) fn tiered_configs(segment_bytes: Option<&str>) -> Vec<CreatableTopicConfig> {
    std::iter::once(("remote.storage.enable", "true"))
        .chain(segment_bytes.map(|value| ("internal.segment.bytes", value)))
        .chain([
            ("local.retention.bytes", "1"),
            ("retention.bytes", "-1"),
            ("retention.ms", "-1"),
        ])
        .map(|(name, value)| CreatableTopicConfig {
            name: name.into(),
            value: Some(value.into()),
            ..Default::default()
        })
        .collect()
}
