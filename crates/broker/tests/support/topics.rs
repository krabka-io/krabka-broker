//! Pure topic request fixtures, usable by both clients and raw socket drivers.

use krabka_protocol::owned::create_topics_request::{
    CreatableTopic, CreatableTopicConfig, CreateTopicsRequest,
};

pub fn topic_configs<'a>(
    configs: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Vec<CreatableTopicConfig> {
    configs
        .into_iter()
        .map(|(name, value)| CreatableTopicConfig {
            name: name.into(),
            value: Some(value.into()),
            ..Default::default()
        })
        .collect()
}

krabka_macros::create_topic_fixture!(configured_topic_request);
krabka_macros::consumer_fetch_fixture!(consumer_fetch_request);

pub fn creatable_topic(
    name: impl Into<String>,
    num_partitions: i32,
    replication_factor: i16,
) -> CreatableTopic {
    CreatableTopic {
        name: name.into(),
        num_partitions,
        replication_factor,
        ..Default::default()
    }
}

pub fn create_topic_request(topic: CreatableTopic, timeout_ms: i32) -> CreateTopicsRequest {
    CreateTopicsRequest {
        topics: vec![topic],
        timeout_ms,
        ..Default::default()
    }
}

/// A metadata lookup row, including name-free topic-ID lookups.
pub fn metadata_topic(
    name: Option<String>,
    topic_id: krabka_protocol::primitives::uuid::Uuid,
) -> krabka_protocol::owned::metadata_request::MetadataRequestTopic {
    krabka_protocol::owned::metadata_request::MetadataRequestTopic {
        name,
        topic_id,
        ..Default::default()
    }
}

/// A configured topic row with caller-owned values and the unchanged protocol defaults.
pub fn creatable_topic_with_configs(
    name: String,
    partitions: i32,
    replicas: i16,
    configs: Vec<CreatableTopicConfig>,
) -> CreatableTopic {
    CreatableTopic {
        configs,
        ..creatable_topic(name, partitions, replicas)
    }
}
