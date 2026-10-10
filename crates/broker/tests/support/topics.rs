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

/// Signed protocol deadlines retain deliberately invalid fixture values.
#[derive(Clone, Copy)]
pub struct CreateTopicsTimeoutMillis(pub i32);

impl Default for CreateTopicsTimeoutMillis {
    fn default() -> Self {
        Self(5_000)
    }
}

#[derive(Clone, Copy, Default)]
pub struct CreateTopicRequestSetup {
    pub timeout: CreateTopicsTimeoutMillis,
}

pub fn create_topic_request(topic: CreatableTopic) -> CreateTopicsRequest {
    create_topic_request_with_setup(topic, CreateTopicRequestSetup::default())
}

pub fn create_topic_request_with_setup(
    topic: CreatableTopic,
    setup: CreateTopicRequestSetup,
) -> CreateTopicsRequest {
    CreateTopicsRequest {
        topics: vec![topic],
        timeout_ms: setup.timeout.0,
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
#[derive(krabka_macros::FieldDefaults)]
pub struct ConfiguredTopicSetup {
    #[default("orders".into())]
    pub name: String,
    #[default(TopicPartitionCount(1))]
    pub partitions: TopicPartitionCount,
    #[default(TopicReplicationFactor(1))]
    pub replicas: TopicReplicationFactor,
    pub configs: Vec<CreatableTopicConfig>,
}

pub fn creatable_topic_with_configs(setup: ConfiguredTopicSetup) -> CreatableTopic {
    let ConfiguredTopicSetup {
        name,
        partitions,
        replicas,
        configs,
    } = setup;
    CreatableTopic {
        configs,
        ..creatable_topic(name, partitions.0, replicas.0)
    }
}

/// A single-partition diskless topic created through the ordinary admin handler.
pub fn diskless_topic_request(name: impl Into<String>, replication: i16) -> CreateTopicsRequest {
    crate::support::topics::create_topic_request_with_setup(
        creatable_topic_with_configs(crate::support::topics::ConfiguredTopicSetup {
            name: name.into(),
            replicas: crate::support::topics::TopicReplicationFactor(replication),
            configs: topic_configs([("krabka.diskless", "true")]),
            ..Default::default()
        }),
        crate::support::topics::CreateTopicRequestSetup {
            timeout: crate::support::topics::CreateTopicsTimeoutMillis(10_000),
        },
    )
}

/// KIP-516 distinguishes a missing nonzero UUID from the all-zero sentinel.
pub fn unresolved_topic_ids(
    id: u128,
) -> [(&'static str, krabka_protocol::primitives::uuid::Uuid); 2] {
    [
        (
            "non-zero id",
            krabka_protocol::primitives::uuid::Uuid(uuid::Uuid::from_u128(id).into_bytes()),
        ),
        ("zero id", krabka_protocol::primitives::uuid::Uuid::ZERO),
    ]
}

/// Create an explicitly assigned partition and wait for every replica to install it.
pub async fn create_assigned_partition<'a>(
    client: &krabka_client_core::Client,
    topic: &str,
    replicas: &[i32],
    brokers: impl IntoIterator<Item = &'a krabka_broker::BrokerHandle>,
) -> krabka_protocol::primitives::uuid::Uuid {
    let response = client
        .send(create_topic_request(super::topic_on(topic, &[replicas])))
        .await
        .unwrap();
    assert2::assert!(response.topics[0].error_code == 0);
    let topic_id = response.topics[0].topic_id;
    for broker in brokers {
        broker.wait_until_partition_present(topic, 0).await;
    }
    topic_id
}
