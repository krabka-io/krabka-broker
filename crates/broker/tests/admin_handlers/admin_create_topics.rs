//! `CreateTopics` (`api_key` 19), KIP-525: the created topic's effective
//! configuration travels back on the create response, so a client learns what
//! it created without a follow-up `DescribeConfigs`.
//!
//! Terraform's `kafka_topic`, Connect's `TopicAdmin.createOrFindTopics` and
//! Streams' `InternalTopicManager` all read
//! `createTopics(...).config(topic)`. What they must see there is the very
//! list `describeConfigs` answers for the same topic, which is what this test
//! pins.

use {
    crate::{
        RESOURCE_TYPE_TOPIC,
        admin_harness::{AdminCluster, admin},
        support::{configs::describe_resource, topics},
    },
    assert2::{assert, check},
    krabka_protocol::owned::{
        create_topics_request::{CreatableTopicConfig, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
        describe_configs_request::DescribeConfigsRequest,
        describe_configs_response::DescribeConfigsResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

/// `ConfigSource.DYNAMIC_TOPIC_CONFIG`, the source a value the create request
/// carried reports.
const DYNAMIC_TOPIC_CONFIG: i8 = 1;

/// The fields both responses share, which is everything KIP-525 puts on the
/// create response.
#[derive(Debug, PartialEq, Eq)]
struct ConfigEntry {
    name: String,
    value: Option<String>,
    source: i8,
    is_sensitive: bool,
    read_only: bool,
}

fn config_entry(
    name: &str,
    value: Option<&str>,
    source: i8,
    is_sensitive: bool,
    read_only: bool,
) -> ConfigEntry {
    ConfigEntry {
        name: name.to_owned(),
        value: value.map(str::to_owned),
        source,
        is_sensitive,
        read_only,
    }
}

macro_rules! config_entries {
    ($entries:expr) => {
        ($entries)
            .iter()
            .map(|entry| {
                config_entry(
                    &entry.name,
                    entry.value.as_deref(),
                    entry.config_source,
                    entry.is_sensitive,
                    entry.read_only,
                )
            })
            .collect::<Vec<_>>()
    };
}

#[derive(Default)]
struct TopicCreation {
    name: String,
    retention: String,
    created: Option<CreateTopicsResponse>,
    described: Option<DescribeConfigsResponse>,
}

#[fixture]
fn topic() -> TopicCreation {
    // Start this scenario with fresh inputs and observations.

    TopicCreation {
        name: String::new(),
        retention: String::new(),
        created: None,
        described: None,
    }
}

#[given("topic {name} requests retention.ms {retention}")]
fn topic_override(topic: &mut TopicCreation, name: String, retention: String) {
    topic.name = name;
    topic.retention = retention;
}

#[when("the admin creates the topic with one partition and one replica")]
async fn create_topic(admin: &AdminCluster, topic: &mut TopicCreation) {
    let request = CreateTopicsRequest {
        topics: vec![
            topics::creatable_topic_with_configs()
                .name(topic.name.clone())
                .configs(vec![CreatableTopicConfig {
                    name: "retention.ms".into(),
                    value: Some(topic.retention.clone()),
                    ..Default::default()
                }])
                .call(),
        ],
        timeout_ms: 5_000,
        ..Default::default()
    };
    topic.created = Some(admin.client().send(request).await.expect("create_topics"));
}

#[then("the topic is created with its effective retention override in the response")]
fn creation_response(topic: &TopicCreation) {
    let created = topic.created.as_ref().expect("the topic was created");
    let row = &created.topics[0];
    assert!(
        row.error_code == 0,
        "create_topics: {:?}",
        row.error_message
    );
    check!(row.topic_config_error_code == 0);
    check!(row.num_partitions == 1);
    check!(row.replication_factor == 1);
    let configs = config_entries!(row.configs.as_ref().expect("KIP-525 configs list"));
    check!(!configs.is_empty(), "the list carries the topic's values");
    check!(
        configs.contains(&ConfigEntry {
            name: "retention.ms".to_owned(),
            value: Some(topic.retention.clone()),
            source: DYNAMIC_TOPIC_CONFIG,
            is_sensitive: false,
            read_only: false,
        }),
        "the request's own override, at DYNAMIC_TOPIC_CONFIG"
    );
}

#[when("the admin describes the created topic configuration")]
async fn describe_created_topic(admin: &AdminCluster, topic: &mut TopicCreation) {
    topic.described = Some(
        admin
            .client()
            .send(DescribeConfigsRequest {
                resources: vec![describe_resource(RESOURCE_TYPE_TOPIC, &topic.name, None)],
                ..Default::default()
            })
            .await
            .expect("describe_configs"),
    );
}

#[then("DescribeConfigs succeeds and returns exactly the effective create configuration")]
fn matching_descriptions(topic: &TopicCreation) {
    let created = topic.created.as_ref().expect("the topic was created");
    let described = topic
        .described
        .as_ref()
        .expect("the topic configuration was described");
    let result = &described.results[0];
    assert!(
        result.error_code == 0,
        "describe_configs: {:?}",
        result.error_message
    );
    let from_create = config_entries!(
        created.topics[0]
            .configs
            .as_ref()
            .expect("KIP-525 configs list")
    );
    let from_describe = config_entries!(&result.configs);
    assert!(from_create == from_describe);
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_create_topics.feature",
    name = "create topics returns the configs describe configs reports"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_topics_returns_the_configs_describe_configs_reports(
    admin: AdminCluster,
    topic: TopicCreation,
) {
    admin.shutdown().await;
}
