//! Ordered tiered-topic configuration shared by the tiered-storage suites.
use krabka_protocol::owned::create_topics_request::CreatableTopicConfig;

pub fn tiered_configs(segment_bytes: Option<&str>) -> Vec<CreatableTopicConfig> {
    crate::support::topics::topic_configs(
        std::iter::once(("remote.storage.enable", "true"))
            .chain(segment_bytes.map(|value| ("internal.segment.bytes", value)))
            .chain([
                ("local.retention.bytes", "1"),
                ("retention.bytes", "-1"),
                ("retention.ms", "-1"),
            ]),
    )
}

/// Create one tiered topic with caller-ordered configs and the suite's wire timeout.
pub async fn create_configured_topic(
    client: &krabka_client_core::Client,
    topic: &str,
    configs: Vec<CreatableTopicConfig>,
) {
    let response = client
        .send(crate::support::topics::create_topic_request(
            krabka_protocol::owned::create_topics_request::CreatableTopic {
                configs,
                ..crate::support::topics::creatable_topic(topic, 1, 1)
            },
            5_000,
        ))
        .await
        .expect("CreateTopics");
    assert2::assert!(
        response.topics[0].error_code == 0,
        "CreateTopics failed: {:?}",
        response.topics[0].error_message
    );
}

/// Pin one tiered partition to the caller's ordered replica list.
pub async fn create_assigned_topic(
    client: &krabka_client_core::Client,
    topic: &str,
    replicas: &[i32],
    segment_bytes: Option<&str>,
) {
    let response = client
        .send(crate::support::topics::create_topic_request(
            krabka_protocol::owned::create_topics_request::CreatableTopic {
                configs: tiered_configs(segment_bytes),
                ..crate::support::topic_on(topic, &[replicas])
            },
            10_000,
        ))
        .await
        .expect("CreateTopics");
    assert2::assert!(
        response.topics[0].error_code == 0,
        "CreateTopics failed: {response:?}"
    );
}

/// Poll each replica's local configuration with the suite's 30s/200ms bound.
pub async fn await_tiered_replicas(
    brokers: &[&krabka_broker::BrokerHandle],
    topic: &str,
    segment_size: Option<krabka_units::ByteSize>,
    failure: &str,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let ready = |broker: &&krabka_broker::BrokerHandle| {
            broker
                .partition_log_config_for_test(topic, 0)
                .is_some_and(|config| {
                    config.remote_storage_enable
                        && segment_size.is_none_or(|size| config.segment_size == size)
                        && config.local_retention_size == Some(krabka_units::bytes(1))
                })
        };
        if brokers.iter().all(ready) {
            return;
        }
        assert2::assert!(std::time::Instant::now() <= deadline, "{failure}");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
