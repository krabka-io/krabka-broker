//! KIP-211 `offsets.retention.minutes`, end to end over the wire.
//!
//! One case reads the two retention knobs back through `DescribeConfigs`, the
//! way `kafka-configs --entity-type brokers --describe` does. The other lets
//! the broker's own background sweep run against a one-millisecond retention
//! and watches a dead group disappear from `ListGroups`.

use std::{sync::Arc, time::Duration};

use assert2::{assert, check};
use krabka_broker::{Broker, BrokerConfig, codes};
use krabka_client_core::Client;
use krabka_protocol::owned::{
    describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
    describe_configs_response::DescribeConfigsResourceResult,
    list_groups_request::ListGroupsRequest,
    offset_commit_request::{
        OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    },
};
use krabka_units::millis;

/// `ConfigResource.Type.BROKER`.
const RESOURCE_TYPE_BROKER: i8 = 4;
/// `ConfigEntry.ConfigSource.DEFAULT_CONFIG`. Verified against
/// `apache/kafka:4.3.1`: `kafka-configs --describe --all` reports both keys
/// with `DEFAULT_CONFIG` on a broker whose properties do not set them.
const CONFIG_SOURCE_DEFAULT: i8 = 5;
const OFFSETS_RETENTION_MINUTES: &str = "offsets.retention.minutes";
const OFFSETS_RETENTION_CHECK_INTERVAL_MS: &str = "offsets.retention.check.interval.ms";

async fn client_for(broker: &krabka_broker::BrokerHandle) -> Arc<Client> {
    Arc::new(
        Client::builder()
            .bootstrap(broker.listen_addr().to_string().as_str())
            .client_id("retention-test")
            .build()
            .await
            .expect("client"),
    )
}

/// Both knobs come back on the broker resource, read-only and static, at
/// Kafka's own defaults.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_configs_reports_the_retention_knobs() {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = client_for(&broker).await;

    let described = client
        .send(DescribeConfigsRequest {
            resources: vec![DescribeConfigsResource {
                resource_type: RESOURCE_TYPE_BROKER,
                resource_name: "1".to_string(),
                configuration_keys: None,
                ..Default::default()
            }],
            include_synonyms: false,
            include_documentation: false,
            ..Default::default()
        })
        .await
        .expect("DescribeConfigs");

    let result = &described.results[0];
    check!(result.error_code == codes::NONE);
    let entry = |name: &str| result.configs.iter().find(|c| c.name == name).cloned();
    check!(
        entry(OFFSETS_RETENTION_MINUTES)
            == Some(DescribeConfigsResourceResult {
                name: OFFSETS_RETENTION_MINUTES.into(),
                // Kafka's default: 10080 minutes, seven days.
                value: Some("10080".into()),
                read_only: true,
                config_source: CONFIG_SOURCE_DEFAULT,
                ..Default::default()
            })
    );
    check!(
        entry(OFFSETS_RETENTION_CHECK_INTERVAL_MS)
            == Some(DescribeConfigsResourceResult {
                name: OFFSETS_RETENTION_CHECK_INTERVAL_MS.into(),
                // Kafka's default: ten minutes.
                value: Some("600000".into()),
                read_only: true,
                config_source: CONFIG_SOURCE_DEFAULT,
                ..Default::default()
            })
    );

    broker.shutdown().await;
}

/// A group that only ever committed offsets — the simple consumer that never
/// joins — is empty from birth, so the broker's own sweep reaps it and drops
/// it from `ListGroups` without an operator running `DeleteGroups`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_broker_sweep_reaps_a_dead_group_on_its_own() {
    const GROUP: &str = "leaked-by-ci";

    let dir = tempfile::TempDir::new().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.offsets_retention = millis(1);
    config.offsets_retention_check_interval = millis(25);
    let broker = Broker::start(config).await.unwrap();
    let client = client_for(&broker).await;

    let committed = client
        .send(OffsetCommitRequest {
            group_id: GROUP.to_string(),
            generation_id_or_member_epoch: -1,
            member_id: String::new(),
            topics: vec![OffsetCommitRequestTopic {
                name: "orders".to_string(),
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: 5,
                    committed_leader_epoch: -1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("OffsetCommit");
    assert!(committed.topics[0].partitions[0].error_code == codes::NONE);

    let listed = |client: Arc<Client>| async move {
        client
            .send(ListGroupsRequest::default())
            .await
            .expect("ListGroups")
            .groups
            .iter()
            .any(|group| group.group_id == GROUP)
    };
    assert!(listed(Arc::clone(&client)).await, "the group starts listed");

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while listed(Arc::clone(&client)).await {
        assert!(
            std::time::Instant::now() < deadline,
            "the sweep never reaped the dead group"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    broker.shutdown().await;
}
