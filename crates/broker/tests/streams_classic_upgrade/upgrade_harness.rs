//! Error codes, broker boot and wire helpers shared by the classic-to-streams
//! upgrade scenarios in this suite.
//!
//! Both scenarios boot a single-node broker, finalize `streams.version`, create
//! the source topic and then read back topic ids or committed offsets, so those
//! steps live here rather than being repeated per scenario module.

use std::sync::Arc;

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        offset_fetch_request::{
            OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopics,
        },
        update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
    },
    primitives::uuid::Uuid as WireUuid,
};

// ── error codes ──────────────────────────────────────────────────────────────
pub const ERR_NONE: i16 = 0;
pub const ERR_MEMBER_ID_REQUIRED: i16 = 79;
pub const ERR_GROUP_ID_NOT_FOUND: i16 = 69;

// ── boot / connect helpers ────────────────────────────────────────────────────

pub async fn boot() -> (krabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

pub async fn connect(bootstrap: &str) -> Arc<Client> {
    Arc::new(
        Client::builder()
            .bootstrap(bootstrap)
            .client_id("c1")
            .build()
            .await
            .unwrap(),
    )
}

pub async fn assert_committed_offset(client: &Client, topic_id: WireUuid, expected: i64) {
    let response = client
        .send(OffsetFetchRequest {
            groups: vec![OffsetFetchRequestGroup {
                group_id: "g".into(),
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: "in".into(),
                    topic_id,
                    partition_indexes: vec![0],
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("OffsetFetch");
    let group = response
        .groups
        .iter()
        .find(|g| g.group_id == "g")
        .expect("group g");
    let topic = group
        .topics
        .iter()
        .find(|t| t.topic_id == topic_id)
        .expect("topic in");
    let partition = topic.partitions.first().expect("partition 0");
    assert!(
        partition.error_code == ERR_NONE,
        "OffsetFetch failed: {partition:?}"
    );
    assert!(
        partition.committed_offset == expected,
        "committed offset was not preserved"
    );
}

pub async fn create_topic(client: &Client, topic: &str, partitions: i32) {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "topic create failed: {resp:?}"
    );
}

/// Finalize `streams.version` to level 1 so the heartbeat/describe handlers
/// stop returning `UNSUPPORTED_VERSION`. `upgrade_type: 1` is UPGRADE.
pub async fn finalize_streams_version(client: &Client) {
    let resp = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: "streams.version".into(),
                max_version_level: 1,
                upgrade_type: 1,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures");
    assert!(
        resp.error_code == 0,
        "streams.version finalize failed: {resp:?}"
    );
}

pub async fn topic_id_for(client: &Client, name: &str) -> WireUuid {
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}
