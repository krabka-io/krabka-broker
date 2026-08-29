//! Cluster boot, topic setup, and offset helpers shared by every test in this
//! suite.
//!
//! The module holds the one-broker in-process cluster and its client, the
//! `CreateTopics` and `UpdateFeatures` calls that a streams group needs before
//! it can form, the `Metadata` lookup that resolves a topic id, the
//! simple-consumer `OffsetCommit` that seeds the offset a downgrade must
//! preserve, and the `BrokerConfig` that restarts a broker on an existing log
//! directory.

use std::sync::Arc;

use assert2::assert;
use krabka_broker::{BootstrapMode, Broker, BrokerConfig};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        offset_commit_request::{
            OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
    },
    primitives::uuid::Uuid as WireUuid,
};

use crate::ERR_NONE;

pub(crate) async fn boot() -> (krabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

pub(crate) async fn connect(bootstrap: &str) -> Arc<Client> {
    Arc::new(
        Client::builder()
            .bootstrap(bootstrap)
            .client_id("c1")
            .build()
            .await
            .unwrap(),
    )
}

pub(crate) async fn create_topic(client: &Client, topic: &str, partitions: i32) {
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

/// Finalizes `streams.version` at level 1, so that the heartbeat and describe
/// handlers stop returning `UNSUPPORTED_VERSION`. `upgrade_type: 1` is
/// UPGRADE.
pub(crate) async fn finalize_streams_version(client: &Client) {
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

pub(crate) async fn topic_id_for(client: &Client, name: &str) -> WireUuid {
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

/// Commits an offset through the "simple consumer" path, with an empty
/// `member_id`, which skips classic-member validation. This is safe for a
/// streams group, because the offset-home actor accepts a commit from a client
/// that has not joined, at generation -1.
pub(crate) async fn commit_offset_simple(
    client: &Client,
    group_id: &str,
    topic: &str,
    topic_id: WireUuid,
    partition: i32,
    offset: i64,
) {
    let cr = client
        .send(OffsetCommitRequest {
            group_id: group_id.into(),
            generation_id_or_member_epoch: -1,
            member_id: String::new(),
            topics: vec![OffsetCommitRequestTopic {
                name: topic.into(),
                topic_id,
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: partition,
                    committed_offset: offset,
                    committed_leader_epoch: 0,
                    committed_metadata: Some(String::new()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("OffsetCommit");
    assert!(
        cr.topics[0].partitions[0].error_code == ERR_NONE,
        "OffsetCommit (simple consumer) failed: {cr:?}"
    );
}

pub(crate) fn rejoin_config(log_dir: std::path::PathBuf) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir);
    cfg.bootstrap_mode = BootstrapMode::Rejoin;
    cfg
}
