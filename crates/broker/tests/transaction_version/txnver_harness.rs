//! Shared fixtures for the `transaction.version` suite: broker boot, an admin
//! client, topic creation, and the `UpdateFeatures` downgrade that moves the
//! cluster onto a lower `transaction.version` level.
//!
//! `downgrade_transaction_version` is what every case in this binary is built
//! on, because the in-process broker self-bootstraps at `TV_2` and each level
//! below it has to be reached through a live feature downgrade.

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_client_core::Client;
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
};
use tempfile::TempDir;

// Kafka error codes asserted below.
pub const NONE: i16 = 0;
pub const TRANSACTION_ABORTABLE: i16 = 120;

pub async fn boot_single() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

pub async fn admin_client(bootstrap: &str) -> Client {
    Client::builder()
        .bootstrap(bootstrap)
        .client_id("krabka-txnv-test")
        .build()
        .await
        .unwrap()
}

pub async fn create_topic(client: &Client, name: &str, partitions: i32) {
    let cr = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        cr.topics[0].error_code == 0 || cr.topics[0].error_code == 36,
        "create_topic {name}: error_code={}",
        cr.topics[0].error_code
    );
}

/// Downgrade the finalized `transaction.version` to `level` with a
/// `SAFE_DOWNGRADE` (`upgrade_type = 2`) `UpdateFeatures` request. Level 1
/// finalizes the Flexible level. Level 0 tombstones the feature (→ absent →
/// Classic). `resolve_txn_version` reads the live image per request, so a new
/// transaction started after this call returns picks up the downgraded level.
pub async fn downgrade_transaction_version(client: &Client, level: i16) {
    let resp = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: "transaction.version".into(),
                max_version_level: level,
                upgrade_type: 2, // SAFE_DOWNGRADE
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures");
    assert!(resp.error_code == 0, "UpdateFeatures top-level: {resp:?}");
    if let Some(row) = resp
        .results
        .iter()
        .find(|r| r.feature == "transaction.version")
    {
        assert!(
            row.error_code == 0,
            "transaction.version downgrade to {level} rejected: {resp:?}"
        );
    }
}
