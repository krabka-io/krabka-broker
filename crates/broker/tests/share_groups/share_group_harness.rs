//! Broker boot, client connection and request-building helpers shared by the
//! KIP-932 share-group scenarios in this suite.
//!
//! Every scenario starts a single-node broker whose share-state topic has one
//! partition, connects a typed client to it, and then drives
//! `ShareGroupHeartbeat` and `ShareGroupDescribe` over the wire. Those steps
//! live here so each scenario module holds only its own assertions.

use std::sync::Arc;

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig};
use krabka_client_core::Client;
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    share_group_describe_request::ShareGroupDescribeRequest,
    share_group_heartbeat_request::ShareGroupHeartbeatRequest,
};

const SHARE_STATE_PARTITIONS: i32 = 1;

pub fn broker_config(log_dir: std::path::PathBuf) -> BrokerConfig {
    let mut config = BrokerConfig::for_tests(log_dir);
    config.share_coordinator.state_topic_num_partitions = SHARE_STATE_PARTITIONS;
    config
}

pub async fn boot() -> (krabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
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

pub fn heartbeat(group: &str, member_id: &str, epoch: i32) -> ShareGroupHeartbeatRequest {
    ShareGroupHeartbeatRequest {
        group_id: group.into(),
        member_id: member_id.into(),
        member_epoch: epoch,
        ..Default::default()
    }
}

pub fn total_assigned(
    resp: &krabka_protocol::owned::share_group_heartbeat_response::ShareGroupHeartbeatResponse,
) -> usize {
    resp.assignment.as_ref().map_or(0, |a| {
        a.topic_partitions.iter().map(|t| t.partitions.len()).sum()
    })
}

pub async fn describe(
    client: &Client,
    group: &str,
) -> krabka_protocol::owned::share_group_describe_response::ShareGroupDescribeResponse {
    client
        .send(ShareGroupDescribeRequest {
            group_ids: vec![group.into()],
            include_authorized_operations: false,
            ..Default::default()
        })
        .await
        .unwrap()
}

/// Resolves the id of a created topic from this broker's metadata image.
pub fn topic_id(broker: &krabka_broker::BrokerHandle, topic: &str) -> uuid::Uuid {
    let image = broker.controller_image_for_test();
    image
        .topic(topic)
        .map(|t| *t.topic_id.as_bytes())
        .map(uuid::Uuid::from_bytes)
        .expect("topic present in image")
}
