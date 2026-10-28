//! KIP-848 / KIP-584: the next-gen consumer-group protocol
//! (`ConsumerGroupHeartbeat`, `api_key` 68) is gated on a finalized
//! `group.version >= 1`. A freshly-bootstrapped broker finalizes
//! `group.version=1`, which enables next-gen. A downgrade to 0, which is a
//! KIP-584 tombstone, disables next-gen. The broker then rejects heartbeats
//! with `UNSUPPORTED_VERSION`, so the client falls back to the classic
//! protocol.
#![cfg(test)]

use assert2::assert;
mod support;

use {
    crate::support::consumer_groups::joining_consumer,
    krabka_protocol::owned::{
        consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
        consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
        update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
        update_features_response::UpdateFeaturesResponse,
    },
};

const UNSUPPORTED_VERSION: i16 = 35;

fn heartbeat(group: &str) -> ConsumerGroupHeartbeatRequest {
    // A well-formed first (join) heartbeat: a client-generated member_id, member_epoch 0,
    // a rebalance timeout, and a subscription. Mirrors the field set used in
    // consumer_group_next_gen.rs. The feature gate is the first check after
    // decode, so this reaches it regardless of whether the topic exists.
    ConsumerGroupHeartbeatRequest {
        subscribed_topic_names: Some(vec!["t".into()]),
        ..joining_consumer(group, uuid::Uuid::new_v4().to_string(), 30_000)
    }
}

struct GroupFeatureGate {
    broker: Option<support::InProcess>,
    downgrade: Option<UpdateFeaturesResponse>,
    heartbeat: Option<ConsumerGroupHeartbeatResponse>,
}
#[rstest::fixture]
fn gate() -> GroupFeatureGate {
    // Start this scenario with fresh inputs and observations.

    GroupFeatureGate {
        broker: None,
        downgrade: None,
        heartbeat: None,
    }
}
#[rstest_bdd_macros::given("a freshly bootstrapped broker finalizing group version 1")]
async fn default_group_feature(gate: &mut GroupFeatureGate) {
    gate.broker = Some(support::start().await);
}
#[rstest_bdd_macros::when("the admin safely downgrades group version to 0")]
async fn disable_group_feature(gate: &mut GroupFeatureGate) {
    gate.downgrade = Some(
        gate.broker
            .as_ref()
            .expect("broker started")
            .client
            .send(UpdateFeaturesRequest {
                feature_updates: vec![FeatureUpdateKey {
                    feature: "group.version".into(),
                    max_version_level: 0,
                    upgrade_type: 2,
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("UpdateFeatures downgrade"),
    );
}
#[rstest_bdd_macros::then("the group version downgrade succeeds")]
fn group_feature_disabled(gate: &GroupFeatureGate) {
    let downgrade = gate.downgrade.as_ref().expect("downgrade requested");
    assert!(
        downgrade.error_code == 0,
        "downgrade to 0 should succeed: {downgrade:?}"
    );
}
#[rstest_bdd_macros::when("a next generation member sends a joining heartbeat to group {group}")]
async fn next_generation_heartbeat(gate: &mut GroupFeatureGate, group: String) {
    gate.heartbeat = Some(
        gate.broker
            .as_ref()
            .expect("broker started")
            .client
            .send(heartbeat(&group))
            .await
            .expect("heartbeat"),
    );
}
#[rstest_bdd_macros::then("the heartbeat passes the group version feature gate")]
fn next_generation_gate_passed(gate: &GroupFeatureGate) {
    let response = gate.heartbeat.as_ref().expect("heartbeat requested");
    assert!(
        response.error_code != UNSUPPORTED_VERSION,
        "gate must pass at gv=1: {response:?}"
    );
}
#[rstest_bdd_macros::then("the heartbeat is rejected with UNSUPPORTED_VERSION error 35")]
fn next_generation_gate_refused(gate: &GroupFeatureGate) {
    let response = gate.heartbeat.as_ref().expect("heartbeat requested");
    assert!(
        response.error_code == UNSUPPORTED_VERSION,
        "next-gen must be rejected once group.version is disabled: {response:?}"
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/group_version.feature",
    name = "next gen accepted when group version finalized"
)]
#[tokio::test]
async fn next_gen_accepted_when_group_version_finalized(gate: GroupFeatureGate) {
    gate.broker.expect("broker started").broker.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/group_version.feature",
    name = "next gen rejected when group version disabled"
)]
#[tokio::test]
async fn next_gen_rejected_when_group_version_disabled(gate: GroupFeatureGate) {
    gate.broker.expect("broker started").broker.shutdown().await;
}
