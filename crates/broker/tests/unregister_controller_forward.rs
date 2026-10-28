//! Kafka trunk's `KafkaApis` answers `UnregisterController` (94, KIP-1312) on
//! a broker listener with `forwardToController`. A krabka node that is not the
//! active controller forwards it the same way, in a KIP-590 `Envelope`, and
//! relays the controller's answer unchanged.
#![cfg(test)]

mod support;

use {
    crate::support::{
        admin,
        client::connect_owned,
        configs::{controller_registration, feature_update},
        shutdown_cluster, start_n_node_with,
    },
    assert2::check,
    krabka_broker::{BrokerConfig, BrokerHandle, api_catalog::UnstableApiVersions},
    krabka_client_core::Client,
    krabka_metadata::{
        NodeId as MetadataNodeId, metadata_version::CONTROLLER_UNREGISTRATION_MIN_LEVEL,
    },
    krabka_protocol::owned::{
        unregister_controller_request::UnregisterControllerRequest,
        unregister_controller_response::UnregisterControllerResponse,
        update_features_request::UpdateFeaturesRequest,
        update_features_response::UpdateFeaturesResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

#[derive(Default)]
struct ForwardedUnregistration {
    cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>,
    leader: Option<krabka_broker::NodeId>,
    follower: Option<Client>,
    upgraded: Option<UpdateFeaturesResponse>,
    answers: Vec<UnregisterControllerResponse>,
}

#[fixture]
fn forwarded_unregistration() -> ForwardedUnregistration {
    // Retain leader state while all admin requests enter through a follower.
    ForwardedUnregistration::default()
}

#[given("two trunk-version controller voters hold controller 7's registration")]
async fn controller_registration_fixture(forwarded_unregistration: &mut ForwardedUnregistration) {
    forwarded_unregistration.cluster = start_n_node_with(2, |_, config| {
        config.features.unstable_api_versions = UnstableApiVersions::Enabled;
        config.features.unstable_feature_versions = krabka_raft::UnstableFeatureVersions::Enabled;
    })
    .await
    .expect("start cluster");
    let leader = forwarded_unregistration.cluster[0]
        .0
        .controller_leader_id()
        .expect("elected controller");
    let (leader_handle, _, _) = forwarded_unregistration
        .cluster
        .iter()
        .find(|(_, config, _)| i64::from(config.broker_id) == i64::try_from(leader.0).unwrap())
        .expect("leader");
    leader_handle
        .submit_metadata_record_for_test(controller_registration(MetadataNodeId(7)))
        .await
        .expect("seed registration");
    let (_, follower_config, _) = forwarded_unregistration
        .cluster
        .iter()
        .find(|(_, config, _)| i64::from(config.broker_id) != i64::try_from(leader.0).unwrap())
        .expect("follower");
    forwarded_unregistration.follower = Some(
        connect_owned(
            format!("127.0.0.1:{}", follower_config.listen_addr.port()),
            "kafka-cluster",
            "client build",
        )
        .await,
    );
    forwarded_unregistration.leader = Some(leader);
}

#[when("the follower receives the metadata-version upgrade required by UnregisterController")]
async fn upgrade_through_follower(forwarded_unregistration: &mut ForwardedUnregistration) {
    forwarded_unregistration.upgraded = Some(
        forwarded_unregistration
            .follower
            .as_ref()
            .expect("follower client")
            .send(UpdateFeaturesRequest {
                feature_updates: vec![feature_update(
                    "metadata.version",
                    CONTROLLER_UNREGISTRATION_MIN_LEVEL,
                    1,
                )],
                ..Default::default()
            })
            .await
            .expect("UpdateFeatures through follower"),
    );
}

#[then("the forwarded metadata-version upgrade succeeds")]
fn forwarded_upgrade(forwarded_unregistration: &ForwardedUnregistration) {
    let response = forwarded_unregistration
        .upgraded
        .as_ref()
        .expect("upgrade answered");
    check!(response.error_code == 0, "{response:?}");
}

#[when("the follower receives two UnregisterController requests for controller 7")]
async fn unregister_through_follower(forwarded_unregistration: &mut ForwardedUnregistration) {
    for controller_id in [7, 7] {
        forwarded_unregistration.answers.push(
            forwarded_unregistration
                .follower
                .as_ref()
                .expect("follower client")
                .send(UnregisterControllerRequest {
                    controller_id,
                    ..Default::default()
                })
                .await
                .expect("UnregisterController through follower"),
        );
    }
    forwarded_unregistration
        .follower
        .take()
        .expect("follower client")
        .close();
}

#[then(
    "the follower relays the successful removal and repeated missing-registration answers unchanged"
)]
fn forwarded_controller_answers(forwarded_unregistration: &ForwardedUnregistration) {
    check!(
        forwarded_unregistration.answers
            == admin::unregister_controller_answers()
                .controller_id(7)
                .call()
    );
}

#[then("the active controller image no longer contains controller 7")]
fn forwarded_controller_removed(forwarded_unregistration: &ForwardedUnregistration) {
    let leader = forwarded_unregistration.leader.expect("active controller");
    let (broker, _, _) = forwarded_unregistration
        .cluster
        .iter()
        .find(|(broker, _, _)| broker.node_id() == leader.0)
        .expect("active controller member");
    check!(
        broker
            .controller_image_for_test()
            .controller(MetadataNodeId(7))
            .is_none()
    );
}

#[scenario(
    path = "tests/features/migrated/tests/unregister_controller_forward.feature",
    name = "a follower forwards unregister controller to the active controller"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_forwards_unregister_controller_to_the_active_controller(
    forwarded_unregistration: ForwardedUnregistration,
) {
    shutdown_cluster(take(&mut forwarded_unregistration.cluster)).await;
    drop(forwarded_unregistration);
}

use std::mem::take;
