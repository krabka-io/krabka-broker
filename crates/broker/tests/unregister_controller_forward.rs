//! Kafka trunk's `KafkaApis` answers `UnregisterController` (94, KIP-1312) on
//! a broker listener with `forwardToController`. A krabka node that is not the
//! active controller forwards it the same way, in a KIP-590 `Envelope`, and
//! relays the controller's answer unchanged.

mod support;

use assert2::check;
use krabka_metadata::{
    ControllerRegistrationRecord, MetadataRecord, NodeId as MetadataNodeId,
    metadata_version::CONTROLLER_UNREGISTRATION_MIN_LEVEL,
};
use krabka_protocol::owned::{
    unregister_controller_request::UnregisterControllerRequest,
    unregister_controller_response::UnregisterControllerResponse,
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
};

use crate::support::start_n_node_with_retry;

/// Kafka trunk's `CONTROLLER_ID_NOT_REGISTERED`.
const CONTROLLER_ID_NOT_REGISTERED: i16 = 136;

/// The cluster is first moved to trunk's 4.4-IV2 (33), the level the RPC
/// needs, through `UpdateFeatures` on the follower, which forwards it too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_forwards_unregister_controller_to_the_active_controller() {
    let cluster = start_n_node_with_retry(2).await;
    let leader = cluster[0]
        .0
        .controller_leader_id()
        .expect("a 2-node cluster has an elected leader");
    let (leader_handle, _, _) = cluster
        .iter()
        .find(|(_, cfg, _)| i64::from(cfg.broker_id) == i64::try_from(leader.0).unwrap())
        .expect("the leader");
    leader_handle
        .submit_metadata_record_for_test(MetadataRecord::V1ControllerRegistration(
            ControllerRegistrationRecord {
                node_id: MetadataNodeId(7),
                incarnation_id: uuid::Uuid::from_u128(7),
                zk_migration_ready: false,
                endpoints: Vec::new(),
                features: std::collections::BTreeMap::from([(
                    "metadata.version".to_owned(),
                    (7, krabka_metadata::metadata_version::METADATA_VERSION_MAX),
                )]),
            },
        ))
        .await
        .expect("seed the registration");

    let (_, follower_cfg, _) = cluster
        .iter()
        .find(|(_, cfg, _)| i64::from(cfg.broker_id) != i64::try_from(leader.0).unwrap())
        .expect("the follower");
    let client = krabka_client_core::Client::builder()
        .bootstrap(format!("127.0.0.1:{}", follower_cfg.listen_addr.port()))
        .client_id("kafka-cluster")
        .build()
        .await
        .expect("client build");
    let upgraded = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: "metadata.version".into(),
                max_version_level: CONTROLLER_UNREGISTRATION_MIN_LEVEL,
                upgrade_type: 1,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures through the follower");
    check!(upgraded.error_code == 0, "{upgraded:?}");

    let mut answers = Vec::new();
    for controller_id in [7, 7] {
        answers.push(
            client
                .send(UnregisterControllerRequest {
                    controller_id,
                    ..Default::default()
                })
                .await
                .expect("UnregisterController through the follower"),
        );
    }
    client.close();

    check!(
        answers
            == vec![
                UnregisterControllerResponse {
                    error_message: Some(String::new()),
                    ..Default::default()
                },
                UnregisterControllerResponse {
                    error_code: CONTROLLER_ID_NOT_REGISTERED,
                    error_message: Some("Controller ID 7 is not currently registered.".into()),
                    ..Default::default()
                },
            ]
    );
    check!(
        leader_handle
            .controller_image_for_test()
            .controller(MetadataNodeId(7))
            .is_none()
    );
    for (handle, _, _) in cluster {
        handle.shutdown().await;
    }
}
