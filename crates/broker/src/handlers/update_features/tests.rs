//! End-to-end tests for the `UpdateFeatures` handler, which drive a live
//! broker and so are kept out of the module root.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    super::*,
    crate::{
        authorizer::AllowAllAuthorizer,
        broker::Broker,
        features::{LATEST_PRODUCTION_METADATA_VERSION, METADATA_VERSION, METADATA_VERSION_MAX},
        handlers::update_features::test_support::{
            apply_request, call, call_with, metadata_update, named_update, start_broker,
            validate_only, wait_for_finalized_feature,
        },
        test_support::{DenyAll, start_broker_no_audit_with},
    },
    assert2::assert,
    krabka_protocol::owned::update_features_response::UpdatableFeatureResult,
    std::{sync::Arc, time::Duration},
    tokio::time,
};

const PREFIX: &str =
    "The update failed for all features since the following feature had an error: ";

fn refused(message: &str) -> UpdateFeaturesResponse {
    krabka_macros::unthrottled_wire!(UpdateFeaturesResponse {
        error_code: codes::INVALID_UPDATE_VERSION,
        error_message: Some(format!("{PREFIX}{message}")),
        results: vec![],
    })
}

fn accepted(features: &[&str]) -> UpdateFeaturesResponse {
    krabka_macros::unthrottled_wire!(UpdateFeaturesResponse {
        error_code: codes::NONE,
        error_message: None,
        results: features
            .iter()
            .map(
                |&feature| krabka_macros::tagged_wire!(UpdatableFeatureResult {
                    feature: feature.into(),
                    error_code: codes::NONE,
                    error_message: Some("NONE".into()),
                })
            )
            .collect(),
    })
}

#[tokio::test]
async fn handle_denies_cluster_alter_with_top_level_error() {
    let req = validate_only(vec![metadata_update(METADATA_VERSION_MAX, 1)]);

    let (resp, broker_handle, _dir) = Box::pin(call_with(Arc::new(DenyAll), req)).await;

    let expected = krabka_macros::unthrottled_wire!(UpdateFeaturesResponse {
        error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
        error_message: Some("Cluster authorization failed.".to_owned()),
        results: vec![],
    });
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[derive(Default)]
struct UpdateFeatureScenario {
    started: Option<HandlerStartedBroker>,
    request: Option<UpdateFeaturesRequest>,
    version: i16,
    response: Option<UpdateFeaturesResponse>,
}
#[rstest::fixture]
fn feature_update() -> UpdateFeatureScenario {
    let started = None;
    UpdateFeatureScenario {
        started,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given("a broker starts with finalized group.version 1")]
async fn default_feature_broker(feature_update: &mut UpdateFeatureScenario) {
    feature_update.started = Some(start_broker(Arc::new(AllowAllAuthorizer)).await);
}
#[rstest_bdd_macros::given(
    "an UpdateFeatures version {version} request has updates {updates} and validate-only {validate}"
)]
fn requested_feature_updates(
    feature_update: &mut UpdateFeatureScenario,
    version: i16,
    updates: String,
    validate: bool,
) {
    feature_update.version = version;
    let feature_updates = if updates == "none" {
        Vec::new()
    } else {
        updates
            .split(';')
            .map(|update| {
                let mut columns = update.split(':');
                let name = columns.next().unwrap();
                let level = columns.next().unwrap().parse().unwrap();
                let mode = columns.next().unwrap().parse().unwrap();
                assert!(columns.next().is_none());
                named_update(name, level, mode)
            })
            .collect()
    };
    feature_update.request = Some(UpdateFeaturesRequest {
        feature_updates,
        validate_only: validate,
        ..Default::default()
    });
}
#[rstest_bdd_macros::when("the broker applies the feature update request")]
async fn apply_feature_update(feature_update: &mut UpdateFeatureScenario) {
    feature_update.response = Some(
        Box::pin(call(
            &feature_update.started.as_ref().unwrap().broker,
            feature_update.request.take().unwrap(),
            feature_update.version,
        ))
        .await,
    );
}
#[rstest_bdd_macros::then("the feature response is {outcome} with successful feature rows {rows}")]
fn check_feature_update_response(
    feature_update: &UpdateFeatureScenario,
    outcome: String,
    rows: String,
) {
    let expected = match outcome.as_str() {
        "accepted" => {
            let names: Vec<_> = if rows == "none" {
                Vec::new()
            } else {
                rows.split(',').collect()
            };
            accepted(&names)
        }
        "atomically refused" => refused(
            "Invalid update version 9 for feature share.version. Local controller 1 only supports versions 0-1",
        ),
        _ => panic!("unknown feature outcome: {outcome}"),
    };
    assert!(feature_update.response.as_ref().unwrap() == &expected);
}
#[rstest_bdd_macros::then("group.version is {level} after the request")]
async fn check_feature_update_state(feature_update: &mut UpdateFeatureScenario, level: String) {
    let expected = if level == "disabled" {
        None
    } else {
        Some(level.parse::<i16>().unwrap())
    };
    let started = feature_update.started.take().unwrap();
    let broker = started.broker.broker_arc_for_test();
    if expected.is_none() {
        wait_for_group_version_off(&broker).await;
    }
    assert!(
        broker
            .controller
            .current_image()
            .finalized_feature("group.version")
            == expected
    );
    started.broker.shutdown().await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_update_feature_atomicity/update.feature",
    name = "Whole requests update features atomically and respect protocol result shape: version=1, updates=group.version:0:2;share.version:9:1, validate=false (1)"
)]
#[tokio::test]
async fn requests_apply_atomically_with_kafkas_response_shape(
    feature_update: UpdateFeatureScenario,
) {
    let _ = (feature_update,);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_update_feature_atomicity/update.feature",
    name = "Whole requests update features atomically and respect protocol result shape: version=2, updates=group.version:0:2;share.version:9:1, validate=false (2)"
)]
#[tokio::test]
async fn requests_apply_atomically_with_kafkas_response_shape_case_2(
    feature_update: UpdateFeatureScenario,
) {
    let _ = (feature_update,);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_update_feature_atomicity/update.feature",
    name = "Whole requests update features atomically and respect protocol result shape: version=0, updates=group.version:1:1, validate=false (3)"
)]
#[tokio::test]
async fn requests_apply_atomically_with_kafkas_response_shape_case_3(
    feature_update: UpdateFeatureScenario,
) {
    let _ = (feature_update,);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_update_feature_atomicity/update.feature",
    name = "Whole requests update features atomically and respect protocol result shape: version=1, updates=group.version:0:2, validate=false (4)"
)]
#[tokio::test]
async fn requests_apply_atomically_with_kafkas_response_shape_case_4(
    feature_update: UpdateFeatureScenario,
) {
    let _ = (feature_update,);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_update_feature_atomicity/update.feature",
    name = "Whole requests update features atomically and respect protocol result shape: version=2, updates=group.version:0:2, validate=false (5)"
)]
#[tokio::test]
async fn requests_apply_atomically_with_kafkas_response_shape_case_5(
    feature_update: UpdateFeatureScenario,
) {
    let _ = (feature_update,);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_update_feature_atomicity/update.feature",
    name = "Whole requests update features atomically and respect protocol result shape: version=2, updates=group.version:0:2, validate=true (6)"
)]
#[tokio::test]
async fn requests_apply_atomically_with_kafkas_response_shape_case_6(
    feature_update: UpdateFeatureScenario,
) {
    let _ = (feature_update,);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_update_feature_atomicity/update.feature",
    name = "Whole requests update features atomically and respect protocol result shape: version=1, updates=none, validate=false (7)"
)]
#[tokio::test]
async fn requests_apply_atomically_with_kafkas_response_shape_case_7(
    feature_update: UpdateFeatureScenario,
) {
    let _ = (feature_update,);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_update_feature_atomicity/update.feature",
    name = "Whole requests update features atomically and respect protocol result shape: version=1, updates=group.version:9:1;group.version:0:2, validate=false (8)"
)]
#[tokio::test]
async fn requests_apply_atomically_with_kafkas_response_shape_case_8(
    feature_update: UpdateFeatureScenario,
) {
    let _ = (feature_update,);
}

async fn wait_for_group_version_off(broker: &Broker) {
    time::timeout(Duration::from_secs(5), async {
        while broker
            .controller
            .current_image()
            .finalized_feature("group.version")
            .is_some()
        {
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("group.version turned off");
}

#[tokio::test]
async fn handle_persists_a_lossless_metadata_downgrade() {
    // 4.4-IV0 is a Kafka trunk level: the controller supports it only under
    // `unstable.feature.versions.enable`.
    let HandlerStartedBroker {
        broker: broker_handle,
        dir: _dir,
    } = start_broker_no_audit_with(|cfg| {
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        cfg.features.unstable_feature_versions = krabka_raft::UnstableFeatureVersions::Enabled;
    })
    .await;
    let target = LATEST_PRODUCTION_METADATA_VERSION - 1;
    // 4.3-IV0 changed metadata, so step up to 4.4-IV0 first and back down
    // to 4.3-IV0, a downgrade across 4.4-IV0 alone.
    let up = Box::pin(call(
        &broker_handle,
        apply_request(vec![metadata_update(target + 2, 1)]),
        1,
    ))
    .await;
    assert!(up == accepted(&[METADATA_VERSION]));
    let broker = broker_handle.broker_arc_for_test();
    wait_for_finalized_feature(&broker, METADATA_VERSION, target + 2).await;

    let down = Box::pin(call(
        &broker_handle,
        apply_request(vec![metadata_update(target + 1, 2)]),
        1,
    ))
    .await;

    assert!(down == accepted(&[METADATA_VERSION]));
    wait_for_finalized_feature(&broker, METADATA_VERSION, target + 1).await;
    broker_handle.shutdown().await;
}
