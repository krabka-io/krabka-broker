//! End-to-end tests for the `UpdateFeatures` handler, which drive a live
//! broker and so are kept out of the module root.

use std::{net::SocketAddr, sync::Arc};

use assert2::assert;
use krabka_protocol::owned::update_features_response::UpdatableFeatureResult;

use super::*;
use crate::{
    handlers::update_features::test_support::{
        VERSION, apply_request, assert_ok_row, assert_row_error, call_with, context,
        metadata_update, principal, start_broker, validate_only, wait_for_finalized_feature,
    },
    test_support::DenyAll,
};

#[tokio::test]
async fn handle_denies_cluster_alter_with_top_level_error() {
    let req = validate_only(vec![metadata_update(
        crate::features::METADATA_VERSION_MAX,
        1,
    )]);

    let (resp, broker_handle, _dir) = Box::pin(call_with(Arc::new(DenyAll), req)).await;

    let expected = UpdateFeaturesResponse {
        throttle_time_ms: 0,
        error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
        error_message: Some("Cluster authorization failed.".to_string()),
        results: vec![],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_rejects_empty_feature_updates() {
    let (resp, broker_handle, _dir) = Box::pin(call_with(
        Arc::new(crate::authorizer::AllowAllAuthorizer),
        validate_only(Vec::new()),
    ))
    .await;

    let expected = UpdateFeaturesResponse {
        throttle_time_ms: 0,
        error_code: codes::INVALID_REQUEST,
        error_message: Some("Can not provide empty feature updates in the request.".to_string()),
        results: vec![],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_accepts_validate_only_metadata_version_at_supported_max() {
    let req = validate_only(vec![metadata_update(
        crate::features::METADATA_VERSION_MAX,
        1,
    )]);

    let (resp, broker_handle, _dir) = Box::pin(call_with(
        Arc::new(crate::authorizer::AllowAllAuthorizer),
        req,
    ))
    .await;

    let expected = UpdateFeaturesResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        error_message: None,
        results: vec![UpdatableFeatureResult {
            feature: crate::features::METADATA_VERSION.to_string(),
            error_code: codes::NONE,
            error_message: None,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        }],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_persists_non_validate_feature_update() {
    let version = VERSION;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal();
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = context(&principal, &peer);
    let req = apply_request(vec![metadata_update(
        crate::features::METADATA_VERSION_MAX - 1,
        2,
    )]);

    let resp = handle(&broker, req, version, &ctx).await;

    assert!(resp.error_code == codes::NONE, "{resp:?}");
    assert_ok_row(&resp, crate::features::METADATA_VERSION);
    wait_for_finalized_feature(
        &broker,
        crate::features::METADATA_VERSION,
        crate::features::METADATA_VERSION_MAX - 1,
    )
    .await;
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_reports_duplicate_feature_on_second_row_only() {
    let req = validate_only(vec![
        metadata_update(crate::features::METADATA_VERSION_MAX, 1),
        metadata_update(crate::features::METADATA_VERSION_MAX, 1),
    ]);

    let (resp, broker_handle, _dir) = Box::pin(call_with(
        Arc::new(crate::authorizer::AllowAllAuthorizer),
        req,
    ))
    .await;

    let expected = UpdateFeaturesResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        error_message: None,
        results: vec![
            UpdatableFeatureResult {
                feature: crate::features::METADATA_VERSION.to_string(),
                error_code: codes::NONE,
                error_message: None,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            },
            UpdatableFeatureResult {
                feature: crate::features::METADATA_VERSION.to_string(),
                error_code: codes::INVALID_REQUEST,
                error_message: Some(
                    "Provided feature can not be updated more than once in the request."
                        .to_string(),
                ),
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            },
        ],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_rejects_negative_level_with_supported_range_message() {
    let req = validate_only(vec![metadata_update(-1, 1)]);

    let (resp, broker_handle, _dir) = Box::pin(call_with(
        Arc::new(crate::authorizer::AllowAllAuthorizer),
        req,
    ))
    .await;

    assert_row_error(&resp, crate::features::METADATA_VERSION, "supported range");
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_rejects_level_above_supported_max_with_range_message() {
    let req = validate_only(vec![metadata_update(
        crate::features::METADATA_VERSION_MAX + 1,
        1,
    )]);

    let (resp, broker_handle, _dir) = Box::pin(call_with(
        Arc::new(crate::authorizer::AllowAllAuthorizer),
        req,
    ))
    .await;

    assert_row_error(&resp, crate::features::METADATA_VERSION, "supported range");
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_accepts_lossless_safe_metadata_downgrade() {
    let req = validate_only(vec![metadata_update(
        krabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL,
        2,
    )]);

    let (resp, broker_handle, _dir) = Box::pin(call_with(
        Arc::new(crate::authorizer::AllowAllAuthorizer),
        req,
    ))
    .await;

    assert!(resp.error_code == codes::NONE, "{resp:?}");
    assert_ok_row(&resp, crate::features::METADATA_VERSION);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_rejects_metadata_downgrade_below_online_floor() {
    let req = validate_only(vec![metadata_update(
        crate::features::METADATA_VERSION_MIN - 1,
        2,
    )]);

    let (resp, broker_handle, _dir) = Box::pin(call_with(
        Arc::new(crate::authorizer::AllowAllAuthorizer),
        req,
    ))
    .await;

    assert_row_error(
        &resp,
        crate::features::METADATA_VERSION,
        "Online metadata.version downgrade",
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_rejects_online_metadata_version_deletion() {
    let req = validate_only(vec![metadata_update(0, 2)]);

    let (resp, broker_handle, _dir) = Box::pin(call_with(
        Arc::new(crate::authorizer::AllowAllAuthorizer),
        req,
    ))
    .await;

    assert_row_error(
        &resp,
        crate::features::METADATA_VERSION,
        "Online metadata.version downgrade",
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_rejects_downgrade_without_downgrade_flag() {
    let req = validate_only(vec![metadata_update(
        crate::features::METADATA_VERSION_MAX - 1,
        1,
    )]);

    let (resp, broker_handle, _dir) = Box::pin(call_with(
        Arc::new(crate::authorizer::AllowAllAuthorizer),
        req,
    ))
    .await;

    assert_row_error(&resp, crate::features::METADATA_VERSION, "downgrade");
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_rejects_delete_zero_without_downgrade_flag() {
    let req = validate_only(vec![metadata_update(0, 1)]);

    let (resp, broker_handle, _dir) = Box::pin(call_with(
        Arc::new(crate::authorizer::AllowAllAuthorizer),
        req,
    ))
    .await;

    assert_row_error(&resp, crate::features::METADATA_VERSION, "downgrade flag");
    broker_handle.shutdown().await;
}
