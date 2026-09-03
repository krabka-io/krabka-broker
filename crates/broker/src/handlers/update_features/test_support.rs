//! Request builders and the broker harness shared by the `UpdateFeatures`
//! tests.
//!
//! The validation tests and the end-to-end handler tests build the same
//! requests and assert on the same result rows, so the fixtures live in one
//! module rather than being duplicated per test file.

use std::{net::SocketAddr, sync::Arc};

use assert2::assert;
use krabka_protocol::owned::{
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
    update_features_response::UpdateFeaturesResponse,
};
use krabka_security::Principal;

use super::handle;
use crate::{
    authorizer::Authorizer,
    broker::{Broker, BrokerHandle},
    codes,
};

pub(super) const VERSION: i16 = 1;

fn feature_update(name: &str, level: i16, upgrade_type: i8) -> FeatureUpdateKey {
    FeatureUpdateKey {
        feature: name.into(),
        max_version_level: level,
        upgrade_type,
        ..Default::default()
    }
}

pub(super) fn metadata_update(level: i16, upgrade_type: i8) -> FeatureUpdateKey {
    feature_update(crate::features::METADATA_VERSION, level, upgrade_type)
}

pub(super) fn elr_update(level: i16, upgrade_type: i8) -> FeatureUpdateKey {
    feature_update(crate::features::ELR_VERSION, level, upgrade_type)
}

pub(super) fn validate_only(updates: Vec<FeatureUpdateKey>) -> UpdateFeaturesRequest {
    UpdateFeaturesRequest {
        feature_updates: updates,
        validate_only: true,
        ..Default::default()
    }
}

pub(super) fn apply_request(updates: Vec<FeatureUpdateKey>) -> UpdateFeaturesRequest {
    UpdateFeaturesRequest {
        feature_updates: updates,
        validate_only: false,
        ..Default::default()
    }
}

pub(super) fn principal() -> Principal {
    crate::test_support::principal("admin")
}

pub(super) fn context<'a>(
    principal: &'a Principal,
    peer: &'a SocketAddr,
) -> crate::handlers::RequestContext<'a> {
    crate::test_support::request_context(principal, peer, "update-features-client")
}

pub(super) async fn start_broker(
    authorizer: Arc<dyn Authorizer>,
) -> (BrokerHandle, tempfile::TempDir) {
    crate::test_support::start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = authorizer;
    })
    .await
}

pub(super) async fn call_with(
    authorizer: Arc<dyn Authorizer>,
    req: UpdateFeaturesRequest,
) -> (UpdateFeaturesResponse, BrokerHandle, tempfile::TempDir) {
    let (broker_handle, dir) = start_broker(authorizer).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal();
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = context(&principal, &peer);
    let resp = handle(&broker, req, VERSION, &ctx).await;
    (resp, broker_handle, dir)
}

pub(super) fn assert_ok_row(resp: &UpdateFeaturesResponse, feature: &str) {
    let row = resp
        .results
        .iter()
        .find(|row| row.feature == feature)
        .expect("feature result row");
    assert!(row.error_code == codes::NONE, "{resp:?}");
    assert!(row.error_message.is_none(), "{resp:?}");
}

pub(super) fn assert_row_error(resp: &UpdateFeaturesResponse, feature: &str, message: &str) {
    let row = resp
        .results
        .iter()
        .find(|row| row.feature == feature)
        .expect("feature result row");
    assert!(row.error_code == codes::INVALID_UPDATE_VERSION, "{resp:?}");
    assert!(
        row.error_message
            .as_deref()
            .is_some_and(|m| m.contains(message)),
        "{resp:?}"
    );
}

pub(super) async fn wait_for_finalized_feature(broker: &Broker, feature: &str, level: i16) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if broker.controller.current_image().finalized_feature(feature) == Some(level) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("feature level visible");
}
