//! The KIP-584 `UpdateFeatures` (`api_key` 57) write path.
//!
//! A finalize of `metadata.version` writes a Raft-persisted `V1FeatureLevel`
//! record, and reports the finalized feature and a real epoch through
//! `ApiVersions`. Validation rejects an unsupported feature and a level
//! outside the range with Kafka's top-level error. `validate_only` runs every
//! check and persists nothing.
#![cfg(test)]

use {
    crate::support::{configs::feature_update, discovery::api_versions_request_for},
    assert2::assert,
    krabka_protocol::owned::{
        api_versions_response::ApiVersionsResponse,
        update_features_response::UpdateFeaturesResponse,
    },
};
mod support;

use {
    krabka_format::LATEST_PRODUCTION_METADATA_VERSION,
    krabka_metadata::metadata_version::METADATA_VERSION_MAX,
    krabka_protocol::owned::update_features_request::UpdateFeaturesRequest,
};

/// Kafka answers a failed feature with `INVALID_UPDATE_VERSION` (95) at the
/// top level, a message naming the feature, and no rows, at every version.
fn assert_feature_error(resp: &UpdateFeaturesResponse, feature: &str) {
    assert!(resp.error_code == 95, "{resp:?}");
    assert!(resp.results.is_empty(), "{resp:?}");
    assert!(
        resp.error_message
            .as_deref()
            .is_some_and(|message| message.starts_with(
                "The update failed for all features since the following feature had an error: "
            ) && message.contains(&format!("for feature {feature}."))),
        "{resp:?}"
    );
}

fn metadata_version(api_versions: &ApiVersionsResponse) -> i16 {
    api_versions
        .finalized_features
        .iter()
        .find(|feature| feature.name == "metadata.version")
        .expect("metadata.version finalized at bootstrap")
        .max_version_level
}

struct FeatureFinalization {
    broker: Option<support::InProcess>,
    update: Option<UpdateFeaturesResponse>,
    versions: Option<ApiVersionsResponse>,
    baseline: Option<ApiVersionsResponse>,
}
impl FeatureFinalization {
    async fn versions(&self) -> ApiVersionsResponse {
        self.broker
            .as_ref()
            .expect("feature broker started")
            .client
            .send(api_versions_request_for("krabka-test", "0.0.0"))
            .await
            .expect("ApiVersions")
    }

    async fn shutdown(self) {
        self.broker
            .expect("feature broker started")
            .broker
            .shutdown()
            .await;
    }
}
#[rstest::fixture]
fn finalization() -> FeatureFinalization {
    // Start this scenario with fresh inputs and observations.

    FeatureFinalization {
        broker: None,
        update: None,
        versions: None,
        baseline: None,
    }
}
#[rstest_bdd_macros::given("unstable feature versions are enabled {enabled}")]
async fn feature_support_policy(finalization: &mut FeatureFinalization, enabled: bool) {
    finalization.broker = Some(
        support::start_configured(|config| {
            config.features.unstable_feature_versions = if enabled {
                krabka_raft::UnstableFeatureVersions::Enabled
            } else {
                krabka_raft::UnstableFeatureVersions::Disabled
            };
        })
        .await,
    );
}
#[rstest_bdd_macros::when(
    "metadata.version is upgraded to the trunk maximum with validate only {validate}"
)]
async fn finalize_trunk_metadata(finalization: &mut FeatureFinalization, validate: bool) {
    finalization.update = Some(
        finalization
            .broker
            .as_ref()
            .expect("feature broker started")
            .client
            .send(UpdateFeaturesRequest {
                feature_updates: vec![feature_update("metadata.version", METADATA_VERSION_MAX, 1)],
                validate_only: validate,
                ..Default::default()
            })
            .await
            .expect("UpdateFeatures"),
    );
}
#[rstest_bdd_macros::when(
    "feature {feature} is upgraded to level {level} with validate only {validate}"
)]
async fn finalize_feature_level(
    finalization: &mut FeatureFinalization,
    feature: String,
    level: i16,
    validate: bool,
) {
    finalization.update = Some(
        finalization
            .broker
            .as_ref()
            .expect("feature broker started")
            .client
            .send(UpdateFeaturesRequest {
                feature_updates: vec![feature_update(&feature, level, 1)],
                validate_only: validate,
                ..Default::default()
            })
            .await
            .expect("UpdateFeatures"),
    );
}
#[rstest_bdd_macros::then("the update succeeds for feature {feature}")]
fn feature_update_succeeded(finalization: &FeatureFinalization, feature: String) {
    let response = finalization
        .update
        .as_ref()
        .expect("feature update requested");
    assert!(response.error_code == 0, "{response:?}");
    if let Some(row) = response.results.iter().find(|row| row.feature == feature) {
        assert!(row.error_code == 0, "{response:?}");
    }
}
#[rstest_bdd_macros::then(
    "the update fails with error 95 no result rows and a message naming feature {feature}"
)]
fn feature_update_failed(finalization: &FeatureFinalization, feature: String) {
    assert_feature_error(
        finalization
            .update
            .as_ref()
            .expect("feature update requested"),
        &feature,
    );
}
#[rstest_bdd_macros::then(
    "the metadata refusal exactly names the requested trunk level and controller supported range 7-30"
)]
fn unstable_metadata_refusal(finalization: &FeatureFinalization) {
    let resp = finalization
        .update
        .as_ref()
        .expect("feature update requested");
    assert!(resp.error_message.as_deref() == Some(format!(
        "The update failed for all features since the following feature had an error: Invalid update version {METADATA_VERSION_MAX} for feature metadata.version. Local controller 1 only supports versions 7-30").as_str()), "{resp:?}");
}
#[rstest_bdd_macros::then("the share refusal names level 2 and controller supported range 0-1")]
fn unstable_share_refusal(finalization: &FeatureFinalization) {
    let resp = finalization
        .update
        .as_ref()
        .expect("feature update requested");
    assert!(resp.error_message.as_deref().is_some_and(|message| message.ends_with(
        "Invalid update version 2 for feature share.version. Local controller 1 only supports versions 0-1")), "{resp:?}");
}
#[rstest_bdd_macros::when("the client reads API versions and feature levels")]
async fn read_finalized_features(finalization: &mut FeatureFinalization) {
    finalization.versions = Some(finalization.versions().await);
}
#[rstest_bdd_macros::when("the client records baseline API versions and feature levels")]
async fn capture_finalized_features(finalization: &mut FeatureFinalization) {
    finalization.baseline = Some(finalization.versions().await);
}
#[rstest_bdd_macros::then(
    "metadata supported levels remain 7 through the latest production version"
)]
fn production_metadata_support(finalization: &FeatureFinalization) {
    let av = finalization
        .versions
        .as_ref()
        .expect("API versions requested");
    let supported = av
        .supported_features
        .iter()
        .find(|feature| feature.name == "metadata.version")
        .map(|feature| (feature.min_version, feature.max_version));
    assert!(
        supported == Some((7, LATEST_PRODUCTION_METADATA_VERSION)),
        "{av:?}"
    );
}
#[rstest_bdd_macros::then(
    "the finalized metadata level is the trunk maximum with a nonnegative epoch"
)]
fn trunk_metadata_finalized(finalization: &FeatureFinalization) {
    let av = finalization
        .versions
        .as_ref()
        .expect("API versions requested");
    let feature = av
        .finalized_features
        .iter()
        .find(|feature| feature.name == "metadata.version")
        .expect("metadata.version finalized");
    assert!(feature.max_version_level == METADATA_VERSION_MAX, "{av:?}");
    assert!(av.finalized_features_epoch >= 0, "{av:?}");
}
#[rstest_bdd_macros::then(
    "share.version supported maximum is {maximum} and finalized level is {level}"
)]
fn share_feature_levels(finalization: &FeatureFinalization, maximum: i16, level: i16) {
    let av = finalization
        .versions
        .as_ref()
        .expect("API versions requested");
    let supported = av
        .supported_features
        .iter()
        .find(|feature| feature.name == "share.version")
        .map(|feature| feature.max_version);
    let finalized = av
        .finalized_features
        .iter()
        .find(|feature| feature.name == "share.version")
        .map(|feature| feature.max_version_level);
    assert!(
        supported == Some(maximum) && finalized == Some(level),
        "{av:?}"
    );
}
#[rstest_bdd_macros::then(
    "baseline metadata is at the latest production level with a nonnegative epoch"
)]
fn initial_metadata_level(finalization: &FeatureFinalization) {
    let before = finalization
        .baseline
        .as_ref()
        .expect("baseline API versions requested");
    assert!(
        metadata_version(before) == LATEST_PRODUCTION_METADATA_VERSION,
        "{before:?}"
    );
    assert!(before.finalized_features_epoch >= 0, "{before:?}");
}
#[rstest_bdd_macros::then(
    "validation leaves the production metadata level and finalized epoch unchanged"
)]
fn feature_validation_not_persisted(finalization: &FeatureFinalization) {
    let before = finalization
        .baseline
        .as_ref()
        .expect("baseline API versions requested");
    let after = finalization.versions.as_ref().expect("API versions reread");
    assert!(
        metadata_version(after) == LATEST_PRODUCTION_METADATA_VERSION,
        "validate_only must not change the level: {after:?}"
    );
    assert!(
        after.finalized_features_epoch == before.finalized_features_epoch,
        "validate_only must not bump the epoch: {after:?}"
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/feature_finalization.feature",
    name = "an unstable metadata version is refused by default"
)]
#[tokio::test]
async fn an_unstable_metadata_version_is_refused_by_default(finalization: FeatureFinalization) {
    finalization.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/feature_finalization.feature",
    name = "finalizes metadata version and surfaces in api versions"
)]
#[tokio::test]
async fn finalizes_metadata_version_and_surfaces_in_api_versions(
    finalization: FeatureFinalization,
) {
    finalization.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/feature_finalization.feature",
    name = "share version two is refused by default"
)]
#[tokio::test]
async fn share_version_two_is_refused_by_default(finalization: FeatureFinalization) {
    finalization.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/feature_finalization.feature",
    name = "share version two is finalizable under unstable feature versions"
)]
#[tokio::test]
async fn share_version_two_is_finalizable_under_unstable_feature_versions(
    finalization: FeatureFinalization,
) {
    finalization.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/feature_finalization.feature",
    name = "validate only does not persist"
)]
#[tokio::test]
async fn validate_only_does_not_persist(finalization: FeatureFinalization) {
    finalization.shutdown().await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/feature_finalization.feature",
    name = "unsupported feature names and out of range levels are refused"
)]
#[tokio::test]
async fn unsupported_feature_update(
    finalization: FeatureFinalization,
    feature: String,
    level: i16,
) {
    let _ = (feature, level);
    finalization.shutdown().await;
}
