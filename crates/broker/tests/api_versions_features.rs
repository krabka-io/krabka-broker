#![cfg(test)]

//! The KIP-584 write-side surface. `ApiVersions` v3 and above expose the
//! feature surface that the JVM admin tooling reads. `supported_features`
//! advertises `metadata.version` from `min = 7` (`3.3-IV3`) to `max = 30`
//! (`4.3-IV0`, Kafka 4.3.1's latest production level, while
//! `unstable.feature.versions.enable` is off), and from v4 every other feature
//! from level 0, as Kafka does.
//!
//! A standalone, self-bootstrapped broker behaves like a freshly formatted
//! Kafka 4.3 cluster. It finalizes every registered feature at its release
//! default for `metadata.version = 30` (`4.3-IV0`, Kafka 4.3's
//! `LATEST_PRODUCTION`), not at trunk's unstable 4.4 levels, which an operator
//! reaches only under `unstable.feature.versions.enable`.
//! At 4.3-IV0 that is `group.version = 1`, `transaction.version = 2`, and ELR,
//! `share.version` and `streams.version` at 1. It also reports a real
//! `finalized_features_epoch` of `>= 0`. `tests/feature_finalization.rs`
//! exercises `UpdateFeatures`. `transaction.version` is advertised up to 2, the
//! highest level Kafka defines. KIP-939 needs no higher one.
//!
//! A finalized `metadata.version` above the connecting JVM client's known
//! `MetadataVersion` enum, or one with `finalized_features_epoch = 0`, makes
//! that client throw `IllegalArgumentException` out of
//! `MetadataVersion.fromFeatureLevel(N)`. That failure once broke 19
//! `broker-jvm-acceptance` tests, and it is why a fresh cluster bootstraps at
//! the latest level a released Kafka knows. This test guards the fresh-broker
//! surface.

use {
    assert2::assert,
    krabka_protocol::owned::api_versions_response::ApiVersionsResponse,
    rstest::fixture,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::{DataTableRow, given, scenario, then, when},
    support::InProcess,
};

mod support;

use krabka_format::LATEST_PRODUCTION_METADATA_VERSION;

struct FeatureDiscovery {
    broker: Option<InProcess>,
    response: Option<ApiVersionsResponse>,
}

#[fixture]
fn features() -> FeatureDiscovery {
    // Start this scenario with fresh inputs and observations.

    FeatureDiscovery {
        broker: None,
        response: None,
    }
}

#[given("a fresh standalone broker at the release feature defaults")]
async fn fresh_broker(features: &mut FeatureDiscovery) {
    features.broker = Some(support::start().await);
}

#[when("a client negotiates ApiVersions")]
async fn discover_features(features: &mut FeatureDiscovery) {
    let broker = features.broker.as_ref().expect("a fresh standalone broker");
    features.response = Some(support::discovery::api_versions(&broker.client).await);
}

#[derive(DataTableRow)]
struct SupportedLevel {
    feature: String,
    minimum: i16,
    maximum: String,
}

#[derive(DataTableRow)]
struct FinalizedLevel {
    feature: String,
    level: String,
}

fn feature_level(value: &str) -> i16 {
    if value == "latest-production" {
        LATEST_PRODUCTION_METADATA_VERSION
    } else {
        value.parse().expect("a feature level or latest-production")
    }
}

#[then("the feature negotiation succeeds")]
fn negotiation_succeeds(features: &FeatureDiscovery) {
    let response = features
        .response
        .as_ref()
        .expect("ApiVersions was negotiated");
    assert!(response.error_code == 0, "{response:?}");
}

#[then("the supported feature levels are:")]
fn supported_levels(features: &FeatureDiscovery, #[datatable] rows: Rows<SupportedLevel>) {
    let response = features
        .response
        .as_ref()
        .expect("ApiVersions was negotiated");
    for row in rows {
        let actual = response
            .supported_features
            .iter()
            .find(|feature| feature.name == row.feature)
            .unwrap_or_else(|| panic!("{} must be advertised: {response:?}", row.feature));
        assert!(
            (actual.min_version, actual.max_version) == (row.minimum, feature_level(&row.maximum)),
            "{response:?}"
        );
    }
}

#[then("the bootstrapped finalized levels are:")]
fn finalized_levels(features: &FeatureDiscovery, #[datatable] rows: Rows<FinalizedLevel>) {
    let response = features
        .response
        .as_ref()
        .expect("ApiVersions was negotiated");
    for row in rows {
        let actual = response
            .finalized_features
            .iter()
            .find(|feature| feature.name == row.feature)
            .unwrap_or_else(|| {
                panic!(
                    "{} must be finalized at bootstrap: {response:?}",
                    row.feature
                )
            });
        assert!(
            actual.max_version_level == feature_level(&row.level),
            "{response:?}"
        );
    }
}

#[then("krabka.version is omitted from finalized features until explicitly enabled")]
fn unfinalized_krabka_version(features: &FeatureDiscovery) {
    let response = features
        .response
        .as_ref()
        .expect("ApiVersions was negotiated");
    assert!(
        response
            .finalized_features
            .iter()
            .all(|feature| feature.name != "krabka.version"),
        "{response:?}"
    );
}

#[then("the finalized feature epoch is nonnegative")]
fn finalized_epoch(features: &FeatureDiscovery) {
    let response = features
        .response
        .as_ref()
        .expect("ApiVersions was negotiated");
    assert!(
        response.finalized_features_epoch >= 0,
        "self-bootstrapped broker finalizes defaults so epoch must be >= 0: {response:?}"
    );
}

#[scenario(
    path = "tests/features/migrated/tests/api_versions_features.feature",
    name = "v3 response advertises supported and bootstrapped finalized features"
)]
#[tokio::test]
async fn v3_response_advertises_supported_and_bootstrapped_finalized_features(
    features: FeatureDiscovery,
) {
    features
        .broker
        .expect("a fresh standalone broker")
        .broker
        .shutdown()
        .await;
}
