// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.

//! The KIP-584 write-side surface. `ApiVersions` v3 and above expose the
//! feature surface that the JVM admin tooling reads. `supported_features`
//! advertises `metadata.version` over the range the feature table supports,
//! `min = 7` (`3.3-IV3`) to `max = 32` (`4.4-IV1`), and from v4 every other
//! feature from level 0, as Kafka does.
//!
//! A standalone, self-bootstrapped broker behaves like a freshly formatted
//! Kafka 4.3 cluster. It finalizes every registered feature at its release
//! default for `metadata.version = 30` (`4.3-IV0`, Kafka 4.3's
//! `LATEST_PRODUCTION`), not at trunk's unstable 4.4 levels, which an operator
//! reaches only through `UpdateFeatures` or `krabka format --release-version`.
//! At 4.3-IV0 that is `group.version = 1`, `transaction.version = 2`, and ELR,
//! `share.version` and `streams.version` at 1. It also reports a real
//! `finalized_features_epoch` of `>= 0`. `tests/feature_finalization.rs`
//! exercises `UpdateFeatures`. `transaction.version = 3` is advertised but
//! remains opt-in for KIP-939.
//!
//! A finalized `metadata.version` above the connecting JVM client's known
//! `MetadataVersion` enum, or one with `finalized_features_epoch = 0`, makes
//! that client throw `IllegalArgumentException` out of
//! `MetadataVersion.fromFeatureLevel(N)`. That failure once broke 19
//! `broker-jvm-acceptance` tests, and it is why a fresh cluster bootstraps at
//! the latest level a released Kafka knows. This test guards the fresh-broker
//! surface.

use assert2::assert;
mod support;

use krabka_format::LATEST_PRODUCTION_METADATA_VERSION;
use krabka_metadata::metadata_version::METADATA_VERSION_MAX;
use krabka_protocol::owned::api_versions_request::ApiVersionsRequest;

#[tokio::test]
async fn v3_response_advertises_supported_and_bootstrapped_finalized_features() {
    let p = support::start().await;

    let resp = p
        .client
        .send(ApiVersionsRequest {
            client_software_name: "krabka-test".into(),
            client_software_version: "0.0.0".into(),
            ..Default::default()
        })
        .await
        .expect("ApiVersions");

    assert!(resp.error_code == 0, "{resp:?}");

    // KIP-584 write-side: supported_features advertises metadata.version over
    // the supported range; the standalone broker self-bootstraps the release
    // defaults, so finalized_features carries metadata.version=4.3-IV0 and
    // group.version=1 with a real (>= 0) epoch. See the module-level note for
    // the JVM compatibility rationale.
    let mv = resp
        .supported_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version advertised in supported_features");
    assert!(mv.min_version == 7, "{resp:?}");
    assert!(mv.max_version == METADATA_VERSION_MAX, "{resp:?}");
    let gv = resp
        .supported_features
        .iter()
        .find(|f| f.name == "group.version")
        .expect("group.version advertised in supported_features");
    // The client negotiates ApiVersions v5, where Kafka advertises every
    // feature from its minimum production level, 0 for all but
    // metadata.version (`BrokerFeatures.defaultSupportedFeatures`). Below v4
    // Kafka omits a zero-minimum feature instead (`alterFeatureLevel0`).
    assert!(gv.min_version == 0, "{resp:?}");
    assert!(gv.max_version == 1, "{resp:?}");
    let tv = resp
        .supported_features
        .iter()
        .find(|f| f.name == "transaction.version")
        .expect("transaction.version advertised in supported_features");
    assert!(tv.min_version == 0, "{resp:?}");
    assert!(tv.max_version == 3, "{resp:?}");

    // A self-bootstrapped broker finalizes the release defaults.
    let finalized_metadata_version = resp
        .finalized_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version finalized at bootstrap");
    assert!(
        finalized_metadata_version.max_version_level == LATEST_PRODUCTION_METADATA_VERSION,
        "{resp:?}"
    );
    let finalized_group_version = resp
        .finalized_features
        .iter()
        .find(|f| f.name == "group.version")
        .expect("group.version finalized at bootstrap");
    assert!(finalized_group_version.max_version_level == 1, "{resp:?}");
    let finalized_transaction_version = resp
        .finalized_features
        .iter()
        .find(|f| f.name == "transaction.version")
        .expect("transaction.version finalized at bootstrap");
    assert!(
        finalized_transaction_version.max_version_level == 2,
        "{resp:?}"
    );
    assert!(
        resp.finalized_features_epoch >= 0,
        "self-bootstrapped broker finalizes defaults so epoch must be >= 0: {resp:?}"
    );

    p.broker.shutdown().await;
}
