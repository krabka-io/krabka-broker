//! KIP-1022 `krabka format --feature` end-to-end test. A standalone-formatted
//! log dir whose feature levels came from `--feature` boots a broker that
//! finalizes exactly those levels and surfaces them through `ApiVersions`.
//!
//! `--standalone` is load-bearing. It writes a `VotersRecord`, so the broker
//! uses the formatted voter set and *skips* the in-process self-bootstrap that
//! would otherwise re-seed every feature at the latest release. That proves the
//! `--feature` overrides survive boot and nothing overwrites them.
#![cfg(test)]

use {
    crate::support::{client::connect_owned, discovery::api_versions_request_for},
    assert2::{assert, check},
    krabka_broker::{Broker, BrokerConfig},
    krabka_protocol::owned::api_versions_response::ApiVersionsResponse,
    std::path::Path,
    tokio::net::TcpListener,
};

mod support;

/// Run `krabka format --standalone … --feature …` as a subprocess. The test
/// shells out through `env!("CARGO")`, because this crate does not get the
/// `krabka-format` `CARGO_BIN_EXE_*` variable. The dev-dep keeps the
/// Formats a standalone log directory with explicit `--feature` overrides.
///
/// Called in process rather than spawned: the formatting is setup for the
/// `ApiVersions` assertion below, not the thing under test, and a subprocess would
/// need a Cargo working tree to build from, which a Bazel test sandbox does not
/// have. `krabka-format`'s own `format_smoke` suite runs the real binary.
async fn run_krabka_format_with_features(
    log_dir: &Path,
    node_id: u64,
    controller_listener: &str,
    features: &[&str],
) {
    let mut argv = vec![
        "krabka-format".to_owned(),
        "--log-dir".to_owned(),
        log_dir.to_str().unwrap().to_owned(),
        "--standalone".to_owned(),
        "--node-id".to_owned(),
        node_id.to_string(),
        "--controller-listener".to_owned(),
        controller_listener.to_owned(),
    ];
    for f in features {
        argv.push("--feature".to_owned());
        argv.push((*f).to_owned());
    }
    let code = krabka_format::run_from_args(argv).await;
    assert!(code == 0, "krabka-format exited {code}");
}

struct FormattedFeatures {
    directory: Option<tempfile::TempDir>,
    config: Option<BrokerConfig>,
    data_listeners: Vec<TcpListener>,
    controller_listeners: Vec<TcpListener>,
    broker: Option<krabka_broker::BrokerHandle>,
    response: Option<ApiVersionsResponse>,
}
#[rstest::fixture]
fn formatted() -> FormattedFeatures {
    // Start this scenario with fresh inputs and observations.

    FormattedFeatures {
        directory: None,
        config: None,
        data_listeners: Vec::new(),
        controller_listeners: Vec::new(),
        broker: None,
        response: None,
    }
}
#[rstest_bdd_macros::given(
    "a standalone log formatted with transaction version 1 and group version 0"
)]
async fn formatted_overrides(formatted: &mut FormattedFeatures) {
    support::init_tracing();

    // Pre-bind a concrete controller port so it can be baked into the
    // VotersRecord at format time and re-bound by the broker on boot.
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(1).await;
    let client_addr = client_addrs[0];
    let controller_addr = controller_addrs[0];

    let dir = tempfile::tempdir().unwrap();
    // `krabka format` creates the dir itself and refuses a non-empty one.
    let boot_dir = dir.path().join("boot");
    run_krabka_format_with_features(
        &boot_dir,
        1,
        &controller_addr.to_string(),
        // transaction.version pinned to 1 (default at the latest release is 2);
        // group.version pinned to 0 (default is 1) → omitted → not finalized.
        &["transaction.version=1", "group.version=0"],
    )
    .await;

    let mut cfg = BrokerConfig::for_tests(boot_dir.clone());
    cfg.broker_id = 1;
    cfg.node_id = krabka_broker::NodeId(1);
    cfg.listen_addr = client_addr;
    cfg.advertised_listener = client_addr.to_string();
    cfg.controller_listen_addr = controller_addr;
    cfg.controller_quorum_voters = vec![(krabka_broker::NodeId(1), controller_addr.to_string())];
    cfg.bootstrap_mode = krabka_broker::BootstrapMode::Bootstrap;

    formatted.directory = Some(dir);
    formatted.config = Some(cfg);
    formatted.data_listeners = client_listeners;
    formatted.controller_listeners = controller_listeners;
}
#[rstest_bdd_macros::when("the broker boots from the formatted voter and feature records")]
async fn boot_formatted_features(formatted: &mut FormattedFeatures) {
    let data_listener = formatted
        .data_listeners
        .pop()
        .expect("reserved data listener");
    let controller_listener = formatted
        .controller_listeners
        .pop()
        .expect("reserved controller listener");
    formatted.broker = Some(
        Broker::start_with_listeners(
            formatted.config.take().expect("formatted configuration"),
            Some(controller_listener),
            Some(data_listener),
        )
        .await
        .expect("broker start"),
    );
}
#[rstest_bdd_macros::when("a client discovers the booted broker feature levels")]
async fn discover_formatted_features(formatted: &mut FormattedFeatures) {
    let bootstrap = formatted
        .broker
        .as_ref()
        .expect("broker booted")
        .listen_addr()
        .to_string();
    let client = connect_owned(&bootstrap, "fmt-feature-test", "client build").await;
    formatted.response = Some(
        client
            .send(api_versions_request_for("krabka-test", "0.0.0"))
            .await
            .expect("ApiVersions"),
    );
}
#[rstest_bdd_macros::then("feature discovery succeeds")]
fn formatted_feature_discovery_accepted(formatted: &FormattedFeatures) {
    assert!(
        formatted
            .response
            .as_ref()
            .expect("feature response")
            .error_code
            == 0
    );
}
#[rstest_bdd_macros::then("transaction version remains finalized at 1")]
fn transaction_override_survives(formatted: &FormattedFeatures) {
    let av = formatted.response.as_ref().expect("feature response");
    check!(
        av.finalized_features
            .iter()
            .find(|feature| feature.name == "transaction.version")
            .map(|feature| feature.max_version_level)
            == Some(1)
    );
}
#[rstest_bdd_macros::then("group version 0 is omitted from finalized features")]
fn disabled_group_not_finalized(formatted: &FormattedFeatures) {
    let av = formatted.response.as_ref().expect("feature response");
    check!(
        !av.finalized_features
            .iter()
            .any(|feature| feature.name == "group.version")
    );
}
#[rstest_bdd_macros::then("metadata version uses the latest production default")]
fn metadata_default_survives(formatted: &FormattedFeatures) {
    let av = formatted.response.as_ref().expect("feature response");
    check!(
        av.finalized_features
            .iter()
            .find(|feature| feature.name == "metadata.version")
            .map(|feature| feature.max_version_level)
            == Some(krabka_format::LATEST_PRODUCTION_METADATA_VERSION)
    );
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/format_features.feature",
    name = "standalone format feature overrides surface in api versions"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_format_feature_overrides_surface_in_api_versions(formatted: FormattedFeatures) {
    formatted.broker.expect("broker booted").shutdown().await;
}
