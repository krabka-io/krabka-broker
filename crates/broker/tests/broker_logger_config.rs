//! End-to-end `BROKER_LOGGER` (resource type 8) round-trip over the wire.
//!
//! `kafka-configs --entity-type broker-loggers --alter` is how an operator
//! raises a broker's log level without restarting it. This suite drives that
//! exchange against a real in-process broker: `IncrementalAlterConfigs` sets
//! a level, `DescribeConfigs` reads it back, `ListConfigResources` lists the
//! resource, and a `tracing` event proves the filter actually moved — the
//! whole point of the feature is the events, not the config entry.
#![cfg(test)]

use {
    crate::support::{
        client::connect_owned,
        configs::{
            ConfigOperation, describe_resource, incremental_config, incremental_request,
            incremental_resource,
        },
    },
    assert2::{assert, check},
    krabka_broker::config,
    std::net::SocketAddr,
    tracing::dispatcher,
};
mod support;

use {
    krabka_protocol::owned::{
        describe_configs_request::DescribeConfigsRequest,
        describe_configs_response::DescribeConfigsResponse,
        incremental_alter_configs_response::IncrementalAlterConfigsResponse,
        list_config_resources_request::ListConfigResourcesRequest,
        list_config_resources_response::ListConfigResourcesResponse,
    },
    krabka_telemetry::LogLevelController,
    std::sync::{Arc, Mutex},
    support::start_n_node_with,
    tracing_subscriber::{Layer, layer::SubscriberExt as _},
};

/// Kafka resource type id for `BROKER_LOGGER`.
const RESOURCE_TYPE_BROKER_LOGGER: i8 = 8;

/// `config_source` `DYNAMIC_BROKER_LOGGER_CONFIG` = 6.
const CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER: i8 = 6;

/// The target the test raises. It sits under `krabka_broker`, so a level set
/// on that logger covers it the way a log4j2 level on a package covers the
/// classes below it.
const CHILD_TARGET: &str = "krabka_broker::broker_logger_wire_test";

/// The filter this broker starts with. It is the production default rather
/// than a spec written for the test, so the alter below exercises the logger
/// names a real `krabka-broker` process actually has: a seed that stopped
/// naming `krabka_broker` would fail this test with `Logger krabka_broker does
/// not exist!` instead of passing against a fixture nobody ships.
const STARTING_SPEC: &str = config::DEFAULT_LOG_FILTER;

// Record admitted events as target:LEVEL and panic on a poisoned capture lock.
krabka_macros::capture_layer_fixture!(CaptureLayer, Captured, unwrap);

/// Emit one `DEBUG` event on [`CHILD_TARGET`] through `dispatch` and report
/// whether the broker's live filter let it through.
///
/// The emission is synchronous inside `with_default`, so it cannot migrate to
/// another worker thread between setting the dispatcher and logging.
fn debug_event_is_emitted(dispatch: &tracing::Dispatch, captured: &Captured) -> bool {
    dispatcher::with_default(dispatch, || {
        tracing::debug!(target: CHILD_TARGET, "broker logger probe");
    });
    let mut seen = captured.lock().unwrap();
    let hit = seen
        .iter()
        .any(|line| line == &format!("{CHILD_TARGET}:DEBUG"));
    seen.clear();
    hit
}

async fn build_client(addr: SocketAddr) -> krabka_client_core::Client {
    connect_owned(
        format!("127.0.0.1:{}", addr.port()),
        "broker-logger-config-test",
        "client build",
    )
    .await
}

/// A `BROKER_LOGGER` describe for `resource_name`.
fn describe_request(resource_name: &str) -> DescribeConfigsRequest {
    DescribeConfigsRequest {
        resources: vec![describe_resource(
            RESOURCE_TYPE_BROKER_LOGGER,
            resource_name.to_owned(),
            None,
        )],
        include_synonyms: false,
        include_documentation: false,
        ..Default::default()
    }
}

struct LoggerConfiguration {
    cluster: Vec<(
        krabka_broker::BrokerHandle,
        krabka_broker::BrokerConfig,
        tempfile::TempDir,
    )>,
    client: Option<krabka_client_core::Client>,
    dispatch: Option<tracing::Dispatch>,
    filter: Option<krabka_telemetry::LogLevelFilter>,
    captured: Captured,
    node: String,
    alter: Option<IncrementalAlterConfigsResponse>,
    describe: Option<DescribeConfigsResponse>,
    listed: Option<ListConfigResourcesResponse>,
    foreign: Option<DescribeConfigsResponse>,
    debug_emitted: Option<bool>,
}
#[rstest::fixture]
fn logger() -> LoggerConfiguration {
    // Start this scenario with fresh inputs and observations.

    LoggerConfiguration {
        cluster: Vec::new(),
        client: None,
        dispatch: None,
        filter: None,
        captured: Arc::new(Mutex::new(Vec::new())),
        node: String::new(),
        alter: None,
        describe: None,
        listed: None,
        foreign: None,
        debug_emitted: None,
    }
}
#[rstest_bdd_macros::given("a broker using the production INFO filter and a capturing subscriber")]
async fn logger_at_info(logger: &mut LoggerConfiguration) {
    // The controller the broker will run with, plus a capturing subscriber
    // over the same live filter. Held for the whole test so a rebuild of the
    // callsite interest cache always sees this dispatcher registered.
    let (levels, filter) = LogLevelController::new(STARTING_SPEC);
    let captured: Captured = Arc::new(Mutex::new(Vec::new()));
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(CaptureLayer(Arc::clone(&captured)).with_filter(levels.filter())),
    );

    let cluster = start_n_node_with(1, |_, cfg| cfg.log_levels = levels.clone())
        .await
        .expect("start_n_node_with");
    let (_, cfg, _dir) = &cluster[0];
    let node = cfg.broker_id.to_string();
    let client = build_client(cfg.listen_addr).await;

    logger.cluster = cluster;
    logger.node = node;
    logger.client = Some(client);
    logger.dispatch = Some(dispatch);
    logger.filter = Some(filter);
    logger.captured = captured;
}
#[rstest_bdd_macros::when("a DEBUG event is emitted on the broker child target")]
fn emit_logger_debug(logger: &mut LoggerConfiguration) {
    logger.debug_emitted = Some(debug_event_is_emitted(
        logger.dispatch.as_ref().expect("subscriber configured"),
        &logger.captured,
    ));
}
#[rstest_bdd_macros::then("the DEBUG event is filtered out")]
fn logger_debug_filtered(logger: &LoggerConfiguration) {
    assert!(logger.debug_emitted == Some(false));
}
#[rstest_bdd_macros::then("the DEBUG event reaches the capturing subscriber")]
fn logger_debug_admitted(logger: &LoggerConfiguration) {
    assert!(logger.debug_emitted == Some(true));
}
#[rstest_bdd_macros::when("the broker logger is set to DEBUG through IncrementalAlterConfigs")]
async fn alter_logger_debug(logger: &mut LoggerConfiguration) {
    let client = logger.client.as_ref().expect("broker client");
    let node = logger.node.clone();
    let alter_resp: IncrementalAlterConfigsResponse = client
        .send(
            incremental_request()
                .resources(vec![incremental_resource(
                    RESOURCE_TYPE_BROKER_LOGGER,
                    node.clone(),
                    vec![incremental_config(
                        "krabka_broker".to_owned(),
                        Some("DEBUG".to_owned()),
                        ConfigOperation::Set,
                    )],
                )])
                .call(),
        )
        .await
        .expect("IncrementalAlterConfigs");
    logger.alter = Some(alter_resp);
}
#[rstest_bdd_macros::then("the broker logger alteration succeeds")]
fn alter_logger_debug_observed(logger: &LoggerConfiguration) {
    let alter_resp = logger.alter.as_ref().expect("request completed");

    assert!(alter_resp.responses.len() == 1, "{alter_resp:?}");
    let altered = &alter_resp.responses[0];
    assert!(
        altered.error_code == 0,
        "BROKER_LOGGER alter must succeed; error_code={} message={:?}",
        altered.error_code,
        altered.error_message
    );
}
#[rstest_bdd_macros::when("the local broker logger configuration is described")]
async fn describe_local_loggers(logger: &mut LoggerConfiguration) {
    let client = logger.client.as_ref().expect("broker client");
    let node = logger.node.clone();
    let describe_resp: DescribeConfigsResponse = client
        .send(describe_request(&node))
        .await
        .expect("DescribeConfigs");
    logger.describe = Some(describe_resp);
}
#[rstest_bdd_macros::then("the logger reports DEBUG while root retains INFO with dynamic sources")]
fn describe_local_loggers_observed(logger: &LoggerConfiguration) {
    let describe_resp = logger.describe.as_ref().expect("request completed");
    let node = &logger.node;

    assert!(describe_resp.results.len() == 1, "{describe_resp:?}");
    let result = &describe_resp.results[0];
    check!(result.error_code == 0, "DescribeConfigs failed: {result:?}");
    check!(result.resource_type == RESOURCE_TYPE_BROKER_LOGGER);
    check!(&result.resource_name == node);

    let logger = |name: &str| {
        result
            .configs
            .iter()
            .find(|config| config.name == name)
            .unwrap_or_else(|| panic!("missing logger {name} in {:?}", result.configs))
    };
    let altered_logger = logger("krabka_broker");
    check!(altered_logger.value.as_deref() == Some("DEBUG"));
    check!(altered_logger.config_source == CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER);
    check!(!altered_logger.is_sensitive);
    check!(!altered_logger.read_only);
    let root = logger("root");
    check!(root.value.as_deref() == Some("INFO"));
    check!(root.config_source == CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER);
}
#[rstest_bdd_macros::when("broker logger resources are listed")]
async fn list_loggers(logger: &mut LoggerConfiguration) {
    let client = logger.client.as_ref().expect("broker client");
    let list_resp: ListConfigResourcesResponse = client
        .send(ListConfigResourcesRequest {
            resource_types: vec![RESOURCE_TYPE_BROKER_LOGGER],
            ..Default::default()
        })
        .await
        .expect("ListConfigResources");
    logger.listed = Some(list_resp);
}
#[rstest_bdd_macros::then("the local broker logger resource is listed")]
fn list_loggers_observed(logger: &LoggerConfiguration) {
    let list_resp = logger.listed.as_ref().expect("request completed");
    let node = &logger.node;

    check!(list_resp.error_code == 0, "{list_resp:?}");
    check!(
        list_resp.config_resources.iter().any(|resource| {
            resource.resource_type == RESOURCE_TYPE_BROKER_LOGGER && &resource.resource_name == node
        }),
        "BROKER_LOGGER {node} missing from {list_resp:?}"
    );
}
#[rstest_bdd_macros::when("broker logger configuration is described for node 99")]
async fn describe_foreign_loggers(logger: &mut LoggerConfiguration) {
    let client = logger.client.as_ref().expect("broker client");
    let foreign: DescribeConfigsResponse = client
        .send(describe_request("99"))
        .await
        .expect("DescribeConfigs for another node");
    logger.foreign = Some(foreign);
}
#[rstest_bdd_macros::then("the foreign logger request is refused as node local")]
fn describe_foreign_loggers_observed(logger: &LoggerConfiguration) {
    let foreign = logger.foreign.as_ref().expect("request completed");

    let refused = &foreign.results[0];
    check!(
        refused.error_code == 42,
        "expected INVALID_REQUEST: {refused:?}"
    );
    check!(
        refused.error_message.as_deref()
            == Some("Unexpected broker id, expected 1 but received 99")
    );
    check!(refused.configs.is_empty());
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/broker_logger_config.feature",
    name = "broker logger alter moves the live filter and the describe agrees"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_logger_alter_moves_the_live_filter_and_the_describe_agrees(
    logger: LoggerConfiguration,
) {
    support::shutdown_cluster(logger.cluster).await;
}
