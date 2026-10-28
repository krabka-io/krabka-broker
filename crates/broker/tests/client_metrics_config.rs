#![cfg(test)]

//! Broker-side integration test for KIP-714 `CLIENT_METRICS` config round-trip.
//!
//! It drives `IncrementalAlterConfigs` → `DescribeConfigs` →
//! `ListConfigResources` for a `CLIENT_METRICS` subscription over the real
//! in-process broker, and it asserts the full operator-facing round-trip.

use {
    crate::support::{configs::client_metrics_subscription, shutdown_cluster, start_n_node_client},
    assert2::{assert, check},
    krabka_broker::{BrokerConfig, BrokerHandle},
    krabka_client_core::Client,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};
mod support;

use krabka_protocol::owned::{
    describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
    describe_configs_response::DescribeConfigsResponse,
    incremental_alter_configs_response::IncrementalAlterConfigsResponse,
    list_config_resources_request::ListConfigResourcesRequest,
    list_config_resources_response::ListConfigResourcesResponse,
};

/// Kafka resource type id for `CLIENT_METRICS` (KIP-714).
const RESOURCE_TYPE_CLIENT_METRICS: i8 = 16;

/// `config_source` `CLIENT_METRICS_CONFIG` = 7.
const CONFIG_SOURCE_CLIENT_METRICS: i8 = 7;

/// `config_source` `DEFAULT_CONFIG` = 5.
const CONFIG_SOURCE_DEFAULT: i8 = 5;

// ── helpers ───────────────────────────────────────────────────────────────────

fn assert_describe_response(response: &DescribeConfigsResponse) {
    assert!(
        response.results.len() == 1,
        "expected exactly one describe result"
    );
    let result = &response.results[0];
    check!(result.error_code == 0, "DescribeConfigs failed: {result:?}");
    check!(result.resource_type == RESOURCE_TYPE_CLIENT_METRICS);
    check!(result.resource_name == "sub-a");

    let config = |name: &str| {
        result
            .configs
            .iter()
            .find(|config| config.name == name)
            .unwrap_or_else(|| panic!("missing {name} config in {:?}", result.configs))
    };
    let metrics = config("metrics");
    assert!(metrics.value.as_deref() == Some("org.apache.kafka.consumer."));
    assert!(metrics.config_source == CONFIG_SOURCE_CLIENT_METRICS);
    let interval = config("interval.ms");
    assert!(interval.value.as_deref() == Some("60000"));
    assert!(interval.config_source == CONFIG_SOURCE_CLIENT_METRICS);
    assert!(config("match").config_source == CONFIG_SOURCE_DEFAULT);
}

fn assert_list_response(response: &ListConfigResourcesResponse) {
    assert!(
        response.error_code == 0,
        "ListConfigResources failed: {response:?}"
    );
    assert!(
        response.config_resources.iter().any(|resource| {
            resource.resource_type == RESOURCE_TYPE_CLIENT_METRICS
                && resource.resource_name == "sub-a"
        }),
        "CLIENT_METRICS sub-a missing from {response:?}"
    );
}

struct MetricsSubscription {
    cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>,
    client: Option<Client>,
    metrics: String,
    altered: Option<IncrementalAlterConfigsResponse>,
    described: Option<DescribeConfigsResponse>,
    listed: Option<ListConfigResourcesResponse>,
}

#[fixture]
fn subscription() -> MetricsSubscription {
    // Start this scenario with fresh inputs and observations.

    MetricsSubscription {
        cluster: Vec::new(),
        client: None,
        metrics: String::new(),
        altered: None,
        described: None,
        listed: None,
    }
}

#[given("a single-broker cluster accepting client metrics subscriptions")]
async fn metrics_cluster(subscription: &mut MetricsSubscription) {
    let (cluster, client) = start_n_node_client(1, "client-metrics-config-test").await;
    subscription.cluster = cluster;
    subscription.client = Some(client);
}

#[given("subscription sub-a selects consumer metrics every 60000 milliseconds")]
fn consumer_metrics_subscription(subscription: &mut MetricsSubscription) {
    subscription.metrics = "org.apache.kafka.consumer.".into();
}

#[when("the operator creates subscription sub-a with IncrementalAlterConfigs")]
async fn create_subscription(subscription: &mut MetricsSubscription) {
    let client = subscription
        .client
        .as_ref()
        .expect("a client metrics cluster");
    let request = client_metrics_subscription()
        .metrics(&subscription.metrics)
        .call();
    subscription.altered = Some(client.send(request).await.expect("IncrementalAlterConfigs"));
}

#[then("the subscription change succeeds for exactly one resource")]
fn subscription_created(subscription: &MetricsSubscription) {
    let response = subscription
        .altered
        .as_ref()
        .expect("a subscription change");
    assert!(
        response.responses.len() == 1,
        "expected exactly one resource response, got {}",
        response.responses.len()
    );
    let resource = &response.responses[0];
    assert!(
        resource.error_code == 0,
        "IncrementalAlterConfigs CLIENT_METRICS must succeed; error_code={} message={:?}",
        resource.error_code,
        resource.error_message
    );
}

#[when("the operator describes subscription sub-a")]
async fn describe_subscription(subscription: &mut MetricsSubscription) {
    let request = DescribeConfigsRequest {
        resources: vec![DescribeConfigsResource {
            resource_type: RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: "sub-a".to_owned(),
            configuration_keys: None,
            ..Default::default()
        }],
        include_synonyms: false,
        include_documentation: false,
        ..Default::default()
    };
    subscription.described = Some(
        subscription
            .client
            .as_ref()
            .expect("a client metrics cluster")
            .send(request)
            .await
            .expect("DescribeConfigs"),
    );
}

#[then(
    "the description preserves metrics and interval with client-metrics sources and the default match"
)]
fn subscription_description(subscription: &MetricsSubscription) {
    assert_describe_response(
        subscription
            .described
            .as_ref()
            .expect("a subscription description"),
    );
}

#[when("the operator lists client metrics configuration resources")]
async fn list_subscriptions(subscription: &mut MetricsSubscription) {
    let request = ListConfigResourcesRequest {
        resource_types: vec![RESOURCE_TYPE_CLIENT_METRICS],
        ..Default::default()
    };
    subscription.listed = Some(
        subscription
            .client
            .as_ref()
            .expect("a client metrics cluster")
            .send(request)
            .await
            .expect("ListConfigResources"),
    );
}

#[then("subscription sub-a appears in the client metrics resource list")]
fn subscription_listed(subscription: &MetricsSubscription) {
    assert_list_response(
        subscription
            .listed
            .as_ref()
            .expect("a client metrics resource list"),
    );
}

#[scenario(
    path = "tests/features/migrated/tests/client_metrics_config.feature",
    name = "client metrics config alter describe list round trip"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_metrics_config_alter_describe_list_round_trip(subscription: MetricsSubscription) {
    shutdown_cluster(subscription.cluster).await;
}
