//! The config surface of the role: `broker.witness` is controller-managed, so
//! `DescribeConfigs` reports it read-only and `IncrementalAlterConfigs` refuses
//! to change it with `INVALID_CONFIG`.
//!
//! This is the only test in the suite that needs no topic and no traffic, and
//! the only one that speaks the config APIs, so it carries their request shapes
//! on its own.

use {
    crate::{
        BROKER_WITNESS, CONFIG_SOURCE_DYNAMIC_BROKER, CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
        RESOURCE_TYPE_BROKER, SITE_A, STRETCH_PREFERRED_LEADER_SITE, WITNESS_ID, cluster_lock,
        support::configs::{
            ConfigOperation, describe_resource, incremental_config, incremental_request,
            incremental_resource,
        },
        witness_cluster::{client_at, shutdown, start_stretch_cluster},
    },
    assert2::check,
    krabka_broker::{BrokerConfig, BrokerHandle, codes},
    krabka_client_core::Client,
    krabka_protocol::owned::{
        describe_configs_request::DescribeConfigsRequest,
        describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResponse},
        incremental_alter_configs_response::IncrementalAlterConfigsResponse,
    },
    std::mem,
    tempfile::TempDir,
    tokio::sync::MutexGuard,
};

/// `DescribeConfigsResponse.ConfigType::BOOLEAN`, the byte the JVM
/// `AdminClient` parses `broker.witness` with.
const CONFIG_TYPE_BOOLEAN: i8 = 1;

/// `DescribeConfigsResponse.ConfigType::STRING`, for the rack name in
/// `stretch.preferred.leader.site`.
const CONFIG_TYPE_STRING: i8 = 2;

#[derive(Default)]
struct WitnessConfig {
    guard: Option<MutexGuard<'static, ()>>,
    cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>,
    client: Option<Client>,
    described: Option<DescribeConfigsResponse>,
    altered: Option<IncrementalAlterConfigsResponse>,
}

#[rstest::fixture]
fn witness_config() -> WitnessConfig {
    // The controller role and its wire responses belong to this scenario.
    WitnessConfig::default()
}

impl WitnessConfig {
    fn client(&self) -> &Client { self.client.as_ref().expect("witness connection") }
}

#[rstest_bdd_macros::given(
    "a three-site cluster registers broker 3 as its witness with preferred leader site A"
)]
async fn configured_witness(witness_config: &mut WitnessConfig) {
    witness_config.guard = Some(cluster_lock().lock().await);
    witness_config.cluster = start_stretch_cluster().await;
    witness_config.client =
        Some(client_at(&witness_config.cluster[2].1.listen_addr.to_string()).await);
}

#[rstest_bdd_macros::when(
    "broker 3 describes all of its broker configs without synonyms or documentation"
)]
async fn describe_witness_config(witness_config: &mut WitnessConfig) {
    witness_config.described = Some(
        witness_config
            .client()
            .send(DescribeConfigsRequest {
                resources: vec![describe_resource(
                    RESOURCE_TYPE_BROKER,
                    WITNESS_ID.to_string(),
                    None,
                )],
                include_synonyms: false,
                include_documentation: false,
                ..Default::default()
            })
            .await
            .expect("DescribeConfigs for the witness broker"),
    );
}

#[rstest_bdd_macros::then(
    "broker.witness is true, read-only boolean from the broker and the preferred site is a read-only cluster-default string"
)]
fn readonly_witness_config(witness_config: &WitnessConfig) {
    let result = &witness_config
        .described
        .as_ref()
        .expect("configs described")
        .results[0];
    check!(result.error_code == codes::NONE, "DescribeConfigs succeeds");

    let witness_entry = result
        .configs
        .iter()
        .find(|entry| entry.name == BROKER_WITNESS)
        .cloned();
    check!(
        witness_entry
            == Some(DescribeConfigsResourceResult {
                name: BROKER_WITNESS.into(),
                value: Some("true".into()),
                read_only: true,
                config_source: CONFIG_SOURCE_DYNAMIC_BROKER,
                config_type: CONFIG_TYPE_BOOLEAN,
                // Not requested, so null, as Kafka sends it.
                documentation: None,
                ..Default::default()
            }),
        "broker.witness is reported, read-only, and typed"
    );
    let site_entry = result
        .configs
        .iter()
        .find(|entry| entry.name == STRETCH_PREFERRED_LEADER_SITE)
        .cloned();
    check!(
        site_entry
            == Some(DescribeConfigsResourceResult {
                name: STRETCH_PREFERRED_LEADER_SITE.into(),
                value: Some(SITE_A.into()),
                read_only: true,
                config_source: CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                config_type: CONFIG_TYPE_STRING,
                // Not requested, so null, as Kafka sends it.
                documentation: None,
                ..Default::default()
            }),
        "the preferred leader site is a read-only cluster default"
    );
}

#[rstest_bdd_macros::when(
    "the operator tries to set broker.witness to false through IncrementalAlterConfigs"
)]
async fn alter_witness_config(witness_config: &mut WitnessConfig) {
    witness_config.altered = Some(
        witness_config
            .client()
            .send(
                incremental_request()
                    .resources(vec![incremental_resource(
                        RESOURCE_TYPE_BROKER,
                        WITNESS_ID.to_string(),
                        vec![incremental_config(
                            BROKER_WITNESS,
                            Some("false".into()),
                            ConfigOperation::Set,
                        )],
                    )])
                    .call(),
            )
            .await
            .expect("IncrementalAlterConfigs round-trip"),
    );
}

#[rstest_bdd_macros::then("the witness rejects the change with INVALID_CONFIG")]
fn rejected_witness_config(witness_config: &WitnessConfig) {
    check!(
        witness_config
            .altered
            .as_ref()
            .expect("alter attempted")
            .responses[0]
            .error_code
            == codes::INVALID_CONFIG,
        "an operator cannot turn the witness role off through the config API: {:?}",
        witness_config
            .altered
            .as_ref()
            .expect("alter attempted")
            .responses[0]
            .error_message
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/witness_role/witness_config_surface.feature",
    name = "witness role is a read only broker config"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn witness_role_is_a_read_only_broker_config(witness_config: WitnessConfig) {
    witness_config.shutdown().await;
    drop(witness_config);
}

impl WitnessConfig {
    async fn shutdown(&mut self) {
        let Self {
            guard,
            cluster,
            client,
            ..
        } = mem::take(self);
        client.expect("witness connection").close();
        shutdown(cluster).await;
        drop(guard);
    }
}
