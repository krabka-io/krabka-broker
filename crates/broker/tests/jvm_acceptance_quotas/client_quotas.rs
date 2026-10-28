//! Stock Kafka quota administration and visibility through committed metadata.
use {
    crate::{
        jvm_acceptance::{
            ADMIN, ADMIN_PASS, ClientPropsFile, KAFKA_IMAGE_TXN, SaslCluster, broker0_advertised,
            start_three_broker_sasl_plaintext_jvm_cluster, write_plain_props,
        },
        support::jvm_docker_command,
    },
    assert2::assert,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::process::Output,
    tokio::sync::MutexGuard,
};
struct QuotaAdministration {
    port_ownership: Option<MutexGuard<'static, ()>>,
    cluster: Option<SaslCluster>,
    admin: Option<ClientPropsFile>,
    entity: String,
    name: String,
    metadata_entity: String,
    quota: String,
    altered: Option<Output>,
    described: Option<Output>,
    deleted: Option<Output>,
}
#[fixture]
fn quota() -> QuotaAdministration {
    // Start this scenario with fresh inputs and observations.

    QuotaAdministration {
        port_ownership: None,
        cluster: None,
        admin: None,
        entity: String::new(),
        name: String::new(),
        metadata_entity: String::new(),
        quota: String::new(),
        altered: None,
        described: None,
        deleted: None,
    }
}
#[given("three registered SASL brokers accept the administrator and these extra PLAIN users")]
async fn cluster(quota: &mut QuotaAdministration, datatable: Vec<Vec<String>>) {
    quota.port_ownership = Some(crate::claim_broker_ports().await);
    let users: Vec<(String, String)> = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let credentials: [String; 2] = row.try_into().expect("username password");
            credentials.into()
        })
        .collect();
    let users: Vec<(&str, &str)> = users
        .iter()
        .map(|(name, password)| (name.as_str(), password.as_str()))
        .collect();
    quota.cluster = Some(
        Box::pin(
            start_three_broker_sasl_plaintext_jvm_cluster()
                .extra_users(&users)
                .wait_until_registered(true)
                .call(),
        )
        .await,
    );
    quota.admin = Some(write_plain_props(ADMIN, ADMIN_PASS));
}
#[given(
    "the operator addresses {entity} entity {name} recorded as metadata entity type {metadata_entity}"
)]
fn entity(quota: &mut QuotaAdministration, entity: String, name: String, metadata_entity: String) {
    quota.entity = entity;
    quota.name = name;
    quota.metadata_entity = metadata_entity;
}
fn run_config(quota: &QuotaAdministration, args: &[&str]) -> Output {
    let mount = quota
        .admin
        .as_ref()
        .expect("SASL administrator configured")
        .mount_str();
    let mut full = vec![
        "kafka-configs",
        "--entity-type",
        quota.entity.as_str(),
        "--entity-name",
        quota.name.as_str(),
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
    ];
    full.extend_from_slice(args);
    jvm_docker_command()
        .image(KAFKA_IMAGE_TXN)
        .mounts(&[&mount])
        .args(&full)
        .call()
        .output()
        .expect("run quota configuration tool")
}
#[when("the administrator sets quota {key} to {value}")]
fn alter(quota: &mut QuotaAdministration, key: String, value: String) {
    quota.altered = Some(run_config(quota, &[
        "--alter",
        "--add-config",
        &format!("{key}={value}"),
    ]));
    quota.quota = key;
}
#[then("the quota alteration command succeeds")]
fn altered(quota: &QuotaAdministration) {
    let output = quota.altered.as_ref().expect("quota alteration completed");
    assert!(
        output.status.success(),
        "quota alter failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
#[when("the administrator describes the addressed quota entity")]
fn describe(quota: &mut QuotaAdministration) {
    quota.described = Some(run_config(quota, &["--describe"]));
}
#[then("the successful quota report includes {setting}")]
fn visible(quota: &QuotaAdministration, setting: String) {
    let output = quota.described.as_ref().expect("quota describe completed");
    assert!(
        output.status.success(),
        "quota describe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(&setting),
        "quota report missing {setting}: {stdout}"
    );
}
#[when("the administrator deletes the configured quota key")]
fn delete(quota: &mut QuotaAdministration) {
    quota.deleted = Some(run_config(quota, &[
        "--alter",
        "--delete-config",
        quota.quota.as_str(),
    ]));
}
#[then("the deletion succeeds and the committed metadata image no longer contains that quota")]
async fn removed(quota: &QuotaAdministration) {
    let output = quota.deleted.as_ref().expect("quota deletion completed");
    assert!(
        output.status.success(),
        "delete-config failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let cluster = quota.cluster.as_ref().expect("SASL quota cluster started");
    cluster
        .0
        .wait_for_image(|image| {
            let key: krabka_metadata::EntityKey =
                vec![(quota.metadata_entity.clone(), Some(quota.name.clone()))];
            image
                .client_quotas()
                .get(&key)
                .and_then(|settings| settings.get(&quota.quota))
                .is_none()
        })
        .await;
}
impl QuotaAdministration {
    async fn shutdown(&mut self) {
        let port_ownership = self.port_ownership.take();
        let (h1, h2, h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
            self.cluster.take().expect("SASL quota cluster started");
        h1.shutdown().await;
        h2.shutdown().await;
        h3.shutdown().await;
        drop(port_ownership);
    }
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_quotas/client_quotas.feature",
    name = "A user producer byte quota is visible and can be removed from committed metadata"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_kafka_configs_alter_client_quota_end_to_end(quota: QuotaAdministration) {
    Box::pin(quota.shutdown()).await;
    drop(quota);
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_quotas/client_quotas.feature",
    name = "An IP connection creation quota is visible and can be removed from committed metadata"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_kafka_configs_alter_ip_quota_end_to_end(quota: QuotaAdministration) {
    Box::pin(quota.shutdown()).await;
    drop(quota);
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_quotas/client_quotas.feature",
    name = "A user controller mutation quota is visible and can be removed from committed metadata"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_kafka_configs_alter_controller_mutation_rate_end_to_end(quota: QuotaAdministration) {
    Box::pin(quota.shutdown()).await;
    drop(quota);
}
