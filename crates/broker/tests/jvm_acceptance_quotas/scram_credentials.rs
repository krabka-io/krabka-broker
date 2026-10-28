//! The KIP-554 read half, `api_key` 50: `kafka-configs --describe
//! --entity-type users` over a SCRAM credential the same tool provisioned.

use {
    crate::jvm_acceptance::{
        ADMIN, ADMIN_PASS, ClientPropsFile, KAFKA_IMAGE_TXN, SaslCluster, broker0_advertised,
        describe_console_entity, docker_run_kafka_tool_with_image_and_mount, nc_check_connectivity,
        start_three_broker_sasl_plaintext_jvm_cluster, write_plain_props,
    },
    assert2::assert,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::process::Output,
};

struct CredentialInspection {
    cluster: Option<SaslCluster>,
    admin: Option<ClientPropsFile>,
    altered: Option<Output>,
    described: Option<Output>,
}
#[fixture]
fn credentials() -> CredentialInspection {
    // Start this scenario with fresh inputs and observations.

    CredentialInspection {
        cluster: None,
        admin: None,
        altered: None,
        described: None,
    }
}
#[given("a three broker SASL cluster accepts the administrator's PLAIN identity")]
async fn secured_cluster(credentials: &mut CredentialInspection) {
    credentials.cluster =
        Some(Box::pin(start_three_broker_sasl_plaintext_jvm_cluster().call()).await);
    nc_check_connectivity();
    credentials.admin = Some(write_plain_props(ADMIN, ADMIN_PASS));
}
#[when("the administrator provisions alice with SCRAM SHA 512 and 4096 iterations")]
fn provision(credentials: &mut CredentialInspection) {
    let admin_mount = credentials
        .admin
        .as_ref()
        .expect("administrator configured")
        .mount_str();
    let alter = docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &admin_mount, &[
        "kafka-configs",
        "--alter",
        "--entity-type",
        "users",
        "--entity-name",
        "alice",
        "--add-config",
        "SCRAM-SHA-512=[iterations=4096,password=alice-secret]",
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
    ]);
    credentials.altered = Some(alter);
}
#[then("the credential alteration succeeds")]
fn provisioned(credentials: &CredentialInspection) {
    let alter = credentials
        .altered
        .as_ref()
        .expect("alter command completed");
    assert!(
        alter.status.success(),
        "alter SCRAM failed: {}",
        String::from_utf8_lossy(&alter.stderr)
    );
}
#[when("the administrator describes alice's credentials with the stock config tool")]
fn describe(credentials: &mut CredentialInspection) {
    let admin_mount = credentials
        .admin
        .as_ref()
        .expect("administrator configured")
        .mount_str();
    credentials.described = Some(describe_console_entity(&admin_mount, "users", "alice"));
}
#[then("the credential report includes SCRAM SHA 512")]
fn mechanism(credentials: &CredentialInspection) {
    let desc = credentials
        .described
        .as_ref()
        .expect("describe command completed");
    let stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(
        stdout.contains("SCRAM-SHA-512"),
        "expected SCRAM-SHA-512 in describe output: {stdout}"
    );
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_quotas/scram_credentials.feature",
    name = "Administrators inspect a provisioned SCRAM credential"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_kafka_configs_describe_users_scram_credentials_end_to_end(
    credentials: CredentialInspection,
) {
    drop(credentials);
}
