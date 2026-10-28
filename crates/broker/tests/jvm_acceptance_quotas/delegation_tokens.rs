//! The KIP-48 delegation-token lifecycle through `kafka-delegation-tokens`:
//! create, authenticate a producer with the minted credentials, describe, and
//! expire.
//!
//! The token producer runs through a directly spawned `docker run`, not the
//! shared helper, because `kafka-console-producer` needs its records on stdin.

use {
    crate::jvm_acceptance::{
        ADMIN, ADMIN_PASS, ClientPropsFile, KAFKA_IMAGE_TXN, SaslCluster, broker0_advertised,
        create_console_topic, docker_run_kafka_tool_with_image_and_mount, extract_jvm_kv,
        nc_check_connectivity, produce_console,
        start_three_broker_sasl_plaintext_jvm_cluster_with_delegation_tokens,
        wait_three_brokers_registered, write_client_props, write_plain_props,
    },
    assert2::assert,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::process::Output,
    tokio::sync::MutexGuard,
};

const TOPIC: &str = "krabka-deleg-token-itest";
struct TokenLifecycle {
    port_ownership: Option<MutexGuard<'static, ()>>,
    cluster: Option<SaslCluster>,
    admin: Option<ClientPropsFile>,
    created: Option<Output>,
    token_id: String,
    hmac: String,
    produced: Option<Output>,
    described: Option<Output>,
    expired: Option<Output>,
}
#[fixture]
fn token() -> TokenLifecycle {
    // Start this scenario with fresh inputs and observations.

    TokenLifecycle {
        port_ownership: None,
        cluster: None,
        admin: None,
        created: None,
        token_id: String::new(),
        hmac: String::new(),
        produced: None,
        described: None,
        expired: None,
    }
}
#[given("three registered SASL brokers share the delegation token master key")]
async fn token_cluster(token: &mut TokenLifecycle) {
    token.port_ownership = Some(crate::claim_broker_ports().await);
    let cluster = start_three_broker_sasl_plaintext_jvm_cluster_with_delegation_tokens(
        ADMIN,
        ADMIN_PASS,
        b"jvm-master-key",
    )
    .await;
    nc_check_connectivity();
    wait_three_brokers_registered(&cluster.0, &cluster.1, &cluster.2, 3).await;
    token.cluster = Some(cluster);
    token.admin = Some(write_plain_props(ADMIN, ADMIN_PASS));
}
#[when("the administrator creates a token with the broker default maximum lifetime")]
fn mint(token: &mut TokenLifecycle) {
    let admin_mount = token
        .admin
        .as_ref()
        .expect("administrator configured")
        .mount_str();
    let create_out = docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &admin_mount, &[
        "kafka-delegation-tokens",
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
        "--create",
        "--max-life-time-period",
        "-1",
    ]);
    let stdout = String::from_utf8_lossy(&create_out.stdout);
    token.token_id = extract_jvm_kv(&stdout, "TOKENID");
    token.hmac = extract_jvm_kv(&stdout, "HMAC");
    token.created = Some(create_out);
}
#[then("the issued token has a nonempty identifier and HMAC")]
fn issued(token: &TokenLifecycle) {
    let output = token.created.as_ref().expect("token creation completed");
    assert!(
        !token.token_id.is_empty(),
        "empty TOKENID: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        !token.hmac.is_empty(),
        "empty HMAC: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}
#[when("a SCRAM SHA 256 token authenticated producer writes hello to the administrator's topic")]
fn authenticate(token: &mut TokenLifecycle) {
    let token_id = &token.token_id;
    let hmac = &token.hmac;
    let props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-256\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required tokenauth=true username=\"{token_id}\" password=\"{hmac}\";\nenable.idempotence=false\nacks=1\n"
    ));
    let admin_mount = token
        .admin
        .as_ref()
        .expect("administrator configured")
        .mount_str();
    create_console_topic(KAFKA_IMAGE_TXN, &[&admin_mount], TOPIC, 1, 1);
    token.produced = Some(produce_console(
        KAFKA_IMAGE_TXN,
        &[&props.mount_str()],
        TOPIC,
        false,
        b"hello\n",
    ));
}
#[then("the token authenticated producer succeeds")]
fn accepted(token: &TokenLifecycle) {
    let output = token.produced.as_ref().expect("producer completed");
    assert!(
        output.status.success(),
        "token producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
#[when("the administrator describes tokens owned by User admin")]
fn describe(token: &mut TokenLifecycle) {
    let admin_mount = token
        .admin
        .as_ref()
        .expect("administrator configured")
        .mount_str();
    let desc_out = docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &admin_mount, &[
        "kafka-delegation-tokens",
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
        "--describe",
        "--owner-principal",
        "User:admin",
    ]);
    token.described = Some(desc_out);
}
#[then("the owner's token listing includes the issued identifier")]
fn listed(token: &TokenLifecycle) {
    let output = token
        .described
        .as_ref()
        .expect("token description completed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(&token.token_id),
        "describe stdout missing token_id={}: {stdout}",
        token.token_id
    );
}
#[when("the administrator expires the token immediately using its HMAC")]
fn expire(token: &mut TokenLifecycle) {
    let admin_mount = token
        .admin
        .as_ref()
        .expect("administrator configured")
        .mount_str();
    let hmac = &token.hmac;
    let exp_out = docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &admin_mount, &[
        "kafka-delegation-tokens",
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
        "--expire",
        "--expiry-time-period",
        "-1",
        "--hmac",
        hmac,
    ]);
    token.expired = Some(exp_out);
}
#[then("the expiration command succeeds")]
fn expired(token: &TokenLifecycle) {
    let output = token.expired.as_ref().expect("expiration completed");
    assert!(
        output.status.success(),
        "expire failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_quotas/delegation_tokens.feature",
    name = "An administrator issues uses inspects and expires a delegation token"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_kafka_delegation_tokens_end_to_end(token: TokenLifecycle) {
    token.shutdown().await;
    drop(token);
}

impl TokenLifecycle {
    async fn shutdown(&mut self) {
        let port_ownership = self.port_ownership.take();
        let (h1, h2, h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
            self.cluster.take().expect("token cluster started");
        h1.shutdown().await;
        h2.shutdown().await;
        h3.shutdown().await;
        drop(port_ownership);
    }
}
