//! Real authenticated console operations shared by the SASL mechanism stories.

use {
    crate::{
        jvm_acceptance::{
            ADMIN, ADMIN_PASS, ALICE, ALICE_PASS, ClientPropsFile, KAFKA_IMAGE, KAFKA_IMAGE_TXN,
            broker0_advertised, consume_console, create_console_topic,
            docker_run_kafka_tool_with_image_and_mount, nc_check_connectivity, numbered_payload,
            oauthbearer_jaas, plain_jaas, produce_console, provision_plain_scram, scram_jaas,
            start_dual_mech_broker, start_dual_mech_broker_with_reauth, start_oauthbearer_broker,
            start_sasl_plaintext_broker, write_client_props, write_plain_props,
        },
        support::jvm_docker_command,
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_units::secs,
    rstest::fixture,
    rstest_bdd_macros::{given, then, when},
    std::process::Output,
    tempfile::TempDir,
};

pub struct Authentication {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    admin: Option<ClientPropsFile>,
    client: Option<ClientPropsFile>,
    image: &'static str,
    topic: String,
    created: Option<Output>,
    produced: Option<Output>,
    consumed: Option<Output>,
    reauthenticated: Option<Output>,
}

#[fixture]
pub fn authentication() -> Authentication {
    // Start this scenario with fresh inputs and observations.

    Authentication {
        broker: None,
        directory: None,
        admin: None,
        client: None,
        image: KAFKA_IMAGE,
        topic: String::new(),
        created: None,
        produced: None,
        consumed: None,
        reauthenticated: None,
    }
}

#[given("PLAIN user alice has password wonderland on topic {topic}")]
async fn plain_identity(authentication: &mut Authentication, topic: String) {
    let (broker, directory) = start_sasl_plaintext_broker(&[("alice", "wonderland")]).await;
    nc_check_connectivity();
    let props = format!(
        "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.jaas.config={}\n",
        plain_jaas("alice", "wonderland")
    );
    authentication.client = Some(write_client_props(&props));
    authentication.broker = Some(broker);
    authentication.directory = Some(directory);
    authentication.topic = topic;
}

#[given("an unsecured OAUTHBEARER token identifies admin on topic {topic}")]
async fn oauth_identity(authentication: &mut Authentication, topic: String) {
    let (broker, directory) = start_oauthbearer_broker().await;
    nc_check_connectivity();
    let props = format!(
        "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=OAUTHBEARER\nsasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler\nsasl.jaas.config={}\n",
        oauthbearer_jaas("admin")
    );
    authentication.client = Some(write_client_props(&props));
    authentication.broker = Some(broker);
    authentication.directory = Some(directory);
    authentication.topic = topic;
}

fn scram_properties(mechanism: &str) -> ClientPropsFile {
    write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\nsasl.mechanism={mechanism}\nsasl.jaas.config={}\nenable.idempotence=false\nacks=1\n",
        scram_jaas(ALICE, ALICE_PASS)
    ))
}

fn grant_topic(admin: &ClientPropsFile, topic: &str, operations: &[&str]) {
    for operation in operations {
        docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &admin.mount_str(), &[
            "kafka-acls",
            "--add",
            "--allow-principal",
            &format!("User:{ALICE}"),
            "--operation",
            operation,
            "--topic",
            topic,
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ]);
    }
}

#[given("{mechanism} credentials for alice permit Read and Write on topic {topic}")]
async fn scram_identity(authentication: &mut Authentication, mechanism: String, topic: String) {
    let (broker, directory) = start_dual_mech_broker(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();
    let admin = write_plain_props(ADMIN, ADMIN_PASS);
    provision_plain_scram(&admin.mount_str(), ALICE, ALICE_PASS, &mechanism);
    create_console_topic(KAFKA_IMAGE_TXN, &[&admin.mount_str()], &topic, 1, 1);
    grant_topic(&admin, &topic, &["Read", "Write"]);
    authentication.client = Some(scram_properties(&mechanism));
    authentication.admin = Some(admin);
    authentication.broker = Some(broker);
    authentication.directory = Some(directory);
    authentication.image = KAFKA_IMAGE_TXN;
    authentication.topic = topic;
}

#[given("a SCRAM-SHA-512 writer has a two-second reauthentication window on topic {topic}")]
async fn reauth_identity(authentication: &mut Authentication, topic: String) {
    let (broker, directory) =
        start_dual_mech_broker_with_reauth(ADMIN, ADMIN_PASS, Some(secs(2))).await;
    nc_check_connectivity();
    let admin = write_plain_props(ADMIN, ADMIN_PASS);
    provision_plain_scram(&admin.mount_str(), ALICE, ALICE_PASS, "SCRAM-SHA-512");
    create_console_topic(KAFKA_IMAGE_TXN, &[&admin.mount_str()], &topic, 1, 1);
    grant_topic(&admin, &topic, &["Write"]);
    authentication.client = Some(scram_properties("SCRAM-SHA-512"));
    authentication.admin = Some(admin);
    authentication.broker = Some(broker);
    authentication.directory = Some(directory);
    authentication.image = KAFKA_IMAGE_TXN;
    authentication.topic = topic;
}

#[when("the authenticated JVM tool creates its topic")]
fn create_authenticated_topic(authentication: &mut Authentication) {
    let mount = authentication
        .client
        .as_ref()
        .expect("client authentication was configured")
        .mount_str();
    authentication.created = Some(
        jvm_docker_command()
            .image(authentication.image)
            .mounts(&[&mount])
            .args(&[
                "kafka-topics",
                "--create",
                "--topic",
                &authentication.topic,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
                "--bootstrap-server",
                broker0_advertised(),
                "--command-config",
                "/client.properties",
            ])
            .call()
            .output()
            .expect("run authenticated topic creation"),
    );
}

#[then("authenticated topic creation succeeds")]
fn authenticated_creation(authentication: &Authentication) {
    let output = authentication
        .created
        .as_ref()
        .expect("topic creation completed");
    assert!(
        output.status.success(),
        "authenticated topic creation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[when("the authenticated console producer writes ten numbered records")]
fn produce_authenticated(authentication: &mut Authentication) {
    let mount = authentication
        .client
        .as_ref()
        .expect("client authentication was configured")
        .mount_str();
    authentication.produced = Some(produce_console(
        authentication.image,
        &[&mount],
        &authentication.topic,
        false,
        numbered_payload("msg", 10).as_bytes(),
    ));
}

#[then("the authenticated producer exits successfully")]
fn authenticated_production(authentication: &Authentication) {
    let output = authentication
        .produced
        .as_ref()
        .expect("the producer completed");
    assert!(
        output.status.success(),
        "producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[when("the authenticated console consumer reads ten records")]
fn consume_authenticated(authentication: &mut Authentication) {
    let mount = authentication
        .client
        .as_ref()
        .expect("client authentication was configured")
        .mount_str();
    authentication.consumed = Some(consume_console(
        authentication.image,
        &[&mount],
        &authentication.topic,
        None,
        10,
        20_000,
        false,
    ));
}

#[then("the authenticated consumer succeeds and returns all ten numbered values")]
fn authenticated_values(authentication: &Authentication) {
    let output = authentication
        .consumed
        .as_ref()
        .expect("the consumer completed");
    assert!(
        output.status.success(),
        "consumer failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    for index in 0..10 {
        let needle = format!("msg-{index:04}");
        assert!(
            stdout.contains(&needle),
            "consumer missing {needle}: {stdout:?}"
        );
    }
}

#[when("the JVM verifiable producer sends ten records at one record per second")]
fn produce_across_windows(authentication: &mut Authentication) {
    let mount = authentication
        .client
        .as_ref()
        .expect("client authentication was configured")
        .mount_str();
    authentication.reauthenticated = Some(
        jvm_docker_command()
            .image(authentication.image)
            .mounts(&[&mount])
            .args(&[
                "kafka-verifiable-producer",
                "--bootstrap-server",
                broker0_advertised(),
                "--topic",
                &authentication.topic,
                "--max-messages",
                "10",
                "--throughput",
                "1",
                "--producer.config",
                "/client.properties",
            ])
            .call()
            .output()
            .expect("run verifiable producer"),
    );
}

#[then("all ten records are acknowledged without a send error across reauthentication windows")]
fn successful_reauth(authentication: &Authentication) {
    let output = authentication
        .reauthenticated
        .as_ref()
        .expect("the verifiable producer completed");
    assert!(
        output.status.success(),
        "verifiable producer failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let acknowledged = stdout.matches("producer_send_success").count();
    assert!(
        acknowledged == 10,
        "expected ten acknowledged records across reauthentication windows, got {acknowledged}: {stdout}"
    );
    assert!(
        !stdout.contains("producer_send_error"),
        "producer reported send errors under in-band reauthentication: {stdout}"
    );
}

pub async fn shutdown_authentication(authentication: Authentication) {
    authentication
        .broker
        .expect("an authenticated broker was started")
        .shutdown()
        .await;
}
