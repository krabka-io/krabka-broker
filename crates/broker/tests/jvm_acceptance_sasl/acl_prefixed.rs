//! The prefixed topic ACL case: a `PREFIXED` pattern grants exactly the topics
//! that carry the prefix and nothing else.
//!
//! The prefix pattern gets its own file because the case needs both outcomes
//! from one grant -- an allowed topic and a denied one -- and so seeds two
//! topics and runs two consumers, which no other ACL case does.

use {
    crate::jvm_acceptance::{
        ADMIN, ADMIN_PASS, ALICE, ALICE_PASS, ClientPropsFile, KAFKA_IMAGE_TXN, broker0_advertised,
        create_console_topic, denied_console_consumer, docker_run_kafka_tool_with_image_and_mount,
        nc_check_connectivity, plain_jaas, produce_console,
        start_sasl_plaintext_broker_with_super_user, write_client_props, write_plain_props,
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    std::process::Output,
    tempfile::TempDir,
};
const PREFIX: &str = "team-";
const TOPIC_OK: &str = "team-foo";
const TOPIC_DENIED: &str = "other-foo";
const GROUP: &str = "cg-prefixed";
struct PrefixAuthorization {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    client: Option<ClientPropsFile>,
    allowed: Option<Output>,
    denied: Option<Output>,
}
#[rstest::fixture]
fn authorization() -> PrefixAuthorization {
    // Start this scenario with fresh inputs and observations.

    PrefixAuthorization {
        broker: None,
        directory: None,
        client: None,
        allowed: None,
        denied: None,
    }
}
#[rstest_bdd_macros::given(
    "alice can read the team prefix and her group while both matching and unrelated topics contain a seed record"
)]
async fn prefix_grant(authorization: &mut PrefixAuthorization) {
    let (broker, directory) = start_sasl_plaintext_broker_with_super_user(ADMIN, &[
        (ADMIN, ADMIN_PASS),
        (ALICE, ALICE_PASS),
    ])
    .await;
    nc_check_connectivity();

    let admin_props = write_plain_props(ADMIN, ADMIN_PASS);
    let admin_mount = admin_props.mount_str();

    // Pre-create both topics.
    for topic in [TOPIC_OK, TOPIC_DENIED] {
        create_console_topic(KAFKA_IMAGE_TXN, &[&admin_mount], topic, 1, 1);
    }

    // Prefixed Read on `team-*` for alice. ACL implications grant Describe from
    // Read on the same topic resource.
    docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &admin_mount, &[
        "kafka-acls",
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
        "--add",
        "--allow-principal",
        "User:alice",
        "--operation",
        "Read",
        "--resource-pattern-type",
        "prefixed",
        "--topic",
        PREFIX,
    ]);

    // Literal Read on group `cg-prefixed`. ACL implications grant Describe from
    // Read on the same group resource.
    docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &admin_mount, &[
        "kafka-acls",
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
        "--add",
        "--allow-principal",
        "User:alice",
        "--operation",
        "Read",
        "--group",
        GROUP,
    ]);

    // Seed one record into each topic as admin (super-user bypasses authorize).
    let admin_producer_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n\
         enable.idempotence=false\n\
         acks=1\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_producer_mount = admin_producer_props.mount_str();

    for topic in [TOPIC_OK, TOPIC_DENIED] {
        let seed_out = produce_console(
            KAFKA_IMAGE_TXN,
            &[&admin_producer_mount],
            topic,
            false,
            format!("seed-{topic}\n").as_bytes(),
        );
        assert!(
            seed_out.status.success(),
            "admin seed producer failed for {topic}: stderr={}",
            String::from_utf8_lossy(&seed_out.stderr),
        );
    }

    authorization.broker = Some(broker);
    authorization.directory = Some(directory);
    authorization.client = Some(write_plain_props(ALICE, ALICE_PASS));
}
#[rstest_bdd_macros::when("alice consumes from the topic covered by her prefix grant")]
fn allowed_read(authorization: &mut PrefixAuthorization) {
    let alice_mount = authorization
        .client
        .as_ref()
        .expect("alice configured")
        .mount_str();
    let consumer_out =
        docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &alice_mount, &[
            "kafka-console-consumer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC_OK,
            "--group",
            GROUP,
            "--from-beginning",
            "--max-messages",
            "1",
            "--timeout-ms",
            "30000",
            "--consumer.config",
            "/client.properties",
        ]);
    authorization.allowed = Some(consumer_out);
}
#[rstest_bdd_macros::then("alice receives the matching topic's seeded record")]
fn visible(authorization: &PrefixAuthorization) {
    let output = authorization
        .allowed
        .as_ref()
        .expect("matching topic was consumed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let needle = format!("seed-{TOPIC_OK}");
    assert!(
        stdout.contains(&needle),
        "alice should read {needle} from prefixed topic; got: {stdout}"
    );
}
#[rstest_bdd_macros::when(
    "alice consumes from the unrelated topic using the same authorized group"
)]
fn denied_read(authorization: &mut PrefixAuthorization) {
    let alice_mount = authorization
        .client
        .as_ref()
        .expect("alice configured")
        .mount_str();
    authorization.denied = Some(denied_console_consumer(
        &alice_mount,
        TOPIC_DENIED,
        GROUP,
        "spawn alice denied consumer",
    ));
}
#[rstest_bdd_macros::then("the unrelated topic is refused with TopicAuthorizationException")]
fn refused(authorization: &PrefixAuthorization) {
    let output = authorization
        .denied
        .as_ref()
        .expect("unrelated topic was requested");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stderr.contains("TopicAuthorizationException"),
        "alice should be denied on {TOPIC_DENIED}; stderr={stderr} stdout={stdout}"
    );
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_sasl/acl_prefixed.feature",
    name = "A prefix read grant permits only matching topic names"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jvm_prefixed_topic_acl_works(authorization: PrefixAuthorization) {
    authorization
        .broker
        .expect("ACL broker started")
        .shutdown()
        .await;
}
