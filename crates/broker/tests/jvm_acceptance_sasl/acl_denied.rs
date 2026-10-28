//! The ACL denial cases: a producer with no topic ACL, and a consumer with a
//! topic ACL but no group ACL.
//!
//! Both live here because they share an assertion strategy that the positive
//! cases do not need: the JVM console tools exit zero even when the broker
//! refuses them, so each case asserts on the exception name the client logs to
//! stderr.

use {
    crate::jvm_acceptance::{
        ADMIN, ADMIN_PASS, ALICE, ALICE_PASS, ClientPropsFile, KAFKA_IMAGE_TXN, add_console_acl,
        create_console_topic, denied_console_consumer, nc_check_connectivity, plain_jaas,
        produce_console, start_plain_acl_topic_with_ops,
        start_sasl_plaintext_broker_with_super_user, write_client_props, write_plain_props,
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    std::process::Output,
    tempfile::TempDir,
};
const TOPIC: &str = "foo";
const BOB: &str = "bob";
const BOB_PASS: &str = "bob-secret";
struct DeniedIdentity {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    client: Option<ClientPropsFile>,
    result: Option<Output>,
}
#[rstest::fixture]
fn denied() -> DeniedIdentity {
    // Start this scenario with fresh inputs and observations.

    DeniedIdentity {
        broker: None,
        directory: None,
        client: None,
        result: None,
    }
}
#[rstest_bdd_macros::given(
    "bob authenticates successfully but only alice has topic Read and Write grants"
)]
async fn missing_topic_grant(denied: &mut DeniedIdentity) {
    let (broker, directory) = start_sasl_plaintext_broker_with_super_user(ADMIN, &[
        (ADMIN, ADMIN_PASS),
        (ALICE, ALICE_PASS),
        (BOB, BOB_PASS),
    ])
    .await;
    nc_check_connectivity();

    // ---- Admin step: pre-create topic + provision alice (not bob).
    let admin_props = write_plain_props(ADMIN, ADMIN_PASS);
    let admin_mount = admin_props.mount_str();

    create_console_topic(KAFKA_IMAGE_TXN, &[&admin_mount], TOPIC, 1, 1);

    // alice gets Read+Write — proves that the broker has ACLs configured
    // (i.e. the empty-ACL ALLOW shim is not active). ACL implications grant
    // Describe from Read/Write so no explicit Describe ACL is needed.
    add_console_acl(
        KAFKA_IMAGE_TXN,
        &[&admin_mount],
        "User:alice",
        &["Read", "Write"],
        "--topic",
        TOPIC,
    );

    // ---- Bob step: attempt to produce. Expect stderr to contain
    //               TopicAuthorizationException.
    let bob_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n\
         enable.idempotence=false\n\
         acks=1\n",
        plain_jaas(BOB, BOB_PASS),
    ));
    denied.broker = Some(broker);
    denied.directory = Some(directory);
    denied.client = Some(bob_props);
}
#[rstest_bdd_macros::given("alice can read foo but has no permission on consumer group cg other")]
async fn missing_group_grant(denied: &mut DeniedIdentity) {
    let (broker, directory, _admin) =
        start_plain_acl_topic_with_ops(TOPIC, ALICE, ALICE_PASS, &["Read"]).await;
    denied.broker = Some(broker);
    denied.directory = Some(directory);
    denied.client = Some(write_plain_props(ALICE, ALICE_PASS));
}
#[rstest_bdd_macros::when("bob's ordinary producer submits an unauthorized record to foo")]
fn produce(denied: &mut DeniedIdentity) {
    let mount = denied
        .client
        .as_ref()
        .expect("bob identity configured")
        .mount_str();
    denied.result = Some(produce_console(
        KAFKA_IMAGE_TXN,
        &[&mount],
        TOPIC,
        false,
        b"unauth-msg\n",
    ));
}
#[rstest_bdd_macros::when("alice's consumer joins cg other to read foo")]
fn consume(denied: &mut DeniedIdentity) {
    let mount = denied
        .client
        .as_ref()
        .expect("alice identity configured")
        .mount_str();
    denied.result = Some(denied_console_consumer(
        &mount,
        TOPIC,
        "cg-other",
        "spawn alice consumer",
    ));
}
#[rstest_bdd_macros::then("the stock client logs {exception}")]
fn exception(denied: &DeniedIdentity, exception: String) {
    let output = denied.result.as_ref().expect("stock client completed");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stderr.contains(&exception),
        "client should log {exception}; stderr={stderr} stdout={stdout}"
    );
}

#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_sasl/acl_denied.feature",
    name = "Valid credentials do not grant topic writes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jvm_unauthorized_produce_fails(denied: DeniedIdentity) {
    denied.broker.expect("ACL broker started").shutdown().await;
}

#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_sasl/acl_denied.feature",
    name = "Topic read permission does not grant consumer group access"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jvm_unauthorized_consumer_fails_group_check(denied: DeniedIdentity) {
    denied.broker.expect("ACL broker started").shutdown().await;
}
