//! The authorized produce-and-consume round-trip for a principal that holds
//! literal topic and group ACLs.
//!
//! It is the positive half of the ACL enforcement cases, and it stands alone
//! because it is the only one that walks the whole consumer-group path --
//! `JoinGroup`, `OffsetFetch` and `OffsetCommit` -- through the authorize
//! preamble to a successful read.

use {
    crate::jvm_acceptance::{
        ALICE, ALICE_PASS, ClientPropsFile, KAFKA_IMAGE_TXN, add_console_acl,
        assert_console_produced, assert_numbered_console_values, broker0_advertised,
        docker_run_kafka_tool_with_image_and_mount, numbered_payload, plain_jaas, produce_console,
        start_plain_acl_topic_with_ops, write_client_props,
    },
    krabka_broker::BrokerHandle,
    std::process::Output,
    tempfile::TempDir,
};
const TOPIC: &str = "foo";
const GROUP: &str = "cg-foo";
struct AuthorizedRecords {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    client: Option<ClientPropsFile>,
    produced: Option<Output>,
    consumed: Option<Output>,
}
#[rstest::fixture]
fn authorized() -> AuthorizedRecords {
    // Start this scenario with fresh inputs and observations.

    AuthorizedRecords {
        broker: None,
        directory: None,
        client: None,
        produced: None,
        consumed: None,
    }
}
#[rstest_bdd_macros::given(
    "ordinary user alice has literal Read Write on topic foo and Read on group cg foo"
)]
async fn grants(authorized: &mut AuthorizedRecords) {
    let (broker, directory, admin_props) =
        start_plain_acl_topic_with_ops(TOPIC, ALICE, ALICE_PASS, &["Read", "Write"]).await;
    let admin_mount = admin_props.mount_str();

    // Allow Read on Group cg-foo for User:alice. ACL implications grant Describe
    // from Read on the same group resource, so no explicit Describe is
    // needed.
    add_console_acl(
        KAFKA_IMAGE_TXN,
        &[&admin_mount],
        "User:alice",
        &["Read"],
        "--group",
        GROUP,
    );

    // ---- Alice step: produce + consume over PLAIN as an ordinary user.
    //
    // `enable.idempotence=false` is required: cp-kafka 7.5 producers default
    // to idempotent mode, which sends `InitProducerId` without a
    // transactional id and so checks `Cluster IdempotentWrite` — a
    // cluster-scoped ACL that alice (a non-super-user with only topic +
    // group ACLs) doesn't hold. Falling back to the non-idempotent path
    // keeps alice's required ACL set bounded to what the plan calls out.
    let alice_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n\
         enable.idempotence=false\n\
         acks=1\n",
        plain_jaas(ALICE, ALICE_PASS),
    ));
    authorized.broker = Some(broker);
    authorized.directory = Some(directory);
    authorized.client = Some(alice_props);
}
#[rstest_bdd_macros::when("alice's non idempotent stock producer writes ten records to foo")]
fn write(authorized: &mut AuthorizedRecords) {
    let alice_mount = authorized
        .client
        .as_ref()
        .expect("alice properties configured")
        .mount_str();
    let payload = numbered_payload("msg", 10);
    let producer_out = produce_console(
        KAFKA_IMAGE_TXN,
        &[&alice_mount],
        TOPIC,
        false,
        payload.as_bytes(),
    );
    authorized.produced = Some(producer_out);
}
#[rstest_bdd_macros::then("alice's authorized producer succeeds")]
fn acknowledged(authorized: &AuthorizedRecords) {
    assert_console_produced(authorized.produced.as_ref().expect("producer completed"));
}
#[rstest_bdd_macros::when("alice's stock consumer joins cg foo and reads ten records")]
fn read(authorized: &mut AuthorizedRecords) {
    let alice_mount = authorized
        .client
        .as_ref()
        .expect("alice properties configured")
        .mount_str();
    let consumer_out =
        docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &alice_mount, &[
            "kafka-console-consumer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--group",
            GROUP,
            "--from-beginning",
            "--max-messages",
            "10",
            "--timeout-ms",
            "30000",
            "--consumer.config",
            "/client.properties",
        ]);
    authorized.consumed = Some(consumer_out);
}
#[rstest_bdd_macros::then("the authorized group consumer emits every numbered record")]
fn decoded(authorized: &AuthorizedRecords) {
    let output = authorized.consumed.as_ref().expect("consumer completed");
    assert_numbered_console_values(output, "msg-", 10);
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_sasl/acl_authorized.feature",
    name = "Literal topic and group grants permit the complete client record workflow"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jvm_authorized_produce_consume(authorized: AuthorizedRecords) {
    authorized
        .broker
        .expect("ACL broker started")
        .shutdown()
        .await;
}
