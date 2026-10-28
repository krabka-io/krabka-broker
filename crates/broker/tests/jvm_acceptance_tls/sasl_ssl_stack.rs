//! Stock SCRAM clients authenticate and exchange records over TLS.
use {
    crate::jvm_acceptance::{
        ADMIN, ADMIN_PASS, ALICE, ALICE_PASS, ClientPropsFile, KAFKA_IMAGE_TXN,
        assert_numbered_console_values, assert_successful_jvm_output, consume_console,
        nc_check_connectivity, numbered_payload, prepare_jks_truststore, produce_console,
        provision_ssl_topic, start_sasl_ssl_broker,
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    std::process::Output,
    tempfile::TempDir,
};
const TOPIC: &str = "krabka-sasl-ssl-itest";
struct EncryptedRecords {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    admin: Option<ClientPropsFile>,
    client: Option<ClientPropsFile>,
    truststore_mount: String,
    produced: Option<Output>,
    consumed: Option<Output>,
}
#[rstest::fixture]
fn records() -> EncryptedRecords {
    // Start this scenario with fresh inputs and observations.

    EncryptedRecords {
        broker: None,
        directory: None,
        admin: None,
        client: None,
        truststore_mount: String::new(),
        produced: None,
        consumed: None,
    }
}
#[rstest_bdd_macros::given(
    "alice has a SCRAM SHA 512 credential and a topic on a trusted SASL SSL broker"
)]
async fn encrypted_identity(records: &mut EncryptedRecords) {
    let (broker, directory) = start_sasl_ssl_broker(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();
    let truststore = prepare_jks_truststore();
    let mount = format!("{}:/truststore.jks:ro", truststore.display());
    let (admin, client) =
        provision_ssl_topic((ADMIN, ADMIN_PASS), (ALICE, ALICE_PASS), &mount, TOPIC, 1);
    records.broker = Some(broker);
    records.directory = Some(directory);
    records.admin = Some(admin);
    records.client = Some(client);
    records.truststore_mount = mount;
}
#[rstest_bdd_macros::when("alice's encrypted console producer writes ten numbered records")]
fn produce(records: &mut EncryptedRecords) {
    let mount = records
        .client
        .as_ref()
        .expect("SCRAM client configured")
        .mount_str();
    records.produced = Some(produce_console(
        KAFKA_IMAGE_TXN,
        &[&mount, &records.truststore_mount],
        TOPIC,
        false,
        numbered_payload("msg", 10).as_bytes(),
    ));
}
#[rstest_bdd_macros::then("the SCRAM authenticated encrypted producer succeeds")]
fn accepted(records: &EncryptedRecords) {
    assert_successful_jvm_output(records.produced.as_ref().expect("producer completed"));
}
#[rstest_bdd_macros::when(
    "alice's encrypted console consumer reads ten records from the beginning"
)]
fn consume(records: &mut EncryptedRecords) {
    let mount = records
        .client
        .as_ref()
        .expect("SCRAM client configured")
        .mount_str();
    records.consumed = Some(consume_console(
        KAFKA_IMAGE_TXN,
        &[&mount, &records.truststore_mount],
        TOPIC,
        None,
        10,
        20000,
        false,
    ));
}
#[rstest_bdd_macros::then(
    "the SCRAM authenticated encrypted consumer succeeds and emits all ten records"
)]
fn decoded(records: &EncryptedRecords) {
    let output = records.consumed.as_ref().expect("consumer completed");
    assert!(
        output.status.success(),
        "consumer failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_numbered_console_values(output, "msg-", 10);
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_tls/sasl_ssl_stack.feature",
    name = "Stock SCRAM clients exchange records over trusted TLS"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jvm_sasl_ssl_full_stack(records: EncryptedRecords) {
    records
        .broker
        .expect("encrypted broker started")
        .shutdown()
        .await;
}
