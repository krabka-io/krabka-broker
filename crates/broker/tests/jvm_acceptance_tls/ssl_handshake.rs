//! The bare TLS handshake against an `SSL`-only listener.
//!
//! This is the smallest of the TLS cases and the only one with no SASL layer
//! above it, so it sits on its own: a JVM client with a JKS truststore has to
//! complete the handshake and exchange one `ApiVersions` request, and nothing
//! else in the suite proves that in isolation.

use {
    crate::{
        jvm_acceptance::{
            ClientPropsFile, KAFKA_IMAGE, broker0_advertised, nc_check_connectivity,
            prepare_jks_truststore, start_ssl_broker, write_client_props,
        },
        support::jvm_docker_command,
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::{
        path::PathBuf,
        process::{Output, Stdio},
    },
    tempfile::TempDir,
};

struct EncryptedDiscovery {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    truststore: Option<PathBuf>,
    properties: Option<ClientPropsFile>,
    output: Option<Output>,
}
#[fixture]
fn discovery() -> EncryptedDiscovery {
    // Start this scenario with fresh inputs and observations.

    EncryptedDiscovery {
        broker: None,
        directory: None,
        truststore: None,
        properties: None,
        output: None,
    }
}
#[given("an SSL only broker and a JVM truststore containing its development certificate")]
async fn trusted_listener(discovery: &mut EncryptedDiscovery) {
    let (broker, directory) = start_ssl_broker().await;
    nc_check_connectivity();
    discovery.truststore = Some(prepare_jks_truststore());
    discovery.properties = Some(write_client_props(
        "security.protocol=SSL\nssl.truststore.location=/truststore.jks\nssl.truststore.password=changeit\nssl.endpoint.identification.algorithm=\n",
    ));
    discovery.broker = Some(broker);
    discovery.directory = Some(directory);
}
#[when("the stock broker API versions tool connects through the encrypted listener")]
fn api_versions(discovery: &mut EncryptedDiscovery) {
    let props = discovery
        .properties
        .as_ref()
        .expect("SSL properties configured");
    let truststore = discovery
        .truststore
        .as_ref()
        .expect("JVM truststore prepared");
    let mount = format!("{}:/truststore.jks:ro", truststore.display());
    let out = jvm_docker_command()
        .image(KAFKA_IMAGE)
        .mounts(&[&props.mount_str(), &mount])
        .args(&[
            "kafka-broker-api-versions",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ])
        .call()
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("spawn kafka-broker-api-versions");
    discovery.output = Some(out);
}
#[then("TLS negotiation and API discovery complete successfully")]
fn handshake(discovery: &EncryptedDiscovery) {
    let output = discovery
        .output
        .as_ref()
        .expect("API versions command completed");
    assert!(
        output.status.success(),
        "ssl handshake failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_tls/ssl_handshake.feature",
    name = "A JVM client discovers broker APIs over trusted TLS"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jvm_ssl_handshake_succeeds(discovery: EncryptedDiscovery) {
    discovery
        .broker
        .expect("SSL broker started")
        .shutdown()
        .await;
}
