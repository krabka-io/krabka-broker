//! The single-broker `SASL_SSL` stack: TLS handshake, then SCRAM-SHA-512 over
//! the encrypted channel, then a produce-and-consume round trip.
//!
//! This is the production-shape client auth path, and it is the only case in
//! the suite that provisions a SCRAM credential and drives records through one
//! broker. It keeps its own file because the two-broker tests differ from it in
//! what they assert, replication rather than the client-side auth stack.

use crate::jvm_acceptance::{
    KAFKA_IMAGE_TXN, broker0_advertised, docker_run_kafka_tool_with_image_and_mounts,
    nc_check_connectivity, prepare_jks_truststore, start_sasl_ssl_broker,
};

/// End-to-end `SASL_SSL` drive of the JVM tools. This is the
/// production-shape auth path: a TLS handshake, then a SCRAM-SHA-512 SASL
/// exchange over the encrypted channel. It mirrors
/// `jvm_sasl_scram_sha512_produce_consume`, but swaps the `SASL_PLAINTEXT`
/// listener for `SASL_SSL` and gives the JVM client a JKS truststore.
///
/// The test uses cp-kafka:7.5.0, so admin's `kafka-configs --alter
/// --entity-type users --add-config 'SCRAM-SHA-512=[...]'` translates to
/// KIP-554's `AlterUserScramCredentials (api_key 51)` rather than the legacy
/// `IncrementalAlterConfigs (44)` path that the broker does not implement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_sasl_ssl_full_stack() {
    const TOPIC: &str = "krabka-sasl-ssl-itest";
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";

    let (broker, _dir) = start_sasl_ssl_broker(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();
    let truststore_path = prepare_jks_truststore();
    let ts_mount = format!("{}:/truststore.jks:ro", truststore_path.display());

    // Step A: provision alice's SCRAM-SHA-512 credential via admin/PLAIN
    // over the SASL_SSL listener.
    let (admin_props, alice_props) = crate::jvm_acceptance::provision_ssl_scram_sha512(
        ADMIN, ADMIN_PASS, ALICE, ALICE_PASS, &ts_mount,
    );
    let alice_props_mount = alice_props.mount_str();

    // 1. Create the topic. Run as `admin` (super-user) so the
    //    `CreateTopics` Cluster-Create authorize check passes. Then grant
    //    alice Read/Write on the topic; the implications auto-grant
    //    Describe via Read and Write.
    crate::jvm_acceptance::create_console_topic(
        KAFKA_IMAGE_TXN,
        &[&admin_props.mount_str(), &ts_mount],
        TOPIC,
        1,
        1,
    );
    for op in ["Read", "Write"] {
        docker_run_kafka_tool_with_image_and_mounts(
            KAFKA_IMAGE_TXN,
            &[&admin_props.mount_str(), &ts_mount],
            &[
                "kafka-acls",
                "--add",
                "--allow-principal",
                &format!("User:{ALICE}"),
                "--operation",
                op,
                "--topic",
                TOPIC,
                "--bootstrap-server",
                broker0_advertised(),
                "--command-config",
                "/client.properties",
            ],
        );
    }

    // 2. Produce 10 records via stdin.

    crate::jvm_acceptance::authenticated_console_round_trip(
        KAFKA_IMAGE_TXN,
        &[&alice_props_mount, &ts_mount],
        TOPIC,
    );

    broker.shutdown().await;
}
