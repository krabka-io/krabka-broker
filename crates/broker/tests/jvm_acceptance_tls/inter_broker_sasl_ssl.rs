//! The two-broker `SASL_SSL` cluster whose controller listener is also
//! `SaslSsl`, driven to rf=2 replication.
//!
//! This is the full production-shape stack in one test: TLS-terminated raft
//! RPC on the controller listener, TLS-terminated SASL on the data plane, and
//! follower replication asserted through both brokers' local logs. It is the
//! longest case in the suite and the only one that asserts rf=2, so it stands
//! apart from the `SASL_PLAINTEXT` variant it otherwise resembles.

use assert2::assert;
use krabka_security::ListenerProtocol;

use crate::jvm_acceptance::{
    KAFKA_IMAGE_TXN, broker0_advertised, docker_run_kafka_tool_with_image_and_mounts,
    nc_check_connectivity, prepare_jks_truststore,
    start_two_sasl_ssl_brokers_with_controller_protocol,
};

/// Two-broker `SASL_SSL` cluster with `controller_listener_protocol =
/// SaslSsl`. The test provisions a SCRAM user, produces rf=2 through the JVM
/// client, and asserts that both brokers replicate the records. It exercises
/// the full production-shape stack: TLS-terminated controller raft RPC,
/// TLS-terminated data-plane SASL, and rf=2 follower replication. The
/// earlier simplified inter-broker test only proved metadata convergence.
///
/// Networking: like the `SASL_PLAINTEXT` inter-broker test, this test
/// advertises `host.docker.internal:<port>` so the JVM containers can reach
/// the brokers. Under WSL2 the broker→broker `InterBrokerClient` hop can
/// fail, because `host.docker.internal` resolves to the Windows host IP and
/// not to the WSL VM where the peers live. The CI runner's `/etc/hosts`
/// setup makes that hop work end-to-end. On WSL the test can time out at
/// the rf=2 offset check even when `SASL_SSL` itself is correctly wired.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_inter_broker_sasl_ssl_raft_replication() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";
    const TOPIC: &str = "krabka-sasl-ssl-raft-rf2";

    let (broker0, broker1, _dir0, _dir1) = start_two_sasl_ssl_brokers_with_controller_protocol(
        ListenerProtocol::SaslSsl,
        ADMIN,
        ADMIN_PASS,
    )
    .await;
    nc_check_connectivity();
    let truststore_path = prepare_jks_truststore();
    let ts_mount = format!("{}:/truststore.jks:ro", truststore_path.display());

    // Wait for both brokers to converge on a 2-broker metadata image —
    // the load-bearing inter-broker SASL_SSL handshake on the controller
    // listener. Without TLS + SASL working in both directions, broker 1
    // never registers and this would time out.
    broker0.wait_until_brokers_registered(2).await;
    broker1.wait_until_brokers_registered(2).await;

    // Step A: provision alice's SCRAM-SHA-512 credential via admin/PLAIN
    // over the SASL_SSL data-plane listener. Use cp-kafka:7.5.0 (KIP-554).
    let (admin_props, alice_props) = crate::jvm_acceptance::provision_ssl_scram_sha512(
        ADMIN, ADMIN_PASS, ALICE, ALICE_PASS, &ts_mount,
    );
    let alice_props_mount = alice_props.mount_str();

    // Create topic rf=2 across both brokers. Run as `admin` (super-user)
    //  for the CreateTopics Cluster-Create authorize check, then
    //  grant alice Read/Write on the topic; the implications
    //  auto-grant Describe via Read and Write.
    crate::jvm_acceptance::create_console_topic(
        KAFKA_IMAGE_TXN,
        &[&admin_props.mount_str(), &ts_mount],
        TOPIC,
        1,
        2,
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

    // Wait for the topic to materialize on both brokers' metadata images.
    broker0.wait_until_partition_present(TOPIC, 0).await;
    broker1.wait_until_partition_present(TOPIC, 0).await;

    // Produce 50 records via `kafka-console-producer` as alice over SASL_SSL.

    let payload: String = (0..50)
        .map(|i| format!("rec-{i}\n"))
        .collect::<Vec<_>>()
        .concat();
    let producer_out = crate::jvm_acceptance::produce_console(
        KAFKA_IMAGE_TXN,
        &[&alice_props_mount, &ts_mount],
        TOPIC,
        false,
        payload.as_bytes(),
    );
    assert!(
        producer_out.status.success(),
        "producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr)
    );

    // Assert BOTH brokers reach offset 50 on partition 0 — proves rf=2
    // follower replication completed over the SASL_SSL inter-broker
    // listener (the production-shape end-to-end claim).
    broker0.wait_until_local_log_end_offset(TOPIC, 0, 50).await;
    broker1.wait_until_local_log_end_offset(TOPIC, 0, 50).await;

    broker0.shutdown().await;
    broker1.shutdown().await;
}
