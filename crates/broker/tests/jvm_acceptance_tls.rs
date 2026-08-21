//! TLS listeners: a plain SSL handshake, the SASL_SSL stack, and inter-broker
//! replication over authenticated and TLS-encrypted connections.
//!
//! The shared harness lives in [`jvm_acceptance`]; see it for the container
//! networking these suites depend on.

mod jvm_acceptance;
mod support;

use std::{
    io::Write,
    process::{Command, Stdio},
};

use assert2::assert;
use jvm_acceptance::*;

/// End-to-end TLS handshake check against an `SSL`-only listener. The test
/// drives `kafka-broker-api-versions` from inside the cp-kafka container
/// with a JKS truststore that holds the broker's dev cert. It verifies that
/// the JVM client completes the TLS handshake and exchanges an
/// `ApiVersions` request over the encrypted channel.
///
/// The test turns off hostname verification with
/// `ssl.endpoint.identification.algorithm=`, because the CN of the dev cert
/// is `crabka-dev`, not `host.docker.internal`. The dev cert is a
/// self-signed ECDSA P-256 end-entity, regenerated from the original
/// ED25519 + CA:TRUE fixture. cp-kafka:6.1.1 ships Java 11, whose
/// `SunJSSE` does not advertise `ed25519` signature schemes during the TLS
/// handshake, so the JVM client would reject ED25519 server certs with
/// `NoSignatureSchemesInCommon`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_ssl_handshake_succeeds() {
    let (broker, _dir) = start_ssl_broker().await;
    nc_check_connectivity();

    let truststore_path = prepare_jks_truststore();

    let props = "security.protocol=SSL\n\
                 ssl.truststore.location=/truststore.jks\n\
                 ssl.truststore.password=changeit\n\
                 ssl.endpoint.identification.algorithm=\n";
    let props_tmp = write_client_props(props);
    let ts_mount = format!("{}:/truststore.jks:ro", truststore_path.display());

    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &props_tmp.mount_str(),
            "-v",
            &ts_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-broker-api-versions",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ])
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("spawn kafka-broker-api-versions");
    eprintln!(
        "CRABKA[test] ssl api-versions status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "ssl handshake failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    broker.shutdown().await;
}

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
    const TOPIC: &str = "crabka-sasl-ssl-itest";
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
    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_SSL\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n\
         ssl.truststore.location=/truststore.jks\n\
         ssl.truststore.password=changeit\n\
         ssl.endpoint.identification.algorithm=\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    docker_run_kafka_tool_with_image_and_mounts(
        KAFKA_IMAGE_TXN,
        &[&admin_props.mount_str(), &ts_mount],
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE,
            "--add-config",
            &format!("SCRAM-SHA-512=[password={ALICE_PASS}]"),
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );

    // Step B: drive produce + consume as alice over SASL_SSL + SCRAM-SHA-512.
    // Disable idempotent producer mode so alice doesn't need
    // `Cluster IdempotentWrite`.
    let alice_props = write_client_props(&format!(
        "security.protocol=SASL_SSL\n\
         sasl.mechanism=SCRAM-SHA-512\n\
         sasl.jaas.config={}\n\
         ssl.truststore.location=/truststore.jks\n\
         ssl.truststore.password=changeit\n\
         ssl.endpoint.identification.algorithm=\n\
         enable.idempotence=false\n\
         acks=1\n",
        scram_jaas(ALICE, ALICE_PASS),
    ));
    let alice_props_mount = alice_props.mount_str();

    // 1. Create the topic. Run as `admin` (super-user) so the
    //    `CreateTopics` Cluster-Create authorize check passes. Then grant
    //    alice Read/Write on the topic; the implications auto-grant
    //    Describe via Read and Write.
    docker_run_kafka_tool_with_image_and_mounts(
        KAFKA_IMAGE_TXN,
        &[&admin_props.mount_str(), &ts_mount],
        &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
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
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "-v",
            &alice_props_mount,
            "-v",
            &ts_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-console-producer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--producer.config",
            "/client.properties",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn producer");
    let payload: String = (0..10)
        .map(|i| format!("msg-{i}\n"))
        .collect::<Vec<_>>()
        .concat();
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(payload.as_bytes())
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr)
    );

    // 3. Consume them back.
    let consumer_out = docker_run_kafka_tool_with_image_and_mounts(
        KAFKA_IMAGE_TXN,
        &[&alice_props_mount, &ts_mount],
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            "10",
            "--timeout-ms",
            "20000",
            "--consumer.config",
            "/client.properties",
        ],
    );
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for i in 0..10 {
        let needle = format!("msg-{i}");
        assert!(s.contains(&needle), "consumer missing {needle}: {s:?}");
    }

    broker.shutdown().await;
}

/// JVM-driven 2-broker test for the `SASL_PLAINTEXT` inter-broker
/// listener. Both brokers boot with the same shared `admin` credential and
/// mechanism=PLAIN. The raft layer authenticates each peer in both
/// directions before the cluster converges on a 2-broker metadata view.
/// A JVM client then SASL-authenticates as the same `admin` identity over
/// the data-plane listener, creates a topic, and produces 50 records.
///
/// Why this test is *not* a follower-replication assertion: the brokers
/// in this test advertise `host.docker.internal:<port>` so the JVM
/// container can reach them with `--add-host=...:host-gateway`. Under WSL2
/// that hostname resolves to the Windows host IP, for example
/// `10.0.0.170`, which is *not* routable back into the WSL VM where the
/// broker peers live. So follower-fetch traffic that flows broker→broker
/// cannot complete on this network topology. That traffic is the
/// `InterBrokerClient` dialing the peer's advertised address from
/// `MetadataImage`. This is not a SASL or replication bug. It is a
/// Docker-on-WSL networking limit. The Rust-driven equivalent is
/// `tests/auth_handlers.rs::two_broker_sasl::two_broker_sasl_plaintext_replication`.
/// It uses 127.0.0.1 advertised addresses for both brokers and *does*
/// exercise end-to-end inter-broker SASL replication. Use that as the
/// load-bearing inter-broker SASL test.
///
/// What this test *does* assert end-to-end through the JVM client:
///
/// 1. Two brokers boot with `SASL_PLAINTEXT` inter-broker auth and exchange
///    raft `AppendEntries` + `BrokerHeartbeat` traffic over SASL.
/// 2. The cluster converges on a 2-broker metadata view (both brokers'
///    `broker_count() >= 2`).
/// 3. The JVM `kafka-topics` and `kafka-console-producer` tools both
///    SASL-authenticate as `admin` and successfully drive a single-partition
///    topic produce against broker 0.
/// 4. Broker 0's local log has all 50 records after produce returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_inter_broker_replication_authed() {
    const TOPIC: &str = "crabka-jvm-inter-broker-itest";
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";

    let (broker0, broker1, _dir0, _dir1) = start_two_sasl_brokers(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();

    // Wait for both brokers to converge on a 2-broker metadata image —
    // the load-bearing inter-broker SASL handshake. If the peer SASL
    // credentials mismatched, broker 1 would never register and this
    // would time out.
    broker0.wait_until_brokers_registered(2).await;
    broker1.wait_until_brokers_registered(2).await;

    // JVM client config: SASL_PLAINTEXT + PLAIN as the admin (super-user).
    let props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let mount = props.mount_str();

    // Create an rf=1 topic (see test doc-comment — JVM-driven rf=2
    // assertion isn't reliable under WSL networking). Single replica is
    // enough to prove the JVM client → broker SASL handshake works in
    // both directions across the two-broker cluster's controller layer.
    docker_run_kafka_tool_with_mount(
        &mount,
        &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );

    // Wait for the topic to materialize in a broker's metadata image (either
    // broker; committed metadata converges on both).
    tokio::select! {
        () = broker0.wait_until_partition_present(TOPIC, 0) => {}
        () = broker1.wait_until_partition_present(TOPIC, 0) => {}
    }

    // Produce 50 records via `kafka-console-producer`. The metadata
    // response steers the producer to whichever broker leads partition 0.
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "-v",
            &mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--producer.config",
            "/client.properties",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn producer");
    let payload: String = (0..50)
        .map(|i| format!("rec-{i}\n"))
        .collect::<Vec<_>>()
        .concat();
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(payload.as_bytes())
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr)
    );

    // Verify the leader has 50 records on disk. We don't know in advance
    // which broker leads partition 0 (raft picks one), so wait for whichever
    // broker's local log reaches offset 50 first; the losing awaiter is
    // dropped (the non-leader never materializes the partition locally).
    tokio::select! {
        () = broker0.wait_until_local_log_end_offset(TOPIC, 0, 50) => {}
        () = broker1.wait_until_local_log_end_offset(TOPIC, 0, 50) => {}
    }

    broker0.shutdown().await;
    broker1.shutdown().await;
}

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
    use crabka_security::ListenerProtocol;

    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";
    const TOPIC: &str = "crabka-sasl-ssl-raft-rf2";

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
    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_SSL\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n\
         ssl.truststore.location=/truststore.jks\n\
         ssl.truststore.password=changeit\n\
         ssl.endpoint.identification.algorithm=\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    docker_run_kafka_tool_with_image_and_mounts(
        KAFKA_IMAGE_TXN,
        &[&admin_props.mount_str(), &ts_mount],
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE,
            "--add-config",
            &format!("SCRAM-SHA-512=[password={ALICE_PASS}]"),
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );

    // Step B: drive create-topic + produce as alice over SASL_SSL+SCRAM.
    // Disable idempotent producer mode so alice doesn't need
    // `Cluster IdempotentWrite`.
    let alice_props = write_client_props(&format!(
        "security.protocol=SASL_SSL\n\
         sasl.mechanism=SCRAM-SHA-512\n\
         sasl.jaas.config={}\n\
         ssl.truststore.location=/truststore.jks\n\
         ssl.truststore.password=changeit\n\
         ssl.endpoint.identification.algorithm=\n\
         enable.idempotence=false\n\
         acks=1\n",
        scram_jaas(ALICE, ALICE_PASS),
    ));
    let alice_props_mount = alice_props.mount_str();

    // Create topic rf=2 across both brokers. Run as `admin` (super-user)
    //  for the CreateTopics Cluster-Create authorize check, then
    //  grant alice Read/Write on the topic; the implications
    //  auto-grant Describe via Read and Write.
    docker_run_kafka_tool_with_image_and_mounts(
        KAFKA_IMAGE_TXN,
        &[&admin_props.mount_str(), &ts_mount],
        &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "2",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
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
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "-v",
            &alice_props_mount,
            "-v",
            &ts_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-console-producer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--producer.config",
            "/client.properties",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn producer");
    let payload: String = (0..50)
        .map(|i| format!("rec-{i}\n"))
        .collect::<Vec<_>>()
        .concat();
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(payload.as_bytes())
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
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
