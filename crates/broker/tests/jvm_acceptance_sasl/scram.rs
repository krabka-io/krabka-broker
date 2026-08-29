//! The SCRAM-SHA-512 and SCRAM-SHA-256 produce-and-consume round-trips over a
//! `SASL_PLAINTEXT` listener.
//!
//! Both digests share a file because they run the same two-stage scenario --
//! a PLAIN super-user provisions the credential through
//! `AlterUserScramCredentials`, then the provisioned user drives the RFC 5802
//! state machine -- and differ only in the digest they name.

use std::{
    io::Write,
    process::{Command, Stdio},
};

use assert2::assert;

use crate::jvm_acceptance::{
    KAFKA_IMAGE_TXN, broker0_advertised, docker_run_kafka_tool_with_image_and_mount,
    nc_check_connectivity, plain_jaas, scram_jaas, start_dual_mech_broker, write_client_props,
};

/// End-to-end `SASL_PLAINTEXT` + SCRAM-SHA-512 drive of the JVM tools
/// against a Rust broker. Exercises two distinct authentication paths in a
/// single run:
///
/// 1. **PLAIN as super-user.** The admin user authenticates with PLAIN and
///    runs `kafka-configs --alter --entity-type users --add-config
///    'SCRAM-SHA-512=[password=...]'`. On `cp-kafka:7.5.0` (Kafka 3.5+) the
///    JVM tool translates this to `AlterUserScramCredentials (api_key 51)`,
///    the KIP-554 typed request, which is what the broker's handler
///    accepts. On the older `cp-kafka:6.1.1` / Kafka 2.7 image the same
///    CLI invocation falls back to `IncrementalAlterConfigs (44)` with
///    `entity_type=USER`, which the broker does not implement.
///
/// 2. **SCRAM-SHA-512 as the provisioned user.** Alice then drives
///    `kafka-topics`, `kafka-console-producer`, and `kafka-console-consumer`
///    with `sasl.mechanism=SCRAM-SHA-512`. This exercises the RFC 5802 state
///    machine end-to-end through the official Kafka client.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_sasl_scram_sha512_produce_consume() {
    const TOPIC: &str = "krabka-sasl-scram-itest";
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";

    let (broker, _dir) = start_dual_mech_broker(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();

    // Step A: provision alice's SCRAM-SHA-512 credential via admin/PLAIN.
    // `kafka-configs --alter --entity-type users --add-config 'SCRAM-SHA-512=[...]'`
    // on Kafka 3.5+ → `AlterUserScramCredentials (51)`. The JVM client
    // performs the PBKDF2 stretch locally and sends the 64-byte
    // `salted_password` in the request.
    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_props.mount_str(),
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

    // Step B: drive produce + consume as alice over SCRAM-SHA-512.
    // Disable idempotent producer mode (cp-kafka 7.5 default) so
    // the producer doesn't request `InitProducerId`, which would require
    // `Cluster IdempotentWrite` ACL alice doesn't hold. acks=1 is a
    // single-broker setup default that pairs cleanly with that.
    let alice_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=SCRAM-SHA-512\n\
         sasl.jaas.config={}\n\
         enable.idempotence=false\n\
         acks=1\n",
        scram_jaas(ALICE, ALICE_PASS),
    ));
    let alice_mount = alice_props.mount_str();

    // 1. Create the topic. Run as `admin` (super-user) so the
    //    `CreateTopics` Cluster-Create authorize check passes via the
    //    super-user bypass. Alice has no Cluster ACLs.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_props.mount_str(),
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

    // 1b. Grant alice the topic ACLs required for produce/consume.
    //     ACL implications: Read/Write each auto-grant Describe on
    //     the same topic, so Describe is no longer seeded explicitly.
    //     Consumer uses `--partition 0` (no consumer group)
    //     so no Group ACL is required.
    for op in ["Read", "Write"] {
        docker_run_kafka_tool_with_image_and_mount(
            KAFKA_IMAGE_TXN,
            &admin_props.mount_str(),
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

    // 2. Produce 10 records via stdin (kafka-console-producer wants
    //    `--producer.config`, not `--command-config`).
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "-v",
            &alice_mount,
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

    // 3. Consume them back (`--consumer.config`).
    let consumer_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &alice_mount,
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

/// SHA-256 analog of `jvm_sasl_scram_sha512_produce_consume`.
/// The test provisions alice's credential with `kafka-configs --add-config
/// 'SCRAM-SHA-256=[password=...]'` (KIP-554 wire byte 1), then drives
/// produce + consume with `sasl.mechanism=SCRAM-SHA-256`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_sasl_scram_sha256_produce_consume() {
    const TOPIC: &str = "krabka-sasl-scram256-itest";
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";

    let (broker, _dir) = start_dual_mech_broker(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_props.mount_str(),
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE,
            "--add-config",
            &format!("SCRAM-SHA-256=[password={ALICE_PASS}]"),
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );

    let alice_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=SCRAM-SHA-256\n\
         sasl.jaas.config={}\n\
         enable.idempotence=false\n\
         acks=1\n",
        scram_jaas(ALICE, ALICE_PASS),
    ));
    let alice_mount = alice_props.mount_str();

    // Create the topic as admin.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_props.mount_str(),
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

    // Grant alice Read + Write on the topic. ACL implications cover
    // Describe.
    for op in ["Read", "Write"] {
        docker_run_kafka_tool_with_image_and_mount(
            KAFKA_IMAGE_TXN,
            &admin_props.mount_str(),
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

    // Produce 10 records.
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "-v",
            &alice_mount,
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

    // Consume them back.
    let consumer_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &alice_mount,
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
