//! The ACL denial cases: a producer with no topic ACL, and a consumer with a
//! topic ACL but no group ACL.
//!
//! Both live here because they share an assertion strategy that the positive
//! cases do not need: the JVM console tools exit zero even when the broker
//! refuses them, so each case asserts on the exception name the client logs to
//! stderr.

use std::{
    io::Write,
    process::{Command, Stdio},
};

use assert2::assert;

use crate::jvm_acceptance::{
    KAFKA_IMAGE_TXN, broker0_advertised, docker_run_kafka_tool_with_image_and_mount,
    nc_check_connectivity, plain_jaas, start_sasl_plaintext_broker_with_super_user,
    write_client_props,
};

/// JVM acceptance: produce by an unauthorized principal must fail.
///
/// Admin (PLAIN super-user) provisions alice with Read+Write on topic `foo`.
/// Read implies Describe, so these are the same effective ACLs as
/// `jvm_authorized_produce_consume`. Bob has valid PLAIN credentials but
/// no ACLs at all. Bob's `kafka-console-producer` must be denied.
///
/// Assertion strategy: `kafka-console-producer` is a fire-and-forget shell
/// wrapper around the Java client. In cp-kafka 7.5.0 it logs
/// `TopicAuthorizationException` on every Metadata-denied response, but
/// the wrapper itself exits 0. It retries silently and never turns the
/// broker-side AUTH failure into a non-zero exit code. So the contract this
/// test asserts is stderr-shaped, not exit-code-shaped: stderr must contain
/// `TopicAuthorizationException`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_unauthorized_produce_fails() {
    const TOPIC: &str = "foo";
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";
    const BOB: &str = "bob";
    const BOB_PASS: &str = "bob-secret";

    let (broker, _dir) = start_sasl_plaintext_broker_with_super_user(
        ADMIN,
        &[(ADMIN, ADMIN_PASS), (ALICE, ALICE_PASS), (BOB, BOB_PASS)],
    )
    .await;
    nc_check_connectivity();

    // ---- Admin step: pre-create topic + provision alice (not bob).
    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
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

    // alice gets Read+Write — proves that the broker has ACLs configured
    // (i.e. the empty-ACL ALLOW shim is not active). ACL implications grant
    // Describe from Read/Write so no explicit Describe ACL is needed.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
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
            "--operation",
            "Write",
            "--topic",
            TOPIC,
        ],
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
    let bob_mount = bob_props.mount_str();

    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "-v",
            &bob_mount,
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
        .expect("spawn bob producer");
    let payload = b"unauth-msg\n";
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(payload)
        .expect("write stdin");
    drop(child.stdin.take());
    let bob_out = child.wait_with_output().expect("wait bob producer");
    let stderr = String::from_utf8_lossy(&bob_out.stderr);
    let stdout = String::from_utf8_lossy(&bob_out.stdout);
    eprintln!(
        "KRABKA[test] bob producer status={} stderr={stderr} stdout={stdout}",
        bob_out.status,
    );
    assert!(
        stderr.contains("TopicAuthorizationException"),
        "bob producer should log TopicAuthorizationException; stderr={stderr} stdout={stdout}",
    );

    broker.shutdown().await;
}

/// JVM acceptance: consumer denied on the group-resource path.
///
/// Alice has Read on topic `foo`, which implies Describe, but she has no
/// ACL on group `cg-other`. `kafka-console-consumer --group cg-other` must
/// fail with `GroupAuthorizationException`. The broker denies her at
/// `JoinGroup`/`OffsetFetch`, before any Fetch happens.
///
/// Assertion strategy: stderr-shaped. This test asserts on stderr content
/// for symmetry with `jvm_unauthorized_produce_fails` and to keep the
/// contract stable across cp-kafka versions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_unauthorized_consumer_fails_group_check() {
    const TOPIC: &str = "foo";
    const GROUP: &str = "cg-other";
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";

    let (broker, _dir) = start_sasl_plaintext_broker_with_super_user(
        ADMIN,
        &[(ADMIN, ADMIN_PASS), (ALICE, ALICE_PASS)],
    )
    .await;
    nc_check_connectivity();

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
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

    // alice: Read on Topic foo (Describe implied by Read). Deliberately
    // no group ACL so the consumer hits GroupAuthorizationException.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
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
            "--topic",
            TOPIC,
        ],
    );

    // ---- Alice consumer using --group cg-other. Expect group-denied stderr.
    let alice_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ALICE, ALICE_PASS),
    ));
    let alice_mount = alice_props.mount_str();

    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &alice_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-console-consumer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--group",
            GROUP,
            "--from-beginning",
            "--max-messages",
            "1",
            "--timeout-ms",
            "15000",
            "--consumer.config",
            "/client.properties",
        ])
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("spawn alice consumer");
    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);
    eprintln!(
        "KRABKA[test] alice consumer group-denied status={} stderr={stderr} stdout={stdout}",
        out.status,
    );
    assert!(
        stderr.contains("GroupAuthorizationException"),
        "consumer should log GroupAuthorizationException; stderr={stderr} stdout={stdout}",
    );

    broker.shutdown().await;
}
