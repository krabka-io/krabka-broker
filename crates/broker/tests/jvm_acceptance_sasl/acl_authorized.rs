//! The authorized produce-and-consume round-trip for a principal that holds
//! literal topic and group ACLs.
//!
//! It is the positive half of the ACL enforcement cases, and it stands alone
//! because it is the only one that walks the whole consumer-group path --
//! `JoinGroup`, `OffsetFetch` and `OffsetCommit` -- through the authorize
//! preamble to a successful read.

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

/// JVM acceptance: authorized produce + consume round-trip.
///
/// Admin (PLAIN super-user) provisions alice with:
/// - `Allow Read+Write Topic LITERAL "foo"`
/// - `Allow Read Group LITERAL "cg-foo"`
///
/// ACL implications grant Describe from Read/Write on the same resource, so
/// the test seeds no explicit Describe ACL. The Metadata per-topic check
/// relies on the implication path.
///
/// Alice (PLAIN, no super-user, no cluster perms) then drives
/// `kafka-console-producer` and `kafka-console-consumer --group cg-foo`
/// against the broker. This exercises the full `Produce`/`Fetch`/
/// `JoinGroup`/`OffsetFetch`/`OffsetCommit` authorize preambles end-to-end.
///
/// The test deliberately avoids topic auto-creation. Admin creates `foo`
/// before it grants alice access, so the Produce path does not have to
/// short-circuit on a missing topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_authorized_produce_consume() {
    const TOPIC: &str = "foo";
    const GROUP: &str = "cg-foo";
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

    // ---- Admin step: pre-create the topic and provision alice's ACLs.
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

    // Allow Read+Write on Topic foo for User:alice. ACL implications grant
    // Describe from Read/Write on the same topic, so no explicit Describe
    // ACL is required here.
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

    // Allow Read on Group cg-foo for User:alice. ACL implications grant Describe
    // from Read on the same group resource, so no explicit Describe is
    // needed.
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
            "--group",
            GROUP,
        ],
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
    let alice_mount = alice_props.mount_str();

    // Produce 10 records via stdin.
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

    // Consume via `--group cg-foo --from-beginning` (the group-coordinator
    // path; exercises JoinGroup/OffsetFetch/OffsetCommit authorize).
    let consumer_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &alice_mount,
        &[
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
        ],
    );
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for i in 0..10 {
        let needle = format!("msg-{i}");
        assert!(s.contains(&needle), "consumer missing {needle}: {s:?}");
    }

    broker.shutdown().await;
}
