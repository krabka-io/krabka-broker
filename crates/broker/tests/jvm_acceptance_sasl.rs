//! SASL over PLAINTEXT -- PLAIN, SCRAM-SHA-256/512 and OAUTHBEARER -- plus the
//! ACL authorization cases that build on an authenticated broker.
//!
//! The shared harness lives in [`jvm_acceptance`]; see it for the container
//! networking these suites depend on.

mod jvm_acceptance;
mod support;

use std::{
    io::Write,
    process::{Command, Stdio},
};

use assert2::{assert, check};
use jvm_acceptance::*;

/// End-to-end `SASL_PLAINTEXT` + PLAIN drive of the JVM `kafka-topics`,
/// `kafka-console-producer`, and `kafka-console-consumer` tools against a
/// Rust broker with a `SASL_PLAINTEXT` listener and a single provisioned
/// PLAIN user. The test verifies the produce/consume round-trip end-to-end
/// through the official Kafka client.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_sasl_plain_produce_consume() {
    const TOPIC: &str = "crabka-sasl-plain-itest";
    const USER: &str = "alice";
    const PASS: &str = "wonderland";

    let (broker, _dir) = start_sasl_plaintext_broker(&[(USER, PASS)]).await;
    nc_check_connectivity();

    // 1. Write client.properties for the JVM tools.
    let props = format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(USER, PASS),
    );
    let props_file = write_client_props(&props);
    let mount = props_file.mount_str();

    // 2. Create the topic. `kafka-topics` uses `--command-config`.
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

    // 3. Produce 10 records via stdin. `kafka-console-producer` uses
    //    `--producer.config` (not `--command-config`).
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

    // 4. Consume them back. `kafka-console-consumer` uses `--consumer.config`.
    let consumer_out = docker_run_kafka_tool_with_mount(
        &mount,
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
    const TOPIC: &str = "crabka-sasl-scram-itest";
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
    const TOPIC: &str = "crabka-sasl-scram256-itest";
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

/// End-to-end `SASL_PLAINTEXT` + OAUTHBEARER drive of the JVM
/// `kafka-topics` / `kafka-console-producer` / `kafka-console-consumer`
/// tools. The JVM client uses the built-in unsecured login module to mint an
/// `alg:none` JWS for `sub=admin`. Crabka parses the RFC 7628 client initial
/// response, validates the token, and derives `User:admin`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_sasl_oauthbearer_produce_consume() {
    const TOPIC: &str = "crabka-sasl-oauthbearer-itest";
    const USER: &str = "admin";

    let (broker, _dir) = start_oauthbearer_broker().await;
    nc_check_connectivity();

    let props = format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=OAUTHBEARER\n\
         sasl.login.callback.handler.class=\
         org.apache.kafka.common.security.oauthbearer.internals.unsecured.\
         OAuthBearerUnsecuredLoginCallbackHandler\n\
         sasl.jaas.config={}\n",
        oauthbearer_jaas(USER),
    );
    let props_file = write_client_props(&props);
    let mount = props_file.mount_str();

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

    let consumer_out = docker_run_kafka_tool_with_mount(
        &mount,
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

/// JVM acceptance: `kafka-acls.sh` end-to-end provision flow.
///
/// The test drives the modern `kafka-acls.sh` flag set (cp-kafka:7.5.0,
/// Kafka 3.5+) against the Rust broker's `CreateAcls (30)`,
/// `DescribeAcls (29)`, and `DeleteAcls (31)` handlers. Admin authenticates
/// as PLAIN super-user, so the super-user short-circuit in `authorize()`
/// bypasses its `Cluster Alter` and `Cluster Describe` checks.
///
/// Sequence:
/// 1. `--add` an Allow Read on `Topic LITERAL "foo"` for `User:alice`.
/// 2. `--list --topic foo` must show that binding.
/// 3. `--remove --force` removes it. `--list --topic foo` must be empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_kafka_acls_provision_via_cli() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";

    let (broker, _dir) =
        start_sasl_plaintext_broker_with_super_user(ADMIN, &[(ADMIN, ADMIN_PASS)]).await;
    nc_check_connectivity();

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let mount = admin_props.mount_str();

    // 1. --add.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &mount,
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
            "foo",
        ],
    );

    // 2. --list --topic foo. Expect a line containing alice + READ + ALLOW.
    let list_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &mount,
        &[
            "kafka-acls",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
            "--list",
            "--topic",
            "foo",
        ],
    );
    let listed = String::from_utf8_lossy(&list_out.stdout);
    check!(
        listed.contains("User:alice"),
        "expected alice in --list output; got: {listed}"
    );
    check!(
        listed.to_ascii_uppercase().contains("READ"),
        "expected READ in --list output; got: {listed}"
    );
    check!(
        listed.to_ascii_uppercase().contains("ALLOW"),
        "expected ALLOW in --list output; got: {listed}"
    );

    // 3. --remove --force. Then re-list and assert alice is no longer present.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &mount,
        &[
            "kafka-acls",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
            "--remove",
            "--force",
            "--allow-principal",
            "User:alice",
            "--operation",
            "Read",
            "--topic",
            "foo",
        ],
    );

    let list_out2 = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &mount,
        &[
            "kafka-acls",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
            "--list",
            "--topic",
            "foo",
        ],
    );
    let listed2 = String::from_utf8_lossy(&list_out2.stdout);
    assert!(
        !listed2.contains("User:alice"),
        "alice should be gone after --remove; got: {listed2}"
    );

    broker.shutdown().await;
}

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
        "CRABKA[test] bob producer status={} stderr={stderr} stdout={stdout}",
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
        "CRABKA[test] alice consumer group-denied status={} stderr={stderr} stdout={stdout}",
        out.status,
    );
    assert!(
        stderr.contains("GroupAuthorizationException"),
        "consumer should log GroupAuthorizationException; stderr={stderr} stdout={stdout}",
    );

    broker.shutdown().await;
}

/// JVM acceptance: a prefixed topic ACL grants exactly the prefix.
///
/// Admin provisions:
/// - `Allow Read Topic PREFIXED "team-"` for alice (Describe implied by Read)
/// - `Allow Read Group LITERAL "cg-prefixed"` for alice (Describe implied by Read)
///
/// Admin then creates two topics: `team-foo`, which the prefix covers, and
/// `other-foo`, which it does NOT cover. Admin seeds one record into each.
/// Admin is a super-user, so it bypasses authorize.
///
/// Alice's consumer:
/// 1. `--topic team-foo` succeeds and reads the seeded record. This
///    exercises the PREFIXED Read path in `authorize`.
/// 2. `--topic other-foo` fails with `TopicAuthorizationException`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_prefixed_topic_acl_works() {
    const PREFIX: &str = "team-";
    const TOPIC_OK: &str = "team-foo";
    const TOPIC_DENIED: &str = "other-foo";
    const GROUP: &str = "cg-prefixed";
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

    // Pre-create both topics.
    for topic in [TOPIC_OK, TOPIC_DENIED] {
        docker_run_kafka_tool_with_image_and_mount(
            KAFKA_IMAGE_TXN,
            &admin_mount,
            &[
                "kafka-topics",
                "--create",
                "--if-not-exists",
                "--topic",
                topic,
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
    }

    // Prefixed Read on `team-*` for alice. ACL implications grant Describe from
    // Read on the same topic resource.
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
            "--resource-pattern-type",
            "prefixed",
            "--topic",
            PREFIX,
        ],
    );

    // Literal Read on group `cg-prefixed`. ACL implications grant Describe from
    // Read on the same group resource.
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

    // Seed one record into each topic as admin (super-user bypasses authorize).
    let admin_producer_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n\
         enable.idempotence=false\n\
         acks=1\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_producer_mount = admin_producer_props.mount_str();

    for topic in [TOPIC_OK, TOPIC_DENIED] {
        let mut child = Command::new("docker")
            .args([
                "run",
                "--rm",
                "-i",
                "-v",
                &admin_producer_mount,
                "--add-host=host.docker.internal:host-gateway",
                KAFKA_IMAGE_TXN,
                "kafka-console-producer",
                "--bootstrap-server",
                broker0_advertised(),
                "--topic",
                topic,
                "--producer.config",
                "/client.properties",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn admin seed producer");
        child
            .stdin
            .as_mut()
            .expect("stdin")
            .write_all(format!("seed-{topic}\n").as_bytes())
            .expect("write seed");
        drop(child.stdin.take());
        let seed_out = child.wait_with_output().expect("wait seed producer");
        assert!(
            seed_out.status.success(),
            "admin seed producer failed for {topic}: stderr={}",
            String::from_utf8_lossy(&seed_out.stderr),
        );
    }

    // ---- Alice: consume team-foo (allowed by prefix).
    let alice_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ALICE, ALICE_PASS),
    ));
    let alice_mount = alice_props.mount_str();

    let consumer_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &alice_mount,
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC_OK,
            "--group",
            GROUP,
            "--from-beginning",
            "--max-messages",
            "1",
            "--timeout-ms",
            "30000",
            "--consumer.config",
            "/client.properties",
        ],
    );
    let stdout = String::from_utf8_lossy(&consumer_out.stdout);
    let needle = format!("seed-{TOPIC_OK}");
    assert!(
        stdout.contains(&needle),
        "alice should read {needle} from prefixed topic; got: {stdout}",
    );

    // ---- Alice: consume other-foo (denied — no matching prefix).
    let denied_out = Command::new("docker")
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
            TOPIC_DENIED,
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
        .expect("spawn alice denied consumer");
    let denied_stderr = String::from_utf8_lossy(&denied_out.stderr);
    let denied_stdout = String::from_utf8_lossy(&denied_out.stdout);
    eprintln!(
        "CRABKA[test] alice denied consumer status={} stderr={denied_stderr} stdout={denied_stdout}",
        denied_out.status,
    );
    assert!(
        denied_stderr.contains("TopicAuthorizationException"),
        "alice should be denied on {TOPIC_DENIED}; stderr={denied_stderr} stdout={denied_stdout}",
    );

    broker.shutdown().await;
}
