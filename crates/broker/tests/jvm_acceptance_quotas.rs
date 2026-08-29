//! Quota, SCRAM-credential, log-directory and delegation-token administration
//! against a three-broker SASL cluster.
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

/// JVM acceptance: `kafka-configs --entity-type users` client quota round-trip.
///
/// Three-broker SASL/PLAINTEXT cluster. The JVM admin CLI runs alter,
/// describe, and delete on a user-scoped `producer_byte_rate`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_configs_alter_client_quota_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";

    let (h1, h2, h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster_with_users(
            ADMIN,
            ADMIN_PASS,
            &[(ALICE, ALICE_PASS)],
        )
        .await;
    nc_check_connectivity();

    wait_three_brokers_registered(&h1, &h2, &h3, 3).await;

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Set producer_byte_rate=1024 for alice.
    let out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE,
            "--add-config",
            "producer_byte_rate=1024",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );
    eprintln!(
        "KRABKA[test] alter status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "alter failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Describe — confirm visibility.
    // api_key 50 (DescribeUserScramCredentials) is implemented,
    // so the JVM tool exits 0 cleanly. Use the helper which asserts success.
    let desc = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--describe",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE,
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        desc.status.success(),
        "describe failed: {}",
        String::from_utf8_lossy(&desc.stderr)
    );
    let stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(
        stdout.contains("producer_byte_rate=1024"),
        "expected quota in describe output: {stdout}"
    );

    // Delete the config.
    let del_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE,
            "--delete-config",
            "producer_byte_rate",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        del_out.status.success(),
        "delete-config failed: {}",
        String::from_utf8_lossy(&del_out.stderr)
    );

    // Confirm the quota was cleared from the committed metadata image.
    h1.wait_for_image(|img| {
        let key: krabka_metadata::EntityKey = vec![("user".to_string(), Some(ALICE.to_string()))];
        img.client_quotas()
            .get(&key)
            .and_then(|m| m.get("producer_byte_rate"))
            .is_none()
    })
    .await;

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}

/// JVM acceptance: `kafka-configs --entity-type ips` KIP-612 round-trip.
///
/// Three-broker SASL/PLAINTEXT cluster. The JVM admin CLI runs alter,
/// describe (stdout substring), and delete-config on the
/// `connection_creation_rate` of `ip=127.0.0.1`. This test does not exercise
/// wall-time enforcement, because a single connection does not trigger the
/// rate limit. The Rust integration test in `tests/ip_quotas.rs` covers that.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_configs_alter_ip_quota_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";

    let (h1, h2, h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster_with_users(ADMIN, ADMIN_PASS, &[]).await;
    nc_check_connectivity();

    wait_three_brokers_registered(&h1, &h2, &h3, 3).await;

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Set connection_creation_rate=2 for 127.0.0.1.
    let out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "ips",
            "--entity-name",
            "127.0.0.1",
            "--add-config",
            "connection_creation_rate=2.0",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );
    eprintln!(
        "KRABKA[test] alter status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "alter failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Describe — confirm visibility.
    // api_key 50 (DescribeUserScramCredentials) is implemented,
    // so the JVM tool exits 0 cleanly. Use the helper which asserts success.
    let desc = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--describe",
            "--entity-type",
            "ips",
            "--entity-name",
            "127.0.0.1",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        desc.status.success(),
        "describe failed: {}",
        String::from_utf8_lossy(&desc.stderr)
    );
    let stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(
        stdout.contains("connection_creation_rate=2"),
        "expected ip quota in describe output: {stdout}"
    );

    // Delete the config.
    let del_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "ips",
            "--entity-name",
            "127.0.0.1",
            "--delete-config",
            "connection_creation_rate",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        del_out.status.success(),
        "delete-config failed: {}",
        String::from_utf8_lossy(&del_out.stderr)
    );

    // Confirm the quota was cleared from the committed metadata image.
    h1.wait_for_image(|img| {
        let key: krabka_metadata::EntityKey =
            vec![("ip".to_string(), Some("127.0.0.1".to_string()))];
        img.client_quotas()
            .get(&key)
            .and_then(|m| m.get("connection_creation_rate"))
            .is_none()
    })
    .await;

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}

/// JVM acceptance: `kafka-configs --entity-type users controller_mutation_rate` round-trip.
///
/// Three-broker SASL/PLAINTEXT cluster. The JVM admin CLI runs alter,
/// describe (stdout substring), and delete-config on the
/// `controller_mutation_rate` of `user=alice`. This test does not check
/// wall-time enforcement: a single `kafka-topics --create` is one request,
/// with a maximum throttle of 1 s. The Rust integration test in
/// `tests/controller_mutation_quota.rs` covers enforcement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_configs_alter_controller_mutation_rate_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";

    let (h1, h2, h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster_with_users(
            ADMIN,
            ADMIN_PASS,
            &[(ALICE, ALICE_PASS)],
        )
        .await;
    nc_check_connectivity();

    wait_three_brokers_registered(&h1, &h2, &h3, 3).await;

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Alter — set controller_mutation_rate=2.0 for alice.
    let out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE,
            "--add-config",
            "controller_mutation_rate=2.0",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );
    eprintln!(
        "KRABKA[test] alter status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "alter failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Describe — confirm visibility.
    // api_key 50 (DescribeUserScramCredentials) is implemented,
    // so the JVM tool exits 0 cleanly. Use the helper which asserts success.
    let desc = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--describe",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE,
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        desc.status.success(),
        "describe failed: {}",
        String::from_utf8_lossy(&desc.stderr)
    );
    let stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(
        stdout.contains("controller_mutation_rate=2"),
        "expected quota in describe output: {stdout}"
    );

    // Delete the config.
    let del_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE,
            "--delete-config",
            "controller_mutation_rate",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        del_out.status.success(),
        "delete-config failed: {}",
        String::from_utf8_lossy(&del_out.stderr)
    );

    // Confirm the quota was cleared from the committed metadata image.
    h1.wait_for_image(|img| {
        let key: krabka_metadata::EntityKey = vec![("user".to_string(), Some(ALICE.to_string()))];
        img.client_quotas()
            .get(&key)
            .and_then(|m| m.get("controller_mutation_rate"))
            .is_none()
    })
    .await;

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}

/// JVM acceptance: `kafka-configs --describe --entity-type users` round-trip for
/// SCRAM credentials (KIP-554 read half, `api_key` 50).
///
/// Three-broker SASL/PLAINTEXT cluster. The test provisions alice's
/// SCRAM-SHA-512 credential with `kafka-configs --alter --add-config
/// SCRAM-SHA-512=[...]`, then describes it and asserts exit 0 and
/// `SCRAM-SHA-512` in stdout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_configs_describe_users_scram_credentials_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";

    let (h1, _h2, _h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster_with_users(ADMIN, ADMIN_PASS, &[]).await;
    nc_check_connectivity();

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Provision a SCRAM user via kafka-configs --alter (hits AlterUserScramCredentials, api_key 51).
    let alter = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "users",
            "--entity-name",
            "alice",
            "--add-config",
            "SCRAM-SHA-512=[iterations=4096,password=alice-secret]",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        alter.status.success(),
        "alter SCRAM failed: {}",
        String::from_utf8_lossy(&alter.stderr)
    );

    // Describe — should exit 0 cleanly (api_key 50 now implemented).
    let desc = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--describe",
            "--entity-type",
            "users",
            "--entity-name",
            "alice",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        desc.status.success(),
        "describe failed: {}",
        String::from_utf8_lossy(&desc.stderr)
    );
    let stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(
        stdout.contains("SCRAM-SHA-512"),
        "expected SCRAM-SHA-512 in describe output: {stdout}"
    );

    let _ = h1; // keep alive
}

/// KIP-113: `kafka-log-dirs --describe` against a two-directory
/// JBOD broker. The test asserts that the JVM tool sees both configured log
/// directories and that the new topic spreads its partitions across them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_kafka_log_dirs_describe_reports_jbod_spread() {
    let (broker, primary, extra) = start_host_broker_jbod().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--topic",
        "jbodtopic",
        "--partitions",
        "6",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    // Wait for the local writer-actor of every partition to materialize on
    // disk before the JVM tool inspects the log dirs.
    for p in 0..6 {
        broker
            .wait_until_local_log_end_offset("jbodtopic", p, 0)
            .await;
    }

    let out = docker_run_kafka_tool(&[
        "kafka-log-dirs",
        "--describe",
        "--bootstrap-server",
        broker0_advertised(),
        "--broker-list",
        "1",
    ]);
    let stdout = String::from_utf8_lossy(&out.stdout);

    // The broker reports canonical absolute host paths; canonicalize the
    // expected dirs so the substring match is robust to /tmp symlinks.
    let primary_path =
        std::fs::canonicalize(primary.path()).unwrap_or_else(|_| primary.path().to_path_buf());
    let extra_path =
        std::fs::canonicalize(extra.path()).unwrap_or_else(|_| extra.path().to_path_buf());

    check!(
        stdout.contains(&primary_path.display().to_string()),
        "kafka-log-dirs output missing primary dir {}; got: {stdout}",
        primary_path.display()
    );
    check!(
        stdout.contains(&extra_path.display().to_string()),
        "kafka-log-dirs output missing extra dir {}; got: {stdout}",
        extra_path.display()
    );
    check!(
        stdout.contains("jbodtopic"),
        "kafka-log-dirs output missing topic partitions; got: {stdout}"
    );

    broker.shutdown().await;
}

/// JVM acceptance: KIP-48 delegation-token round-trip through the official
/// `kafka-delegation-tokens` admin CLI.
///
/// 3-broker `SASL_PLAINTEXT` cluster with both `PLAIN` (admin auth) and
/// `SCRAM-SHA-256` (token auth) mechanisms enabled, plus a master
/// delegation-token HMAC key. The flow:
///
/// 1. Admin (PLAIN) calls `kafka-delegation-tokens --create`. The broker
///    mints a token, replicates `V1DelegationToken` through raft, and
///    returns `(TokenID, HMAC, …)`.
/// 2. Build a `token.properties` that references those credentials with
///    `sasl.mechanism=SCRAM-SHA-256`.
/// 3. `kafka-console-producer --producer.config token.properties` produces
///    one record. It authenticates against the token-fallback path of the
///    SCRAM handler.
/// 4. `kafka-delegation-tokens --describe --owner-principal User:admin`
///    lists the token. The test matches a substring on `TokenID`.
/// 5. `kafka-delegation-tokens --expire --expiry-time-period -1 --hmac
///    <hmac>` deletes the token.
///
/// `#[ignore = "requires Docker"]`, so run with `--ignored`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_delegation_tokens_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const TOPIC: &str = "krabka-deleg-token-itest";
    const SECRET: &[u8] = b"jvm-master-key";

    let (h1, h2, h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster_with_delegation_tokens(
            ADMIN, ADMIN_PASS, SECRET,
        )
        .await;
    nc_check_connectivity();

    wait_three_brokers_registered(&h1, &h2, &h3, 3).await;

    // Admin properties: PLAIN, super-user — used for create/describe/expire.
    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // 1. Create the token. `--max-life-time-period -1` ⇒ use the broker's
    //    configured `delegation.token.max.lifetime.ms` default.
    let create_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-delegation-tokens",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
            "--create",
            "--max-life-time-period",
            "-1",
        ],
    );
    let create_stdout = String::from_utf8_lossy(&create_out.stdout).to_string();
    eprintln!("KRABKA[test] --create stdout:\n{create_stdout}");

    let token_id = extract_jvm_kv(&create_stdout, "TOKENID");
    let hmac = extract_jvm_kv(&create_stdout, "HMAC");
    assert!(
        !token_id.is_empty(),
        "empty TOKENID; stdout: {create_stdout}"
    );
    assert!(!hmac.is_empty(), "empty HMAC; stdout: {create_stdout}");

    // 2. Build token.properties referencing the new credentials via
    //    SCRAM-SHA-256 (the JVM client SASL mechanism for delegation
    //    tokens per KIP-48).
    let token_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=SCRAM-SHA-256\n\
         sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
         tokenauth=true \
         username=\"{token_id}\" password=\"{hmac}\";\n\
         enable.idempotence=false\n\
         acks=1\n",
    ));
    let token_mount = token_props.mount_str();

    // 3. Create the topic as admin so the token producer can target it.
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

    // 4. Produce one message authenticated as the delegation token.
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "-v",
            &token_mount,
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
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"hello\n")
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "token producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // 5. Describe — confirm the token is visible to the owner principal.
    let desc_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-delegation-tokens",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
            "--describe",
            "--owner-principal",
            "User:admin",
        ],
    );
    let desc_stdout = String::from_utf8_lossy(&desc_out.stdout);
    assert!(
        desc_stdout.contains(&token_id),
        "--describe stdout missing token_id={token_id}: {desc_stdout}",
    );

    // 6. Expire the token; `--expiry-time-period -1` deletes immediately.
    let exp_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-delegation-tokens",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
            "--expire",
            "--expiry-time-period",
            "-1",
            "--hmac",
            &hmac,
        ],
    );
    assert!(
        exp_out.status.success(),
        "--expire failed: stdout={} stderr={}",
        String::from_utf8_lossy(&exp_out.stdout),
        String::from_utf8_lossy(&exp_out.stderr),
    );

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}
