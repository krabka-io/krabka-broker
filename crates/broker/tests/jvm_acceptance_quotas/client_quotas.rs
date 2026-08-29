//! KIP-546 client-quota administration through `kafka-configs`: the alter,
//! describe, and delete round-trip for a user-scoped `producer_byte_rate`, an
//! `ip`-scoped `connection_creation_rate`, and a user-scoped
//! `controller_mutation_rate`.
//!
//! Each test ends by waiting for the deletion to reach the committed metadata
//! image, which is what proves the JVM tool's write went through raft rather
//! than only through the broker that served it.

use assert2::assert;

use crate::jvm_acceptance::{
    KAFKA_IMAGE_TXN, broker0_advertised, docker_run_kafka_tool_with_image_and_mount,
    nc_check_connectivity, plain_jaas, start_three_broker_sasl_plaintext_jvm_cluster_with_users,
    wait_three_brokers_registered, write_client_props,
};

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
