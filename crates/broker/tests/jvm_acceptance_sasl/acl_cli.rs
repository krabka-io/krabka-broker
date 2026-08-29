//! The `kafka-acls` administration round-trip: add a binding, list it, remove
//! it, and list again.
//!
//! This file covers the ACL *administration* surface -- `CreateAcls`,
//! `DescribeAcls` and `DeleteAcls` as the JVM CLI drives them -- and is
//! separate from the files that check what those bindings then permit or
//! deny on the data plane.

use assert2::{assert, check};

use crate::jvm_acceptance::{
    KAFKA_IMAGE_TXN, broker0_advertised, docker_run_kafka_tool_with_image_and_mount,
    nc_check_connectivity, plain_jaas, start_sasl_plaintext_broker_with_super_user,
    write_client_props,
};

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
