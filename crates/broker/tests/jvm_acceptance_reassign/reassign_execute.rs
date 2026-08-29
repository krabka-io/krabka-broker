//! The plain `kafka-reassign-partitions --execute` and `--verify` round-trip
//! against a three-broker SASL cluster.
//!
//! The test injects the post-move ISR rather than waiting for inter-broker
//! replication, which does not route back into the VM under WSL2; the throttled
//! variant of the same flow lives in the sibling `reassign_throttle` module.

use assert2::assert;

use crate::jvm_acceptance::{
    KAFKA_IMAGE_TXN, broker0_advertised, docker_run_kafka_tool_with_image_and_mount,
    nc_check_connectivity, plain_jaas, start_three_broker_sasl_plaintext_jvm_cluster,
    wait_three_brokers_registered, write_client_props, write_temp_file,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_reassign_partitions_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const TOPIC: &str = "krabka-reassign-itest";

    let (h1, h2, h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();

    wait_three_brokers_registered(&h1, &h2, &h3, 3).await;

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Create rf=2 topic.
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
            "2",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );

    // Wait for broker 1 to see the partition in the committed metadata image.
    h1.wait_until_partition_present(TOPIC, 0).await;

    // Determine initial replicas and pick the third broker as the new target.
    // Broker node IDs are i32 on the wire but stored as u64 in PartitionRecord.
    let pr = h1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record");
    let initial = pr.replicas.clone();
    // node IDs are 1-3; find the one not in the initial replica set.
    let new_node: u64 = (1u64..=3)
        .find(|n| !initial.contains(&krabka_metadata::NodeId(*n)))
        .expect("free broker");
    let staying: u64 = initial.first().unwrap().0;
    eprintln!("KRABKA[test] initial replicas={initial:?} staying={staying} new_node={new_node}");

    // Write reassignment JSON: move partition 0 to [staying, new_node].
    let json = format!(
        r#"{{"version":1,"partitions":[{{"topic":"{TOPIC}","partition":0,"replicas":[{staying},{new_node}]}}]}}"#,
    );
    let json_file = write_temp_file("reassignment.json", &json);
    let json_mount = format!("{}:/reassignment.json", json_file.host_path());

    // Execute reassignment.
    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &admin_mount,
            "-v",
            &json_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-reassign-partitions",
            "--execute",
            "--reassignment-json-file",
            "/reassignment.json",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ])
        .output()
        .expect("spawn kafka-reassign-partitions --execute");
    eprintln!(
        "KRABKA[test] --execute status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "kafka-reassign-partitions --execute failed: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Inject ISR including new_node so the background reassignment-completion
    // task can see the new broker in ISR without relying on inter-broker
    // replication (which is broken under WSL2 due to host-gateway routing;
    // the reassignment tests use the same technique).
    let pr_after = h1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record after alter");
    let removing_replica = pr_after
        .removing_replicas
        .first()
        .copied()
        .unwrap_or_else(|| {
            initial
                .last()
                .copied()
                .unwrap_or(krabka_metadata::NodeId(0))
        });
    let injected = krabka_metadata::PartitionRecord {
        isr: vec![
            krabka_metadata::NodeId(staying),
            krabka_metadata::NodeId(new_node),
            removing_replica,
        ],
        ..pr_after.clone()
    };
    h1.submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(injected))
        .await
        .expect("inject ISR for reassignment completion");

    // Wait until adding_replicas and removing_replicas are both drained from
    // the committed metadata image.
    h1.wait_for_image(|img| {
        img.partition(TOPIC, 0)
            .is_some_and(|pr| pr.adding_replicas.is_empty() && pr.removing_replicas.is_empty())
    })
    .await;
    // After completion the replica set must match [staying, new_node].
    let pr = h1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record after reassignment");
    let got: std::collections::HashSet<u64> = pr.replicas.iter().map(|n| n.0).collect();
    let want: std::collections::HashSet<u64> = [staying, new_node].into_iter().collect();
    assert!(
        got == want,
        "reassignment completed but replicas mismatch: got={got:?} want={want:?}"
    );
    eprintln!("KRABKA[test] reassignment completed; running --verify");

    // --verify should report completion.
    let verify_out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &admin_mount,
            "-v",
            &json_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-reassign-partitions",
            "--verify",
            "--reassignment-json-file",
            "/reassignment.json",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ])
        .output()
        .expect("spawn kafka-reassign-partitions --verify");
    eprintln!(
        "KRABKA[test] --verify status={} stdout={} stderr={}",
        verify_out.status,
        String::from_utf8_lossy(&verify_out.stdout),
        String::from_utf8_lossy(&verify_out.stderr),
    );
    // Broker-scoped IncrementalAlterConfigs (resource_type=4) is supported,
    // so --verify can clear throttles and exit 0.
    assert!(
        verify_out.status.success(),
        "kafka-reassign-partitions --verify failed: stderr={}",
        String::from_utf8_lossy(&verify_out.stderr)
    );

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}
