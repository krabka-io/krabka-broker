//! Cluster identity discovery through Kafka's stock administration tool.

use {
    crate::jvm_acceptance::{
        KAFKA_IMAGE_TXN, broker0_advertised, docker_run_kafka_tool_with_image,
        nc_check_connectivity, start_host_broker,
    },
    assert2::assert,
    base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD},
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

struct ClusterDiscovery {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    expected_id: String,
    stdout: String,
}

#[fixture]
fn cluster() -> ClusterDiscovery {
    // Start this scenario with fresh inputs and observations.

    ClusterDiscovery {
        broker: None,
        directory: None,
        expected_id: String::new(),
        stdout: String::new(),
    }
}

#[given("a host broker with its immutable cluster identity is reachable by Kafka tools")]
async fn reachable_cluster(cluster: &mut ClusterDiscovery) {
    let (broker, directory) = start_host_broker().await;
    nc_check_connectivity();
    let image = broker.controller_image_for_test();
    cluster.expected_id = URL_SAFE_NO_PAD.encode(image.cluster_id().as_bytes());
    cluster.broker = Some(broker);
    cluster.directory = Some(directory);
}

#[when("the Kafka 7.5 cluster-id subcommand queries the broker")]
fn query_cluster_id(cluster: &mut ClusterDiscovery) {
    let output = docker_run_kafka_tool_with_image(KAFKA_IMAGE_TXN, &[
        "kafka-cluster",
        "cluster-id",
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    cluster.stdout = String::from_utf8_lossy(&output.stdout).into_owned();
}

#[then("the tool identifies the broker's current cluster id")]
fn cluster_identity(cluster: &ClusterDiscovery) {
    assert!(
        cluster.stdout.contains("Cluster ID")
            || cluster.stdout.contains("cluster ID")
            || cluster.stdout.contains("00000000"),
        "cluster-id output missing cluster id: {}",
        cluster.stdout
    );
    assert!(
        cluster.stdout.contains(&cluster.expected_id),
        "cluster-id output did not report {}: {}",
        cluster.expected_id,
        cluster.stdout
    );
}

// This image ships `cluster-id`; older suite images do not ship kafka-cluster.
#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/cluster.feature",
    name = "The stock Kafka tool discovers the broker's immutable cluster identity"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_cluster_describe(cluster: ClusterDiscovery) {
    cluster
        .broker
        .expect("a host broker was started")
        .shutdown()
        .await;
}
