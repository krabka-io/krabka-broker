//! Throttled stock reassignments install a live throttle and verification clears it.
use {
    crate::{
        cluster::{
            RegisteredCluster, assert_reassigned, assignment, plan_file, reassignment_cluster,
            removed_replica,
        },
        jvm_acceptance::{ClientPropsFile, KAFKA_IMAGE_TXN, TempFileMount, broker0_advertised},
        support::jvm_docker_command,
    },
    assert2::assert,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::process::Output,
};
struct ThrottledMove {
    cluster: Option<RegisteredCluster>,
    admin: Option<ClientPropsFile>,
    admin_mount: String,
    topic: String,
    initial: Vec<krabka_metadata::NodeId>,
    staying: Option<u64>,
    new_node: Option<u64>,
    plan: Option<TempFileMount>,
    plan_mount: String,
    executed: Option<Output>,
    described: Option<Output>,
    verified: Option<Output>,
}
#[fixture]
fn movement() -> ThrottledMove {
    // Start this scenario with fresh inputs and observations.

    ThrottledMove {
        cluster: None,
        admin: None,
        admin_mount: String::new(),
        topic: String::new(),
        initial: Vec::new(),
        staying: None,
        new_node: None,
        plan: None,
        plan_mount: String::new(),
        executed: None,
        described: None,
        verified: None,
    }
}
#[given(
    "three registered SASL brokers host two replicas of topic {topic} and select a free destination while retaining the first replica"
)]
async fn ready(movement: &mut ThrottledMove, topic: String) {
    let (cluster, admin, mount) = Box::pin(reassignment_cluster(&topic)).await;
    let (initial, new_node, staying) = assignment(&cluster.h1, &topic, false);
    movement.cluster = Some(cluster);
    movement.admin = Some(admin);
    movement.admin_mount = mount;
    movement.topic = topic;
    movement.initial = initial;
    movement.staying = Some(staying);
    movement.new_node = Some(new_node);
}
#[when("the stock reassignment tool executes the selected move with throttle {rate}")]
fn execute(movement: &mut ThrottledMove, rate: u64) {
    let file = plan_file(
        &movement.topic,
        movement.staying.expect("retained replica"),
        movement.new_node.expect("free target"),
    );
    let mount = format!("{}:/reassignment.json", file.host_path());
    let rate = rate.to_string();
    movement.executed = Some(
        jvm_docker_command()
            .image(KAFKA_IMAGE_TXN)
            .mounts(&[&movement.admin_mount, &mount])
            .args(&[
                "kafka-reassign-partitions",
                "--execute",
                "--reassignment-json-file",
                "/reassignment.json",
                "--throttle",
                &rate,
                "--bootstrap-server",
                broker0_advertised(),
                "--command-config",
                "/client.properties",
            ])
            .call()
            .output()
            .expect("execute throttled reassignment"),
    );
    movement.plan = Some(file);
    movement.plan_mount = mount;
}
#[then("the throttled reassignment execution succeeds")]
fn executed(movement: &ThrottledMove) {
    let output = movement
        .executed
        .as_ref()
        .expect("reassignment execution completed");
    assert!(
        output.status.success(),
        "execute failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
#[when("the stock config tool describes broker one's dynamic configuration")]
fn describe(movement: &mut ThrottledMove) {
    movement.described = Some(
        jvm_docker_command()
            .image(KAFKA_IMAGE_TXN)
            .mounts(&[&movement.admin_mount])
            .args(&[
                "kafka-configs",
                "--describe",
                "--entity-type",
                "brokers",
                "--entity-name",
                "1",
                "--bootstrap-server",
                broker0_advertised(),
                "--command-config",
                "/client.properties",
            ])
            .call()
            .output()
            .expect("describe broker throttle"),
    );
}
#[then("the broker config report includes {setting}")]
fn throttle_visible(movement: &ThrottledMove, setting: String) {
    let output = movement
        .described
        .as_ref()
        .expect("broker configuration described");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(&setting),
        "broker config missing{setting}: {stdout}"
    );
}
#[when(
    "the controller receives an ISR update including the new replica and completes the reassignment"
)]
async fn catch_up(movement: &mut ThrottledMove) {
    let cluster = movement
        .cluster
        .as_ref()
        .expect("reassignment cluster started");
    let current = cluster
        .h1
        .partition_record_for_test(&movement.topic, 0)
        .expect("partition after execute");
    let removing = removed_replica(&current, &movement.initial);
    let injected = krabka_metadata::PartitionRecord {
        isr: vec![
            krabka_metadata::NodeId(movement.staying.expect("retained replica")),
            krabka_metadata::NodeId(movement.new_node.expect("new replica")),
            removing,
        ],
        ..current
    };
    cluster
        .h1
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(injected))
        .await
        .expect("inject ISR completion");
    cluster
        .h1
        .wait_for_image(|image| {
            image
                .partition(&movement.topic, 0)
                .is_some_and(|partition| {
                    partition.adding_replicas.is_empty() && partition.removing_replicas.is_empty()
                })
        })
        .await;
}
#[then("the final replica set contains exactly the retained and new brokers")]
fn reassigned(movement: &ThrottledMove) {
    assert_reassigned(
        &movement
            .cluster
            .as_ref()
            .expect("reassignment cluster started")
            .h1,
        &movement.topic,
        movement.staying.expect("retained replica"),
        movement.new_node.expect("new replica"),
    );
}
#[when("the stock reassignment tool verifies the completed move")]
fn verify(movement: &mut ThrottledMove) {
    movement.verified = Some(
        jvm_docker_command()
            .image(KAFKA_IMAGE_TXN)
            .mounts(&[&movement.admin_mount, &movement.plan_mount])
            .args(&[
                "kafka-reassign-partitions",
                "--verify",
                "--reassignment-json-file",
                "/reassignment.json",
                "--bootstrap-server",
                broker0_advertised(),
                "--command-config",
                "/client.properties",
            ])
            .call()
            .output()
            .expect("verify throttled reassignment"),
    );
}
#[then("verification succeeds and broker one's committed leader throttle is removed")]
async fn throttle_removed(movement: &ThrottledMove) {
    let output = movement
        .verified
        .as_ref()
        .expect("reassignment verification completed");
    assert!(
        output.status.success(),
        "verify failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    movement
        .cluster
        .as_ref()
        .expect("reassignment cluster started")
        .h1
        .wait_for_image(|image| {
            image
                .broker_throttle_rate(
                    krabka_metadata::NodeId(1),
                    krabka_metadata::ThrottleKind::Leader,
                )
                .is_none()
        })
        .await;
}
#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_reassign/reassign_throttle.feature",
    name = "A stock reassignment installs a visible throttle and verification removes it after completion"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_kafka_reassign_partitions_with_throttle_end_to_end(movement: ThrottledMove) {
    let cluster = movement.cluster.expect("reassignment cluster started");
    cluster.h1.shutdown().await;
    cluster.h2.shutdown().await;
    cluster.h3.shutdown().await;
}
