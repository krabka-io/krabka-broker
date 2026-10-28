//! `kafka-console-consumer` runs that join a consumer group: the default
//! assignor, KIP-345 static membership, and the KIP-429
//! `CooperativeStickyAssignor`.
//!
//! All three drive the same coordinator sequence of `JoinGroup`, `SyncGroup`,
//! `Heartbeat` and `Fetch`, which is why they share a file.

use {
    crate::jvm_acceptance::{
        KAFKA_IMAGE, KAFKA_IMAGE_TXN, broker0_advertised, describe_console_group,
        docker_run_kafka_tool_with_image, nc_check_connectivity, produce_console_checked,
        start_console_broker,
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::process::Output,
    tempfile::TempDir,
};

struct GroupConsumption {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    topic: String,
    consumed: Option<Output>,
    described: Option<Output>,
}
#[fixture]
fn consumption() -> GroupConsumption {
    // Start this scenario with fresh inputs and observations.

    GroupConsumption {
        broker: None,
        directory: None,
        topic: String::new(),
        consumed: None,
        described: None,
    }
}

#[given("topic {topic} has {partitions} partitions containing console values {values}")]
async fn populated(
    consumption: &mut GroupConsumption,
    topic: String,
    partitions: i32,
    values: String,
) {
    let (broker, directory) = start_console_broker(&topic, partitions).await;
    nc_check_connectivity();
    let payload = format!("{}\n", values.replace(',', "\n"));
    produce_console_checked(KAFKA_IMAGE, &topic, payload.as_bytes());
    consumption.broker = Some(broker);
    consumption.directory = Some(directory);
    consumption.topic = topic;
}

#[when("a default console consumer joins group {group}")]
fn default_member(consumption: &mut GroupConsumption, group: String) {
    consume(consumption, &group, KAFKA_IMAGE, 20000, &[]);
}
#[when("a static console consumer joins group {group} with instance {instance}")]
fn static_member(consumption: &mut GroupConsumption, group: String, instance: String) {
    consume(consumption, &group, KAFKA_IMAGE, 20000, &[
        "--consumer-property",
        &format!("group.instance.id={instance}"),
    ]);
}
#[when("a cooperative sticky consumer joins group {group}")]
fn cooperative_member(consumption: &mut GroupConsumption, group: String) {
    consume(consumption, &group, KAFKA_IMAGE_TXN, 30000, &[
        "--consumer-property",
        "partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor",
    ]);
}
fn consume(
    consumption: &mut GroupConsumption,
    group: &str,
    image: &str,
    timeout: u32,
    properties: &[&str],
) {
    let timeout = timeout.to_string();
    let mut args = vec![
        "kafka-console-consumer",
        "--bootstrap-server",
        broker0_advertised(),
        "--topic",
        &consumption.topic,
        "--from-beginning",
        "--group",
        group,
        "--max-messages",
        "3",
        "--timeout-ms",
        &timeout,
    ];
    args.extend_from_slice(properties);
    consumption.consumed = Some(docker_run_kafka_tool_with_image(image, &args));
}
#[then("the group consumer emits values {values}")]
fn values(consumption: &GroupConsumption, values: String) {
    let output = consumption
        .consumed
        .as_ref()
        .expect("group consumer completed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    for expected in values.split(',') {
        assert!(
            stdout.contains(expected),
            "consumer did not emit {expected}: stdout={stdout:?} stderr={:?}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
#[when("the operator describes group {group}")]
fn describe(consumption: &mut GroupConsumption, group: String) {
    consumption.described = Some(describe_console_group(&group));
}
#[then("the group report includes its subscribed topic")]
fn subscribed(consumption: &GroupConsumption) {
    let output = consumption.described.as_ref().expect("group was described");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(&consumption.topic),
        "describe missing topic {}: {stdout}",
        consumption.topic
    );
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/console_groups.feature",
    name = "Default assignment consumes all records"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn console_consumer_with_group_round_trip(consumption: GroupConsumption) {
    consumption.broker.expect("running broker").shutdown().await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/console_groups.feature",
    name = "Static membership consumes records and retains the subscription"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn console_consumer_with_static_membership(consumption: GroupConsumption) {
    consumption.broker.expect("running broker").shutdown().await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/console_groups.feature",
    name = "Cooperative sticky assignment consumes all records"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cooperative_sticky_kafka_console_consumer(consumption: GroupConsumption) {
    consumption.broker.expect("running broker").shutdown().await;
}
