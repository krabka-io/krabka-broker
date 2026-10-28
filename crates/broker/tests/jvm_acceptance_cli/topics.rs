//! Topic administration through Kafka's own command-line tools.

use {
    crate::jvm_acceptance::{
        broker0_advertised, docker_run_kafka_tool, nc_check_connectivity, start_console_broker,
        start_host_broker,
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

struct TopicAdministration {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    topic: String,
    description: String,
    configuration: String,
    listing: String,
}

#[fixture]
fn topics() -> TopicAdministration {
    // Start this scenario with fresh inputs and observations.

    TopicAdministration {
        broker: None,
        directory: None,
        topic: String::new(),
        description: String::new(),
        configuration: String::new(),
        listing: String::new(),
    }
}

#[given("a host broker is reachable by the Kafka topic administration tools")]
async fn reachable_broker(topics: &mut TopicAdministration) {
    let (broker, directory) = start_host_broker().await;
    nc_check_connectivity();
    topics.broker = Some(broker);
    topics.directory = Some(directory);
}

#[given("the existing topic {topic} has one partition")]
async fn existing_topic(topics: &mut TopicAdministration, topic: String) {
    let (broker, directory) = start_console_broker(&topic, 1).await;
    topics.broker = Some(broker);
    topics.directory = Some(directory);
    topics.topic = topic;
}

#[when("kafka-topics creates topic {topic} with {partitions} partitions")]
fn create_topic(topics: &mut TopicAdministration, topic: String, partitions: i32) {
    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--topic",
        &topic,
        "--partitions",
        &partitions.to_string(),
        "--replication-factor",
        "1",
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    topics.topic = topic;
}

#[when("kafka-topics increases the topic to three partitions")]
fn grow_topic(topics: &TopicAdministration) {
    docker_run_kafka_tool(&[
        "kafka-topics",
        "--alter",
        "--topic",
        &topics.topic,
        "--partitions",
        "3",
        "--bootstrap-server",
        broker0_advertised(),
    ]);
}

#[when("kafka-topics describes the administered topic")]
fn describe_topic(topics: &mut TopicAdministration) {
    let output = docker_run_kafka_tool(&[
        "kafka-topics",
        "--describe",
        "--topic",
        &topics.topic,
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    topics.description = String::from_utf8_lossy(&output.stdout).into_owned();
}

#[then("the topic description names {topic} with PartitionCount {partitions}")]
fn described_identity(topics: &TopicAdministration, topic: String, partitions: i32) {
    assert!(
        topics.description.contains(&format!("Topic: {topic}")),
        "describe missing topic line: {}",
        topics.description
    );
    assert!(
        topics
            .description
            .contains(&format!("PartitionCount: {partitions}")),
        "describe missing partition count: {}",
        topics.description
    );
}

#[then("the topic description reports three partitions")]
fn described_growth(topics: &TopicAdministration) {
    assert!(
        topics.description.contains("PartitionCount: 3")
            || topics.description.contains("Partitions: 3"),
        "describe missing PartitionCount: 3: {}",
        topics.description
    );
}

#[when("kafka-topics lists the current topics")]
fn list_topics(topics: &mut TopicAdministration) {
    let output = docker_run_kafka_tool(&[
        "kafka-topics",
        "--list",
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    topics.listing = String::from_utf8_lossy(&output.stdout).into_owned();
}

#[then("the administered topic appears in the topic listing")]
fn listed_topic(topics: &TopicAdministration) {
    assert!(
        topics
            .listing
            .lines()
            .any(|line| line.trim() == topics.topic),
        "topic missing before deletion: {}",
        topics.listing
    );
}

#[when("kafka-topics deletes the administered topic")]
fn delete_topic(topics: &TopicAdministration) {
    docker_run_kafka_tool(&[
        "kafka-topics",
        "--delete",
        "--topic",
        &topics.topic,
        "--bootstrap-server",
        broker0_advertised(),
    ]);
}

#[then("the administered topic is absent from the topic listing")]
fn unlisted_topic(topics: &TopicAdministration) {
    assert!(
        !topics
            .listing
            .lines()
            .any(|line| line.trim() == topics.topic),
        "topic still listed after deletion: {}",
        topics.listing
    );
}

#[when("kafka-topics creates topic {topic} with compact and delete cleanup")]
fn create_compact_delete(topics: &mut TopicAdministration, topic: String) {
    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--topic",
        &topic,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--config",
        "cleanup.policy=compact,delete",
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    topics.topic = topic;
}

#[when("kafka-configs describes the topic configuration")]
fn describe_configuration(topics: &mut TopicAdministration) {
    let output = docker_run_kafka_tool(&[
        "kafka-configs",
        "--describe",
        "--entity-type",
        "topics",
        "--entity-name",
        &topics.topic,
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    topics.configuration = String::from_utf8_lossy(&output.stdout).into_owned();
}

#[then("both administrative tools preserve cleanup.policy as compact,delete")]
fn preserved_list_policy(topics: &TopicAdministration) {
    assert!(
        topics.description.contains("cleanup.policy=compact,delete"),
        "kafka-topics --describe missing the list value: {}",
        topics.description
    );
    assert!(
        topics
            .configuration
            .contains("cleanup.policy=compact,delete"),
        "kafka-configs --describe missing the list value: {}",
        topics.configuration
    );
}

async fn stop_topic_broker(topics: TopicAdministration) {
    topics
        .broker
        .expect("a host broker was started")
        .shutdown()
        .await;
}

// Blocking Docker commands require another Tokio worker to serve the broker.
#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/topics.feature",
    name = "Creating a topic exposes its name and partition count to Kafka tools"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_topics_describe_smokes_metadata(topics: TopicAdministration) {
    stop_topic_broker(topics).await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/topics.feature",
    name = "Increasing topic partitions is visible in a subsequent description"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_topics_alter_partitions(topics: TopicAdministration) {
    stop_topic_broker(topics).await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/topics.feature",
    name = "Deleting a topic removes it from the Kafka tool listing"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_topics_delete_removes_the_topic(topics: TopicAdministration) {
    stop_topic_broker(topics).await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/topics.feature",
    name = "A compact and delete cleanup list survives creation and both describe tools"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_topics_create_accepts_a_compact_and_delete_cleanup_policy(
    topics: TopicAdministration,
) {
    stop_topic_broker(topics).await;
}
