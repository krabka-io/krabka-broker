//! Topic configuration and configuration-entity administration through Kafka tools.

use {
    crate::{
        config_output::{Configs, parse_describe, quota_entities},
        jvm_acceptance::{
            alter_console_topic_config, broker0_advertised, docker_run_kafka_tool,
            docker_run_kafka_tool_allowing_failure, nc_check_connectivity, start_console_broker,
            start_host_broker, tool_output,
        },
        oracle::{CliRun, OracleComparison},
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::{collections::BTreeMap, process::Output},
    tempfile::TempDir,
};

struct TopicConfiguration {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    topic: String,
    settings: Vec<(String, String)>,
    description: String,
    streams: BTreeMap<String, Vec<String>>,
    streams_descriptions: BTreeMap<String, String>,
    refused_alter: Option<Output>,
}

#[fixture]
fn configuration() -> TopicConfiguration {
    // Start this scenario with fresh inputs and observations.

    TopicConfiguration {
        broker: None,
        directory: None,
        topic: String::new(),
        settings: Vec::new(),
        description: String::new(),
        streams: BTreeMap::new(),
        streams_descriptions: BTreeMap::new(),
        refused_alter: None,
    }
}

#[given("administrative topic {topic} exists with one partition")]
async fn administrative_topic(configuration: &mut TopicConfiguration, topic: String) {
    let (broker, directory) = start_console_broker(&topic, 1).await;
    configuration.broker = Some(broker);
    configuration.directory = Some(directory);
    configuration.topic = topic;
}

#[given("the requested topic configuration settings are")]
fn topic_settings(configuration: &mut TopicConfiguration, datatable: Vec<Vec<String>>) {
    configuration.settings = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let columns: [String; 2] = row.try_into().expect("configuration key and value");
            columns.into()
        })
        .collect();
}

#[when("kafka-configs applies the requested topic settings")]
fn alter_topic(configuration: &TopicConfiguration) {
    let added = configuration
        .settings
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",");
    alter_console_topic_config(&configuration.topic, &added);
}

fn describe_topic_settings(configuration: &mut TopicConfiguration, effective: bool) {
    let mut args = vec!["kafka-configs", "--describe"];
    if effective {
        args.push("--all");
    }
    args.extend_from_slice(&[
        "--entity-type",
        "topics",
        "--entity-name",
        &configuration.topic,
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let output = docker_run_kafka_tool(&args);
    configuration.description = String::from_utf8_lossy(&output.stdout).into_owned();
}

#[when("kafka-configs describes the topic overrides")]
fn describe_overrides(configuration: &mut TopicConfiguration) {
    describe_topic_settings(configuration, false);
}

#[when("kafka-configs describes all effective topic values")]
fn describe_effective(configuration: &mut TopicConfiguration) {
    describe_topic_settings(configuration, true);
}

#[when("kafka-configs sets the cluster default unclean.leader.election.enable to true")]
fn alter_cluster_default() {
    docker_run_kafka_tool(&[
        "kafka-configs",
        "--alter",
        "--entity-type",
        "brokers",
        "--entity-default",
        "--add-config",
        "unclean.leader.election.enable=true",
        "--bootstrap-server",
        broker0_advertised(),
    ]);
}

#[then("every requested topic setting is described unchanged")]
fn described_overrides(configuration: &TopicConfiguration) {
    for (key, value) in &configuration.settings {
        assert!(
            configuration
                .description
                .contains(&format!("{key}={value}")),
            "describe output missing {key}={value}: {}",
            configuration.description
        );
    }
}

#[then("the effective description includes these value and provenance chains")]
fn described_provenance(configuration: &TopicConfiguration, datatable: Vec<Vec<String>>) {
    for row in datatable.into_iter().skip(1) {
        let [expected]: [String; 1] = row.try_into().expect("one rendered provenance chain");
        assert!(
            configuration.description.contains(&expected),
            "expected {expected:?} in {}",
            configuration.description
        );
    }
}

#[given("Kafka Streams requires these internal topic configurations")]
async fn streams_settings(configuration: &mut TopicConfiguration, datatable: Vec<Vec<String>>) {
    let (broker, directory) = start_host_broker().await;
    nc_check_connectivity();
    configuration.broker = Some(broker);
    configuration.directory = Some(directory);
    for row in datatable.into_iter().skip(1) {
        let [topic, key, value]: [String; 3] =
            row.try_into().expect("topic and configuration setting");
        configuration
            .streams
            .entry(topic)
            .or_default()
            .push(format!("{key}={value}"));
    }
}

#[when("kafka-topics creates and kafka-configs describes those Streams topics")]
fn create_streams_topics(configuration: &mut TopicConfiguration) {
    for (topic, settings) in &configuration.streams {
        let mut args = vec![
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
        ];
        for setting in settings {
            args.extend_from_slice(&["--config", setting]);
        }
        docker_run_kafka_tool(&args);
        let output = docker_run_kafka_tool(&[
            "kafka-configs",
            "--describe",
            "--entity-type",
            "topics",
            "--entity-name",
            topic,
            "--bootstrap-server",
            broker0_advertised(),
        ]);
        configuration.streams_descriptions.insert(
            topic.clone(),
            String::from_utf8_lossy(&output.stdout).into_owned(),
        );
    }
}

#[then("the Streams topic cleanup policies are preserved")]
fn described_streams(configuration: &TopicConfiguration, datatable: Vec<Vec<String>>) {
    for row in datatable.into_iter().skip(1) {
        let [topic, policy]: [String; 2] = row.try_into().expect("topic and cleanup policy");
        let description = configuration
            .streams_descriptions
            .get(&topic)
            .expect("the topic was described");
        assert!(
            description.contains(&format!("cleanup.policy={policy}")),
            "{topic}: describe output missing cleanup.policy={policy}: {description}"
        );
    }
}

#[given("compacted topic {topic} uses compact and delete cleanup")]
async fn compacted_topic(configuration: &mut TopicConfiguration, topic: String) {
    let (broker, directory) = start_host_broker().await;
    nc_check_connectivity();
    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
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
    configuration.broker = Some(broker);
    configuration.directory = Some(directory);
    configuration.topic = topic;
}

#[when("kafka-configs enables remote storage on the compacted topic")]
fn enable_tiering(configuration: &mut TopicConfiguration) {
    configuration.refused_alter = Some(docker_run_kafka_tool_allowing_failure(&[
        "kafka-configs",
        "--alter",
        "--entity-type",
        "topics",
        "--entity-name",
        &configuration.topic,
        "--add-config",
        "remote.storage.enable=true",
        "--bootstrap-server",
        broker0_advertised(),
    ]));
}

#[then("tiered storage is refused with Kafka's configuration validation explanation")]
fn rejected_tiering(configuration: &TopicConfiguration) {
    let output = configuration
        .refused_alter
        .as_ref()
        .expect("the configuration alter completed");
    let rendered = tool_output(output);
    assert!(
        !output.status.success(),
        "the alter must be refused, not applied: {rendered}"
    );
    assert!(rendered.contains("Remote log storage only supports topics with cleanup.policy=delete or cleanup.policy being an empty list.")
        || rendered.contains("Tiered Storage functionality is disabled in the broker."), "alter should be refused with Kafka's message: {rendered}");
}

struct EntitySetting {
    label: String,
    selector: Vec<String>,
    settings: String,
    keys: Vec<String>,
    quota_entity: Option<String>,
}

struct EntityAnswer {
    side: String,
    altered: CliRun,
    described: CliRun,
    values: Configs,
    quota_entities: Vec<String>,
}

struct EntityConfiguration {
    comparison: Option<OracleComparison>,
    cases: Vec<EntitySetting>,
    answers: Vec<Vec<EntityAnswer>>,
}

#[fixture]
fn entities() -> EntityConfiguration {
    // Start this scenario with fresh inputs and observations.

    EntityConfiguration {
        comparison: None,
        cases: Vec::new(),
        answers: Vec::new(),
    }
}

#[given("stock Kafka and krabka receive these configuration entity changes")]
async fn entity_changes(entities: &mut EntityConfiguration, datatable: Vec<Vec<String>>) {
    entities.comparison = Some(OracleComparison::start("configs-entities").await);
    entities.cases = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [label, selector, settings, keys, quota_entity]: [String; 5] =
                row.try_into().expect("five configuration entity columns");
            EntitySetting {
                label,
                selector: selector.split_whitespace().map(str::to_owned).collect(),
                settings,
                keys: keys.split(',').map(str::to_owned).collect(),
                quota_entity: (quota_entity != "none").then_some(quota_entity),
            }
        })
        .collect();
}

#[when("the same Kafka tool alters and describes every entity on both brokers")]
fn compare_entities(entities: &mut EntityConfiguration) {
    let comparison = entities
        .comparison
        .as_ref()
        .expect("the two brokers were started");
    let sides = comparison.sides();
    for case in &entities.cases {
        let mut answers = Vec::new();
        let selector = case.selector.iter().map(String::as_str).collect::<Vec<_>>();
        for side in &sides {
            let mut alter = vec!["--bootstrap-server", side.bootstrap(), "--alter"];
            alter.extend_from_slice(&selector);
            alter.extend_from_slice(&["--add-config", &case.settings]);
            let altered = side.run("kafka-configs", &alter);
            let mut describe = vec!["--bootstrap-server", side.bootstrap(), "--describe"];
            describe.extend_from_slice(&selector);
            let described = side.run("kafka-configs", &describe);
            answers.push(EntityAnswer {
                side: side.label().into(),
                values: parse_describe(&described.stdout),
                quota_entities: quota_entities(&described.stdout),
                altered,
                described,
            });
        }
        entities.answers.push(answers);
    }
}

#[then(
    "both tools accept every change and return all requested keys and complete entity identities"
)]
fn accepted_entities(entities: &EntityConfiguration) {
    assert!(entities.answers.len() == entities.cases.len());
    for (case, answers) in entities.cases.iter().zip(&entities.answers) {
        assert!(answers.len() == 2);
        for answer in answers {
            assert!(
                answer.altered.succeeded(),
                "{}: --alter over {} was refused:\n{}",
                answer.side,
                case.label,
                answer.altered.text()
            );
            assert!(
                answer.described.succeeded(),
                "{}: --describe over {} was refused:\n{}",
                answer.side,
                case.label,
                answer.described.text()
            );
            for key in &case.keys {
                assert!(
                    answer.values.contains_key(key),
                    "{}: --describe over {} lost {key}: {:?}",
                    answer.side,
                    case.label,
                    answer.values
                );
            }
            let expected = case.quota_entity.iter().cloned().collect::<Vec<_>>();
            assert!(
                answer.quota_entities == expected,
                "{}: --describe over {} named {:?}, expected {expected:?}",
                answer.side,
                case.label,
                answer.quota_entities
            );
        }
    }
}

#[then("krabka and stock Kafka agree on every described configuration value")]
fn equivalent_values(entities: &EntityConfiguration) {
    for (case, answers) in entities.cases.iter().zip(&entities.answers) {
        assert!(
            answers[0].values == answers[1].values,
            "{}: krabka and Apache Kafka disagreed",
            case.label
        );
    }
}

async fn stop_configuration(configuration: TopicConfiguration) {
    configuration
        .broker
        .expect("a host broker was started")
        .shutdown()
        .await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/configs.feature",
    name = "A topic configuration alteration is visible in its description"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_configs_alter_round_trip(configuration: TopicConfiguration) {
    stop_configuration(configuration).await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/configs.feature",
    name = "Effective topic values retain their dynamic and inherited provenance"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_configs_describe_all_shows_effective_values_and_their_sources(
    configuration: TopicConfiguration,
) {
    stop_configuration(configuration).await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/configs.feature",
    name = "All registered topic configuration keys round trip through the stock tool"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_configs_alter_round_trips_every_registered_topic_key(
    configuration: TopicConfiguration,
) {
    stop_configuration(configuration).await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/configs.feature",
    name = "Kafka Streams internal topic configurations are accepted unchanged"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_topics_create_accepts_the_streams_internal_topic_configs(
    configuration: TopicConfiguration,
) {
    stop_configuration(configuration).await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/configs.feature",
    name = "A compacted topic cannot enable tiered storage"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_configs_refuses_tiered_storage_on_a_compacted_topic(
    configuration: TopicConfiguration,
) {
    stop_configuration(configuration).await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/configs.feature",
    name = "Configuration entity round trips match stock Kafka across both admin APIs"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_configs_entity_types_round_trip_as_apache_kafka_does(entities: EntityConfiguration) {
    entities
        .comparison
        .expect("two brokers were started")
        .broker
        .shutdown()
        .await;
}
