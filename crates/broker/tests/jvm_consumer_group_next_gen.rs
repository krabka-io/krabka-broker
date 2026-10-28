//! JVM-acceptance tests for KIP-848. They drive the GA Kafka 4.0 client
//! against an in-process Krabka broker. `group.protocol=consumer`
//! activates the next-gen heartbeat path on the client.
#![cfg(test)]

mod support;

use {
    assert2::assert,
    std::{collections::BTreeSet, fmt::Write as _, process::Command},
    support::jvm_docker_run as docker_run,
    tokio::{task, task::JoinHandle},
};

/// Ports for this test process, allocated once rather than fixed at 9092.
///
/// Every container suite used to hard-code 9092/9093, so two could not run at
/// the same time: the second to start lost the bind and reported `Address
/// already in use` as a test failure. That is why these targets ran one at a
/// time. A port per process lets them overlap.
///
/// `&'static str`, so these read as the constants they replaced.
fn bootstrap_addr() -> &'static str { &support::jvm_listeners().advertised }

const KAFKA_IMAGE_NEXT_GEN: &str = "mirror.gcr.io/apache/kafka:4.0.0";
/// Kafka 4.3.1 is the oracle for broker-side subscription regexes: from 4.1 the
/// console consumer subscribes with `SubscriptionPattern` when
/// `group.protocol=consumer`, so `--include` reaches the broker as the
/// heartbeat's `SubscribedTopicRegex` instead of being compiled client-side.
const KAFKA_IMAGE_CLASSIC: &str = "mirror.gcr.io/confluentinc/cp-kafka:7.4.0";

use support::{StartedBroker, start_host_broker};
/// Pre-create a topic with the classic admin tooling. Krabka's broker does
/// not auto-create topics on the produce path, so tests must create them
/// explicitly. This matches the existing `jvm_acceptance.rs` convention.
fn create_topic(name: &str, partitions: i32) {
    let out = docker_run(KAFKA_IMAGE_CLASSIC, &[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--bootstrap-server",
        bootstrap_addr(),
        "--topic",
        name,
        "--partitions",
        &partitions.to_string(),
        "--replication-factor",
        "1",
    ]);
    assert!(
        out.status.success(),
        "create topic {name} failed: stdout={:?} stderr={:?}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Spawn a console-consumer container on a blocking thread and return a handle
/// that resolves to its stdout. This runs two overlapping consumers in the same
/// group. The caller awaits both handles, so the containers run concurrently
/// and not back-to-back. `--add-host` mirrors `docker_run`, so the
/// container can reach the host-process broker at `host.docker.internal`.
fn spawn_consumer(image: &'static str, script: String) -> JoinHandle<String> {
    task::spawn_blocking(move || {
        let out = Command::new("docker")
            .arg("run")
            .arg("--rm")
            .arg("--add-host=host.docker.internal:host-gateway")
            .arg(image)
            .arg("bash")
            .arg("-c")
            .arg(&script)
            .output()
            .expect("docker run");
        eprintln!(
            "KRABKA[test] consumer {image} status={} stderr={}",
            out.status,
            String::from_utf8_lossy(&out.stderr),
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    })
}

/// Extract the set of partition numbers from console-consumer stdout produced
/// with `--property print.partition=true`. The `DefaultMessageFormatter`
/// emits one `Partition:<n>` token per record line, for example
/// `Partition:2\t<value>` when it prints no key. This function tolerates any
/// surrounding columns and pulls the integer after each `Partition:` marker.
fn parse_partitions(stdout: &str) -> BTreeSet<i32> {
    let mut set = BTreeSet::new();
    for line in stdout.lines() {
        for token in line.split(['\t', ' ']) {
            if let Some(rest) = token.strip_prefix("Partition:")
                && let Ok(n) = rest.trim().parse::<i32>()
            {
                set.insert(n);
            }
        }
    }
    set
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::process::Output,
};
struct GroupProtocol {
    broker: Option<StartedBroker>,
    topic: String,
    produced: Option<Output>,
    consumed: Option<Output>,
    described: Option<Output>,
    assigned: Option<String>,
    keyed_payload: String,
}
#[fixture]
fn protocol() -> GroupProtocol {
    // Start this scenario with fresh inputs and observations.

    GroupProtocol {
        broker: None,
        topic: String::new(),
        produced: None,
        consumed: None,
        described: None,
        assigned: None,
        keyed_payload: String::new(),
    }
}
#[given(
    "a broker with bidirectional group migration hosts topic {topic} with {partitions} partitions"
)]
async fn group_topic(protocol: &mut GroupProtocol, topic: String, partitions: i32) {
    protocol.broker = Some(start_host_broker().await);
    create_topic(&topic, partitions);
    protocol.topic = topic;
}
#[given("a deterministic keyed migration batch contains")]
fn keyed_batch(protocol: &mut GroupProtocol, datatable: Vec<Vec<String>>) {
    protocol.keyed_payload.clear();
    for row in datatable.into_iter().skip(1) {
        let [key, value]: [String; 2] = row.try_into().expect("key and value");
        writeln!(protocol.keyed_payload, "{key}:{value}").expect("format keyed input");
    }
}
fn publish(protocol: &mut GroupProtocol, values: &str, max_block: Option<u32>) {
    let bootstrap = bootstrap_addr();
    let topic = &protocol.topic;
    let payload = values.replace(',', "\\n");
    let extra = max_block.map_or_else(String::new, |timeout| {
        format!(" --producer-property max.block.ms={timeout}")
    });
    protocol.produced = Some(docker_run(KAFKA_IMAGE_CLASSIC, &[
        "bash",
        "-c",
        &format!(
            "printf '{payload}\\n' | kafka-console-producer --bootstrap-server {bootstrap} --topic {topic}{extra}"
        ),
    ]));
}
#[when("the classic producer publishes values {values}")]
fn produce(protocol: &mut GroupProtocol, values: String) { publish(protocol, &values, None); }
#[when("the classic producer publishes a b and c with a ten second metadata block limit")]
fn bounded_produce(protocol: &mut GroupProtocol) { publish(protocol, "a,b,c", Some(10_000)); }
#[when("the deterministic keyed migration batch is produced")]
fn produce_keyed(protocol: &mut GroupProtocol) {
    let bootstrap = bootstrap_addr();
    let topic = &protocol.topic;
    let payload = &protocol.keyed_payload;
    protocol.produced = Some(docker_run(KAFKA_IMAGE_CLASSIC, &[
        "bash",
        "-c",
        &format!(
            "printf '{payload}' | kafka-console-producer --bootstrap-server {bootstrap} --topic {topic} --property parse.key=true --property key.separator=: --producer-property max.block.ms=15000"
        ),
    ]));
}
#[then("the classic producer completes successfully")]
fn produced(protocol: &GroupProtocol) {
    let output = protocol
        .produced
        .as_ref()
        .expect("classic producer completed");
    assert!(output.status.success(), "producer failed: {output:?}");
}
fn consumer_command(
    protocol: &GroupProtocol,
    group: &str,
    kind: &str,
    count: usize,
    partition_output: bool,
) -> (&'static str, String) {
    let bootstrap = bootstrap_addr();
    let topic = &protocol.topic;
    let (image, consumer, properties) = match kind {
        "classic" => (KAFKA_IMAGE_CLASSIC, "kafka-console-consumer", ""),
        "consumer" => (
            KAFKA_IMAGE_NEXT_GEN,
            "/opt/kafka/bin/kafka-console-consumer.sh",
            " --consumer-property group.protocol=consumer",
        ),
        _ => panic!("unknown group protocol {kind}"),
    };
    let (timeout, printing) = if partition_output {
        (25_000, " --property print.partition=true")
    } else {
        (10_000, "")
    };
    (
        image,
        format!(
            "{consumer} --bootstrap-server {bootstrap} --topic {topic} --group {group}{properties} --from-beginning{printing} --timeout-ms {timeout} --max-messages {count}"
        ),
    )
}
#[when("a {kind} protocol console consumer in group {group} reads {count} records")]
fn consume(protocol: &mut GroupProtocol, kind: String, group: String, count: usize) {
    let (image, script) = consumer_command(protocol, &group, &kind, count, false);
    protocol.consumed = Some(docker_run(image, &["bash", "-c", &script]));
}
#[then("the console consumer emits values {values}")]
fn consumed(protocol: &GroupProtocol, values: String) {
    let output = protocol
        .consumed
        .as_ref()
        .expect("console consumer completed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    for value in values.split(',') {
        assert!(
            stdout.contains(value),
            "expected {value} in consumer output: {stdout}"
        );
    }
}
#[when(
    "a sole {kind} protocol member drains eight records from group g migrate and prints assignments"
)]
async fn assigned_consume(protocol: &mut GroupProtocol, kind: String) {
    let (image, script) = consumer_command(protocol, "g-migrate", &kind, 8, true);
    protocol.assigned = Some(
        spawn_consumer(image, script)
            .await
            .expect("consumer task completed"),
    );
}
#[then("the member covers every partition zero through three")]
fn all_partitions(protocol: &GroupProtocol) {
    let stdout = protocol
        .assigned
        .as_ref()
        .expect("assignment consumer completed");
    let partitions = parse_partitions(stdout);
    let expected: BTreeSet<i32> = (0..4).collect();
    assert!(
        partitions == expected,
        "consumer must cover all partitions: {partitions:?}\nstdout:{stdout}"
    );
}
#[when("the stock consumer group tool describes group {group}")]
fn describe(protocol: &mut GroupProtocol, group: String) {
    let bootstrap = bootstrap_addr();
    protocol.described = Some(docker_run(KAFKA_IMAGE_NEXT_GEN, &[
        "bash",
        "-c",
        &format!(
            "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server {bootstrap} --describe --group {group}"
        ),
    ]));
}
#[then("the description contains group {group}")]
fn group_visible(protocol: &GroupProtocol, group: String) {
    let stdout =
        String::from_utf8_lossy(&protocol.described.as_ref().expect("group described").stdout);
    assert!(
        stdout.contains(&group),
        "expected group {group} in describe output: {stdout}"
    );
}
#[then("the migrated group description includes its subscribed topic")]
fn topic_visible(protocol: &GroupProtocol) {
    let stdout = String::from_utf8_lossy(
        &protocol
            .described
            .as_ref()
            .expect("migrated group described")
            .stdout,
    );
    assert!(
        stdout.contains(&protocol.topic),
        "describe missing topic {}: {stdout}",
        protocol.topic
    );
}
#[when("the stock consumer group tool deletes group g del")]
fn delete(protocol: &mut GroupProtocol) {
    let bootstrap = bootstrap_addr();
    protocol.described = Some(docker_run(KAFKA_IMAGE_NEXT_GEN, &[
        "bash",
        "-c",
        &format!(
            "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server {bootstrap} --delete --group g-del"
        ),
    ]));
}
#[then("the consumer group deletion succeeds")]
fn deleted(protocol: &GroupProtocol) {
    let output = protocol
        .described
        .as_ref()
        .expect("group deletion completed");
    assert!(output.status.success(), "delete failed: {output:?}");
}

impl GroupProtocol {
    async fn shutdown(mut self) {
        let StartedBroker { broker, dir } =
            self.broker.take().expect("group protocol broker started");
        broker.shutdown().await;
        drop(dir);
        drop(self);
    }
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_consumer_group_next_gen.feature",
    name = "A consumer protocol member reads stock producer records"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_kip848_single_consumer_round_trip(protocol: GroupProtocol) {
    protocol.shutdown().await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_consumer_group_next_gen.feature",
    name = "A consumer protocol group can be described by stock tooling"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_kip848_describe_group(protocol: GroupProtocol) { protocol.shutdown().await; }

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_consumer_group_next_gen.feature",
    name = "An inactive consumer protocol group can be deleted"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_kip848_delete_group(protocol: GroupProtocol) { protocol.shutdown().await; }

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_consumer_group_next_gen.feature",
    name = "Classic and consumer protocol groups independently consume the same topic"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_kip848_coexists_with_classic(protocol: GroupProtocol) { protocol.shutdown().await; }

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_consumer_group_next_gen.feature",
    name = "A group migrates in place from classic to the consumer protocol"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_kip848_classic_and_consumer_in_one_group_migrate(protocol: GroupProtocol) {
    protocol.shutdown().await;
}
// A `SubscribedTopicRegex` that does not compile is answered
// `INVALID_REGULAR_EXPRESSION` (128) before any member record is written, but
// there is no JVM-lane case for it: no stock JVM client sends an invalid
// pattern to the broker. `KafkaConsumer.subscribe(Pattern)` and
// `kafka-console-consumer --include` compile it locally with
// `java.util.regex`, so `apache/kafka:4.3.1` fails inside
// `ConsoleConsumer$ConsumerWrapper` with `PatternSyntaxException: Unclosed
// group` before opening a connection. Only a compiled 4.x application using
// the `SubscriptionPattern` overload sends the string through, and no image
// here carries both a JDK and the 4.x client jars. The broker behaviour is
// pinned over the wire instead, by
// `consumer_group_next_gen::an_invalid_subscribed_topic_regex_fails_the_heartbeat`,
// and in `coordinator::unified::actor::member_state`'s unit tests.
