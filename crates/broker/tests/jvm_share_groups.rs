//! JVM differential / interop test for KIP-932 share groups.
//!
//! This test drives a REAL Apache Kafka 4.x
//! `kafka-console-share-consumer.sh`, which runs a `KafkaShareConsumer`,
//! inside a `mirror.gcr.io/apache/kafka:4.1.0` container. It runs against an
//! in-process Krabka broker on the host. This exercises Krabka's share-group
//! wire protocol end to end against the real JVM client:
//!
//! - `ApiVersions` negotiation (key 18; `share.version` advertised by Krabka),
//! - `FindCoordinator(GROUP/SHARE)` (key 10),
//! - `ShareGroupHeartbeat` (key 76) membership + assignment,
//! - `ShareFetch` (key 78) acquire + record bytes,
//! - `ShareAcknowledge` (key 79) implicit-ack on poll.
//!
//! Where a share partition starts is a GROUP config, not a client property:
//! `KafkaShareConsumer` has no `auto.offset.reset` of its own, so these tests
//! set `share.auto.offset.reset` with `kafka-configs.sh --entity-type groups`
//! before the consumer's first fetch, exactly as an operator would. The
//! `earliest` cases then read every produced record, the `by_duration` case
//! reads only the records inside its window, and the default (`latest`) case
//! reads none of the records produced before it joined.
//!
//! The test is gated with `#[ignore = "requires Docker"]`. Run it with
//! `--ignored`.
//!
//! The networking mirrors `jvm_consumer_group_next_gen.rs` and
//! `jvm_acceptance.rs`. The broker binds `0.0.0.0:9092` and advertises
//! `host.docker.internal:9092`. The container reaches it through
//! `--add-host=host.docker.internal:host-gateway`.
#![cfg(test)]

mod support;

use {
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_protocol::records::{Record, RecordBatch},
    std::{
        process::Output,
        time::{SystemTime, UNIX_EPOCH},
    },
};

/// Port that the broker binds on the host, and that the container reaches
/// through `host.docker.internal`.
/// Ports for this test process, allocated once rather than fixed at 9092.
///
/// Every container suite used to hard-code 9092/9093, so two could not run at
/// the same time: the second to start lost the bind and reported `Address
/// already in use` as a test failure. That is why these targets ran one at a
/// time. A port per process lets them overlap.
///
/// `&'static str`, so these read as the constants they replaced.
fn bootstrap_addr() -> &'static str { &support::jvm_listeners().advertised }

/// Official Apache Kafka image. It ships KIP-932 share groups, which are GA in
/// 4.x, and the `kafka-console-share-consumer.sh` and `kafka-share-groups.sh`
/// tools.
const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.1.0";
const SHARE_CONSUMER: &str = "/opt/kafka/bin/kafka-console-share-consumer.sh";
const SHARE_GROUPS: &str = "/opt/kafka/bin/kafka-share-groups.sh";
const CONFIGS: &str = "/opt/kafka/bin/kafka-configs.sh";

const SHARE_STATE_TOPIC: &str = "__share_group_state";
const SHARE_STATE_PARTITIONS: i32 = 50;

use {
    crate::support::topics::CreateTopicSetup,
    support::{StartedBroker, share::coordinator_key as share_coordinator_key, start_host_broker},
};
async fn connect() -> Client {
    support::client::connect_owned(
        support::jvm_client_addr(),
        "krabka-share-test",
        "client build",
    )
    .await
}

/// Creates `topic` with 1 partition and waits until this broker leads
/// partition 0.
async fn create_topic(broker: &BrokerHandle, client: &Client, topic: &str) -> uuid::Uuid {
    support::client::create_led_topic(broker, client, CreateTopicSetup {
        topic,
        ..Default::default()
    })
    .await;
    support::share::topic_id(broker, topic)
}

/// Creates `__share_group_state` in advance, as a KIP-932 client would create
/// it lazily through `FindCoordinator(SHARE)`. It then waits until every state
/// partition is local, so the share coordinator can accept writes before the
/// JVM consumer drives `ShareFetch` and `ShareAcknowledge`.
async fn bootstrap_share_state(broker: &BrokerHandle, client: &Client, key: &str) {
    support::find_coordinator(client, support::KEY_TYPE_SHARE, key).await;
    for p in 0..SHARE_STATE_PARTITIONS {
        broker
            .wait_until_partition_present(SHARE_STATE_TOPIC, p)
            .await;
    }
}

/// Wall-clock milliseconds, the unit a record timestamp carries.
fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis(),
    )
    .expect("timestamp fits i64")
}

/// Produces `values` stamped with the current time.
async fn produce(client: &Client, topic: &str, tid: uuid::Uuid, values: &[&str]) {
    produce_at(client, topic, tid, values, now_ms()).await;
}

/// Produces the supplied `values` as one batch into `(topic, 0)`, every record
/// stamped `timestamp_ms`. It retries while the new partition still
/// materializes its leader.
///
/// The timestamp is what a `by_duration` share group resolves against, so a
/// test can place records inside or outside the window without waiting.
async fn produce_at(
    client: &Client,
    topic: &str,
    tid: uuid::Uuid,
    values: &[&str],
    timestamp_ms: i64,
) {
    let records = values
        .iter()
        .enumerate()
        .map(|(i, value)| Record {
            offset_delta: i32::try_from(i).unwrap(),
            value: Some(bytes::Bytes::copy_from_slice(value.as_bytes())),
            ..Default::default()
        })
        .collect();
    support::share::produce_batch()
        .client(client)
        .batch(RecordBatch {
            last_offset_delta: i32::try_from(values.len() - 1).unwrap(),
            base_timestamp: timestamp_ms,
            max_timestamp: timestamp_ms,
            records,
            ..Default::default()
        })
        .topic(topic)
        .topic_id(tid)
        .call()
        .await;
}

/// Runs a docker container against the host broker and returns its output. The
/// share consumer exits with a non-zero status on an idle timeout, even after
/// it consumed records, so callers check stdout and not the exit status.
fn docker_run(args: &[&str]) -> Output { support::jvm_docker_run(KAFKA_IMAGE, args) }

fn run_share_consumer(bootstrap: &str, topic: &str, group: &str, max_messages: usize) -> Output {
    docker_run(&[
        "bash",
        "-c",
        &format!(
            "{SHARE_CONSUMER} \
                --bootstrap-server {bootstrap} \
                --topic {topic} \
                --group {group} \
                --timeout-ms 20000 \
                --max-messages {max_messages}"
        ),
    ])
}

/// Sets `share.auto.offset.reset` for `group` through the real
/// `kafka-configs.sh --alter --entity-type groups`, which drives
/// `IncrementalAlterConfigs` (`api_key` 44) against Krabka.
///
/// `KafkaShareConsumer` has no client-side `auto.offset.reset`; the group
/// config is the only way to move a share partition's start offset, and it has
/// to be in place before the group's first `ShareFetch` resolves it.
fn set_share_auto_offset_reset(bootstrap: &str, group: &str, value: &str) {
    let out = docker_run(&[
        "bash",
        "-c",
        &format!(
            "{CONFIGS} --bootstrap-server {bootstrap} --alter \
                --entity-type groups --entity-name {group} \
                --add-config share.auto.offset.reset={value}"
        ),
    ]);
    assert!(
        out.status.success(),
        "kafka-configs --alter share.auto.offset.reset={value} failed: {}",
        String::from_utf8_lossy(&out.stderr),
    );
}

/// A client and the id of the share topic it created.
struct ShareTopic {
    client: Client,
    tid: uuid::Uuid,
}

async fn prepare_share_topic(broker: &BrokerHandle, topic: &str, group: &str) -> ShareTopic {
    let client = connect().await;
    let tid = create_topic(broker, &client, topic).await;
    bootstrap_share_state(broker, &client, &share_coordinator_key(group, tid, 0)).await;
    ShareTopic { client, tid }
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};
struct ShareWorkflow {
    broker: Option<StartedBroker>,
    client: Option<Client>,
    topic: String,
    group: String,
    consumed: Option<Output>,
    administration: Option<Output>,
}
#[fixture]
fn share() -> ShareWorkflow {
    // Start this scenario with fresh inputs and observations.

    ShareWorkflow {
        broker: None,
        client: None,
        topic: String::new(),
        group: String::new(),
        consumed: None,
        administration: None,
    }
}
#[given("share topic {topic} for group {group} contains values {values}")]
async fn populated(share: &mut ShareWorkflow, topic: String, group: String, values: String) {
    let started = start_host_broker().await;
    let ShareTopic { client, tid } = prepare_share_topic(&started.broker, &topic, &group).await;
    let values: Vec<&str> = values.split(',').collect();
    produce(&client, &topic, tid, &values).await;
    share.broker = Some(started);
    share.client = Some(client);
    share.topic = topic;
    share.group = group;
}
#[given(
    "share group {group} has registered on topic {topic} after a stock consumer reads values {values}"
)]
async fn registered(share: &mut ShareWorkflow, group: String, topic: String, values: String) {
    let started = start_host_broker().await;
    let values: Vec<&str> = values.split(',').collect();
    register_jvm_share_group(&started.broker, &topic, &group, &values).await;
    share.broker = Some(started);
    share.topic = topic;
    share.group = group;
}
#[given("share topic {topic} for group {group} contains timestamped batches")]
async fn timestamped(
    share: &mut ShareWorkflow,
    topic: String,
    group: String,
    datatable: Vec<Vec<String>>,
) {
    let started = start_host_broker().await;
    let ShareTopic { client, tid } = prepare_share_topic(&started.broker, &topic, &group).await;
    let rows: Vec<(i64, String)> = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [age, value]: [String; 2] = row.try_into().expect("age and value");
            (age.parse().expect("millisecond age"), value)
        })
        .collect();
    let mut ages = Vec::new();
    for (age, _) in &rows {
        if !ages.contains(age) {
            ages.push(*age);
        }
    }
    let now = now_ms();
    for age in ages {
        let values: Vec<&str> = rows
            .iter()
            .filter_map(|(candidate, value)| (*candidate == age).then_some(value.as_str()))
            .collect();
        produce_at(&client, &topic, tid, &values, now - age).await;
    }
    share.broker = Some(started);
    share.client = Some(client);
    share.topic = topic;
    share.group = group;
}
#[when(
    "the operator sets share reset strategy {strategy} and a stock share consumer reads {count} records"
)]
fn consume_reset(share: &mut ShareWorkflow, strategy: String, count: usize) {
    set_share_auto_offset_reset(bootstrap_addr(), &share.group, &strategy);
    share.consumed = Some(run_share_consumer(
        bootstrap_addr(),
        &share.topic,
        &share.group,
        count,
    ));
}
#[when(
    "a stock share consumer joins with the unchanged default reset strategy and exits after its idle timeout"
)]
fn consume_default(share: &mut ShareWorkflow) {
    let bootstrap = bootstrap_addr();
    let topic = &share.topic;
    let group = &share.group;
    share.consumed = Some(docker_run(&[
        "bash",
        "-c",
        &format!(
            "{SHARE_CONSUMER} --bootstrap-server {bootstrap} --topic {topic} --group {group} --timeout-ms 15000"
        ),
    ]));
}
#[then("the share consumer delivers these values")]
fn delivered(share: &ShareWorkflow, datatable: Vec<Vec<String>>) {
    let output = share
        .consumed
        .as_ref()
        .expect("stock share consumer completed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    for row in datatable.into_iter().skip(1) {
        let value = row.into_iter().next().expect("expected delivered value");
        assert!(
            stdout.contains(&value),
            "share consumer missing {value}: stdout={stdout} stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
#[then("the share consumer withholds these values")]
fn withheld(share: &ShareWorkflow, datatable: Vec<Vec<String>>) {
    let stdout = String::from_utf8_lossy(
        &share
            .consumed
            .as_ref()
            .expect("stock share consumer completed")
            .stdout,
    );
    for row in datatable.into_iter().skip(1) {
        let value = row.into_iter().next().expect("expected withheld value");
        assert!(
            !stdout.contains(&value),
            "share consumer unexpectedly delivered {value}: {stdout}"
        );
    }
}
#[when("the stock share group tool describes the group's coordinator and state")]
fn describe(share: &mut ShareWorkflow) {
    let bootstrap = bootstrap_addr();
    let group = &share.group;
    share.administration = Some(docker_run(&[
        "bash",
        "-c",
        &format!(
            "{SHARE_GROUPS} --bootstrap-server {bootstrap} --describe --state --group {group}"
        ),
    ]));
}
#[when("the stock share group tool lists all share groups")]
fn list(share: &mut ShareWorkflow) {
    let bootstrap = bootstrap_addr();
    share.administration = Some(docker_run(&[
        "bash",
        "-c",
        &format!("{SHARE_GROUPS} --bootstrap-server {bootstrap} --list"),
    ]));
}
#[then("the administrative report contains the registered share group identity")]
fn visible(share: &ShareWorkflow) {
    let output = share
        .administration
        .as_ref()
        .expect("share group administration completed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(&share.group),
        "share group {} missing from report: {stdout} stderr={}",
        share.group,
        String::from_utf8_lossy(&output.stderr)
    );
}

impl ShareWorkflow {
    async fn shutdown(mut self) {
        let StartedBroker { broker, dir } = self.broker.take().expect("share broker started");
        broker.shutdown().await;
        drop(dir);
        drop(self);
    }
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_share_groups.feature",
    name = "An earliest share group reads every preexisting record"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_share_consumer_reads_krabka(share: ShareWorkflow) { share.shutdown().await; }

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_share_groups.feature",
    name = "Stock share administration describes a registered group"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_share_groups_describe_state(share: ShareWorkflow) { share.shutdown().await; }

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_share_groups.feature",
    name = "Stock share administration lists a registered group"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_share_groups_list(share: ShareWorkflow) { share.shutdown().await; }

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_share_groups.feature",
    name = "A one hour share reset window skips older batches"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_share_consumer_by_duration_skips_records_outside_the_window(share: ShareWorkflow) {
    share.shutdown().await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_share_groups.feature",
    name = "A default latest share group joins without reading old records"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_share_consumer_defaults_to_latest(share: ShareWorkflow) { share.shutdown().await; }

async fn register_jvm_share_group(
    broker: &BrokerHandle,
    topic: &str,
    group: &str,
    values: &[&str],
) {
    let ShareTopic { client, tid } = prepare_share_topic(broker, topic, group).await;
    produce(&client, topic, tid, values).await;
    let bootstrap = bootstrap_addr();
    set_share_auto_offset_reset(bootstrap, group, "earliest");
    let _ = docker_run(&[
        "bash",
        "-c",
        &format!(
            "{SHARE_CONSUMER} --bootstrap-server {bootstrap} --topic {topic} --group {group} --timeout-ms 15000 --max-messages {}",
            values.len()
        ),
    ]);
}
