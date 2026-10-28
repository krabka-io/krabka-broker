//! JVM differential / interop test for KIP-1071 streams groups (the Streams
//! Rebalance Protocol).
//!
//! The test drives the REAL Apache Kafka 4.1.0 `kafka-streams-groups.sh` admin
//! tool, a `KafkaStreamsGroupsCommand` that wraps the JVM `AdminClient`. The
//! tool runs inside an `mirror.gcr.io/apache/kafka:4.1.0` container against an
//! in-process Krabka broker on the host. The container has a JRE-only Kafka
//! image with no `javac` or `jshell`, so we cannot compile a custom
//! `KafkaStreams` app. Instead we use the native `krabka-client-core` client to
//! make a streams group EXIST on Krabka. The client finalizes
//! `streams.version=1`, creates a source topic, and drives a
//! `StreamsGroupHeartbeat` so the group has a live member with an assignment.
//! We then point the bundled JVM admin tool at Krabka and prove it round-trips
//! the streams-group admin wire path.
//!
//! The compiled topology this suite cannot run lives in
//! `tests/jvm_streams_app.rs`: it builds a real `KafkaStreams` app in the
//! `cp-kafka:7.5.0` image, which does ship `javac`, and runs it against Krabka
//! on both the classic protocol and `group.protocol=streams`. This suite stays
//! the one that reads the KIP-1071 ADMIN wire path off a real
//! `AdminClient`'s DEBUG log.
//!
//! The real `apache-kafka-java` 4.1.0 `AdminClient` drives this flow, read
//! empirically from its DEBUG wire log:
//!
//! - `ApiVersions` negotiation (key 18): Krabka advertises api keys 88/89 and
//!   the finalized `streams.version` feature,
//! - `Metadata` (key 13) to discover the broker set,
//! - `ListGroups` (key 16, v5) with `typesFilter=[Streams]`, which is the
//!   KIP-1071 `ListGroupsOptions.forStreamsGroups()` filter, and Krabka returns
//!   the live streams group,
//! - `FindCoordinator` (key 10) for the group,
//! - `StreamsGroupDescribe` (key 89): Krabka returns the full `DescribedGroup`,
//!   which the JVM `DescribeStreamsGroupsHandler` accepts. That group holds the
//!   group state and epochs, the resolved topology, and the member with its
//!   active task assignment.
//!
//! The test is gated `#[ignore = "requires Docker"]`. Run it with `--ignored`.
//!
//! Networking mirrors `jvm_share_groups.rs`: the broker binds `0.0.0.0:9092`
//! and advertises `host.docker.internal:9092`. The container reaches it with
//! `--add-host=host.docker.internal:host-gateway`.
#![cfg(test)]

mod support;

use {
    assert2::{assert, check},
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_protocol::owned::{
        common::streams_group_heartbeat_request::task_ids::TaskIds as ReqTaskIds,
        streams_group_heartbeat_request::Topology,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    },
    std::process::Output,
};

/// Port the broker binds on the host and that the container reaches through
/// `host.docker.internal`.
/// Ports for this test process, allocated once rather than fixed at 9092.
///
/// Every container suite used to hard-code 9092/9093, so two could not run at
/// the same time: the second to start lost the bind and reported `Address
/// already in use` as a test failure. That is why these targets ran one at a
/// time. A port per process lets them overlap.
///
/// `&'static str`, so these read as the constants they replaced.
fn bootstrap_addr() -> &'static str { &support::jvm_listeners().advertised }

/// Official Apache Kafka image. It ships KIP-1071 streams groups plus the
/// `kafka-streams-groups.sh` admin tool (`StreamsGroupDescribe` / `ListGroups`).
const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.1.0";
const STREAMS_GROUPS: &str = "/opt/kafka/bin/kafka-streams-groups.sh";
use support::StartedBroker;

/// Boot one host broker and wait for its group coordinator, which the streams
/// group heartbeat needs.
async fn start_host_broker() -> StartedBroker {
    let started = support::start_host_broker().await;
    started.broker.wait_until_group_coordinator_ready().await;
    started
}
/// Native client that connects to the broker's local loopback listener. The
/// container reaches the same broker through `host.docker.internal`.
async fn connect() -> Client {
    support::client::connect_owned(
        support::jvm_client_addr(),
        "krabka-streams-test",
        "client build",
    )
    .await
}

/// Create `topic` (`partitions` partitions) and wait until this broker leads
/// partition 0.
async fn create_topic(broker: &BrokerHandle, client: &Client, topic: &str, partitions: i32) {
    support::client::create_led_topic(broker, client, CreateTopicSetup {
        topic,
        num_partitions: TopicPartitionCount(partitions),
        ..Default::default()
    })
    .await;
}

use crate::support::topics::{CreateTopicSetup, TopicPartitionCount};
/// Finalize `streams.version` to level 1 so the heartbeat/describe handlers
/// stop returning `UNSUPPORTED_VERSION`. `upgrade_type: 1` is UPGRADE.
use support::streams::{active_partition_count, finalize_streams_version, follow_up};

fn topology(source_topic: &str) -> Topology { support::streams::topology(source_topic, Vec::new()) }

async fn join_and_converge(
    client: &Client,
    group: &str,
    topo: Topology,
    want_active: usize,
    tries: usize,
) -> (String, StreamsGroupHeartbeatResponse) {
    support::streams::streams_join_and_converge(client, group, topo, want_active, tries, true).await
}

/// Heartbeat once more to keep the live member's session fresh while the JVM
/// admin tool runs. The group then stays non-Empty with a member and an
/// assignment.
async fn keepalive(client: &Client, group: &str, member_id: &str, epoch: i32) {
    let active = Some(vec![ReqTaskIds {
        subtopology_id: "0".into(),
        partitions: vec![0, 1],
        ..Default::default()
    }]);
    let _ = client
        .send(
            follow_up()
                .group(group)
                .member_id(member_id)
                .epoch(epoch)
                .maybe_active(active)
                .call(),
        )
        .await;
}

/// Run a docker container against the host broker and return its output. The
/// admin tool may exit non-zero even on a successful round-trip, as the
/// `jvm_share_groups.rs` note records. So callers check stdout, not exit status.
fn docker_run(args: &[&str]) -> Output { support::jvm_docker_run(KAFKA_IMAGE, args) }

/// A DEBUG-level log4j2 config, written into the container at `/tmp/d.yaml`, so
/// the JVM tool's `NetworkClient`/`KafkaAdminClient` logs every request and
/// response. The tool's own stdout is empty when no streams group surfaces. See
/// the test below. So the test reads the wire-level interop checkpoints it
/// asserts on from these DEBUG lines, captured with `2>&1`.
// NOTE: YAML is indentation-sensitive, so this is written WITHOUT Rust
// line-continuation (`\` at EOL eats the next line's leading spaces). Each
// `\n` is a real newline and the two/four/six-space indents are literal.
const TOOL_DEBUG_PREAMBLE: &str = concat!(
    "cat > /tmp/d.yaml <<'YAML'\n",
    "Configuration:\n",
    "  Appenders:\n",
    "    Console:\n",
    "      name: STDERR\n",
    "      target: SYSTEM_ERR\n",
    "      PatternLayout:\n",
    "        Pattern: \"%d %p %c %m%n\"\n",
    "  Loggers:\n",
    "    Root:\n",
    "      level: DEBUG\n",
    "      AppenderRef:\n",
    "        ref: STDERR\n",
    "YAML\n",
    "export KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/tmp/d.yaml'\n",
);

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};
struct StreamsAdministration {
    broker: Option<StartedBroker>,
    client: Option<Client>,
    topic: String,
    group: String,
    member: String,
    heartbeat: Option<StreamsGroupHeartbeatResponse>,
    wire: Option<Output>,
}
#[fixture]
fn streams() -> StreamsAdministration {
    // Start this scenario with fresh inputs and observations.

    StreamsAdministration {
        broker: None,
        client: None,
        topic: String::new(),
        group: String::new(),
        member: String::new(),
        heartbeat: None,
        wire: None,
    }
}
#[given(
    "a broker finalizes streams version one and hosts topic {topic} with {partitions} partitions"
)]
async fn ready(streams: &mut StreamsAdministration, topic: String, partitions: i32) {
    let started = start_host_broker().await;
    let client = connect().await;
    finalize_streams_version(&client).await;
    create_topic(&started.broker, &client, &topic, partitions).await;
    streams.broker = Some(started);
    streams.client = Some(client);
    streams.topic = topic;
}
#[when("a sole native Streams member joins group {group} and converges on both input partitions")]
async fn join(streams: &mut StreamsAdministration, group: String) {
    let (member, response) = join_and_converge(
        streams.client.as_ref().expect("Streams client connected"),
        &group,
        topology(&streams.topic),
        2,
        50,
    )
    .await;
    streams.group = group;
    streams.member = member;
    streams.heartbeat = Some(response);
}
#[then("the join succeeds with a broker minted member identity and {partitions} active partitions")]
fn assigned(streams: &StreamsAdministration, partitions: usize) {
    let response = streams.heartbeat.as_ref().expect("Streams join completed");
    check!(
        response.error_code == 0,
        "lone member must join cleanly: {response:?}"
    );
    check!(
        !streams.member.is_empty(),
        "lone member must receive an identity"
    );
    check!(
        active_partition_count(response) == partitions,
        "lone member must own all input partitions: {response:?}"
    );
}
#[when(
    "the member heartbeats and the stock Streams group tool describes the live group with wire logging"
)]
async fn describe(streams: &mut StreamsAdministration) {
    keepalive(
        streams.client.as_ref().expect("Streams client connected"),
        &streams.group,
        &streams.member,
        streams
            .heartbeat
            .as_ref()
            .expect("member joined")
            .member_epoch,
    )
    .await;
    let bootstrap = bootstrap_addr();
    let group = &streams.group;
    streams.wire = Some(docker_run(&[
        "bash",
        "-c",
        &format!(
            "{TOOL_DEBUG_PREAMBLE}{STREAMS_GROUPS} --bootstrap-server {bootstrap} --describe --group {group} 2>&1; echo EXIT=$?"
        ),
    ]));
}
#[then("the stock admin wire log has these required and forbidden protocol observations")]
fn observed(streams: &StreamsAdministration, datatable: Vec<Vec<String>>) {
    let output = streams
        .wire
        .as_ref()
        .expect("stock Streams admin completed");
    let wire = String::from_utf8_lossy(&output.stdout);
    for row in datatable.into_iter().skip(1) {
        let [needle, presence]: [String; 2] = row
            .try_into()
            .expect("wire observation and required presence");
        let expected: bool = presence.parse().expect("boolean observation expectation");
        assert!(
            wire.contains(&needle) == expected,
            "wire observation {needle:?} expected presence{expected}: {wire}"
        );
    }
}
#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_streams_groups.feature",
    name = "A stock Streams administrator negotiates lists and describes the full live group"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_streams_groups_admin_round_trips_krabka(streams: StreamsAdministration) {
    let StartedBroker { broker, dir } = streams.broker.expect("Streams admin broker started");
    broker.shutdown().await;
    drop(dir);
}
