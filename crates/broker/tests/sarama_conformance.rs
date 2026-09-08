//! `kaf` -- a Go CLI on IBM/sarama -- against krabka.
//!
//! Every other non-JVM client this tree drives is librdkafka, so one decoder
//! stands behind every non-JVM row of the evidence set. sarama is an
//! independent reimplementation of the protocol: its own request-version
//! floors, its own header and tagged-field handling, its own tolerance for
//! absent optional fields. A response only librdkafka and the JVM client
//! accept would pass every other suite here and fail this one.
//!
//! ## What the client is pinned to
//!
//! The image is built in this repository from an apko lock
//! (`//bazel/images:kaf_sarama_tar`), and that lock is what pins the binary.
//! `kaf --version` is no help: the release build stamps the version through
//! goreleaser, and the Wolfi package does not, so the binary reports
//! `latest (HEAD)` whatever it is. There is therefore no version banner to
//! assert on, and nothing below asserts on one -- what the suite pins is
//! behaviour.
//!
//! ## Which RPC each command issues
//!
//! sarama at the version this client builds against implements neither
//! `DescribeCluster` (api key 60) nor `DescribeTopicPartitions` (api key 75):
//! `request.go` lists both as unimplemented, and `ClusterAdmin::DescribeCluster`
//! and `ClusterAdmin::DescribeTopics` are both `Metadata` round-trips. So
//! `kaf nodes` and `kaf topic describe` do *not* exercise those two APIs, and
//! this suite does not claim they do. What it establishes instead is that the
//! answers agree: krabka's own `DescribeCluster` and `DescribeTopicPartitions`
//! responses are read on the host and used to *build* the rows `kaf` must
//! print from `Metadata`. A broker whose newer cluster and topic APIs drifted
//! from what it says in `Metadata` fails here.
//!
//! `kaf` also pins sarama's protocol version at Kafka 1.1 unless a config file
//! raises it, which is the point of running it: every request below is at a
//! pre-flexible version, negotiated by an `ApiVersions` exchange from a client
//! that has never seen krabka.

mod jvm_acceptance;
mod support;

use std::{
    collections::BTreeMap,
    io::Write,
    process::{Child, Command, Output, Stdio},
    time::Duration,
};

use assert2::assert;
use krabka_client_admin::{AdminClient, CreateTopicSpec};
use krabka_client_core::Client;
use krabka_protocol::owned::{
    describe_cluster_request::DescribeClusterRequest,
    describe_cluster_response::DescribeClusterResponse,
    describe_topic_partitions_request::{DescribeTopicPartitionsRequest, TopicRequest},
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponse, DescribeTopicPartitionsResponsePartition,
    },
};

use crate::jvm_acceptance::{broker0_advertised, start_host_broker};

/// The image, built here and named in the `sarama_conformance` entry of the
/// `docker` map in `BUILD.bazel`.
const IMAGE: &str = "docker.io/krabka-io/kaf:0.2.14";
/// The binary inside it.
const PROGRAM: &str = "/usr/bin/kaf";

const TOPIC: &str = "sarama-conformance";
const GROUP: &str = "sarama-conformance-group";
/// More than one, so the partition count `kaf` prints is a number it read
/// rather than the only number a single-partition topic could have.
const PARTITIONS: i32 = 3;
/// The one record, produced to partition 0 with sarama's manual partitioner.
const PAYLOAD: &str = "hello-from-sarama";

/// How long the group consumer may take to join, read and commit. Generous:
/// a cold container plus a join and a rebalance sit under it.
const COMMIT_TIMEOUT: Duration = Duration::from_secs(90);

/// `docker run` for one `kaf` invocation, with the persistent `--brokers` flag
/// already pointed at the broker's advertised listener.
fn kaf_command(container_name: Option<&str>, args: &[&str]) -> Command {
    let mut cmd = Command::new("docker");
    cmd.args([
        "run",
        "--rm",
        "-i",
        "--add-host=host.docker.internal:host-gateway",
    ]);
    if let Some(name) = container_name {
        cmd.args(["--name", name]);
    }
    cmd.args(["--entrypoint", PROGRAM, IMAGE, "--brokers"]);
    cmd.arg(broker0_advertised());
    cmd.args(args);
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

/// Run one `kaf` subcommand that terminates on its own, and return its stdout.
fn run_kaf(args: &[&str], input: Option<&str>) -> String {
    let mut child = kaf_command(None, args).spawn().expect("spawn kaf");
    if let Some(input) = input {
        child
            .stdin
            .as_mut()
            .expect("kaf stdin")
            .write_all(input.as_bytes())
            .expect("write kaf stdin");
    }
    drop(child.stdin.take());
    let out = child.wait_with_output().expect("wait for kaf");
    assert!(
        out.status.success(),
        "kaf {args:?} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8(out.stdout).expect("kaf stdout is utf-8")
}

/// The cells of one `kaf` table row. Every table `kaf` prints is padded with
/// a `text/tabwriter`, so a column is only recoverable by splitting on runs of
/// whitespace -- which is also why every value asserted below is one token.
fn cells(line: &str) -> Vec<String> {
    line.split_whitespace().map(str::to_owned).collect()
}

/// Go's `%v` for a slice of ints: `[1]`, `[1 2]`. `kaf` prints the replica and
/// ISR lists that way, so the expected value has to be built that way too.
fn go_slice(nodes: &[i32]) -> String {
    let joined = nodes
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    format!("[{joined}]")
}

/// krabka's own `DescribeCluster` answer, read on the host.
async fn krabka_cluster(client: &Client) -> DescribeClusterResponse {
    client
        .send(DescribeClusterRequest::default())
        .await
        .expect("DescribeCluster")
}

/// krabka's own `DescribeTopicPartitions` answer for [`TOPIC`], partitions in
/// index order.
async fn krabka_partitions(client: &Client) -> Vec<DescribeTopicPartitionsResponsePartition> {
    let resp: DescribeTopicPartitionsResponse = client
        .send(DescribeTopicPartitionsRequest {
            topics: vec![TopicRequest {
                name: TOPIC.into(),
                ..Default::default()
            }],
            response_partition_limit: 2000,
            cursor: None,
            ..Default::default()
        })
        .await
        .expect("DescribeTopicPartitions");
    assert!(resp.topics.len() == 1, "{resp:?}");
    let mut partitions = resp.topics[0].partitions.clone();
    partitions.sort_by_key(|partition| partition.partition_index);
    partitions
}

/// `kaf topics`: the `Metadata` listing, asserted as a whole row.
fn metadata_lists_topic() {
    let listing = run_kaf(&["topics"], None);
    let row = listing
        .lines()
        .map(cells)
        .find(|row| row.first().map(String::as_str) == Some(TOPIC))
        .unwrap_or_default();
    // NAME, PARTITIONS, REPLICAS -- the last from the length of partition 0's
    // replica list, which is the replication factor the topic was created at.
    assert!(
        row == vec![TOPIC.to_string(), PARTITIONS.to_string(), "1".to_string()],
        "kaf topics did not list {TOPIC} as krabka created it: {listing}"
    );
}

/// `kaf produce`: one line of stdin becomes one record, and `kaf` reports the
/// partition and offset the broker acknowledged.
fn produce_lands() {
    let receipt = run_kaf(
        &["produce", TOPIC, "--partition", "0"],
        Some(&format!("{PAYLOAD}\n")),
    );
    assert!(
        receipt == "Sent record to partition 0 at offset 0.\n",
        "unexpected produce receipt: {receipt}"
    );
}

/// Start `kaf consume -g ... --commit` in a named container.
///
/// It never returns on its own: `kaf` hands the group session a context that
/// is only cancelled by the process ending, so the caller stops the container
/// once the commit it is waiting for has landed.
fn spawn_group_consumer(container: &str) -> Child {
    let mut child = kaf_command(
        Some(container),
        &[
            "consume", TOPIC, "--group", GROUP, "--commit", "--offset", "oldest", "--output", "raw",
        ],
    )
    .spawn()
    .expect("spawn kaf consume");
    drop(child.stdin.take());
    child
}

/// Poll krabka for the group's committed offsets until one appears.
async fn committed_offsets(admin: &mut AdminClient) -> BTreeMap<(String, i32), i64> {
    let deadline = tokio::time::Instant::now() + COMMIT_TIMEOUT;
    loop {
        let offsets = admin
            .list_consumer_group_offsets(GROUP)
            .await
            .expect("list consumer group offsets");
        if !offsets.is_empty() {
            return offsets;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no committed offset for {GROUP} within {COMMIT_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Stop the consumer container and collect what it printed.
fn stop_consumer(container: &str, child: Child) -> Output {
    // The container may already be gone if `kaf` exited on an error, so a
    // failed `docker kill` is not itself a failure -- the assertions on the
    // client's own output are what decide the case.
    let _ = Command::new("docker")
        .args(["kill", container])
        .output()
        .expect("run docker kill");
    child.wait_with_output().expect("wait for kaf consume")
}

/// `kaf nodes`: `DescribeCluster` in sarama's sense, which is a `Metadata`
/// round-trip, checked against krabka's own `DescribeCluster` answer.
fn nodes_match_cluster(cluster: &DescribeClusterResponse) {
    let expected: Vec<Vec<String>> = cluster
        .brokers
        .iter()
        .map(|broker| {
            vec![
                broker.broker_id.to_string(),
                format!("{}:{}", broker.host, broker.port),
                (broker.broker_id == cluster.controller_id).to_string(),
            ]
        })
        .collect();
    let listing = run_kaf(&["nodes"], None);
    // The header row is `ID ADDRESS CONTROLLER`; `kaf nodes` has no flag to
    // suppress it (`--no-headers` is registered on `kaf node ls` alone).
    let rows: Vec<Vec<String>> = listing
        .lines()
        .map(cells)
        .filter(|row| row.first().map(String::as_str) != Some("ID"))
        .filter(|row| !row.is_empty())
        .collect();
    assert!(
        rows == expected,
        "kaf nodes disagrees with krabka's DescribeCluster: {listing}"
    );
}

/// `kaf topic describe`: sarama's `DescribeTopics`, again a `Metadata` round
/// trip, checked against krabka's own `DescribeTopicPartitions` answer.
fn topic_describe_matches_partitions(partitions: &[DescribeTopicPartitionsResponsePartition]) {
    let expected: Vec<Vec<String>> = partitions
        .iter()
        .map(|partition| {
            // One record was produced, to partition 0, so that partition's
            // high watermark is 1 and every other partition's is 0.
            let high_watermark = i32::from(partition.partition_index == 0);
            vec![
                partition.partition_index.to_string(),
                high_watermark.to_string(),
                partition.leader_id.to_string(),
                go_slice(&partition.replica_nodes),
                go_slice(&partition.isr_nodes),
            ]
        })
        .collect();

    let description = run_kaf(&["topic", "describe", TOPIC], None);
    // The partition table sits between its dashed divider and the summed
    // watermark line; the config table below it has its own dividers.
    let rows: Vec<Vec<String>> = description
        .lines()
        .skip_while(|line| !line.trim_start().starts_with("---------"))
        .skip(1)
        .take_while(|line| !line.starts_with("Summed"))
        .map(cells)
        .filter(|row| !row.is_empty())
        .collect();
    assert!(
        rows == expected,
        "kaf topic describe disagrees with krabka's DescribeTopicPartitions: {description}"
    );
}

/// `kaf group describe`: `DescribeGroups` plus `OffsetFetch` plus
/// `ListOffsets`, checked against the offsets krabka reports for the group.
fn group_describe_shows_commit(offsets: &BTreeMap<(String, i32), i64>) {
    let description = run_kaf(&["group", "describe", GROUP], None);
    let rows: Vec<Vec<String>> = description.lines().map(cells).collect();
    for ((topic, partition), offset) in offsets {
        // Partition, Group Offset, High Watermark, Lag. The one record is the
        // whole log, so the watermark is the committed offset and the lag 0.
        let expected = vec![
            partition.to_string(),
            offset.to_string(),
            offset.to_string(),
            "0".to_string(),
        ];
        assert!(
            rows.contains(&expected),
            "kaf group describe has no row for {topic}-{partition} at offset \
             {offset}: {description}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn sarama_round_trip_and_cluster_views_agree_with_krabka() {
    let (broker, _dir) = start_host_broker().await;
    let mut admin = AdminClient::connect(&[broker.listen_addr().to_string()])
        .await
        .expect("admin client");
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: TOPIC.to_string(),
                partitions: PARTITIONS,
                replicas: 1,
                configs: BTreeMap::default(),
            }],
            krabka_units::secs(5),
        )
        .await
        .expect("create conformance topic");
    let client = Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("krabka-sarama-conformance")
        .build()
        .await
        .expect("client build");

    metadata_lists_topic();
    produce_lands();

    let container = format!("krabka-sarama-consumer-{}", std::process::id());
    let child = spawn_group_consumer(&container);
    let offsets = committed_offsets(&mut admin).await;
    let consumed = stop_consumer(&container, child);
    let consumed_stdout = String::from_utf8(consumed.stdout).expect("kaf stdout is utf-8");
    assert!(
        consumed_stdout == format!("{PAYLOAD}\n"),
        "kaf consume did not return the produced payload: stdout={consumed_stdout}, stderr={}",
        String::from_utf8_lossy(&consumed.stderr)
    );
    // The record is offset 0, so the group's committed position is 1, on the
    // one partition it was produced to and on no other.
    assert!(
        offsets == BTreeMap::from([((TOPIC.to_string(), 0), 1)]),
        "unexpected committed offsets for {GROUP}: {offsets:?}"
    );
    group_describe_shows_commit(&offsets);

    let cluster = krabka_cluster(&client).await;
    nodes_match_cluster(&cluster);
    let partitions = krabka_partitions(&client).await;
    assert!(
        partitions.len() == usize::try_from(PARTITIONS).expect("partition count fits usize"),
        "krabka describes {} partitions, not {PARTITIONS}",
        partitions.len()
    );
    topic_describe_matches_partitions(&partitions);

    broker.shutdown().await;
}
