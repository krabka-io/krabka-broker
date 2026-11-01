//! JVM cross-validation: a real JVM consumer is unaffected by barrier markers.
//!
//! This suite carries the load-bearing compatibility claim of the barrier
//! feature. A barrier marker is a Kafka control record, and the whole design
//! rests on the promise that an ordinary consumer skips one without noticing.
//! Every other test in this repository checks that promise against krabka's
//! own client, which krabka also wrote. This one checks it against Apache
//! Kafka's.
//!
//! The broker runs on the host and advertises `host.docker.internal`, and the
//! JVM tools run in a container. [`jvm_acceptance`] documents that networking.
//!
//! What the suite proves, per partition:
//!
//! 1. The JVM consumer reads exactly the produced records, in produce order,
//!    and no marker reaches it as a record.
//! 2. The log end offset that the JVM tooling reports is larger than the record
//!    count, by exactly the number of markers injected into that partition. The
//!    markers hold real offsets, and the consumer stepped over them.
//! 3. Neither claim changes under `read_committed`. A barrier marker carries no
//!    producer id, so the isolation level has nothing to say about it.
//!
//! Point 2 is the one that distinguishes "the consumer skipped the markers"
//! from "the broker never wrote them". Without it a broker that dropped every
//! marker would pass.
//!
//! ```text
//! cargo test -p krabka-broker --test jvm_barrier_markers -- --ignored --nocapture
//! ```
#![cfg(test)]

mod jvm_acceptance;
mod support;

use {
    crate::support::jvm_parse_offset,
    assert2::{assert, check},
    jvm_acceptance::{
        KAFKA_IMAGE_TXN, docker_run_kafka_tool_with_image, rlmm_broker0_advertised,
        start_host_broker_with,
    },
    krabka_broker::BrokerHandle,
    krabka_client_admin::{AdminClient, AdminClientLike as _},
    krabka_client_core::Client,
    krabka_client_producer::{Producer, ProducerRecord},
    krabka_protocol::krabka::barrier::{
        AlterBarrierGroupsRequest, AlterableBarrierGroup, CUT_STATUS_COMPLETE,
        DescribeBarrierGroupsRequest, TriggerBarrierRequest, TriggerBarrierResponse,
    },
    std::{
        collections::BTreeMap,
        slice,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::time,
};

const TOPIC: &str = "barrier-invisibility";
const BARRIER_GROUP: &str = "invisibility-cut";
const PARTITIONS: i32 = 3;
/// Records produced into each partition before the first cut, and again
/// between the two cuts. Every partition therefore ends with twice this many
/// records and two markers.
const RECORDS_PER_ROUND: i32 = 4;

/// The value of record `index` in partition `partition`.
///
/// The value carries both coordinates, so a record served from the wrong
/// partition fails the comparison rather than passing by luck.
fn value_of(partition: i32, index: i32) -> String { format!("p{partition}-r{index}") }

/// Produce one round of records into every partition.
async fn produce_round(producer: &Producer, round: i32) {
    for partition in 0..PARTITIONS {
        for index in 0..RECORDS_PER_ROUND {
            let value = value_of(partition, round * RECORDS_PER_ROUND + index);
            producer
                .send(ProducerRecord {
                    topic: TOPIC.to_owned(),
                    partition: Some(partition),
                    key: None,
                    value: Some(value.into_bytes().into()),
                    headers: Vec::new(),
                    timestamp_ms: None,
                })
                .await
                .expect("produce");
        }
    }
    producer.flush().await.expect("flush");
}

/// Block until the barrier coordinator answers.
///
/// `__barrier_state` is provisioned asynchronously at startup, so a define
/// sent too early is refused with `COORDINATOR_NOT_AVAILABLE`. Waiting is what
/// keeps this suite from depending on that timing.
async fn wait_for_coordinator(client: &Client) {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let answered = client
            .send(DescribeBarrierGroupsRequest::default())
            .await
            .is_ok();
        if answered {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the barrier coordinator never became available"
        );
        time::sleep(Duration::from_millis(200)).await;
    }
}

/// Define the barrier group over the topic.
async fn create_barrier_group(client: &Client) {
    let response = client
        .send(AlterBarrierGroupsRequest {
            groups: vec![AlterableBarrierGroup {
                group: BARRIER_GROUP.to_owned(),
                topics: vec![TOPIC.to_owned()],
                // Manual only. The test drives every injection itself, so a
                // scheduled one cannot add a marker mid-assertion.
                interval_ms: -1,
                retained_cuts: 8,
                delete: false,
                ..AlterableBarrierGroup::default()
            }],
            ..AlterBarrierGroupsRequest::default()
        })
        .await
        .expect("alter barrier groups");
    let result = response.results.first().expect("one result row");
    assert!(
        result.error_code == 0,
        "create group failed: code={} message={:?}",
        result.error_code,
        result.error_message
    );
}

/// Inject one cut and return the marker offset it took in each partition.
async fn inject_cut(client: &Client) -> TriggerBarrierResponse {
    client
        .send(TriggerBarrierRequest {
            group: BARRIER_GROUP.to_owned(),
            timeout_ms: 30_000,
            ..TriggerBarrierRequest::default()
        })
        .await
        .expect("trigger barrier")
}

/// Read one partition from the beginning with the JVM console consumer, and
/// return the record values in the order it served them.
///
/// `expected` bounds `--max-messages`, so the consumer returns as soon as it
/// has that many rather than waiting out the timeout. A marker delivered as a
/// record would push a marker's bytes into this list and fail the comparison.
fn jvm_consume(partition: i32, expected: usize, isolation: &str) -> Vec<String> {
    let out = docker_run_kafka_tool_with_image(KAFKA_IMAGE_TXN, &[
        "kafka-console-consumer",
        "--bootstrap-server",
        &format!("host.docker.internal:{}", jvm_acceptance::host_port()),
        "--topic",
        TOPIC,
        "--partition",
        &partition.to_string(),
        "--offset",
        "earliest",
        "--max-messages",
        &expected.to_string(),
        "--consumer-property",
        &format!("isolation.level={isolation}"),
        "--timeout-ms",
        "30000",
    ]);
    support::jvm_output_lines(&out)
}

/// The log end offset that the JVM tooling reports for one partition.
///
/// `kafka-get-offsets` prints `topic:partition:offset`. The test calls the
/// wrapper script rather than the class, because `GetOffsetShell` changed
/// package in Kafka 3.4 and the script did not.
fn jvm_log_end_offset(partition: i32) -> i64 {
    let out = docker_run_kafka_tool_with_image(KAFKA_IMAGE_TXN, &[
        "kafka-get-offsets",
        "--bootstrap-server",
        &format!("host.docker.internal:{}", jvm_acceptance::host_port()),
        "--topic-partitions",
        &format!("{TOPIC}:{partition}"),
    ]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let line = stdout
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with(&format!("{TOPIC}:{partition}:")))
        .unwrap_or_else(|| {
            panic!("kafka-get-offsets printed no row for partition {partition}: {stdout}")
        });
    jvm_parse_offset(line)
}

struct BarrierReads {
    broker: Option<(BrokerHandle, TempDir)>,
    producer: Option<Producer>,
    client: Option<Client>,
    cuts: Vec<TriggerBarrierResponse>,
    ends: BTreeMap<i32, i64>,
    reads: BTreeMap<(i32, String), Vec<String>>,
}
#[rstest::fixture]
fn barriers() -> BarrierReads {
    // Start this scenario with fresh inputs and observations.

    BarrierReads {
        broker: None,
        producer: None,
        client: None,
        cuts: Vec::new(),
        ends: BTreeMap::new(),
        reads: BTreeMap::new(),
    }
}
#[rstest_bdd_macros::given(
    "a three-partition topic barrier-invisibility and manual barrier group invisibility-cut on a single broker"
)]
async fn manual_barriers(barriers: &mut BarrierReads) {
    // One state partition at replication factor one, because this is a single
    // node: the 50-at-factor-3 default leaves the partition this group hashes
    // to without a leader, and the coordinator then refuses every request.
    let (broker, dir) = start_host_broker_with(|config| {
        config.barrier_state_num_partitions = 1;
        config.barrier_state_replication_factor = 1;
    })
    .await;
    // Despite the name, this accessor is broker 0 over loopback: the host-side
    // clients use it, and only the containers use the advertised name.
    let bootstrap = rlmm_broker0_advertised().to_owned();

    let mut admin = AdminClient::connect(slice::from_ref(&bootstrap))
        .await
        .expect("admin connect");
    admin
        .create_topics(
            &[support::admin::topic_spec(TOPIC, PARTITIONS, 1)],
            krabka_client_admin::TopicMutationOptions::with_timeout(krabka_units::secs(10)),
        )
        .await
        .expect("create topic");

    let producer = Producer::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .expect("producer");
    let client = support::client::connect_owned(&bootstrap, "barrier-jvm", "client build").await;

    wait_for_coordinator(&client).await;
    create_barrier_group(&client).await;

    barriers.broker = Some((broker, dir));
    barriers.producer = Some(producer);
    barriers.client = Some(client);
}
#[rstest_bdd_macros::when(
    "four records per partition are written, a cut completes, four more records are written, and a second cut completes"
)]
async fn interleaved_cuts(barriers: &mut BarrierReads) {
    produce_round(barriers.producer.as_ref().expect("producer"), 0).await;
    barriers
        .cuts
        .push(inject_cut(barriers.client.as_ref().expect("client")).await);
    produce_round(barriers.producer.as_ref().expect("producer"), 1).await;
    barriers
        .cuts
        .push(inject_cut(barriers.client.as_ref().expect("client")).await);
    for partition in 0..PARTITIONS {
        barriers
            .ends
            .insert(partition, jvm_log_end_offset(partition));
    }
}
#[rstest_bdd_macros::then(
    "both cuts succeed completely and JVM offsets include exactly two marker offsets per partition"
)]
fn marker_offsets(barriers: &BarrierReads, datatable: Vec<Vec<String>>) {
    assert!(barriers.cuts.len() == 2);
    for cut in &barriers.cuts {
        check!(cut.error_code == 0);
        check!(
            cut.status == CUT_STATUS_COMPLETE,
            "missing: {:?}",
            cut.missing
        );
        check!(
            cut.topics
                .iter()
                .find(|t| t.topic == TOPIC)
                .expect("topic in cut")
                .partitions
                .len()
                == 3
        );
    }
    for row in datatable.into_iter().skip(1) {
        let [partition, first, second, end]: [String; 4] =
            row.try_into().expect("partition first second end");
        let partition: i32 = partition.parse().expect("partition");
        for (cut, expected) in barriers.cuts.iter().zip([first, second]) {
            let offset = cut
                .topics
                .iter()
                .find(|t| t.topic == TOPIC)
                .expect("cut topic")
                .partitions
                .iter()
                .find(|p| p.partition == partition)
                .expect("cut partition")
                .offset;
            check!(offset == expected.parse::<i64>().expect("marker offset"));
        }
        check!(barriers.ends[&partition] == end.parse::<i64>().expect("end offset"));
    }
}
#[rstest_bdd_macros::when(
    "JVM consumers read eight records per partition using read_uncommitted and read_committed"
)]
fn jvm_reads(barriers: &mut BarrierReads) {
    for partition in 0..PARTITIONS {
        for isolation in ["read_uncommitted", "read_committed"] {
            barriers.reads.insert(
                (partition, isolation.to_owned()),
                jvm_consume(partition, 8, isolation),
            );
        }
    }
}
#[rstest_bdd_macros::then(
    "both isolation levels return only these ordered record values and no markers"
)]
fn invisible_markers(barriers: &BarrierReads, datatable: Vec<Vec<String>>) {
    for row in datatable.into_iter().skip(1) {
        let [partition, values]: [String; 2] = row.try_into().expect("partition values");
        let partition: i32 = partition.parse().expect("partition");
        let expected: Vec<String> = values.split(',').map(str::to_owned).collect();
        for isolation in ["read_uncommitted", "read_committed"] {
            check!(barriers.reads[&(partition, isolation.to_owned())] == expected);
        }
    }
}

/// A JVM consumer reads across barrier markers and never sees one.
#[ignore = "requires docker"]
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_barrier_markers.feature",
    name = "a jvm consumer reads across barrier markers unchanged"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_jvm_consumer_reads_across_barrier_markers_unchanged(barriers: BarrierReads) {
    barriers
        .producer
        .take()
        .expect("producer")
        .close()
        .await
        .expect("producer close");
    barriers.broker.take().expect("broker").0.shutdown().await;
    drop(barriers);
}
