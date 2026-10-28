#![cfg(test)]

//! End-to-end test of JBOD, multi-log-dir, and `DescribeLogDirs` (KIP-113).
//!
//! It boots a single broker with two log directories, creates a 6-partition
//! topic, and asserts:
//!   1. the partition data is spread across both directories, from
//!      least-loaded placement, and
//!   2. `DescribeLogDirs` reports one result per directory, and the union of
//!      those results covers every partition and matches what is on disk.

mod kafka_wire;

mod support;

use {
    crate::support::{
        storage::{count_partition_dirs, start_two_dir_broker},
        topics::{ConfiguredTopicSetup, TopicPartitionCount},
        wire::HeaderEncoding as RequestHeaderEncoding,
    },
    assert2::assert,
    krabka_protocol::owned::{
        describe_log_dirs_request::DescribeLogDirsRequest,
        describe_log_dirs_response::DescribeLogDirsResponse,
    },
    std::{fs, net::SocketAddr, path::Path},
    tokio::net::TcpStream,
};

const CLIENT_ID: &str = "krabka-jbod-test";

async fn describe_log_dirs(addr: SocketAddr) -> DescribeLogDirsResponse {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    kafka_wire::exchange()
        .stream(&mut stream)
        .request(&DescribeLogDirsRequest {
            topics: None,
            ..Default::default()
        })
        .api_key(35)
        .version(4)
        .client_id(CLIENT_ID)
        .header(RequestHeaderEncoding::Flexible)
        .call()
        .await
        .unwrap()
}

/// Count `topic-partition` subdirs for `topic` directly under `dir`.
fn count_topic_dirs(dir: &Path, topic: &str) -> usize {
    count_partition_dirs(fs::read_dir(dir).unwrap(), topic, true)
}

use {
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};
struct JbodPlacement {
    storage: Option<(BrokerHandle, TempDir, TempDir, SocketAddr)>,
    topic: String,
    report: Option<DescribeLogDirsResponse>,
}
#[fixture]
fn placement() -> JbodPlacement {
    // Start this scenario with fresh inputs and observations.

    JbodPlacement {
        storage: None,
        topic: String::new(),
        report: None,
    }
}
#[given("one broker uses two independent log directories")]
async fn two_directories(placement: &mut JbodPlacement) {
    placement.storage = Some(start_two_dir_broker().await);
}
#[when("topic {topic} is created with {partitions} materialized local partitions")]
async fn create(placement: &mut JbodPlacement, topic: String, partitions: i32) {
    let (broker, _, _, address) = placement
        .storage
        .as_ref()
        .expect("two directory broker started");
    kafka_wire::create_configured_topic_plaintext()
        .addr(*address)
        .client_id(CLIENT_ID)
        .topic(ConfiguredTopicSetup {
            name: topic.clone(),
            partitions: TopicPartitionCount(partitions),
            ..Default::default()
        })
        .call()
        .await;
    for partition in 0..partitions {
        broker
            .wait_until_local_log_end_offset(&topic, partition, 0)
            .await;
    }
    placement.topic = topic;
}
#[then("both log directories hold partitions totaling {partitions}")]
fn spread(placement: &JbodPlacement, partitions: usize) {
    let (_, primary, extra, _) = placement
        .storage
        .as_ref()
        .expect("two directory broker started");
    let primary = count_topic_dirs(primary.path(), &placement.topic);
    let extra = count_topic_dirs(extra.path(), &placement.topic);
    assert!(primary + extra == partitions, "all partitions on disk");
    assert!(
        primary > 0 && extra > 0,
        "partitions must spread across both dirs: primary={primary} extra={extra}"
    );
}
#[when("the client describes all configured log directories")]
async fn inspect(placement: &mut JbodPlacement) {
    placement.report = Some(
        describe_log_dirs(
            placement
                .storage
                .as_ref()
                .expect("two directory broker started")
                .3,
        )
        .await,
    );
}
#[then(
    "two successful directory reports contain nonfuture nonnegative partition sizes for exactly"
)]
fn report(placement: &JbodPlacement, datatable: Vec<Vec<String>>) {
    let response = placement
        .report
        .as_ref()
        .expect("DescribeLogDirs completed");
    assert!(response.error_code == 0);
    assert!(response.results.len() == 2, "one result per log dir");
    let mut reported = Vec::new();
    for result in &response.results {
        assert!(result.error_code == 0);
        for topic in &result.topics {
            if topic.name == placement.topic {
                for partition in &topic.partitions {
                    reported.push(partition.partition_index);
                    assert!(partition.partition_size >= 0);
                    assert!(!partition.is_future_key);
                }
            }
        }
    }
    reported.sort_unstable();
    let expected: Vec<i32> = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            row.into_iter()
                .next()
                .expect("expected partition")
                .parse()
                .expect("integer partition")
        })
        .collect();
    assert!(reported == expected, "all partitions reported");
}
#[scenario(
    path = "tests/features/migrated/tests/jbod.feature",
    name = "New partitions spread across both directories and remain discoverable"
)]
#[tokio::test]
async fn partitions_spread_across_dirs_and_describe_log_dirs_reports_them(
    placement: JbodPlacement,
) {
    let (broker, _primary, _extra, _address) =
        placement.storage.expect("two directory broker started");
    broker.shutdown().await;
}
