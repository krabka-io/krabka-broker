//! Spins up a 3-broker cluster on loopback, creates a topic with
//! replication-factor 3, produces records to the leader, and asserts
//! every follower's local log converges to the leader's
//! `log_end_offset`. It exercises the full replication path: the
//! supervisor reconcile, the follower Fetch loop, and
//! `Partition::replicate_batch`.
#![cfg(test)]

// Test-file pragmatism: casts turn 1-based `i` into broker ids.
// Hoisting these into named helpers would obscure the per-test narrative.

use {
    crate::support::{
        client::connect_client,
        offsets::{delete_records_partition, delete_records_request, delete_records_topic},
        produce::{SinglePartitionProduceSetup, batch_request},
        records::batch_from_records,
        registered_cluster, shutdown_cluster,
        topics::{
            CreateTopicSetup, TopicPartitionCount, TopicReplicationFactor, create_topic_request,
        },
    },
    assert2::assert,
    krabka_broker::{BrokerConfig, BrokerHandle},
    krabka_protocol::{
        owned::create_topics_request::{CreatableTopic, CreatableTopicConfig},
        primitives::uuid::Uuid,
        records::{Record, RecordBatch},
    },
    std::{
        net::SocketAddr,
        time::{Duration, Instant},
    },
    support::{client::scrape_metrics as scrape, cluster_lock},
    tempfile::TempDir,
    tokio::time,
};

mod support;

// Cargo compiles this file as its own test binary, so `#[path]` re-bases the
// part onto the sibling `replication/` directory, where it does not become a
// test binary of its own.
#[path = "replication/compaction_replicas.rs"]
mod compaction_replicas;
#[path = "replication/describe_producers_replicas.rs"]
mod describe_producers_replicas;
#[path = "replication/idempotent_failover.rs"]
mod idempotent_failover;

type Cluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;

/// A created topic: where its leader listens and the id `CreateTopics` gave it.
struct ReplicatedPartition {
    leader_addr: String,
    topic_id: Uuid,
}

async fn create_replicated_partition(
    cluster: &Cluster,
    topic: CreatableTopic,
) -> ReplicatedPartition {
    let leader_addr = cluster[0].1.listen_addr.to_string();
    let admin = connect_client(leader_addr.clone(), None).await;
    let name = topic.name.clone();
    let response = admin.send(create_topic_request(topic)).await.unwrap();
    assert!(response.topics[0].error_code == 0);
    // Produce v13 identifies topics by the CreateTopics-assigned UUID (KIP-516).
    let topic_id = response.topics[0].topic_id;
    for (handle, _, _) in cluster {
        handle.wait_until_partition_present(&name, 0).await;
    }
    ReplicatedPartition {
        leader_addr,
        topic_id,
    }
}

async fn three_replica_topic(topic: &str) -> (Cluster, ReplicatedPartition) {
    let cluster = registered_cluster(3).await;
    let partition =
        create_replicated_partition(&cluster, support::topic_on(topic, &[&[1, 2, 3]])).await;
    (cluster, partition)
}

/// A cluster and the metrics address of its first broker.
struct MetricsCluster {
    cluster: Cluster,
    metrics_addr: SocketAddr,
}

async fn metrics_cluster(topic: &str) -> MetricsCluster {
    let cluster = support::start_n_node_with(3, |_, config| {
        config.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());
    })
    .await
    .expect("3-broker cluster");
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    let admin = connect_client(cluster[0].1.listen_addr.to_string(), None).await;
    support::client::create_topic_with(&admin, CreateTopicSetup {
        topic,
        num_partitions: TopicPartitionCount(12),
        replication_factor: TopicReplicationFactor(3),
        ..Default::default()
    })
    .await;
    for (handle, _, _) in &cluster {
        for partition in 0..12 {
            handle.wait_until_partition_present(topic, partition).await;
        }
    }
    let metrics_addr = cluster[0]
        .0
        .metrics_addr()
        .expect("metrics server should be bound");
    MetricsCluster {
        cluster,
        metrics_addr,
    }
}

/// Reads one labelled counter out of a broker's `/metrics` body, summed over
/// every series whose label set contains `label_value`.
async fn scrape_counter(addr: SocketAddr, name: &str, label_value: &str) -> i64 {
    let body = scrape(addr).await;
    body.lines()
        .filter(|line| line.starts_with(name) && line.contains(label_value))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<i64>().ok())
        .sum()
}

/// Reads one label-free gauge out of a broker's `/metrics` body.
async fn scrape_gauge(addr: SocketAddr, name: &str) -> i64 {
    scrape(addr)
        .await
        .lines()
        .find_map(|line| line.strip_prefix(name)?.trim().parse::<i64>().ok())
        .unwrap_or(-1)
}

async fn produce_replicated(
    client: &krabka_client_core::Client,
    topic: &str,
    topic_id: Uuid,
    batch: RecordBatch,
) {
    let response = client
        .send(batch_request(batch, SinglePartitionProduceSetup {
            topic: (topic).into(),
            topic_id,
            ..SinglePartitionProduceSetup::replicated()
        }))
        .await
        .unwrap();
    assert!(response.responses[0].partition_responses[0].error_code == 0);
}

use {
    krabka_client_core::Client,
    krabka_protocol::owned::delete_records_response::DeleteRecordsPartitionResult,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::mem::take,
    tokio::sync::MutexGuard,
};

#[derive(Default)]
struct ReplicationScenario {
    cluster: Cluster,
    guard: Option<MutexGuard<'static, ()>>,
    topic: String,
    partition: Option<ReplicatedPartition>,
    producer: Option<Client>,
    metrics: Option<SocketAddr>,
    deleted: Option<Vec<DeleteRecordsPartitionResult>>,
    sessions: Option<i64>,
    fetch_counts: Option<(i64, i64, Duration)>,
}

#[fixture]
fn replication_case() -> ReplicationScenario {
    // Keep actual local logs and wire/metrics observations in the same fresh cluster.
    ReplicationScenario::default()
}

impl ReplicationScenario {
    async fn shutdown(&mut self) {
        shutdown_cluster(take(&mut self.cluster)).await;
        self.producer.take();
        self.guard.take();
    }
}

#[given("a three-replica topic {topic} assigns brokers one, two, and three")]
async fn three_replica_partition(replication_case: &mut ReplicationScenario, topic: String) {
    replication_case.guard = Some(cluster_lock().lock().await);
    let (cluster, partition) = three_replica_topic(&topic).await;
    cluster[0]
        .0
        .wait_until_local_partition_leader(&topic, 0, krabka_broker::NodeId(1))
        .await;
    replication_case.producer = Some(connect_client(&partition.leader_addr, None).await);
    replication_case.cluster = cluster;
    replication_case.partition = Some(partition);
    replication_case.topic = topic;
}

#[given(
    "topic seams has replicas one and two with one-MiB segments in a registered three-broker cluster"
)]
async fn segmented_replicated_partition(replication_case: &mut ReplicationScenario) {
    replication_case.guard = Some(cluster_lock().lock().await);
    let cluster = support::start_n_node_with_retry(3).await;
    for (broker, _, _) in &cluster {
        broker.wait_until_brokers_registered(3).await;
    }
    let partition = create_replicated_partition(&cluster, CreatableTopic {
        configs: vec![CreatableTopicConfig {
            name: "segment.bytes".into(),
            value: Some("1048576".into()),
            ..Default::default()
        }],
        ..support::topic_on("seams", &[&[1, 2]])
    })
    .await;
    replication_case.producer = Some(connect_client(&partition.leader_addr, None).await);
    replication_case.cluster = cluster;
    replication_case.partition = Some(partition);
    replication_case.topic = "seams".into();
}

#[given("a metrics-enabled three-broker cluster replicates twelve partitions of topic {topic}")]
async fn measured_replicated_partitions(replication_case: &mut ReplicationScenario, topic: String) {
    replication_case.guard = Some(cluster_lock().lock().await);
    let MetricsCluster {
        cluster,
        metrics_addr,
    } = metrics_cluster(&topic).await;
    replication_case.cluster = cluster;
    replication_case.metrics = Some(metrics_addr);
    replication_case.topic = topic;
}

#[when("the leader appends {records} records in batches of {batch_size}")]
async fn append_replication_batches(
    replication_case: &ReplicationScenario,
    records: i32,
    batch_size: i32,
) {
    assert!(records % batch_size == 0);
    for _ in 0..records / batch_size {
        produce_replicated(
            replication_case.producer.as_ref().expect("leader producer"),
            &replication_case.topic,
            replication_case
                .partition
                .as_ref()
                .expect("created partition")
                .topic_id,
            support::client::value_batch().records(batch_size).call(),
        )
        .await;
    }
}

#[then("every replica's local log reaches end offset {offset}")]
async fn replicated_log_end(replication_case: &ReplicationScenario, offset: i64) {
    for (broker, _, _) in &replication_case.cluster {
        broker
            .wait_until_local_log_end_offset(&replication_case.topic, 0, offset)
            .await;
    }
}

#[when("broker three loses its local log and the leader advances its log start to twenty-five")]
async fn lose_replication_prefix(replication_case: &ReplicationScenario) {
    replication_case.cluster[2]
        .0
        .test_truncate_local_log(&replication_case.topic, 0, 0)
        .await
        .expect("truncate broker three");
    replication_case.cluster[0]
        .0
        .test_advance_log_start(&replication_case.topic, 0, 25)
        .await
        .expect("advance leader log start");
}

#[then("broker three recovers to end fifty with log start twenty-five and batch bases {bases}")]
async fn recovered_replication_prefix(replication_case: &ReplicationScenario, bases: String) {
    replication_case.cluster[2]
        .0
        .wait_until_local_log_end_offset(&replication_case.topic, 0, 50)
        .await;
    support::poll::within(
        "follower log start reaching leader",
        Duration::from_secs(30),
        async {
            while replication_case.cluster[2]
                .0
                .partition_log_start_for_test(&replication_case.topic, 0)
                != Some(25)
            {
                time::sleep(Duration::from_millis(20)).await;
            }
        },
    )
    .await;
    let expected: Vec<i64> = if let Some((first, end)) = bases.split_once("..") {
        (first.parse().unwrap()..end.parse().unwrap()).collect()
    } else {
        bases.split(',').map(|base| base.parse().unwrap()).collect()
    };
    assert!(
        replication_case.cluster[2]
            .0
            .local_batch_base_offsets_for_test(&replication_case.topic, 0)
            == Some(expected)
    );
    assert!(
        replication_case.cluster[2]
            .0
            .partition_log_start_for_test(&replication_case.topic, 0)
            == Some(25)
    );
}

#[when("the seams leader appends twelve separate batches with 300000-byte records")]
async fn append_segmented_replication(replication_case: &ReplicationScenario) {
    for _ in 0..12 {
        produce_replicated(
            replication_case.producer.as_ref().expect("leader producer"),
            &replication_case.topic,
            replication_case
                .partition
                .as_ref()
                .expect("created partition")
                .topic_id,
            batch_from_records(vec![Record {
                value: Some(bytes::Bytes::from(vec![b'x'; 300_000])),
                ..Default::default()
            }]),
        )
        .await;
    }
}

#[then("broker two's local log reaches end offset twelve")]
async fn segmented_follower_caught_up(replication_case: &ReplicationScenario) {
    replication_case.cluster[1]
        .0
        .wait_until_local_log_end_offset(&replication_case.topic, 0, 12)
        .await;
}

#[when("broker two's local log is truncated to zero")]
async fn empty_segmented_follower(replication_case: &ReplicationScenario) {
    replication_case.cluster[1]
        .0
        .test_truncate_local_log(&replication_case.topic, 0, 0)
        .await
        .expect("truncate follower");
}

#[then("leader and follower both contain every batch base zero through eleven")]
async fn every_segmented_batch_replicated(replication_case: &ReplicationScenario) {
    replication_case.cluster[1]
        .0
        .wait_until_local_log_end_offset(&replication_case.topic, 0, 12)
        .await;
    for index in 0..2 {
        assert!(
            replication_case.cluster[index]
                .0
                .local_batch_base_offsets_for_test(&replication_case.topic, 0)
                == Some((0..12).collect())
        );
    }
}

#[when("DeleteRecords trims the replicated partition to offset ten")]
async fn trim_replicated_partition(replication_case: &mut ReplicationScenario) {
    let response = replication_case
        .producer
        .as_ref()
        .expect("leader producer")
        .send(delete_records_request(
            vec![delete_records_topic(&replication_case.topic, vec![
                delete_records_partition(0, 10),
            ])],
            30_000,
        ))
        .await
        .unwrap();
    replication_case.deleted = Some(response.topics[0].partitions.clone());
}

#[then("DeleteRecords returns exactly partition zero at low watermark ten without error")]
fn trimmed_replication_response(replication_case: &ReplicationScenario) {
    assert!(
        replication_case
            .deleted
            .as_ref()
            .expect("DeleteRecords answered")
            == &vec![DeleteRecordsPartitionResult {
                partition_index: 0,
                low_watermark: 10,
                error_code: 0,
                ..Default::default()
            }]
    );
}

#[then("all three replicas already report log start ten without waiting")]
fn trimmed_replication_logs(replication_case: &ReplicationScenario) {
    let starts: Vec<_> = replication_case
        .cluster
        .iter()
        .map(|(broker, _, _)| broker.partition_log_start_for_test(&replication_case.topic, 0))
        .collect();
    assert!(starts == vec![Some(10); 3]);
}

#[when("incremental replication session count is observed for up to twenty seconds")]
async fn count_replication_sessions(replication_case: &mut ReplicationScenario) {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut sessions = -1;
    while Instant::now() < deadline {
        sessions = scrape_gauge(
            replication_case.metrics.expect("metrics listener"),
            "krabka_broker_incremental_fetch_sessions",
        )
        .await;
        if sessions == 2 {
            break;
        }
        time::sleep(Duration::from_millis(100)).await;
    }
    replication_case.sessions = Some(sessions);
}

#[then("the leader holds exactly two incremental fetch sessions for its two followers")]
fn batched_replication_sessions(replication_case: &ReplicationScenario) {
    assert!(replication_case.sessions == Some(2));
}

#[when("followers settle for two seconds and Fetch requests are sampled over three seconds")]
async fn sample_idle_replication_requests(replication_case: &mut ReplicationScenario) {
    time::sleep(Duration::from_secs(2)).await;
    let before = scrape_counter(
        replication_case.metrics.expect("metrics listener"),
        "krabka_broker_api_requests_total",
        "Fetch",
    )
    .await;
    let window = Duration::from_secs(3);
    time::sleep(window).await;
    let after = scrape_counter(
        replication_case.metrics.expect("metrics listener"),
        "krabka_broker_api_requests_total",
        "Fetch",
    )
    .await;
    replication_case.fetch_counts = Some((before, after, window));
}

#[then("the caught-up leader receives fewer than forty Fetch requests per second")]
fn bounded_idle_replication_requests(replication_case: &ReplicationScenario) {
    let (before, after, window) = replication_case
        .fetch_counts
        .expect("request window sampled");
    let observed = i32::try_from(after - before).expect("three-second request count fits i32");
    let per_second = f64::from(observed) / window.as_secs_f64();
    assert!(
        per_second < 40.0,
        "caught-up follower must not busy-fetch: {per_second:.1} Fetch/s"
    );
}

#[scenario(
    path = "tests/features/migrated/tests/replication.feature",
    name = "replication factor three propagates to all followers"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_factor_three_propagates_to_all_followers(
    replication_case: ReplicationScenario,
) {
    replication_case.shutdown().await;
    drop(replication_case);
}

#[scenario(
    path = "tests/features/migrated/tests/replication.feature",
    name = "out of range at a batch boundary restarts at the leader log start"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn out_of_range_at_a_batch_boundary_restarts_at_the_leader_log_start(
    replication_case: ReplicationScenario,
) {
    replication_case.shutdown().await;
    drop(replication_case);
}

#[scenario(
    path = "tests/features/migrated/tests/replication.feature",
    name = "out of range inside a batch restarts at the batch base"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn out_of_range_inside_a_batch_restarts_at_the_batch_base(
    replication_case: ReplicationScenario,
) {
    replication_case.shutdown().await;
    drop(replication_case);
}

#[scenario(
    path = "tests/features/migrated/tests/replication.feature",
    name = "a follower that catches up across segments copies every batch"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_that_catches_up_across_segments_copies_every_batch(
    replication_case: ReplicationScenario,
) {
    replication_case.shutdown().await;
    drop(replication_case);
}

#[scenario(
    path = "tests/features/migrated/tests/replication.feature",
    name = "delete records moves every replica log start before it answers"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_records_moves_every_replica_log_start_before_it_answers(
    replication_case: ReplicationScenario,
) {
    replication_case.shutdown().await;
    drop(replication_case);
}

#[scenario(
    path = "tests/features/migrated/tests/replication.feature",
    name = "a follower batches every partition of one leader into one session"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_batches_every_partition_of_one_leader_into_one_session(
    replication_case: ReplicationScenario,
) {
    replication_case.shutdown().await;
    drop(replication_case);
}

#[scenario(
    path = "tests/features/migrated/tests/replication.feature",
    name = "a caught up follower fetches a few times a second whatever it follows"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_caught_up_follower_fetches_a_few_times_a_second_whatever_it_follows(
    replication_case: ReplicationScenario,
) {
    replication_case.shutdown().await;
    drop(replication_case);
}
