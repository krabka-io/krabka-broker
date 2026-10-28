//! Integration tests for EOS durability: the high watermark and acks=all.
//!
//! These tests are gated off Windows, like the other multi-broker tests.
//! openraft and `tokio` scheduling on Windows runners cause flakes that have
//! nothing to do with the protocol under test.
#![cfg(test)]

use {
    crate::support::{
        client::{BatchProduceSetup, connect_client},
        durability::replicated_topic_fixture,
        fetch::single_partition_fetch,
        produce::{ProduceAcknowledgements, ProduceTimeoutMillis},
        producer::{producer_record, transactional_producer},
        registered_cluster, shutdown_cluster,
        topics::{CreateTopicSetup, TopicPartitionCount, TopicReplicationFactor},
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_broker::{
        BootstrapMode, Broker, BrokerConfig, BrokerHandle, testing::produce_partitions_request,
    },
    krabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel},
    krabka_protocol::{
        owned::{fetch_request::FetchRequest, produce_request::PartitionProduceData},
        records::RecordBatch,
    },
    std::{
        mem,
        time::{Duration, Instant},
    },
    support::{
        client::metric_value,
        durability::{
            create_topic_on_replicas as create_topic, produce_acks, produce_batch,
            record_batch_with_values,
        },
        topic_id_for,
        transaction_wire::close_transaction_clients,
    },
    tempfile::TempDir,
};

mod support;

pub use crate::support::boot_single;

/// An idempotent batch with an explicit `(producer_id, base_sequence)`, so
/// that a test can replay a "retry" deterministically by sending the same
/// batch again.
fn idempotent_batch(pid: i64, base_seq: i32, values: &[&str]) -> RecordBatch {
    let mut b = record_batch_with_values(values);
    b.producer_id = pid;
    b.producer_epoch = 0;
    b.base_sequence = base_seq;
    b
}

/// Create the topic and wait until every partition is materialized.
async fn create_topic_with_partitions(
    broker: &BrokerHandle,
    bootstrap: &str,
    setup: CreateTopicSetup<'_>,
) {
    let client = connect_client(bootstrap.to_owned(), None).await;
    support::client::create_topic_named(&client, setup).await;
    for index in 0..setup.num_partitions.0 {
        broker
            .wait_until_partition_present(setup.topic, index)
            .await;
    }
}

/// One partition row of a Produce response: its index and error code.
#[derive(Debug)]
struct PartitionAnswer {
    /// Only the failure messages name it.
    _index: i32,
    error_code: i16,
}

/// One Produce carrying every partition of `topic`, returning each row's index
/// and error code.
async fn produce_every_partition(
    bootstrap: &str,
    topic: &str,
    partitions: i32,
    acks: i16,
    timeout_ms: i32,
) -> Vec<PartitionAnswer> {
    let client = connect_client(bootstrap.to_owned(), None).await;
    let topic_id = topic_id_for(&client, topic).await;
    let resp = client
        .send(
            produce_partitions_request()
                .acks(acks)
                .timeout_ms(timeout_ms)
                .topic(topic.into())
                .topic_id(topic_id)
                .partitions(
                    (0..partitions)
                        .map(|index| PartitionProduceData {
                            index,
                            records: Some(record_batch_with_values(&["a"]).into()),
                            ..Default::default()
                        })
                        .collect(),
                )
                .call(),
        )
        .await
        .expect("Produce");
    resp.responses[0]
        .partition_responses
        .iter()
        .map(|row| PartitionAnswer {
            _index: row.index,
            error_code: row.error_code,
        })
        .collect()
}

// BDD scenario entry points; acceptance steps are defined above.

#[derive(Default)]
struct DurabilityBehavior {
    broker: Option<BrokerHandle>,
    cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>,
    bootstrap: String,
    topic: String,
    produced: Option<Result<i64, i16>>,
    elapsed: Option<Duration>,
    fetch_bounds: Option<(i16, i64)>,
    committed_values: Option<Vec<String>>,
    phase_times: Option<(f64, f64)>,
    partition_answers: Option<Vec<PartitionAnswer>>,
    directory_guard: Option<TempDir>,
}
#[rstest::fixture]
fn durability_behavior() -> DurabilityBehavior {
    // Broker logs and replication gates are fresh for each durability contract.
    DurabilityBehavior::default()
}
#[rstest_bdd_macros::given(
    "a durability topic {topic} on {brokers} brokers with {partitions} partitions and replication factor {replicas}"
)]
async fn durability_topic(
    durability_behavior: &mut DurabilityBehavior,
    topic: String,
    brokers: u64,
    partitions: i32,
    replicas: i16,
) {
    if brokers == 1 {
        let (broker, bootstrap, dir) = boot_single().await;
        create_topic()
            .broker(&broker)
            .bootstrap(&bootstrap)
            .topic(&topic)
            .call()
            .await;
        durability_behavior.broker = Some(broker);
        durability_behavior.bootstrap = bootstrap;
        durability_behavior.directory_guard = Some(dir);
    } else if partitions == 1 {
        support::init_tracing();
        let (cluster, bootstrap) = replicated_topic_fixture()
            .brokers(brokers)
            .topic(&topic)
            .replicas(TopicReplicationFactor(replicas))
            .call()
            .await;
        cluster[0]
            .0
            .wait_until_isr_len(&topic, 0, usize::try_from(replicas).expect("ISR count"))
            .await;
        durability_behavior.cluster = cluster;
        durability_behavior.bootstrap = bootstrap;
    } else {
        support::init_tracing();
        let cluster = registered_cluster(brokers).await;
        let bootstrap = cluster[0].1.listen_addr.to_string();
        create_topic_with_partitions(&cluster[0].0, &bootstrap, CreateTopicSetup {
            topic: &topic,
            num_partitions: TopicPartitionCount(partitions),
            replication_factor: TopicReplicationFactor(replicas),
            ..Default::default()
        })
        .await;
        for index in 0..partitions {
            cluster[0]
                .0
                .wait_until_isr_len(&topic, index, usize::try_from(replicas).expect("ISR count"))
                .await;
        }
        durability_behavior.cluster = cluster;
        durability_behavior.bootstrap = bootstrap;
    }
    durability_behavior.topic = topic;
}
#[derive(serde::Deserialize)]
struct DurableProduce {
    values: Vec<String>,
    acks: i16,
    timeout: i32,
    producer_id: Option<i64>,
    sequence: i32,
}
#[rstest_bdd_macros::when("the client produces durability batch {request}")]
async fn durable_produce(durability_behavior: &mut DurabilityBehavior, request: String) {
    let request: DurableProduce = serde_json::from_str(&request).expect("produce inputs");
    let values = request
        .values
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let acknowledgements = match request.acks {
        1 => ProduceAcknowledgements::Leader,
        -1 => ProduceAcknowledgements::AllReplicas,
        other => panic!("acks {other}"),
    };
    let setup = BatchProduceSetup {
        topic: &durability_behavior.topic,
        acknowledgements,
        timeout: ProduceTimeoutMillis(request.timeout),
        ..Default::default()
    };
    let start = Instant::now();
    let result = if let Some(pid) = request.producer_id {
        produce_batch(
            &durability_behavior.bootstrap,
            idempotent_batch(pid, request.sequence, &values),
            setup,
        )
        .await
    } else {
        produce_acks(&durability_behavior.bootstrap, &values, setup).await
    };
    durability_behavior.elapsed = Some(start.elapsed());
    durability_behavior.produced = Some(result);
}
#[rstest_bdd_macros::then("the durability produce succeeds at base offset {offset}")]
fn durable_base_offset(durability_behavior: &DurabilityBehavior, offset: i64) {
    assert!(
        durability_behavior
            .produced
            .as_ref()
            .expect("produce executed")
            == &Ok(offset)
    );
}
#[rstest_bdd_macros::then("the durability produce succeeds")]
fn durable_produce_success(durability_behavior: &DurabilityBehavior) {
    assert!(
        durability_behavior
            .produced
            .as_ref()
            .expect("produce executed")
            .is_ok()
    );
}
#[rstest_bdd_macros::then(
    "the produce duration is at least {minimum} and below {maximum} milliseconds"
)]
fn durable_produce_duration(durability_behavior: &DurabilityBehavior, minimum: u64, maximum: u64) {
    let elapsed = durability_behavior.elapsed.expect("produce timed");
    check!(elapsed >= Duration::from_millis(minimum));
    check!(elapsed < Duration::from_millis(maximum));
}
#[rstest_bdd_macros::when("the leader truncates its log to the returned batch base offset")]
async fn truncate_durable_batch(durability_behavior: &mut DurabilityBehavior) {
    let base = *durability_behavior
        .produced
        .as_ref()
        .expect("produce executed")
        .as_ref()
        .expect("successful batch");
    durability_behavior
        .broker
        .as_ref()
        .expect("single broker")
        .test_truncate_local_log(&durability_behavior.topic, 0, base)
        .await
        .expect("truncate local log");
}
#[rstest_bdd_macros::when("the disk-backed broker restarts in Rejoin mode and leads its partition")]
async fn restart_durable_broker(durability_behavior: &mut DurabilityBehavior) {
    durability_behavior
        .broker
        .take()
        .expect("single broker")
        .shutdown()
        .await;
    let mut config = BrokerConfig::for_tests(
        durability_behavior
            .directory_guard
            .as_ref()
            .expect("disk directory")
            .path()
            .to_path_buf(),
    );
    config.bootstrap_mode = BootstrapMode::Rejoin;
    let broker = Broker::start(config).await.expect("restart broker");
    broker
        .wait_until_local_partition_leader(
            &durability_behavior.topic,
            0,
            krabka_broker::NodeId(broker.node_id()),
        )
        .await;
    durability_behavior.bootstrap = broker.listen_addr().to_string();
    durability_behavior.broker = Some(broker);
}
#[rstest_bdd_macros::when(
    "the RF1 high watermark reaches {watermark} and a consumer fetches partition zero"
)]
async fn fetch_durable_watermark(durability_behavior: &mut DurabilityBehavior, watermark: i64) {
    durability_behavior
        .broker
        .as_ref()
        .expect("single broker")
        .wait_until_high_watermark(&durability_behavior.topic, 0, watermark)
        .await;
    let client = connect_client(durability_behavior.bootstrap.clone(), None).await;
    let topic_id = topic_id_for(&client, &durability_behavior.topic).await;
    let response = client
        .send(FetchRequest {
            replica_id: -1,
            ..single_partition_fetch()
                .topic(durability_behavior.topic.clone())
                .topic_id(topic_id)
                .call()
        })
        .await
        .expect("Fetch");
    let row = &response.responses[0].partitions[0];
    durability_behavior.fetch_bounds = Some((row.error_code, row.high_watermark));
}
#[rstest_bdd_macros::then(
    "the consumer fetch returns Kafka code {code} and high watermark {watermark}"
)]
fn durable_fetch_bounds(durability_behavior: &DurabilityBehavior, code: i16, watermark: i64) {
    assert!(durability_behavior.fetch_bounds.expect("fetch executed") == (code, watermark));
}
#[rstest_bdd_macros::when(
    "transaction rc-tid commits payloads {records} and read-committed group rc-g polls for ten seconds every 200 milliseconds"
)]
async fn read_durable_commit(durability_behavior: &mut DurabilityBehavior, records: String) {
    let records: Vec<String> = serde_json::from_str(&records).expect("transaction payloads");
    let broker = durability_behavior.broker.as_ref().expect("single broker");
    broker.wait_until_transaction_coordinator_ready().await;
    let producer = transactional_producer(durability_behavior.bootstrap.clone(), "rc-tid").await;
    let transaction = producer
        .begin_transaction()
        .await
        .expect("begin transaction");
    for value in &records {
        drop(
            producer
                .enqueue(
                    producer_record()
                        .topic(durability_behavior.topic.clone())
                        .value(Some(Bytes::from(value.clone())))
                        .call(),
                )
                .await
                .expect("record queued"),
        );
    }
    transaction.commit().await.expect("transaction committed");
    let mut consumer = Consumer::builder()
        .bootstrap(durability_behavior.bootstrap.clone())
        .group_id("rc-g")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe([durability_behavior.topic.clone()])
        .build()
        .await
        .expect("consumer");
    broker
        .wait_until_local_log_end_offset(
            &durability_behavior.topic,
            0,
            i64::try_from(records.len()).expect("record count"),
        )
        .await;
    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while seen.len() < records.len() && Instant::now() < deadline {
        for record in consumer
            .poll(krabka_units::millis(200))
            .await
            .expect("consumer poll")
        {
            seen.push(String::from_utf8_lossy(record.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    durability_behavior.committed_values = Some(seen);
    close_transaction_clients(producer.close(), consumer).await;
}
#[rstest_bdd_macros::then("the read-committed consumer sees exactly {records}")]
fn durable_committed_records(durability_behavior: &DurabilityBehavior, records: String) {
    let expected: Vec<String> =
        serde_json::from_str(&records).expect("expected committed payloads");
    assert!(
        durability_behavior
            .committed_values
            .as_ref()
            .expect("consumer polled")
            == &expected
    );
}
#[rstest_bdd_macros::when("the last in-sync follower is shut down")]
async fn stop_durable_follower(durability_behavior: &mut DurabilityBehavior) {
    let follower = durability_behavior.cluster.pop().expect("follower");
    follower.0.shutdown().await;
}
#[rstest_bdd_macros::when("the leader's Produce local and remote duration sums are scraped")]
async fn durable_produce_metrics(durability_behavior: &mut DurabilityBehavior) {
    let leader = &durability_behavior.cluster[0].0;
    let local = metric_value(
        leader,
        "krabka_broker_request_local_duration_seconds_sum{api_key=\"Produce\"}",
    )
    .await;
    let remote = metric_value(
        leader,
        "krabka_broker_request_remote_duration_seconds_sum{api_key=\"Produce\"}",
    )
    .await;
    durability_behavior.phase_times = Some((local, remote));
}
#[rstest_bdd_macros::then(
    "remote Produce time is at least {minimum} seconds and exceeds local time by factor {factor}"
)]
fn durable_phase_accounting(durability_behavior: &DurabilityBehavior, minimum: f64, factor: f64) {
    let (local, remote) = durability_behavior.phase_times.expect("metrics scraped");
    check!(remote >= minimum);
    check!(local * factor < remote);
}
#[rstest_bdd_macros::when(
    "one acks-all request writes {partitions} partitions with timeout {timeout} milliseconds"
)]
async fn produce_overlapping_partitions(
    durability_behavior: &mut DurabilityBehavior,
    partitions: i32,
    timeout: i32,
) {
    let start = Instant::now();
    let rows = produce_every_partition(
        &durability_behavior.bootstrap,
        &durability_behavior.topic,
        partitions,
        -1,
        timeout,
    )
    .await;
    durability_behavior.elapsed = Some(start.elapsed());
    durability_behavior.partition_answers = Some(rows);
}
#[rstest_bdd_macros::then(
    "all {count} partition rows return and at least {minimum} carry Kafka error {code}"
)]
fn durable_overlapping_rows(
    durability_behavior: &DurabilityBehavior,
    count: usize,
    minimum: usize,
    code: i16,
) {
    let rows = durability_behavior
        .partition_answers
        .as_ref()
        .expect("multi-partition produce executed");
    check!(rows.len() == count);
    check!(rows.iter().filter(|row| row.error_code == code).count() >= minimum);
}
#[rstest_bdd_macros::then("the durability brokers are shut down")]
async fn finish_durability(durability_behavior: &mut DurabilityBehavior) {
    if let Some(broker) = durability_behavior.broker.take() {
        broker.shutdown().await;
    }
    shutdown_cluster(mem::take(&mut durability_behavior.cluster)).await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/durability.feature",
    name = "idempotent retry reappends after truncation instead of stalling"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_retry_reappends_after_truncation_instead_of_stalling(
    durability_behavior: DurabilityBehavior,
) {
    let _ = durability_behavior;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/durability.feature",
    name = "disk backed restart recovers idempotent producer state"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_backed_restart_recovers_idempotent_producer_state(
    durability_behavior: DurabilityBehavior,
) {
    let _ = durability_behavior;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/durability.feature",
    name = "acks one returns quickly on rf1 broker"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acks_one_returns_quickly_on_rf1_broker(durability_behavior: DurabilityBehavior) {
    let _ = durability_behavior;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/durability.feature",
    name = "acks all returns quickly on rf1 broker"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acks_all_returns_quickly_on_rf1_broker(durability_behavior: DurabilityBehavior) {
    let _ = durability_behavior;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/durability.feature",
    name = "consumer clamps at hw when followers lag"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_clamps_at_hw_when_followers_lag(durability_behavior: DurabilityBehavior) {
    let _ = durability_behavior;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/durability.feature",
    name = "read committed under rf1 unchanged"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_committed_under_rf1_unchanged(durability_behavior: DurabilityBehavior) {
    let _ = durability_behavior;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/durability.feature",
    name = "acks all completes via isr shrink when follower dead"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_all_completes_via_isr_shrink_when_follower_dead(
    durability_behavior: DurabilityBehavior,
) {
    let _ = durability_behavior;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/durability.feature",
    name = "acks all stalled follower wait lands in remote time not local time"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_all_stalled_follower_wait_lands_in_remote_time_not_local_time(
    durability_behavior: DurabilityBehavior,
) {
    let _ = durability_behavior;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/durability.feature",
    name = "acks all waits for every partition at once not one after another"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_all_waits_for_every_partition_at_once_not_one_after_another(
    durability_behavior: DurabilityBehavior,
) {
    let _ = durability_behavior;
}
