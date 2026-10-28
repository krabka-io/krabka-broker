//! Committed, aborted, and interleaved transaction visibility to Kafka consumers.

use {
    crate::{
        support::{
            client::connect_client,
            discovery::topic_metadata_request,
            fetch::{FetchLimits, RequestWaitMillis, single_partition_fetch},
            producer::{default_producer, enqueue_string_values, transactional_producer},
            topics::metadata_topic,
            transaction_wire::{observed_values, read_committed_at_least},
        },
        txn_harness::{boot_single, create_topic, create_topic_with_segment_bytes, rec, send_ok},
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_client_consumer::{Consumer, IsolationLevel},
    krabka_client_producer::Producer,
    krabka_protocol::{
        owned::{fetch_request::FetchRequest, fetch_response::FetchResponse},
        primitives::uuid::Uuid,
    },
    krabka_units::bytes,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::time::{Duration, Instant},
    tempfile::TempDir,
    tokio::time,
};

const SEGMENT_BYTES: u64 = 128;

#[derive(Default)]
struct IsolationScenario {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    bootstrap: String,
    topic: String,
    producer: Option<Producer>,
    later: Option<Producer>,
    consumers: Vec<Consumer>,
    committed: Vec<String>,
    uncommitted: Vec<String>,
    fetched: Option<FetchResponse>,
}

impl IsolationScenario {
    fn broker(&self) -> &BrokerHandle {
        self.broker
            .as_ref()
            .expect("the isolation broker is running")
    }

    fn producer(&self) -> &Producer {
        self.producer
            .as_ref()
            .expect("a transactional producer was initialized")
    }

    async fn shutdown(&mut self) {
        if let Some(later) = self.later.take() {
            later.close().await.unwrap();
        }
        self.producer
            .take()
            .expect("a transactional producer was initialized")
            .close()
            .await
            .unwrap();
        for consumer in self.consumers.drain(..) {
            consumer.close().await.unwrap();
        }
        self.broker
            .take()
            .expect("the isolation broker is running")
            .shutdown()
            .await;
        drop(self.directory.take());
    }
}

#[fixture]
fn isolation() -> IsolationScenario {
    // Each scenario owns fresh services and observations.
    IsolationScenario {
        broker: None,
        ..Default::default()
    }
}

async fn initialized_topic(
    isolation: &mut IsolationScenario,
    topic: String,
    transactional_id: &str,
    segments: Option<u64>,
) {
    let (broker, bootstrap, directory) = boot_single().await;
    if let Some(size) = segments {
        create_topic_with_segment_bytes(&bootstrap, &topic, size).await;
        let deadline = Instant::now() + Duration::from_secs(10);
        while !broker
            .partition_log_config_for_test(&topic, 0)
            .is_some_and(|config| config.segment_size == bytes(u32::try_from(size).unwrap()))
        {
            assert!(
                Instant::now() < deadline,
                "internal.segment.bytes did not reach the transaction log"
            );
            time::sleep(Duration::from_millis(25)).await;
        }
    } else {
        create_topic(&bootstrap, &topic).await;
    }
    let producer = transactional_producer(bootstrap.clone(), transactional_id).await;
    isolation.broker = Some(broker);
    isolation.directory = Some(directory);
    isolation.bootstrap = bootstrap;
    isolation.topic = topic;
    isolation.producer = Some(producer);
}

#[given("transactional producer {transactional_id} uses an empty topic {topic}")]
async fn ordinary_transaction_topic(
    isolation: &mut IsolationScenario,
    transactional_id: String,
    topic: String,
) {
    initialized_topic(isolation, topic, &transactional_id, None).await;
}

#[given("transaction abort-tid uses topic ta with 128-byte segments")]
async fn short_segment_transaction_topic(isolation: &mut IsolationScenario) {
    initialized_topic(isolation, "ta".into(), "abort-tid", Some(SEGMENT_BYTES)).await;
}

#[when("the producer commits records {values}")]
async fn commit_values(isolation: &IsolationScenario, values: String) {
    let transaction = isolation.producer().begin_transaction().await.unwrap();
    let values: Vec<_> = values.split(',').collect();
    enqueue_string_values(isolation.producer(), &isolation.topic, &values).await;
    transaction.commit().await.unwrap();
}

#[when("the producer acknowledges and aborts records {values}")]
async fn abort_values(isolation: &IsolationScenario, values: String) {
    let transaction = isolation.producer().begin_transaction().await.unwrap();
    for value in values.split(',') {
        send_ok(isolation.producer(), rec(&isolation.topic, value)).await;
    }
    transaction.abort().await.unwrap();
}

#[when("a nontransactional producer appends after and seals the abort marker")]
async fn append_after_abort(isolation: &mut IsolationScenario) {
    let producer = default_producer(isolation.bootstrap.clone()).await;
    send_ok(&producer, rec(&isolation.topic, "after")).await;
    isolation
        .broker()
        .wait_until_high_watermark(&isolation.topic, 0, 5)
        .await;
    isolation.later = Some(producer);
}

#[when("a read_committed wire fetch starts at offset zero of the sealed transaction log")]
async fn fetch_abort_index(isolation: &mut IsolationScenario) {
    let client = connect_client(isolation.bootstrap.clone(), None).await;
    let metadata = client
        .send(topic_metadata_request(Some(vec![metadata_topic(
            Some(isolation.topic.clone()),
            Uuid::default(),
        )])))
        .await
        .unwrap();
    isolation.fetched = Some(
        client
            .send(FetchRequest {
                replica_id: -1,
                isolation_level: 1,
                ..single_partition_fetch()
                    .topic(isolation.topic.clone())
                    .topic_id(metadata.topics[0].topic_id)
                    .limits(FetchLimits::one_mebibyte(RequestWaitMillis(1_000)))
                    .call()
            })
            .await
            .unwrap(),
    );
}

#[then("the sealed segment reports exactly one aborted transaction beginning at offset zero")]
fn sealed_abort_is_described(isolation: &IsolationScenario) {
    let aborted = isolation
        .fetched
        .as_ref()
        .expect("the wire fetch completed")
        .responses[0]
        .partitions[0]
        .aborted_transactions
        .as_deref()
        .unwrap_or_default();
    assert!(
        aborted.len() == 1 && aborted[0].first_offset == 0,
        "read_committed fetch must describe the abort from the sealed segment: {aborted:?}"
    );
}

#[when("read_committed group {group} polls until at least {count} records arrive")]
async fn read_committed_values(isolation: &mut IsolationScenario, group: String, count: usize) {
    let (consumer, seen) =
        read_committed_at_least(isolation.bootstrap.clone(), group, &isolation.topic, count).await;
    isolation.consumers.push(consumer);
    isolation.committed = seen;
}

#[then("read_committed sees exactly {values}")]
fn committed_values_match(isolation: &IsolationScenario, values: String) {
    let expected: Vec<_> = values.split(',').collect();
    assert!(
        isolation.committed == expected,
        "read_committed exposed the wrong committed records"
    );
}

#[when("read_uncommitted group g-abort-uc polls through the four data records")]
async fn read_uncommitted_values(isolation: &mut IsolationScenario) {
    let (consumer, seen) = observed_values(
        isolation.bootstrap.clone(),
        "g-abort-uc",
        &isolation.topic,
        (IsolationLevel::ReadUncommitted, None),
        Duration::from_secs(30),
        |seen| seen.len() >= 4,
        None,
    )
    .await;
    isolation.consumers.push(consumer);
    isolation.uncommitted = seen;
}

#[then("read_uncommitted sees exactly x,y,z,after")]
fn aborted_values_are_uncommitted_visible(isolation: &IsolationScenario) {
    assert!(
        isolation.uncommitted == ["x", "y", "z", "after"],
        "read_uncommitted must see aborted records"
    );
}

#[scenario(
    path = "tests/features/migrated/tests/transactions/txn_isolation.feature",
    name = "commit then read committed sees records"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_then_read_committed_sees_records(isolation: IsolationScenario) {
    isolation.shutdown().await;
    drop(isolation);
}

#[scenario(
    path = "tests/features/migrated/tests/transactions/txn_isolation.feature",
    name = "abort then read committed skips records"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_then_read_committed_skips_records(isolation: IsolationScenario) {
    isolation.shutdown().await;
    drop(isolation);
}

#[scenario(
    path = "tests/features/migrated/tests/transactions/txn_isolation.feature",
    name = "interleaved commit and abort"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interleaved_commit_and_abort(isolation: IsolationScenario) {
    isolation.shutdown().await;
    drop(isolation);
}
