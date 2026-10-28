//! Real producer, transaction and `ListOffsets` operations shared by isolation-bound stories.
use {
    crate::{
        support,
        wire::{
            EndOfPartition, both_levels, create_topic, end_of_partition, latest_row, matched_row,
            refused_row, wait_for_settled_log,
        },
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_client_producer::{OwnedTransaction, Producer, ProducerRecord},
    rstest::fixture,
    rstest_bdd_macros::{given, then, when},
    std::sync::Arc,
};
pub struct Isolation {
    process: Option<support::InProcess>,
    topic: String,
    plain: Option<Producer>,
    producer: Option<Arc<Producer>>,
    transaction: Option<OwnedTransaction>,
    current: Option<EndOfPartition>,
    hold_returns: Vec<bool>,
    resolved_queries: Vec<EndOfPartition>,
}
#[fixture]
pub fn isolation() -> Isolation {
    // Start this scenario with fresh inputs and observations.

    Isolation {
        process: None,
        topic: String::new(),
        plain: None,
        producer: None,
        transaction: None,
        current: None,
        hold_returns: Vec::new(),
        resolved_queries: Vec::new(),
    }
}
#[given("a standalone broker is running for isolation bound queries")]
async fn standalone(isolation: &mut Isolation) { isolation.process = Some(support::start().await); }
#[given("the standalone broker's transaction coordinator is ready")]
async fn coordinator(isolation: &mut Isolation) {
    isolation
        .process
        .as_ref()
        .expect("isolation broker started")
        .broker
        .wait_until_transaction_coordinator_ready()
        .await;
}
#[given("topic {topic} has the following settled nontransactional records")]
async fn settled(isolation: &mut Isolation, topic: String, datatable: Vec<Vec<String>>) {
    let process = isolation
        .process
        .as_ref()
        .expect("isolation broker started");
    create_topic(&process.client, &topic).await;
    process.broker.wait_until_partition_present(&topic, 0).await;
    isolation.plain = Some(
        Producer::builder()
            .bootstrap(process.broker.listen_addr().to_string())
            .build()
            .await
            .expect("plain producer build"),
    );
    let records = record_rows(datatable);
    let end = i64::try_from(records.len()).expect("record count fits");
    for (value, timestamp) in records {
        isolation
            .plain
            .as_ref()
            .expect("plain producer configured")
            .send(record(&topic, value, timestamp))
            .await
            .expect("settled record acknowledged");
    }
    wait_for_settled_log(&process.broker, &topic, end).await;
    isolation.topic = topic;
}
fn record_rows(datatable: Vec<Vec<String>>) -> Vec<(String, Option<i64>)> {
    datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [value, timestamp]: [String; 2] =
                row.try_into().expect("record value and timestamp");
            let timestamp = if timestamp == "auto" {
                None
            } else {
                Some(timestamp.parse().expect("timestamp milliseconds"))
            };
            (value, timestamp)
        })
        .collect()
}
fn record(topic: &str, value: String, timestamp_ms: Option<i64>) -> ProducerRecord {
    ProducerRecord {
        timestamp_ms,
        ..support::producer::producer_record()
            .topic(topic.to_owned())
            .maybe_value(Some(Some(Bytes::from(value))))
            .call()
    }
}
#[when("transactional producer {tid} opens a transaction and appends these records")]
async fn open(isolation: &mut Isolation, tid: String, datatable: Vec<Vec<String>>) {
    let process = isolation
        .process
        .as_ref()
        .expect("isolation broker started");
    let before = process
        .broker
        .local_log_end_offset(&isolation.topic, 0)
        .expect("local partition");
    isolation.producer = Some(Arc::new(
        Producer::builder()
            .bootstrap(process.broker.listen_addr().to_string())
            .transactional_id(tid)
            .build()
            .await
            .expect("transactional producer build"),
    ));
    isolation
        .producer
        .as_ref()
        .expect("transactional producer configured")
        .init_transactions()
        .await
        .expect("init transactions");
    isolation.transaction = Some(
        Arc::clone(
            isolation
                .producer
                .as_ref()
                .expect("transactional producer configured"),
        )
        .begin_transaction_owned()
        .await
        .expect("begin transaction"),
    );
    let records = record_rows(datatable);
    let added = i64::try_from(records.len()).expect("record count fits");
    for (value, timestamp) in records {
        isolation
            .producer
            .as_ref()
            .expect("transactional producer configured")
            .send(record(&isolation.topic, value, timestamp))
            .await
            .expect("transactional record acknowledged");
    }
    wait_for_settled_log(&process.broker, &isolation.topic, before + added).await;
}
#[when(
    "the open transaction resolves by {resolution} and its control marker settles at log end {offset}"
)]
async fn resolve(isolation: &mut Isolation, resolution: String, offset: i64) {
    let transaction = isolation
        .transaction
        .take()
        .expect("open transaction exists");
    match resolution.as_str() {
        "commit" => transaction.commit().await.expect("commit"),
        "abort" => transaction.abort().await.expect("abort"),
        _ => panic!("unknown transaction resolution {resolution}"),
    }
    wait_for_settled_log(
        &isolation
            .process
            .as_ref()
            .expect("isolation broker started")
            .broker,
        &isolation.topic,
        offset,
    )
    .await;
}
#[when("LATEST is requested at both isolation levels")]
async fn latest(isolation: &mut Isolation) {
    isolation.current = Some(
        end_of_partition(
            &isolation
                .process
                .as_ref()
                .expect("isolation broker started")
                .client,
            &isolation.topic,
        )
        .await,
    );
}
#[then(
    "the complete LATEST rows report uncommitted offset {uncommitted} and committed offset {committed}"
)]
fn latest_offsets(isolation: &Isolation, uncommitted: i64, committed: i64) {
    check!(
        isolation.current.as_ref()
            == Some(&EndOfPartition {
                read_uncommitted: latest_row(uncommitted),
                read_committed: latest_row(committed)
            })
    );
}
#[when(
    "the high watermark is held at {offset} while both isolation levels are queried with at most {attempts} attempts"
)]
async fn hold(isolation: &mut Isolation, offset: i64, attempts: usize) {
    let process = isolation
        .process
        .as_ref()
        .expect("isolation broker started");
    isolation.current = None;
    for _ in 0..attempts {
        isolation.hold_returns.push(
            process
                .broker
                .hold_high_watermark_for_test(&isolation.topic, 0, offset)
                .await,
        );
        let reading = end_of_partition(&process.client, &isolation.topic).await;
        if process
            .broker
            .high_watermark_for_test(&isolation.topic, 0)
            .await
            == Some(offset)
        {
            isolation.current = Some(reading);
            break;
        }
    }
}
#[then("every watermark hold targeted a partition actually hosted by the broker")]
fn hosted(isolation: &Isolation) {
    assert!(!isolation.hold_returns.is_empty());
    check!(isolation.hold_returns.iter().all(|held| *held));
}
#[when("timestamp selection {timestamp} is requested at both isolation levels")]
async fn timestamp(isolation: &mut Isolation, timestamp: i64) {
    isolation.current = Some(
        both_levels(
            &isolation
                .process
                .as_ref()
                .expect("isolation broker started")
                .client,
            &isolation.topic,
            timestamp,
        )
        .await,
    );
}
#[then("the complete selected rows contain these offsets and timestamps with no errors")]
fn selected(isolation: &Isolation, datatable: Vec<Vec<String>>) {
    let mut uncommitted = None;
    let mut committed = None;
    for row in datatable.into_iter().skip(1) {
        let [level, offset, timestamp]: [String; 3] =
            row.try_into().expect("isolation offset timestamp");
        let expected = matched_row(
            offset.parse().expect("offset"),
            timestamp.parse().expect("timestamp"),
        );
        match level.as_str() {
            "read_uncommitted" => uncommitted = Some(expected),
            "read_committed" => committed = Some(expected),
            _ => panic!("unknown isolation level {level}"),
        }
    }
    check!(
        isolation.current.as_ref()
            == Some(&EndOfPartition {
                read_uncommitted: uncommitted.expect("uncommitted expectation"),
                read_committed: committed.expect("committed expectation")
            })
    );
}
#[when("these timestamp selections are requested after the transaction resolves")]
async fn after_resolution(isolation: &mut Isolation, datatable: Vec<Vec<String>>) {
    let process = isolation
        .process
        .as_ref()
        .expect("isolation broker started");
    for row in datatable.into_iter().skip(1) {
        let timestamp = row
            .into_iter()
            .next()
            .expect("timestamp selection")
            .parse()
            .expect("timestamp value");
        isolation
            .resolved_queries
            .push(both_levels(&process.client, &isolation.topic, timestamp).await);
    }
}
#[then("all post resolution selections agree between isolation levels and resolve actual records")]
fn resolved(isolation: &Isolation) {
    assert!(isolation.resolved_queries.len() == 3);
    for answer in &isolation.resolved_queries {
        check!(answer.read_committed == answer.read_uncommitted);
        check!(answer.read_committed != refused_row());
        check!(answer.read_committed.error_code == 0);
        check!(answer.read_committed.offset >= 0);
    }
}
pub async fn shutdown(isolation: &mut Isolation) {
    if let Some(transaction) = isolation.transaction.take() {
        transaction
            .abort()
            .await
            .expect("abort remaining transaction");
    }
    if let Some(producer) = isolation.producer.take() {
        Arc::into_inner(producer)
            .expect("owned transaction released producer")
            .close()
            .await
            .expect("transactional producer close");
    }
    if let Some(plain) = isolation.plain.take() {
        plain.close().await.expect("plain producer close");
    }
    isolation
        .process
        .take()
        .expect("isolation broker started")
        .broker
        .shutdown()
        .await;
}
