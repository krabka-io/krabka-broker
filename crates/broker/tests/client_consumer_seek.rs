//! Integration test for `Consumer::seek`.
//!
//! A seek issued before the first poll takes effect once the partition is
//! assigned, so the consumer resumes from the sought offset instead of from
//! `auto.offset.reset`. This test proves that the seek wins over the
//! post-assignment prime, that it drops no pre-seek records, and that it skips
//! none above the sought offset.
#![cfg(test)]

mod support;

use {
    crate::support::{
        client::standalone_topic,
        producer::{default_producer, producer_record},
    },
    krabka_client_consumer::{AutoOffsetReset, Consumer},
    std::time::{Duration, Instant},
};

async fn produce_n(bootstrap: &str, topic: &str, n: u32) {
    let producer = default_producer(bootstrap).await;
    for i in 0..n {
        producer
            .send(
                producer_record()
                    .topic((topic).into())
                    .maybe_partition(Some(0))
                    .maybe_key(Some(format!("k{i}").into()))
                    .value(Some(bytes::Bytes::from(format!("v{i}"))))
                    .call(),
            )
            .await
            .unwrap();
    }
    producer.flush().await.unwrap();
    drop(producer);
}

struct ConsumerSeeking {
    directory: Option<tempfile::TempDir>,
    broker: Option<krabka_broker::BrokerHandle>,
    consumer: Option<Consumer>,
    topic: String,
    seek: Option<Result<(), String>>,
    offsets: Option<Vec<i64>>,
}
#[rstest::fixture]
fn seeking() -> ConsumerSeeking {
    // Start this scenario with fresh inputs and observations.

    ConsumerSeeking {
        directory: None,
        broker: None,
        consumer: None,
        topic: String::new(),
        seek: None,
        offsets: None,
    }
}
#[rstest_bdd_macros::given(
    "topic {topic} has {records} records and an unpolled Earliest consumer in group {group}"
)]
async fn unpolled_consumer(
    seeking: &mut ConsumerSeeking,
    topic: String,
    records: u32,
    group: String,
) {
    let (directory, broker, bootstrap, _) = standalone_topic(&topic).await;
    produce_n(&bootstrap, &topic, records).await;
    seeking.consumer = Some(
        Consumer::builder()
            .bootstrap(&bootstrap)
            .group_id(&group)
            .subscribe(vec![topic.clone()])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build()
            .await
            .unwrap(),
    );
    seeking.directory = Some(directory);
    seeking.broker = Some(broker);
    seeking.topic = topic;
}
#[rstest_bdd_macros::when("the consumer seeks partition 0 to offset {offset}")]
async fn seek_consumer_offset(seeking: &mut ConsumerSeeking, offset: i64) {
    seeking.seek = Some(
        seeking
            .consumer
            .as_ref()
            .expect("consumer configured")
            .seek(&seeking.topic, 0, offset)
            .await
            .map_err(|error| error.to_string()),
    );
}
#[rstest_bdd_macros::then("the seek is accepted")]
fn consumer_seek_accepted(seeking: &ConsumerSeeking) {
    assert2::assert!(seeking.seek.as_ref().expect("seek attempted").is_ok());
}
#[rstest_bdd_macros::then("the seek is rejected")]
fn consumer_seek_rejected(seeking: &ConsumerSeeking) {
    assert2::assert!(seeking.seek.as_ref().expect("seek attempted").is_err());
}
#[rstest_bdd_macros::when("the consumer polls until three records arrive or 15 seconds elapse")]
async fn poll_sought_records(seeking: &mut ConsumerSeeking) {
    let consumer = seeking.consumer.as_mut().expect("consumer configured");
    let mut offsets = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while offsets.len() < 3 && Instant::now() < deadline {
        let records = consumer.poll(krabka_units::millis(500)).await.unwrap();
        offsets.extend(records.iter().map(|record| record.offset));
    }
    seeking.offsets = Some(offsets);
}
#[rstest_bdd_macros::then("exactly offsets 2 3 and 4 are delivered without pre-seek records")]
fn sought_offsets_delivered(seeking: &ConsumerSeeking) {
    assert2::assert!(
        seeking
            .offsets
            .as_ref()
            .expect("consumer polled")
            .as_slice()
            == [2, 3, 4]
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/client_consumer_seek.feature",
    name = "seek before first poll resumes from sought offset"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seek_before_first_poll_resumes_from_sought_offset(seeking: ConsumerSeeking) {
    seeking
        .consumer
        .expect("consumer configured")
        .close()
        .await
        .unwrap();
    seeking.broker.expect("broker started").shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/client_consumer_seek.feature",
    name = "seek rejects negative offset"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seek_rejects_negative_offset(seeking: ConsumerSeeking) {
    seeking
        .consumer
        .expect("consumer configured")
        .close()
        .await
        .unwrap();
    seeking.broker.expect("broker started").shutdown().await;
}
