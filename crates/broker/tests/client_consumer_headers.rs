#![cfg(test)]

mod support;

use {
    crate::support::{
        client::standalone_topic,
        producer::{default_producer, producer_record},
    },
    krabka_client_consumer::{AutoOffsetReset, Consumer, Header as ConsumerHeader},
    krabka_client_producer::{Header, ProducerRecord},
};

struct HeaderDelivery {
    directory: Option<tempfile::TempDir>,
    broker: Option<krabka_broker::BrokerHandle>,
    bootstrap: String,
    record: Option<ProducerRecord>,
    headers: Option<Vec<ConsumerHeader>>,
    consumer: Option<Consumer>,
}
#[rstest::fixture]
fn delivery() -> HeaderDelivery {
    // Start this scenario with fresh inputs and observations.

    HeaderDelivery {
        directory: None,
        broker: None,
        bootstrap: String::new(),
        record: None,
        headers: None,
        consumer: None,
    }
}
#[rstest_bdd_macros::given("topic h and a record with value v and header trace equal to abc")]
async fn header_record_input(delivery: &mut HeaderDelivery) {
    let (directory, broker, bootstrap, _) = standalone_topic("h").await;
    delivery.record = Some(ProducerRecord {
        headers: vec![Header {
            key: "trace".into(),
            value: Some("abc".into()),
        }],
        ..producer_record()
            .topic("h".into())
            .value(Some(bytes::Bytes::from_static(b"v")))
            .call()
    });
    delivery.directory = Some(directory);
    delivery.broker = Some(broker);
    delivery.bootstrap = bootstrap;
}
#[rstest_bdd_macros::when("the producer sends and flushes the record")]
async fn produce_header_record(delivery: &mut HeaderDelivery) {
    let producer = default_producer(&delivery.bootstrap).await;
    producer
        .send(delivery.record.take().expect("record prepared"))
        .await
        .unwrap();
    producer.flush().await.unwrap();
    drop(producer);
}
#[rstest_bdd_macros::when("an Earliest consumer in group g reads the first record")]
async fn consume_header_record(delivery: &mut HeaderDelivery) {
    let mut consumer = Consumer::builder()
        .bootstrap(&delivery.bootstrap)
        .group_id("g")
        .subscribe(vec!["h".to_owned()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    let records = loop {
        let records = consumer.poll(krabka_units::secs(2)).await.unwrap();
        if !records.is_empty() {
            break records;
        }
    };
    delivery.headers = Some(records[0].headers.clone());
    delivery.consumer = Some(consumer);
}
#[rstest_bdd_macros::then("the consumed record has exactly the trace header with value abc")]
fn consumed_header_preserved(delivery: &HeaderDelivery) {
    assert2::assert!(
        delivery
            .headers
            .as_ref()
            .expect("record consumed")
            .as_slice()
            == [ConsumerHeader {
                key: "trace".into(),
                value: Some("abc".into())
            }]
    );
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/client_consumer_headers.feature",
    name = "consumer record carries headers"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_record_carries_headers(delivery: HeaderDelivery) {
    delivery
        .consumer
        .expect("consumer configured")
        .close()
        .await
        .unwrap();
    delivery.broker.expect("broker started").shutdown().await;
}
