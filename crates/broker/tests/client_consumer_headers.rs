mod support;

use krabka_broker::{Broker, BrokerConfig};
use krabka_client_consumer::{AutoOffsetReset, Consumer, Header as ConsumerHeader};
use krabka_client_core::Client;
use krabka_client_producer::{Header, Producer, ProducerRecord};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_record_carries_headers() {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    // Create the topic before producing.
    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .unwrap();
    crate::support::client::create_topic(&admin, "h", 1).await;

    let producer = Producer::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .unwrap();
    producer
        .send(ProducerRecord {
            topic: "h".into(),
            partition: None,
            key: None,
            value: Some("v".into()),
            headers: vec![Header {
                key: "trace".into(),
                value: Some("abc".into()),
            }],
            timestamp_ms: None,
        })
        .await
        .unwrap();
    producer.flush().await.unwrap();
    let mut consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .group_id("g")
        .subscribe(vec!["h".to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    let recs = loop {
        let r = consumer.poll(krabka_units::secs(2)).await.unwrap();
        if !r.is_empty() {
            break r;
        }
    };
    assert2::assert!(
        recs[0].headers
            == vec![ConsumerHeader {
                key: "trace".into(),
                value: Some("abc".into()),
            }]
    );
}
