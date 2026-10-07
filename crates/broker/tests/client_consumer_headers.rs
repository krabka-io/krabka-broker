mod support;

use krabka_client_consumer::{AutoOffsetReset, Consumer, Header as ConsumerHeader};
use krabka_client_producer::{Header, ProducerRecord};

use crate::support::client::connect_client;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_record_carries_headers() {
    let (_dir, broker) = crate::support::standalone_broker().await;
    let bootstrap = broker.listen_addr().to_string();

    // Create the topic before producing.
    let admin = connect_client(&bootstrap, None).await;
    crate::support::client::create_topic(&admin, "h", 1).await;

    let producer = crate::support::producer::default_producer(&bootstrap).await;
    producer
        .send(ProducerRecord {
            headers: vec![Header {
                key: "trace".into(),
                value: Some("abc".into()),
            }],
            ..crate::support::producer::producer_record("h", None, None, Some("v".into()))
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
