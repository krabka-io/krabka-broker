//! Client-producer fixtures with caller-supplied record bytes and routing.

use bytes::Bytes;
use krabka_client_producer::{Producer, ProducerRecord};

pub fn producer_record(
    topic: impl Into<String>,
    partition: Option<i32>,
    key: Option<Bytes>,
    value: Option<Bytes>,
) -> ProducerRecord {
    ProducerRecord {
        topic: topic.into(),
        partition,
        key,
        value,
        ..Default::default()
    }
}

/// Build a producer with its default client configuration.
///
/// # Panics
/// Panics if the producer cannot connect to the bootstrap server.
pub async fn default_producer(bootstrap: impl Into<String>) -> Producer {
    Producer::builder()
        .bootstrap(bootstrap)
        .build()
        .await
        .unwrap()
}

/// An acks-all producer whose acknowledgements cannot be hidden by client retries.
///
/// # Panics
/// Panics if the producer cannot connect to its bootstrap server.
pub async fn no_retry_acks_all_producer(
    bootstrap: &str,
    batch_size: Option<usize>,
    context: &str,
) -> Producer {
    let builder = Producer::builder()
        .bootstrap(bootstrap)
        .acks(krabka_client_producer::Acks::All)
        .enable_idempotence(false)
        .retries(0)
        .linger(std::time::Duration::ZERO);
    match batch_size {
        Some(size) => builder.batch_size(size).build().await.expect(context),
        None => builder.build().await.expect(context),
    }
}

/// A value-only record with the transaction fixtures' original owned string bytes.
pub fn string_record(topic: &str, value: &str) -> ProducerRecord {
    producer_record(topic, None, None, Some(Bytes::from(value.to_string())))
}

/// Build and initialize a transactional producer with otherwise default settings.
///
/// # Panics
/// Panics if the producer cannot connect or initialize its transaction state.
pub async fn transactional_producer(
    bootstrap: impl Into<String>,
    transactional_id: impl Into<String>,
) -> Producer {
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .transactional_id(transactional_id)
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();
    producer
}

/// Queue caller-provided unkeyed values and drop each delivery handle immediately.
/// The caller still owns flush and close ordering.
///
/// # Panics
/// Panics if any record cannot be queued.
pub async fn enqueue_unkeyed_values(
    producer: &Producer,
    topic: &str,
    values: impl IntoIterator<Item = Bytes>,
) {
    for value in values {
        drop(
            producer
                .enqueue(producer_record(topic, None, None, Some(value)))
                .await
                .expect("record is queued"),
        );
    }
}

/// Queue the transaction fixtures' owned string values without waiting on delivery handles.
///
/// # Panics
/// Panics if any record cannot be queued.
pub async fn enqueue_string_values(producer: &Producer, topic: &str, values: &[&str]) {
    enqueue_unkeyed_values(
        producer,
        topic,
        values.iter().map(|value| Bytes::from(value.to_string())),
    )
    .await;
}
