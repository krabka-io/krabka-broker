//! Producer sequence and retry behavior for LZ4 batches retained verbatim.

use {
    crate::{
        harness::{boot, create_topic, idempotent_lz4_batch, produce_one, topic_id_for},
        support::client::connect_client,
    },
    assert2::check,
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_protocol::primitives::uuid::Uuid,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

struct ProducerSequences {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    client: Option<Client>,
    topic: String,
    topic_id: Uuid,
    producer: i64,
    answer: Option<Result<i64, i16>>,
}

#[fixture]
fn sequences() -> ProducerSequences {
    // Start this scenario with fresh inputs and observations.

    ProducerSequences {
        broker: None,
        directory: None,
        client: None,
        topic: String::new(),
        topic_id: Uuid::ZERO,
        producer: -1,
        answer: None,
    }
}

#[given("topic {topic} receives LZ4 batches from producer {producer}")]
async fn producer_topic(sequences: &mut ProducerSequences, topic: String, producer: i64) {
    let (broker, bootstrap, directory) = boot().await;
    create_topic(&broker, &bootstrap, &topic).await;
    let client = connect_client(bootstrap, None).await;
    sequences.topic_id = topic_id_for(&client, &topic).await;
    sequences.broker = Some(broker);
    sequences.directory = Some(directory);
    sequences.client = Some(client);
    sequences.topic = topic;
    sequences.producer = producer;
}

#[when("a producer batch starts at sequence {sequence} with record count {records}")]
async fn send_sequence(sequences: &mut ProducerSequences, records: usize, sequence: i32) {
    let client = sequences
        .client
        .as_ref()
        .expect("a producer topic was created");
    sequences.answer = Some(
        produce_one(
            client,
            &sequences.topic,
            sequences.topic_id,
            idempotent_lz4_batch(sequences.producer, sequence, records),
        )
        .await,
    );
}

#[when("the producer sends three records starting just before sequence rollover")]
async fn cross_rollover(sequences: &mut ProducerSequences) {
    send_sequence(sequences, 3, i32::MAX - 1).await;
}

#[when("the partition settles at log end five")]
async fn settle_first_batches(sequences: &ProducerSequences) {
    sequences
        .broker
        .as_ref()
        .expect("a broker was started")
        .wait_until_local_log_end_offset(&sequences.topic, 0, 5)
        .await;
}

#[then("the batch succeeds at base offset {offset}")]
fn accepted_sequence(sequences: &ProducerSequences, offset: i64) {
    check!(sequences.answer.as_ref().expect("the batch was sent") == &Ok(offset));
}

#[then("the batch is refused with OUT_OF_ORDER_SEQUENCE_NUMBER 45")]
fn rejected_sequence(sequences: &ProducerSequences) {
    check!(sequences.answer.as_ref().expect("the batch was sent") == &Err(45));
}

#[then("the producer topic log end remains {offset}")]
fn unchanged_log(sequences: &ProducerSequences, offset: i64) {
    check!(
        sequences
            .broker
            .as_ref()
            .expect("a broker was started")
            .local_log_end_offset(&sequences.topic, 0)
            == Some(offset)
    );
}

async fn stop_sequence_broker(sequences: ProducerSequences) {
    sequences
        .broker
        .expect("a broker was started")
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/produce_verbatim_passthrough/idempotence.feature",
    name = "A retry reuses its assigned offset while a sequence gap is refused"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_dedup_over_verbatim_path(sequences: ProducerSequences) {
    stop_sequence_broker(sequences).await;
}

#[scenario(
    path = "tests/features/migrated/tests/produce_verbatim_passthrough/idempotence.feature",
    name = "Producer sequences wrap modulo 2^31 without weakening exact-retry matching"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_sequence_rollover_over_verbatim_path(sequences: ProducerSequences) {
    stop_sequence_broker(sequences).await;
}
