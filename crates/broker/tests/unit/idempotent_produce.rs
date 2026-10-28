//! Retrying and skipping sequences for an idempotent producer.

use {
    crate::{
        harness::{create_topic, topic_id_for},
        support::{
            self, InProcess,
            produce::{SinglePartitionProduceSetup, batch_request},
            records::producer_values_batch,
            transactions::claim_idempotent_producer,
        },
    },
    assert2::assert,
    krabka_protocol::{
        owned::{produce_request::ProduceRequest, produce_response::ProduceResponse},
        primitives::uuid::Uuid,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct IdempotentScenario {
    broker: Option<InProcess>,
    topic: String,
    topic_id: Uuid,
    producer_id: i64,
    request: Option<ProduceRequest>,
    responses: Vec<ProduceResponse>,
}

impl IdempotentScenario {
    fn broker(&self) -> &InProcess {
        self.broker
            .as_ref()
            .expect("the idempotent producer broker is running")
    }

    async fn shutdown(self) {
        self.broker
            .expect("the idempotent producer broker is running")
            .broker
            .shutdown()
            .await;
    }
}

#[fixture]
fn idempotence() -> IdempotentScenario {
    // Each scenario owns fresh services and observations.
    IdempotentScenario {
        broker: None,
        ..Default::default()
    }
}

#[given("an idempotent producer for topic {topic} with records {values}")]
async fn producer_batch(idempotence: &mut IdempotentScenario, topic: String, values: String) {
    let broker = support::start().await;
    create_topic(&broker, &topic, 1).await;
    let topic_id = topic_id_for(&broker.client, &topic).await;
    let producer_id = claim_idempotent_producer(&broker.client).await.producer_id;
    let values: Vec<_> = values.split(',').collect();
    let request = batch_request(
        producer_values_batch()
            .pid(producer_id)
            .values(&values)
            .call(),
        SinglePartitionProduceSetup {
            topic: topic.clone(),
            topic_id,
            ..SinglePartitionProduceSetup::replicated()
        },
    );
    idempotence.broker = Some(broker);
    idempotence.topic = topic;
    idempotence.topic_id = topic_id;
    idempotence.producer_id = producer_id;
    idempotence.request = Some(request);
}

#[when("the producer sends its initial sequence-zero batch")]
async fn send_producer_batch(idempotence: &mut IdempotentScenario) {
    let request = idempotence
        .request
        .clone()
        .expect("a producer batch was prepared");
    let response = idempotence
        .broker()
        .client
        .send(request)
        .await
        .expect("Produce");
    idempotence.responses.push(response);
}

#[when("the identical producer batch is retried")]
async fn retry_producer_batch(idempotence: &mut IdempotentScenario) {
    send_producer_batch(idempotence).await;
}

#[when("the producer skips to sequence ten with records x and y")]
async fn skip_sequences(idempotence: &mut IdempotentScenario) {
    let request = batch_request(
        producer_values_batch()
            .pid(idempotence.producer_id)
            .base_seq(10)
            .values(&["x", "y"])
            .call(),
        SinglePartitionProduceSetup {
            topic: idempotence.topic.clone(),
            topic_id: idempotence.topic_id,
            ..SinglePartitionProduceSetup::replicated()
        },
    );
    let response = idempotence
        .broker()
        .client
        .send(request)
        .await
        .expect("Produce seq=10");
    idempotence.responses.push(response);
}

#[then("produce response {number} succeeds at base offset {offset}")]
fn acknowledged_offset(idempotence: &IdempotentScenario, number: usize, offset: i64) {
    let partition = &idempotence.responses[number - 1].responses[0].partition_responses[0];
    assert!(partition.error_code == 0);
    assert!(partition.base_offset == offset);
}

#[then("the sequence gap returns OUT_OF_ORDER_SEQUENCE_NUMBER")]
fn rejected_sequence_gap(idempotence: &IdempotentScenario) {
    assert!(idempotence.responses[1].responses[0].partition_responses[0].error_code == 45);
}

#[scenario(
    path = "tests/features/migrated/tests/unit/idempotent_produce.feature",
    name = "idempotent produce dedups duplicate batch"
)]
#[tokio::test]
async fn idempotent_produce_dedups_duplicate_batch(idempotence: IdempotentScenario) {
    idempotence.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/unit/idempotent_produce.feature",
    name = "out of order returns 45"
)]
#[tokio::test]
async fn out_of_order_returns_45(idempotence: IdempotentScenario) { idempotence.shutdown().await; }
