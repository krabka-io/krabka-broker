//! The setup every `DescribeProducers` test shares: creating a topic, reading
//! back its id, claiming a producer id, and building the record batches the
//! produce calls send.
//!
//! Both batch builders live here because the transactional one is the
//! idempotent one with the transactional attribute bit set.

pub use crate::support::topic_id_for;
use {
    crate::support::{
        self,
        client::create_topic_named,
        discovery::coordinator_lookup_request,
        records::ProducerSequence,
        topics::CreateTopicSetup,
        transactions::{
            ProducerEpoch, ProducerIdentity, claim_idempotent_producer, init_producer_request,
        },
    },
    assert2::assert,
    krabka_protocol::records::{Attributes, RecordBatch},
    std::time::Duration,
    tokio::{time, time::Instant},
};

pub async fn create_topic(client: &krabka_client_core::Client, topic: &str) {
    create_topic_named(client, CreateTopicSetup {
        topic,
        ..Default::default()
    })
    .await;
}

pub async fn init_producer(p: &support::InProcess) -> (i64, i16) {
    // A null transactional id is an idempotent producer; an empty one is invalid.
    let init = claim_idempotent_producer(&p.client).await;
    (init.producer_id, init.producer_epoch)
}

pub async fn init_transactional_producer(
    p: &support::InProcess,
    transactional_id: &str,
) -> ProducerIdentity {
    let coordinator = p
        .client
        .send(coordinator_lookup_request(transactional_id, 1, vec![
            transactional_id.into(),
        ]))
        .await
        .expect("transactional FindCoordinator");
    assert!(
        coordinator.error_code == 0
            || coordinator
                .coordinators
                .iter()
                .all(|entry| entry.error_code == 0),
        "transactional FindCoordinator: {coordinator:?}"
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let init = p
            .client
            .send(
                init_producer_request()
                    .maybe_transactional_id(Some(transactional_id.into()))
                    .call(),
            )
            .await
            .expect("transactional InitProducerId");
        if init.error_code == 0 {
            return ProducerIdentity::from_wire((init.producer_id, init.producer_epoch));
        }
        assert!(
            matches!(init.error_code, 15 | 16) && Instant::now() < deadline,
            "transactional InitProducerId: {init:?}"
        );
        time::sleep(Duration::from_millis(100)).await;
    }
}

pub use crate::support::records::producer_values_batch as batch;

#[bon::builder]
pub fn transactional_batch(
    #[builder(into, default = krabka_ids::ProducerId(7))] pid: krabka_ids::ProducerId,
    #[builder(into, default)] epoch: ProducerEpoch,
    #[builder(into, default)] base_seq: ProducerSequence,
    #[builder(default = & ["v"])] values: &'_ [&'_ str],
) -> RecordBatch {
    RecordBatch {
        attributes: Attributes::default().with_transactional(true),
        ..batch()
            .pid(pid.0)
            .epoch(epoch.0)
            .base_seq(base_seq.0)
            .values(values)
            .call()
    }
}
