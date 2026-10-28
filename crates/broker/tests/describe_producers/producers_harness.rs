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
        topics::CreateTopicSetup,
        transactions::{ProducerIdentity, claim_idempotent_producer, init_producer_request},
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

/// `batch` marked transactional.
pub fn transactional(batch: RecordBatch) -> RecordBatch {
    RecordBatch {
        attributes: Attributes::default().with_transactional(true),
        ..batch
    }
}

use {
    crate::support::{
        admin::describe_producers_request,
        produce::{SinglePartitionProduceSetup, send_batch},
    },
    krabka_broker::{BrokerConfig, BrokerHandle},
    krabka_client_core::Client,
    krabka_protocol::{
        owned::{
            describe_producers_response::DescribeProducersResponse,
            produce_response::ProduceResponse, write_txn_markers_response::WriteTxnMarkersResponse,
        },
        primitives::uuid::Uuid,
    },
    tempfile::TempDir,
};
pub struct ProducerSnapshot {
    pub process: Option<support::InProcess>,
    pub cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>,
    pub connection: Option<Client>,
    pub response: Option<DescribeProducersResponse>,
    pub identities: Vec<(i64, i16)>,
    pub transactional: Option<ProducerIdentity>,
    pub topic_id: Option<Uuid>,
    pub produced: Option<ProduceResponse>,
    pub marker: Option<WriteTxnMarkersResponse>,
}
impl ProducerSnapshot {
    pub fn client(&self) -> &Client {
        self.connection.as_ref().unwrap_or_else(|| {
            &self
                .process
                .as_ref()
                .expect("standalone broker started")
                .client
        })
    }

    pub async fn shutdown(self) {
        drop(self.connection);
        if let Some(process) = self.process {
            process.broker.shutdown().await;
        }
        support::shutdown_cluster(self.cluster).await;
    }
}
#[rstest::fixture]
pub fn snapshot() -> ProducerSnapshot {
    // Start this scenario with fresh inputs and observations.

    ProducerSnapshot {
        process: None,
        cluster: Vec::new(),
        connection: None,
        response: None,
        identities: Vec::new(),
        transactional: None,
        topic_id: None,
        produced: None,
        marker: None,
    }
}
#[rstest_bdd_macros::given("a standalone broker is ready to inspect producer state")]
async fn producer_snapshot_broker(snapshot: &mut ProducerSnapshot) {
    snapshot.process = Some(support::start().await);
}
#[rstest_bdd_macros::given("producer topic {topic} exists with one partition")]
async fn producer_snapshot_topic(snapshot: &mut ProducerSnapshot, topic: String) {
    create_topic(snapshot.client(), &topic).await;
}
#[rstest_bdd_macros::given("{count} idempotent producer identities have been claimed")]
async fn producer_snapshot_identities(snapshot: &mut ProducerSnapshot, count: usize) {
    let process = snapshot
        .process
        .as_ref()
        .expect("standalone broker started");
    for _ in 0..count {
        snapshot.identities.push(init_producer(process).await);
    }
    for (id, _) in &snapshot.identities {
        assert!(*id >= 0);
    }
    if count == 2 {
        assert!(
            snapshot.identities[0].0 != snapshot.identities[1].0,
            "InitProducerId must return distinct ids on back-to-back calls"
        );
    }
}
#[rstest_bdd_macros::given(
    "each claimed producer appended values {values} to {topic} starting at sequence 0"
)]
async fn producer_snapshot_history(snapshot: &mut ProducerSnapshot, values: String, topic: String) {
    let topic_id = topic_id_for(snapshot.client(), &topic).await;
    let values: Vec<_> = values.split(',').collect();
    for (pid, epoch) in &snapshot.identities {
        let response = send_batch(
            snapshot.client(),
            batch()
                .pid(*pid)
                .epoch(*epoch)
                .base_seq(0)
                .values(&values)
                .call(),
            SinglePartitionProduceSetup {
                topic: topic.clone(),
                topic_id,
                ..SinglePartitionProduceSetup::replicated()
            },
        )
        .await;
        assert!(response.responses[0].partition_responses[0].error_code == 0);
    }
}
#[rstest_bdd_macros::when("the client describes producers for topic {topic} partitions {indexes}")]
async fn describe_snapshot_producers(
    snapshot: &mut ProducerSnapshot,
    topic: String,
    indexes: String,
) {
    let indexes = indexes
        .split(',')
        .map(|index| index.parse().expect("partition index"))
        .collect();
    snapshot.response = Some(
        snapshot
            .client()
            .send(describe_producers_request(topic, indexes))
            .await
            .expect("DescribeProducers"),
    );
}
