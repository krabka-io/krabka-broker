//! Zombie-fencing tests (KIP-98 and KIP-447).
//!
//! A second producer that claims the same `transactional_id` bumps the epoch
//! and fences the first, and the broker rejects a `TxnOffsetCommit` whose group
//! metadata is stale — a classic-group generation that no longer matches, an
//! unknown member, or a next-gen member epoch that is behind or ahead of the
//! live one.

use {
    crate::{
        support::{
            self,
            client::connect_client,
            consumer_groups::joining_consumer,
            producer::transactional_producer,
            transactions::{
                ProducerIdentity, init_producer_request, txn_offset_partition, txn_offset_topic,
            },
        },
        txn_harness::{
            boot_single, boot_single_trunk, create_topic, init_transaction, rec, send_ok,
        },
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_client_consumer::{AutoOffsetReset, Consumer},
    krabka_client_core::Client,
    krabka_client_producer::{EndTransactionError, OwnedTransaction, Producer, ProducerError},
    krabka_protocol::{
        owned::{
            init_producer_id_response::InitProducerIdResponse,
            txn_offset_commit_request::TxnOffsetCommitRequest,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::sync::Arc,
    tempfile::TempDir,
};

#[derive(Default)]
struct FencingScenario {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    bootstrap: String,
    client: Option<Client>,
    producer: Option<Arc<Producer>>,
    replacement: Option<Producer>,
    transaction: Option<OwnedTransaction>,
    failures: Vec<ProducerError>,
    identity: Option<ProducerIdentity>,
    init_response: Option<InitProducerIdResponse>,
    consumer: Option<Consumer>,
    topic_id: Option<WireUuid>,
    member_id: String,
    generation: i32,
    offset_codes: Vec<i16>,
}

#[fixture]
fn fencing_case() -> FencingScenario {
    // Preserve live producer and member identities between actual protocol operations.
    FencingScenario::default()
}

impl FencingScenario {
    fn client(&self) -> &Client { self.client.as_ref().expect("coordinator client") }

    async fn shutdown(&mut self) {
        self.transaction.take();
        self.replacement.take();
        self.producer.take();
        if let Some(consumer) = self.consumer.take() {
            consumer.close().await.unwrap();
        }
        if let Some(client) = self.client.take() {
            client.close();
        }
        self.broker.take().expect("broker running").shutdown().await;
        self.directory.take();
    }
}

#[given("a fencing broker with unstable protocol versions {unstable}")]
async fn fencing_broker(fencing_case: &mut FencingScenario, unstable: bool) {
    let (broker, bootstrap, directory) = if unstable {
        boot_single_trunk().await
    } else {
        boot_single().await
    };
    fencing_case.client = Some(connect_client(bootstrap.clone(), None).await);
    fencing_case.broker = Some(broker);
    fencing_case.bootstrap = bootstrap;
    fencing_case.directory = Some(directory);
}

#[given("producer A has acknowledged first in an open shared-tid transaction")]
async fn acknowledged_transaction(fencing_case: &mut FencingScenario) {
    create_topic(&fencing_case.bootstrap, "tf").await;
    fencing_case.producer = Some(Arc::new(
        transactional_producer(fencing_case.bootstrap.clone(), "shared-tid").await,
    ));
    fencing_case.transaction = Some(
        Arc::clone(fencing_case.producer.as_ref().expect("producer A"))
            .begin_transaction_owned()
            .await
            .unwrap(),
    );
    send_ok(
        fencing_case.producer.as_ref().expect("producer A"),
        rec("tf", "first"),
    )
    .await;
}

#[when("producer B initializes the same shared-tid transactional identity")]
async fn replacement_producer(fencing_case: &mut FencingScenario) {
    fencing_case.replacement =
        Some(transactional_producer(fencing_case.bootstrap.clone(), "shared-tid").await);
}

#[when("producer A attempts to commit its acknowledged transaction")]
async fn fenced_commit(fencing_case: &mut FencingScenario) {
    let EndTransactionError {
        transaction,
        source,
    } = fencing_case
        .transaction
        .take()
        .expect("open transaction")
        .commit()
        .await
        .expect_err("commit must fail after fencing");
    fencing_case.transaction = Some(transaction);
    fencing_case.failures.push(source);
}

#[then("producer A's latest commit fails with FencedProducer")]
fn producer_fenced(fencing_case: &FencingScenario) {
    assert!(matches!(
        fencing_case.failures.last().expect("commit attempted"),
        ProducerError::FencedProducer
    ));
}

#[given("the coordinator holds a fresh kip360-tid producer identity")]
async fn initialized_identity(fencing_case: &mut FencingScenario) {
    fencing_case.identity = Some(init_transaction(fencing_case.client(), "kip360-tid").await);
}

#[when(
    "InitProducerId supplies identity mode {mode} with producer ID delta {id_delta} and epoch delta {epoch_delta}"
)]
async fn request_identity(
    fencing_case: &mut FencingScenario,
    mode: String,
    id_delta: i64,
    epoch_delta: i16,
) {
    let producer = fencing_case
        .identity
        .expect("initialized producer identity");
    let identity = match mode.as_str() {
        "absent" => ProducerIdentity::from_wire((-1, -1)),
        "relative" => {
            ProducerIdentity::from_wire((producer.id.0 + id_delta, producer.epoch.0 + epoch_delta))
        }
        _ => panic!("unknown identity mode {mode}"),
    };
    fencing_case.init_response = Some(
        fencing_case
            .client()
            .send(
                init_producer_request()
                    .maybe_transactional_id(Some("kip360-tid".to_owned()))
                    .producer(identity)
                    .call(),
            )
            .await
            .unwrap(),
    );
}

#[then("InitProducerId returns error {error} and withholds an identity on refusal")]
fn identity_result(fencing_case: &FencingScenario, error: i16) {
    let response = fencing_case
        .init_response
        .as_ref()
        .expect("InitProducerId answered");
    assert!(response.error_code == error, "{response:?}");
    if error != 0 {
        assert!(
            (response.producer_id, response.producer_epoch) == (-1, -1),
            "{response:?}"
        );
    }
}

#[given("a classic consumer has joined fence-g for topic fence-in")]
async fn classic_membership(fencing_case: &mut FencingScenario) {
    create_topic(&fencing_case.bootstrap, "fence-in").await;
    let consumer = Consumer::builder()
        .bootstrap(fencing_case.bootstrap.clone())
        .group_id("fence-g")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe(["fence-in".to_owned()])
        .build()
        .await
        .unwrap();
    let metadata = consumer.group_metadata();
    assert!(
        !metadata.member_id.is_empty(),
        "consumer should have a member id: {metadata:?}"
    );
    fencing_case.member_id = metadata.member_id.clone();
    fencing_case.generation = metadata.generation_id;
    fencing_case.consumer = Some(consumer);
    fencing_case.identity = Some(init_transaction(fencing_case.client(), "fence-tid").await);
    fencing_case.topic_id = Some(support::topic_id_for(fencing_case.client(), "fence-in").await);
}

#[given("a next-generation member has joined ng-g for topic ng-in")]
async fn next_gen_membership(fencing_case: &mut FencingScenario) {
    create_topic(&fencing_case.bootstrap, "ng-in").await;
    fencing_case.identity = Some(init_transaction(fencing_case.client(), "ng-tid").await);
    fencing_case.topic_id = Some(support::topic_id_for(fencing_case.client(), "ng-in").await);
    let mut heartbeat = joining_consumer("ng-g", uuid::Uuid::new_v4().to_string(), 60_000);
    heartbeat.subscribed_topic_names = Some(vec!["ng-in".into()]);
    let response = fencing_case.client().send(heartbeat).await.unwrap();
    assert!(response.error_code == 0, "heartbeat failed: {response:?}");
    assert!(
        response.member_epoch >= 1,
        "member should have a positive epoch: {response:?}"
    );
    fencing_case.member_id = response.member_id.unwrap();
    fencing_case.generation = response.member_epoch;
}

#[when(
    "a transactional offset commit for {protocol} uses generation delta {delta} and member {member}"
)]
async fn commit_member_offset(
    fencing_case: &mut FencingScenario,
    protocol: String,
    delta: i32,
    member: String,
) {
    let (topic, group, transactional_id) = match protocol.as_str() {
        "classic" => ("fence-in", "fence-g", "fence-tid"),
        "next-generation" => ("ng-in", "ng-g", "ng-tid"),
        _ => panic!("unknown group protocol {protocol}"),
    };
    let member_id = match member.as_str() {
        "current" => fencing_case.member_id.clone(),
        "unknown" => "ghost-member".into(),
        _ => panic!("unknown member selection {member}"),
    };
    let producer = fencing_case
        .identity
        .expect("initialized producer identity");
    let response = fencing_case
        .client()
        .send(TxnOffsetCommitRequest {
            transactional_id: transactional_id.into(),
            group_id: group.into(),
            producer_id: producer.id.0,
            producer_epoch: producer.epoch.0,
            generation_id_or_member_epoch: fencing_case.generation + delta,
            member_id,
            topics: vec![txn_offset_topic(
                topic,
                fencing_case.topic_id.expect("topic ID"),
                vec![txn_offset_partition(
                    krabka_ids::PartitionIndex(0),
                    krabka_ids::Offset(1),
                )],
            )],
            ..Default::default()
        })
        .await
        .unwrap();
    fencing_case
        .offset_codes
        .push(response.topics[0].partitions[0].error_code);
}

#[then("the latest member offset commit returns error {error}")]
fn member_offset_result(fencing_case: &FencingScenario, error: i16) {
    assert!(
        fencing_case.offset_codes.last() == Some(&error),
        "{:?}",
        fencing_case.offset_codes
    );
}

#[scenario(
    path = "tests/features/migrated/tests/transactions/txn_fencing.feature",
    name = "fenced producer cannot commit"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fenced_producer_cannot_commit(fencing_case: FencingScenario) {
    fencing_case.shutdown().await;
    drop(fencing_case);
}

#[scenario(
    path = "tests/features/migrated/tests/transactions/txn_fencing.feature",
    name = "init producer id fences a stale producer identity"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_producer_id_fences_a_stale_producer_identity(
    fencing_case: FencingScenario,
    example: String,
    mode: String,
    id_delta: i64,
    epoch_delta: i16,
    error: i16,
) {
    let _ = (example, mode, id_delta, epoch_delta, error);
    fencing_case.shutdown().await;
    drop(fencing_case);
}

#[scenario(
    path = "tests/features/migrated/tests/transactions/txn_fencing.feature",
    name = "txn offset commit fences classic generation and member"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn txn_offset_commit_fences_classic_generation_and_member(fencing_case: FencingScenario) {
    fencing_case.shutdown().await;
    drop(fencing_case);
}

#[scenario(
    path = "tests/features/migrated/tests/transactions/txn_fencing.feature",
    name = "txn offset commit fences next gen member epoch"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn txn_offset_commit_fences_next_gen_member_epoch(fencing_case: FencingScenario) {
    fencing_case.shutdown().await;
    drop(fencing_case);
}
