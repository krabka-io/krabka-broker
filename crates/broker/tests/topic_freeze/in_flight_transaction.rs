//! The one append a freeze deliberately lets through: the marker of a
//! transaction that enlisted the partition before the freeze landed.
//!
//! The commit decision is already durable in `__transaction_state` by the time
//! the marker is written, so refusing the marker would not undo the
//! transaction. It would leave it permanently open, which pins the last stable
//! offset and stops every `read_committed` consumer of a topic the freeze was
//! meant to keep readable.

use {
    crate::{
        control_plane::freeze_scope,
        support::{self, InProcess, producer::producer_record},
        wire::{CONTROL, ProduceOutcome, accepted, create_topic, produce_outcome, refused},
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_broker::codes,
    krabka_client_core::Client,
    krabka_client_producer::{OwnedTransaction, Producer},
    krabka_protocol::{
        krabka::freeze::PATTERN_TYPE_LITERAL,
        owned::list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        primitives::uuid::Uuid,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::time,
};

/// The `read_committed` end of one partition.
///
/// This is `ListOffsets(LATEST)` at `isolation_level=1`, which is what a
/// `read_committed` consumer calling `endOffsets` sends. The broker answers it
/// with the partition's last stable offset, so an open transaction pins the
/// value and a resolved one releases it -- which is the whole reading this
/// suite takes.
async fn stable_offset(client: &Client, topic: &str) -> i64 {
    let response = client
        .send(ListOffsetsRequest {
            // -1 is `CONSUMER_REPLICA_ID`; the isolation level is read only for
            // a client, so a request that forgot it would be answered from the
            // unbounded log end offset instead.
            replica_id: -1,
            // 1 is `read_committed`, the isolation level an open transaction
            // holds back.
            isolation_level: 1,
            topics: vec![ListOffsetsTopic {
                name: topic.into(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: 0,
                    // -1 is `LATEST_TIMESTAMP`, the end of the partition.
                    timestamp: -1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("ListOffsets");
    let partition = &response.topics[0].partitions[0];
    assert!(
        partition.error_code == codes::NONE,
        "ListOffsets({topic}): {partition:?}"
    );
    partition.offset
}

async fn wait_for_stable_offset(client: &Client, topic: &str, want: i64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let offset = stable_offset(client, topic).await;
        if offset == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "stable offset for {topic} never reached {want}; last={offset}"
        );
        time::sleep(Duration::from_millis(25)).await;
    }
}

#[derive(Default)]
struct FrozenTransaction {
    broker: Option<InProcess>,
    producer: Option<Arc<Producer>>,
    transaction: Option<OwnedTransaction>,
    frozen: Uuid,
    control: Uuid,
    initial_stable_offset: Option<i64>,
    committed_stable_offset: Option<i64>,
    frozen_write: Option<ProduceOutcome>,
    control_write: Option<ProduceOutcome>,
}

impl FrozenTransaction {
    fn broker(&self) -> &InProcess {
        self.broker
            .as_ref()
            .expect("the transaction broker is running")
    }
}

#[fixture]
fn frozen_transaction() -> FrozenTransaction {
    // Each scenario owns fresh services and observations.
    FrozenTransaction {
        broker: None,
        ..Default::default()
    }
}

#[given("an open transaction has acknowledged one orders record before a freeze")]
async fn enlisted_transaction(frozen_transaction: &mut FrozenTransaction) {
    let broker = support::start().await;
    broker
        .broker
        .wait_until_transaction_coordinator_ready()
        .await;
    let frozen = create_topic(&broker.broker, &broker.client, "orders").await;
    let control = create_topic(&broker.broker, &broker.client, CONTROL).await;
    frozen_transaction.producer = Some(Arc::new(
        Producer::builder()
            .bootstrap(broker.broker.listen_addr().to_string())
            .transactional_id("cutover-tid")
            .build()
            .await
            .expect("producer build"),
    ));
    frozen_transaction
        .producer
        .as_ref()
        .expect("the transactional producer was built")
        .init_transactions()
        .await
        .expect("init_transactions");
    let transaction = Arc::clone(
        frozen_transaction
            .producer
            .as_ref()
            .expect("the transactional producer was built"),
    )
    .begin_transaction_owned()
    .await
    .expect("begin_transaction");
    frozen_transaction
        .producer
        .as_ref()
        .expect("the transactional producer was built")
        .send(
            producer_record()
                .value(Some(Bytes::from_static(b"in-flight")))
                .call(),
        )
        .await
        .expect("the in-flight record is acknowledged");
    broker
        .broker
        .wait_until_local_log_end_offset("orders", 0, 1)
        .await;
    frozen_transaction.broker = Some(broker);
    frozen_transaction.transaction = Some(transaction);
    frozen_transaction.frozen = frozen;
    frozen_transaction.control = control;
}

#[when("a committed reader asks for the end of the open orders transaction")]
async fn read_open_transaction_end(frozen_transaction: &mut FrozenTransaction) {
    frozen_transaction.initial_stable_offset =
        Some(stable_offset(&frozen_transaction.broker().client, "orders").await);
}

#[then("the open transaction pins the last stable offset at zero")]
fn transaction_pins_visibility(frozen_transaction: &FrozenTransaction) {
    check!(frozen_transaction.initial_stable_offset == Some(0));
}

#[when("orders is frozen for cutover while its transaction remains open")]
async fn freeze_open_transaction(frozen_transaction: &FrozenTransaction) {
    freeze_scope(
        &frozen_transaction.broker().client,
        PATTERN_TYPE_LITERAL,
        "orders",
        "cutover",
    )
    .await;
}

#[when("a new plain write attempts to enter frozen orders")]
async fn attempt_new_frozen_write(frozen_transaction: &mut FrozenTransaction) {
    frozen_transaction.frozen_write = Some(
        produce_outcome(
            &frozen_transaction.broker().broker,
            &frozen_transaction.broker().client,
            "orders",
            frozen_transaction.frozen,
        )
        .await,
    );
}

#[then("the new write is refused without advancing orders beyond its enlisted record")]
fn frozen_plain_write_refused(frozen_transaction: &FrozenTransaction) {
    check!(
        frozen_transaction
            .frozen_write
            .as_ref()
            .expect("the new write was attempted")
            == &refused("literal", "orders", "cutover", 1)
    );
}

#[when("the transaction enlisted before the freeze commits")]
async fn commit_enlisted_transaction(frozen_transaction: &mut FrozenTransaction) {
    frozen_transaction
        .transaction
        .take()
        .expect("the transaction is open")
        .commit()
        .await
        .expect("a transaction that enlisted before the freeze still commits");
    frozen_transaction
        .broker()
        .broker
        .wait_until_local_log_end_offset("orders", 0, 2)
        .await;
    wait_for_stable_offset(&frozen_transaction.broker().client, "orders", 2).await;
    frozen_transaction.committed_stable_offset =
        Some(stable_offset(&frozen_transaction.broker().client, "orders").await);
}

#[then("the commit marker advances both the orders log and stable offset to two")]
fn committed_marker_is_visible(frozen_transaction: &FrozenTransaction) {
    check!(
        frozen_transaction
            .broker()
            .broker
            .local_log_end_offset("orders", 0)
            == Some(2)
    );
    check!(frozen_transaction.committed_stable_offset == Some(2));
}

#[when("the enlisted-transaction control topic receives a record")]
async fn write_transaction_control(frozen_transaction: &mut FrozenTransaction) {
    frozen_transaction.control_write = Some(
        produce_outcome(
            &frozen_transaction.broker().broker,
            &frozen_transaction.broker().client,
            CONTROL,
            frozen_transaction.control,
        )
        .await,
    );
}

#[then("the enlisted-transaction control write is accepted at log end one")]
fn transaction_control_accepts(frozen_transaction: &FrozenTransaction) {
    check!(
        frozen_transaction
            .control_write
            .as_ref()
            .expect("control was written")
            == &accepted(1)
    );
}

#[scenario(
    path = "tests/features/migrated/tests/topic_freeze/in_flight_transaction.feature",
    name = "a transaction that enlisted before the freeze still commits"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transaction_that_enlisted_before_the_freeze_still_commits(
    frozen_transaction: FrozenTransaction,
) {
    Arc::try_unwrap(
        frozen_transaction
            .producer
            .take()
            .expect("a producer was started"),
    )
    .unwrap_or_else(|_| panic!("the completed transaction released its producer"))
    .close()
    .await
    .expect("producer close");
    frozen_transaction
        .broker
        .take()
        .expect("the transaction broker is running")
        .broker
        .shutdown()
        .await;
    drop(frozen_transaction);
}
