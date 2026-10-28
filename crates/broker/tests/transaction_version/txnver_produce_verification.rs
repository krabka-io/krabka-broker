//! KIP-890 part 1: a transactional `Produce` verifies its partition with the
//! transaction coordinator before it starts a transaction there.
//!
//! Kafka's `ReplicaManager.handleProduceAppend` asks the coordinator through
//! `AddPartitionsToTxnManager`: below `Produce` v12 it only verifies that the
//! client added the partition, and from v12 it adds the partition. The append
//! then refuses a transactional batch that no verification covers
//! (`UnifiedLog.analyzeAndValidateProducerState`), a batch at a stale epoch,
//! and a non-transactional batch from a producer with an open transaction
//! (`ProducerAppendInfo.appendDataBatch`).

use {
    crate::{
        support::{
            discovery::coordinator_lookup_request,
            offsets::{list_offset_partition, single_partition_list_offsets},
            produce::{SinglePartitionProduceSetup, batch_request},
            records::{BatchTransaction, ProducerSequence, transaction_probe_batch},
            topics::TopicPartitionCount,
            transaction_wire::{
                initialize_transactional, partition_error, partitions_request, retry_coordinator,
                transaction_topic,
            },
            transactions::{ProducerEpoch, ProducerIdentity, end_transaction_request},
            wire::At,
        },
        txnver_harness::{admin_client, boot_single, create_topic},
    },
    assert2::assert,
    krabka_client_core::Client,
    krabka_ids::ProducerId,
    krabka_protocol::{
        owned::{
            list_offsets_request::ListOffsetsRequest, produce_request::ProduceRequest,
            produce_response::PartitionProduceResponse,
        },
        records::RecordBatch,
    },
    std::time::Duration,
};

const NOT_COORDINATOR: i16 = 16;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const CONCURRENT_TRANSACTIONS: i16 = 51;

/// One transactional producer.
#[derive(Clone, Copy, Debug)]
struct Producer {
    id: ProducerId,
    epoch: ProducerEpoch,
}

/// What a case does before its probe.
#[derive(Clone, Copy, Debug)]
enum Setup {
    /// Nothing: the producer has no transaction on the partition.
    Nothing,
    /// `AddPartitionsToTxn` for the partition, and nothing appended.
    Added,
    /// An open transaction: the partition added and one batch appended.
    Open,
    /// A committed transaction: one batch appended, then `EndTxn(commit)`.
    Committed,
}

#[derive(Clone, Copy, Debug)]
struct ProduceVersion(i16);

struct PartitionSetup {
    name: String,
    setup: Setup,
    initializations: usize,
}

async fn produce_at(
    client: &Client,
    version: ProduceVersion,
    topic: &str,
    transactional_id: Option<&str>,
    batch: RecordBatch,
) -> PartitionProduceResponse {
    let request = ProduceRequest {
        transactional_id: transactional_id.map(str::to_owned),
        ..batch_request(batch, SinglePartitionProduceSetup {
            topic: (topic).into(),
            ..SinglePartitionProduceSetup::replicated()
        })
    };
    let response = match version.0 {
        10 => client.send(At::<_, 10>(request)).await,
        11 => client.send(At::<_, 11>(request)).await,
        _ => client.send(At::<_, 12>(request)).await,
    }
    .expect("Produce");
    response.responses[0].partition_responses[0].clone()
}

async fn log_end(client: &Client, topic: &str) -> i64 {
    let response = client
        .send(ListOffsetsRequest {
            replica_id: -1,
            ..single_partition_list_offsets(topic, list_offset_partition(0, -1))
        })
        .await
        .expect("ListOffsets");
    response.topics[0].partitions[0].offset
}

/// Send a coordinator request until the coordinator has loaded its state.
async fn until_ready<F, Fut, T>(send: F, code: impl Fn(&T) -> i16) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = T>,
{
    retry_coordinator(
        send,
        code,
        |code| {
            matches!(
                code,
                COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR | CONCURRENT_TRANSACTIONS
            )
        },
        Duration::from_secs(10),
    )
    .await
}

async fn init(client: &Client, transactional_id: &str) -> Producer {
    let _ = client
        .send(coordinator_lookup_request(transactional_id, 1, vec![
            transactional_id.into(),
        ]))
        .await;
    let response = initialize_transactional(
        client,
        transactional_id,
        |code| {
            matches!(
                code,
                COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR | CONCURRENT_TRANSACTIONS
            )
        },
        Duration::from_secs(10),
    )
    .await;
    Producer {
        id: ProducerId(response.producer_id),
        epoch: ProducerEpoch(response.producer_epoch),
    }
}

async fn add_partition(client: &Client, transactional_id: &str, producer: Producer, topic: &str) {
    let added = transaction_topic(topic, vec![0]);
    let code = until_ready(
        || async {
            let response = client
                .send(
                    partitions_request()
                        .transactional_id(transactional_id)
                        .producer(ProducerIdentity::from_wire((
                            producer.id.0,
                            producer.epoch.0,
                        )))
                        .topics(vec![added.clone()])
                        .call(),
                )
                .await
                .expect("AddPartitionsToTxn");
            partition_error(&response, true)
        },
        |code| *code,
    )
    .await;
    assert!(code == 0, "AddPartitionsToTxn: {code}");
}

async fn set_up(client: &Client, case: &PartitionSetup) -> Producer {
    create_topic(client, &case.name, TopicPartitionCount(1)).await;
    let mut producer = init(client, &case.name).await;
    for _ in 0..case.initializations {
        producer = init(client, &case.name).await;
    }
    if matches!(case.setup, Setup::Added | Setup::Open | Setup::Committed) {
        add_partition(client, &case.name, producer, &case.name).await;
    }
    if matches!(case.setup, Setup::Open | Setup::Committed) {
        let row = produce_at(
            client,
            ProduceVersion(11),
            &case.name,
            Some(case.name.as_str()),
            transaction_probe_batch()
                .producer(ProducerIdentity {
                    id: producer.id,
                    epoch: producer.epoch,
                })
                .sequence(0)
                .transaction(BatchTransaction::Transactional)
                .call(),
        )
        .await;
        assert!(row.error_code == 0, "{}: setup produce {row:?}", case.name);
    }
    if matches!(case.setup, Setup::Committed) {
        let end = client
            .send(
                end_transaction_request()
                    .transactional_id(&case.name)
                    .producer(ProducerIdentity::from_wire((
                        producer.id.0,
                        producer.epoch.0,
                    )))
                    .call(),
            )
            .await
            .expect("EndTxn");
        assert!(end.error_code == 0, "{}: EndTxn {end:?}", case.name);
    }
    producer
}

use {
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::{DataTableRow, given, scenario, then, when},
    tempfile::TempDir,
};

#[derive(Default)]
struct TransactionalProduce {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    client: Option<Client>,
    case: Option<PartitionSetup>,
    producer: Option<Producer>,
    before: Option<i64>,
    after: Option<i64>,
    response: Option<PartitionProduceResponse>,
}

#[fixture]
fn transactional_produce() -> TransactionalProduce {
    // Preserve the live transaction and pre-append boundary independently of its response.
    TransactionalProduce::default()
}

#[given(
    "transactional producer {identity} has partition state {state} after {initializations} additional initializations"
)]
async fn transaction_partition_state(
    transactional_produce: &mut TransactionalProduce,
    identity: String,
    state: String,
    initializations: usize,
) {
    let setup = match state.as_str() {
        "unregistered" => Setup::Nothing,
        "registered" => Setup::Added,
        "open" => Setup::Open,
        "committed" => Setup::Committed,
        _ => panic!("unknown partition state {state}"),
    };
    let case = PartitionSetup {
        name: identity,
        setup,
        initializations,
    };
    let (broker, bootstrap, directory) = boot_single().await;
    let client = admin_client(&bootstrap).await;
    transactional_produce.producer = Some(set_up(&client, &case).await);
    transactional_produce.before = Some(log_end(&client, &case.name).await);
    transactional_produce.broker = Some(broker);
    transactional_produce.directory = Some(directory);
    transactional_produce.client = Some(client);
    transactional_produce.case = Some(case);
}

#[when(
    "Produce version {version} sends a batch with transaction ID {included}, transactional flag {transactional}, epoch delta {epoch_delta}, and sequence {sequence}"
)]
async fn probe_transactional_produce(
    transactional_produce: &mut TransactionalProduce,
    version: i16,
    included: bool,
    transactional: bool,
    epoch_delta: i16,
    sequence: i32,
) {
    let case = transactional_produce
        .case
        .as_ref()
        .expect("partition state prepared");
    let producer = transactional_produce
        .producer
        .expect("initialized producer");
    let client = transactional_produce
        .client
        .as_ref()
        .expect("producer client");
    transactional_produce.response = Some(
        produce_at(
            client,
            ProduceVersion(version),
            &case.name,
            included.then_some(case.name.as_str()),
            transaction_probe_batch()
                .producer(ProducerIdentity {
                    id: producer.id,
                    epoch: ProducerEpoch(producer.epoch.0 + epoch_delta),
                })
                .sequence(ProducerSequence(sequence))
                .transaction(if transactional {
                    BatchTransaction::Transactional
                } else {
                    BatchTransaction::Ordinary
                })
                .call(),
        )
        .await,
    );
    transactional_produce.after = Some(log_end(client, &case.name).await);
}

#[derive(DataTableRow)]
struct ProduceExpectation {
    error: i16,
    base_offset: String,
    log_start: i64,
    log_append_time: i64,
    message: String,
    append_delta: i64,
}

#[then("the complete Produce response and log delta match")]
fn verified_produce_result(
    transactional_produce: &TransactionalProduce,
    #[datatable] rows: Rows<ProduceExpectation>,
) {
    let before = transactional_produce.before.expect("pre-append boundary");
    let rows: Vec<_> = rows.into_iter().collect();
    assert!(rows.len() == 1);
    let row = &rows[0];
    let expected = PartitionProduceResponse {
        index: 0,
        error_code: row.error,
        base_offset: if row.base_offset == "previous_log_end" {
            before
        } else {
            row.base_offset.parse().expect("base offset")
        },
        log_append_time_ms: row.log_append_time,
        log_start_offset: row.log_start,
        error_message: (row.message != "absent").then(|| row.message.clone()),
        ..Default::default()
    };
    assert!(
        transactional_produce
            .response
            .as_ref()
            .expect("Produce answered")
            == &expected
    );
    assert!(
        transactional_produce.after.expect("post-append boundary") - before == row.append_delta
    );
}

#[scenario(
    path = "tests/features/migrated/tests/transaction_version/txnver_produce_verification.feature",
    name = "a transactional produce needs a verified transaction"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transactional_produce_needs_a_verified_transaction(
    transactional_produce: TransactionalProduce,
) {
    transactional_produce
        .broker
        .take()
        .expect("broker")
        .shutdown()
        .await;
    drop(transactional_produce);
}

#[scenario(
    path = "tests/features/migrated/tests/transaction_version/txnver_produce_verification.feature",
    name = "v11 verifies unregistered partitions without appending"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verified_produce_boundary_1(transactional_produce: TransactionalProduce) {
    transactional_produce
        .broker
        .take()
        .expect("broker")
        .shutdown()
        .await;
    drop(transactional_produce);
}

#[scenario(
    path = "tests/features/migrated/tests/transaction_version/txnver_produce_verification.feature",
    name = "v11 accepts a registered partition"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verified_produce_boundary_2(transactional_produce: TransactionalProduce) {
    transactional_produce
        .broker
        .take()
        .expect("broker")
        .shutdown()
        .await;
    drop(transactional_produce);
}

#[scenario(
    path = "tests/features/migrated/tests/transaction_version/txnver_produce_verification.feature",
    name = "v12 automatically adds an unregistered partition"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verified_produce_boundary_3(transactional_produce: TransactionalProduce) {
    transactional_produce
        .broker
        .take()
        .expect("broker")
        .shutdown()
        .await;
    drop(transactional_produce);
}

#[scenario(
    path = "tests/features/migrated/tests/transaction_version/txnver_produce_verification.feature",
    name = "a missing request transaction ID is unauthorized"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verified_produce_boundary_4(transactional_produce: TransactionalProduce) {
    transactional_produce
        .broker
        .take()
        .expect("broker")
        .shutdown()
        .await;
    drop(transactional_produce);
}

#[scenario(
    path = "tests/features/migrated/tests/transaction_version/txnver_produce_verification.feature",
    name = "an open transaction rejects a stale producer epoch"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verified_produce_boundary_5(transactional_produce: TransactionalProduce) {
    transactional_produce
        .broker
        .take()
        .expect("broker")
        .shutdown()
        .await;
    drop(transactional_produce);
}

#[scenario(
    path = "tests/features/migrated/tests/transaction_version/txnver_produce_verification.feature",
    name = "an open transaction rejects an ordinary batch at the log"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verified_produce_boundary_6(transactional_produce: TransactionalProduce) {
    transactional_produce
        .broker
        .take()
        .expect("broker")
        .shutdown()
        .await;
    drop(transactional_produce);
}

#[scenario(
    path = "tests/features/migrated/tests/transaction_version/txnver_produce_verification.feature",
    name = "a committed transaction rejects replay at the old epoch"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verified_produce_boundary_7(transactional_produce: TransactionalProduce) {
    transactional_produce
        .broker
        .take()
        .expect("broker")
        .shutdown()
        .await;
    drop(transactional_produce);
}
