//! A transaction whose `PrepareCommit` or `PrepareAbort` record is durable
//! must complete, and nothing may overwrite it while it completes.
//!
//! Kafka's `TransactionCoordinator` answers `EndTxn` with `NONE` once the
//! `Prepare*` record is in the transaction log, and its
//! `TransactionMarkerChannelManager` then writes the markers and the
//! `Complete*` record. `TransactionStateManager` does the same for every
//! `Prepare*` transaction it loads. `InitProducerId` answers
//! `CONCURRENT_TRANSACTIONS` until the transaction is complete.
//!
//! Each case stops or fails the marker fan-out with the broker's test gate,
//! so the durable state is `Prepare*` at a known point.

use {
    crate::{
        support::{
            produce::{SinglePartitionProduceSetup, batch_request},
            topics::TopicPartitionCount,
            transaction_wire::{
                add_partition_request, coordinator_loading, initialize_producer, partition_added,
                read_committed_through as read_wire_committed_through, records_batch,
                retry_coordinator, settled_produce,
            },
            transactions::{
                ProducerIdentity, TransactionOutcome, end_transaction_request,
                init_producer_request,
            },
        },
        txnver_harness::{
            admin_client, config, create_topic, downgrade_transaction_version, find_coordinator,
            topic_id,
        },
    },
    assert2::assert,
    krabka_broker::{BootstrapMode, Broker, BrokerHandle, MarkerFanoutMode},
    krabka_client_core::Client,
    krabka_protocol::owned::{
        end_txn_request::EndTxnRequest, end_txn_response::EndTxnResponse,
        init_producer_id_response::InitProducerIdResponse, produce_request::ProduceRequest,
    },
    std::{path::Path, time::Duration},
    tempfile::TempDir,
};

const CONCURRENT_TRANSACTIONS: i16 = 51;

/// The time a retried request may take to see a transaction complete.
const SETTLE: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, Copy)]
struct Identity {
    producer_id: i64,
    epoch: i16,
}

fn coordinator_is_loading(code: i16) -> bool { coordinator_loading(code) }

async fn init_producer_id(
    client: &Client,
    transactional_id: &str,
    identity: Identity,
) -> InitProducerIdResponse {
    client
        .send(
            init_producer_request()
                .maybe_transactional_id(Some(transactional_id.into()))
                .producer(ProducerIdentity::from_wire((
                    identity.producer_id,
                    identity.epoch,
                )))
                .call(),
        )
        .await
        .expect("InitProducerId")
}

async fn init_producer(client: &Client, transactional_id: &str) -> Identity {
    find_coordinator(client, transactional_id).await;
    let response = initialize_producer(
        || {
            init_producer_id(client, transactional_id, Identity {
                producer_id: -1,
                epoch: -1,
            })
        },
        coordinator_is_loading,
        SETTLE,
    )
    .await;
    Identity {
        producer_id: response.producer_id,
        epoch: response.producer_epoch,
    }
}

async fn add_partition(client: &Client, transactional_id: &str, producer: Identity, topic: &str) {
    partition_added(client.send(add_partition_request(
        transactional_id,
        topic,
        (producer.producer_id, producer.epoch),
    )))
    .await;
}

/// A transaction a produce request belongs to: the transactional id the
/// request carries, and the producer that opened it.
#[derive(Clone, Copy)]
struct Transaction<'a> {
    transactional_id: &'a str,
    producer: Identity,
}

/// Produce `values` in one batch. `transaction` is `None` for a plain batch,
/// and otherwise names the transactional id the request carries, as a Kafka
/// producer's request does.
async fn produce(
    client: &Client,
    topic: &str,
    transaction: Option<Transaction<'_>>,
    values: &[&'static str],
) {
    let producer = transaction.map(|transaction| transaction.producer);
    let batch = records_batch(
        producer.map(|producer| (producer.producer_id, producer.epoch)),
        values,
    );
    let request = ProduceRequest {
        transactional_id: transaction.map(|transaction| transaction.transactional_id.to_owned()),
        ..batch_request(batch, SinglePartitionProduceSetup {
            topic: (topic).into(),
            topic_id: topic_id(client, topic).await,
            ..SinglePartitionProduceSetup::replicated()
        })
    };
    let response = settled_produce(client, request, SETTLE).await;
    let code = response.responses[0].partition_responses[0].error_code;
    assert!(code == 0, "Produce: {response:?}");
}

fn end_txn_request(
    transactional_id: &str,
    producer: Identity,
    outcome: TransactionOutcome,
) -> EndTxnRequest {
    end_transaction_request()
        .transactional_id(transactional_id)
        .producer(ProducerIdentity::from_wire((
            producer.producer_id,
            producer.epoch,
        )))
        .outcome(outcome)
        .call()
}

/// Retry `EndTxn` the way a Kafka producer does after a lost answer, until
/// the answer is not retriable or the time is up.
async fn end_txn_until_answered(
    client: &Client,
    transactional_id: &str,
    producer: Identity,
    outcome: TransactionOutcome,
) -> EndTxnResponse {
    find_coordinator(client, transactional_id).await;
    retry_coordinator(
        || async {
            client
                .send(end_txn_request(transactional_id, producer, outcome))
                .await
                .expect("EndTxn")
        },
        |response| response.error_code,
        |code| coordinator_is_loading(code) || code == CONCURRENT_TRANSACTIONS,
        SETTLE,
    )
    .await
}

/// The values a `read_committed` consumer reads, up to and including `last`.
async fn read_committed_through(bootstrap: &str, topic: &str, last: &str) -> Vec<String> {
    read_wire_committed_through(bootstrap, topic, last, SETTLE).await
}

struct Started {
    broker: BrokerHandle,
    client: Client,
    bootstrap: String,
}

async fn start(log_dir: &Path, bootstrap_mode: Option<BootstrapMode>) -> Started {
    let broker = Broker::start(config(log_dir.to_path_buf(), bootstrap_mode))
        .await
        .expect("start broker");
    let bootstrap = broker.listen_addr().to_string();
    let client = admin_client(&bootstrap).await;
    Started {
        broker,
        client,
        bootstrap,
    }
}

/// Start a transaction with three records on `topic`, after a downgrade to
/// `transaction.version` `downgrade_to` when it is set.
async fn open_transaction(
    client: &Client,
    transactional_id: &str,
    topic: &str,
    downgrade_to: Option<i16>,
) -> Identity {
    create_topic(client, topic, TopicPartitionCount(1)).await;
    if let Some(level) = downgrade_to {
        downgrade_transaction_version(client, level).await;
    }
    let producer = init_producer(client, transactional_id).await;
    add_partition(client, transactional_id, producer, topic).await;
    produce(
        client,
        topic,
        Some(Transaction {
            transactional_id,
            producer,
        }),
        &["a", "b", "c"],
    )
    .await;
    producer
}

/// A started broker holding a transaction, and the producer that opened it.
struct FanoutTransaction {
    started: Started,
    producer: Identity,
}

async fn fanout_transaction(
    log_dir: &Path,
    transactional_id: &str,
    topic: &str,
    downgrade_to: Option<i16>,
    mode: MarkerFanoutMode,
) -> FanoutTransaction {
    let started = start(log_dir, None).await;
    let producer = open_transaction(&started.client, transactional_id, topic, downgrade_to).await;
    started.broker.set_transaction_marker_fanout_for_test(mode);
    FanoutTransaction { started, producer }
}

/// The `EndTxn` v5 answer for a completed transaction. A v5 request is a `TV_2`
/// client whatever `transaction.version` the cluster finalized, so completion
/// bumps the epoch (`epoch_bump` 1) at every level. A client below v5 would keep
/// it.
fn completed(producer: Identity, epoch_bump: i16) -> EndTxnResponse {
    EndTxnResponse {
        producer_id: producer.producer_id,
        producer_epoch: producer.epoch + epoch_bump,
        ..Default::default()
    }
}

use {
    krabka_client_core::ClientError,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tokio::task::JoinHandle,
};

#[derive(Default)]
struct PreparedTransaction {
    directory: Option<TempDir>,
    started: Option<Started>,
    producer: Option<Identity>,
    transactional_id: String,
    outcome: Option<TransactionOutcome>,
    pending: Option<JoinHandle<Result<EndTxnResponse, ClientError>>>,
    lost: Option<Result<EndTxnResponse, ClientError>>,
    responses: Vec<EndTxnResponse>,
    initialized: Option<InitProducerIdResponse>,
    seen: Option<Vec<String>>,
}

#[fixture]
fn prepared_transaction() -> PreparedTransaction {
    // A real EndTxn task and durable state span marker failure or broker restart.
    PreparedTransaction::default()
}

#[given(
    "transaction {transactional_id} contains a,b,c at feature level {level} with marker fanout {fanout}"
)]
async fn prepare_marker_fixture(
    prepared_transaction: &mut PreparedTransaction,
    transactional_id: String,
    level: i16,
    fanout: String,
) {
    let mode = match fanout.as_str() {
        "held" => MarkerFanoutMode::Hold,
        "failing" => MarkerFanoutMode::Fail,
        _ => panic!("unknown marker fanout mode {fanout}"),
    };
    let directory = TempDir::new().expect("tempdir");
    let FanoutTransaction { started, producer } = fanout_transaction(
        directory.path(),
        &transactional_id,
        &transactional_id,
        (level != 2).then_some(level),
        mode,
    )
    .await;
    prepared_transaction.directory = Some(directory);
    prepared_transaction.started = Some(started);
    prepared_transaction.producer = Some(producer);
    prepared_transaction.transactional_id = transactional_id;
}

fn transaction_outcome(outcome: &str) -> TransactionOutcome {
    match outcome {
        "commit" => TransactionOutcome::Commit,
        "abort" => TransactionOutcome::Abort,
        _ => panic!("unknown transaction outcome {outcome}"),
    }
}

#[when("the producer requests {outcome} and EndTxn reaches held marker fanout")]
async fn hold_prepared_end(prepared_transaction: &mut PreparedTransaction, outcome: String) {
    let outcome = transaction_outcome(&outcome);
    let started = prepared_transaction
        .started
        .as_ref()
        .expect("transaction broker");
    let client = admin_client(&started.bootstrap).await;
    let request = end_txn_request(
        &prepared_transaction.transactional_id,
        prepared_transaction.producer.expect("producer identity"),
        outcome,
    );
    prepared_transaction.pending = Some(tokio::spawn(async move { client.send(request).await }));
    started
        .broker
        .wait_for_transaction_marker_fanouts_for_test(1)
        .await;
    prepared_transaction.outcome = Some(outcome);
}

#[when("the broker shuts down while its EndTxn waits for markers")]
async fn cut_prepared_end(prepared_transaction: &mut PreparedTransaction) {
    prepared_transaction
        .started
        .take()
        .expect("transaction broker")
        .broker
        .shutdown()
        .await;
    prepared_transaction.lost = Some(
        prepared_transaction
            .pending
            .take()
            .expect("held EndTxn task")
            .await
            .expect("cut EndTxn task"),
    );
}

#[then("the held EndTxn connection closes without a response")]
fn prepared_end_lost(prepared_transaction: &PreparedTransaction) {
    let answer = prepared_transaction
        .lost
        .as_ref()
        .expect("cut EndTxn observed");
    assert!(answer.is_err(), "shutdown must cut held EndTxn: {answer:?}");
}

#[when("the prepared transaction broker restarts from its durable directory")]
async fn restart_prepared_transaction(prepared_transaction: &mut PreparedTransaction) {
    prepared_transaction.started = Some(
        start(
            prepared_transaction
                .directory
                .as_ref()
                .expect("durable directory")
                .path(),
            Some(BootstrapMode::Rejoin),
        )
        .await,
    );
}

#[when("the producer retries the prepared transaction's EndTxn")]
async fn retry_prepared_end(prepared_transaction: &mut PreparedTransaction) {
    let started = prepared_transaction
        .started
        .as_ref()
        .expect("transaction broker");
    prepared_transaction.responses.push(
        end_txn_until_answered(
            &started.client,
            &prepared_transaction.transactional_id,
            prepared_transaction.producer.expect("producer identity"),
            prepared_transaction.outcome.expect("requested outcome"),
        )
        .await,
    );
}

#[then("the latest prepared EndTxn succeeds with the next producer epoch")]
fn prepared_end_completed(prepared_transaction: &PreparedTransaction) {
    assert!(
        prepared_transaction
            .responses
            .last()
            .expect("EndTxn answered")
            == &completed(prepared_transaction.producer.expect("producer identity"), 1)
    );
}

#[when("a committed reader reads the prepared transaction followed by a plain z")]
async fn read_prepared_transaction(prepared_transaction: &mut PreparedTransaction) {
    let started = prepared_transaction
        .started
        .as_ref()
        .expect("transaction broker");
    produce(
        &started.client,
        &prepared_transaction.transactional_id,
        None,
        &["z"],
    )
    .await;
    prepared_transaction.seen = Some(
        read_committed_through(
            &started.bootstrap,
            &prepared_transaction.transactional_id,
            "z",
        )
        .await,
    );
}

#[then("the prepared transaction's committed values are {values}")]
fn prepared_transaction_values(prepared_transaction: &PreparedTransaction, values: String) {
    let expected: Vec<String> = values.split(',').map(str::to_owned).collect();
    assert!(
        prepared_transaction
            .seen
            .as_ref()
            .expect("committed reader finished")
            == &expected
    );
}

#[when("InitProducerId probes the prepared transaction using {identity}")]
async fn initialize_during_prepare(
    prepared_transaction: &mut PreparedTransaction,
    identity: String,
) {
    let producer = prepared_transaction.producer.expect("producer identity");
    let identity = match identity.as_str() {
        "no identity" => Identity {
            producer_id: -1,
            epoch: -1,
        },
        "the transaction identity" => producer,
        "another producer ID" => Identity {
            producer_id: producer.producer_id + 1_000,
            epoch: 0,
        },
        _ => panic!("unknown identity selection {identity}"),
    };
    prepared_transaction.initialized = Some(
        init_producer_id(
            &prepared_transaction
                .started
                .as_ref()
                .expect("transaction broker")
                .client,
            &prepared_transaction.transactional_id,
            identity,
        )
        .await,
    );
}

#[then("the prepared InitProducerId returns error {error_code} and no identity")]
fn prepared_initialization(prepared_transaction: &PreparedTransaction, error_code: i16) {
    assert!(
        prepared_transaction
            .initialized
            .as_ref()
            .expect("InitProducerId answered")
            == &InitProducerIdResponse {
                error_code,
                producer_id: -1,
                producer_epoch: -1,
                ..Default::default()
            }
    );
}

#[when("marker fanout is reopened")]
fn open_prepared_fanout(prepared_transaction: &PreparedTransaction) {
    prepared_transaction
        .started
        .as_ref()
        .expect("transaction broker")
        .broker
        .set_transaction_marker_fanout_for_test(MarkerFanoutMode::Open);
}

#[when("the held EndTxn finishes after marker fanout resumes")]
async fn finish_held_prepared_end(prepared_transaction: &mut PreparedTransaction) {
    prepared_transaction.responses.push(
        prepared_transaction
            .pending
            .take()
            .expect("held EndTxn task")
            .await
            .expect("EndTxn task")
            .expect("EndTxn answer"),
    );
}

#[when("the producer commits while marker fanout fails")]
async fn fail_prepared_fanout(prepared_transaction: &mut PreparedTransaction) {
    prepared_transaction.outcome = Some(TransactionOutcome::Commit);
    prepared_transaction.responses.push(
        prepared_transaction
            .started
            .as_ref()
            .expect("transaction broker")
            .client
            .send(end_txn_request(
                &prepared_transaction.transactional_id,
                prepared_transaction.producer.expect("producer identity"),
                TransactionOutcome::Commit,
            ))
            .await
            .expect("EndTxn"),
    );
}

#[scenario(
    path = "tests/features/migrated/tests/transaction_version/txnver_prepare_recovery.feature",
    name = "end txn cut between prepare and complete completes after restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_txn_cut_between_prepare_and_complete_completes_after_restart(
    prepared_transaction: PreparedTransaction,
    transactional_id: String,
    level: i16,
    outcome: String,
    values: String,
) {
    let _ = (transactional_id, level, outcome, values);
    prepared_transaction
        .started
        .take()
        .expect("transaction broker")
        .broker
        .shutdown()
        .await;
    drop(prepared_transaction);
}

#[scenario(
    path = "tests/features/migrated/tests/transaction_version/txnver_prepare_recovery.feature",
    name = "init producer id during prepare commit is concurrent transactions"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn init_producer_id_during_prepare_commit_is_concurrent_transactions(
    prepared_transaction: PreparedTransaction,
) {
    prepared_transaction
        .started
        .take()
        .expect("transaction broker")
        .broker
        .shutdown()
        .await;
    drop(prepared_transaction);
}

#[scenario(
    path = "tests/features/migrated/tests/transaction_version/txnver_prepare_recovery.feature",
    name = "end txn with a failed marker fanout answers none and completes later"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_txn_with_a_failed_marker_fanout_answers_none_and_completes_later(
    prepared_transaction: PreparedTransaction,
) {
    prepared_transaction
        .started
        .take()
        .expect("transaction broker")
        .broker
        .shutdown()
        .await;
    drop(prepared_transaction);
}
