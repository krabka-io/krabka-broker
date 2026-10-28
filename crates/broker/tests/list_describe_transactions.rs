#![cfg(test)]

//! KIP-664 `ListTransactions` (`api_key` 66) and `DescribeTransactions`
//! (`api_key` 65). Both surface the broker's local `TxnCoordinator`
//! state. List returns a `(transactional_id, producer_id, state)` summary.
//! Describe returns the full per-tid detail: timeout, start time, and
//! partitions.

mod support;

pub use crate::support::boot_single;
use {
    crate::support::{
        client::connect_client,
        producer::string_record,
        topics::{creatable_topic, create_topic_request},
    },
    assert2::{assert, check},
    krabka_broker::{BrokerHandle, codes},
    krabka_client_producer::{OwnedTransaction, Producer, ProducerRecord},
    krabka_protocol::owned::{
        describe_transactions_request::DescribeTransactionsRequest,
        describe_transactions_response::{
            TopicData, TransactionState as DescribedTransactionState,
        },
        find_coordinator_request::FindCoordinatorRequest,
        list_transactions_request::ListTransactionsRequest,
        list_transactions_response::{
            ListTransactionsResponse, TransactionState as ListedTransactionState,
        },
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::time,
};

/// `ListTransactions`, retried while the coordinator loads its state
/// partitions. Kafka answers `COORDINATOR_LOAD_IN_PROGRESS` (14) until every
/// partition it leads is loaded, and the admin client retries.
async fn list_transactions(
    client: &krabka_client_core::Client,
    request: &ListTransactionsRequest,
) -> ListTransactionsResponse {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = client
            .send(request.clone())
            .await
            .expect("ListTransactions");
        if response.error_code != codes::COORDINATOR_LOAD_IN_PROGRESS || Instant::now() >= deadline
        {
            return response;
        }
        // intentional: the coordinator load has no awaiter reachable from a
        // client; the answer is the signal.
        time::sleep(Duration::from_millis(50)).await;
    }
}

/// `DescribeTransactions` for one id, after the coordinator of that id has
/// loaded its `__transaction_state` partition. `FindCoordinator` creates the
/// topic, and the row carries `COORDINATOR_LOAD_IN_PROGRESS` (14) or
/// `NOT_COORDINATOR` (16) until the load ends.
async fn describe_transaction(
    client: &krabka_client_core::Client,
    transactional_id: &str,
) -> DescribedTransactionState {
    let _ = client
        .send(FindCoordinatorRequest {
            key: transactional_id.into(),
            key_type: 1,
            coordinator_keys: vec![transactional_id.into()],
            ..Default::default()
        })
        .await
        .expect("FindCoordinator");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = client
            .send(DescribeTransactionsRequest {
                transactional_ids: vec![transactional_id.into()],
                ..Default::default()
            })
            .await
            .expect("DescribeTransactions");
        let row = response
            .transaction_states
            .first()
            .expect("one row per requested id")
            .clone();
        if !matches!(
            row.error_code,
            codes::COORDINATOR_LOAD_IN_PROGRESS | codes::NOT_COORDINATOR
        ) || Instant::now() >= deadline
        {
            return row;
        }
        // intentional: the coordinator load has no awaiter reachable from a
        // client; the answer is the signal.
        time::sleep(Duration::from_millis(50)).await;
    }
}

async fn create_topic(bootstrap: &str, name: &str) {
    let client = connect_client(bootstrap, None).await;
    let cr = client
        .send(create_topic_request(
            creatable_topic().name((name).into()).call(),
        ))
        .await
        .unwrap();
    assert!(
        cr.topics[0].error_code == 0 || cr.topics[0].error_code == 36,
        "create_topic {name}: error_code={}",
        cr.topics[0].error_code
    );
}

fn rec(topic: &str, v: &str) -> ProducerRecord { string_record(topic, v) }

async fn admin_client(bootstrap: &str) -> krabka_client_core::Client {
    connect_client(bootstrap, None).await
}

/// A broker holding one `Ongoing` transaction, with the handles that keep it
/// alive.
struct OngoingTxn {
    broker: BrokerHandle,
    bootstrap: String,
    dir: TempDir,
    producer: Arc<Producer>,
    transaction: OwnedTransaction,
}

/// Boot a broker, init a transactional producer with the given tid,
/// begin a txn, and produce one record to `topic`. It leaves the txn
/// in `Ongoing` so the admin APIs can see it. It returns the broker and the
/// producer, and the caller closes both.
async fn boot_with_ongoing_txn(tid: &str, topic: &str) -> OngoingTxn {
    let (broker, bootstrap, dir) = boot_single().await;
    create_topic(&bootstrap, topic).await;
    // The producer does not retry COORDINATOR_NOT_AVAILABLE from
    // FindCoordinator. Bring the transaction coordinator up first.
    broker.wait_until_transaction_coordinator_ready().await;

    let producer = Arc::new(
        Producer::builder()
            .bootstrap(bootstrap.clone())
            .transactional_id(tid)
            .build()
            .await
            .unwrap(),
    );
    producer.init_transactions().await.unwrap();
    let transaction = Arc::clone(&producer)
        .begin_transaction_owned()
        .await
        .unwrap();
    // `enqueue` enqueues into the producer's local batch; without
    // `flush()` the AddPartitionsToTxn round-trip that registers
    // `(topic, partition)` on the coordinator's TxnEntry may not run
    // before the admin call. The drop pattern around `enqueue` is
    // intentional — it's a future-of-record-metadata handle we don't
    // need (commits will never fire on this test path).
    drop(
        producer
            .enqueue(rec(topic, "v"))
            .await
            .expect("record is queued"),
    );
    producer.flush().await.unwrap();
    // Don't commit/abort — we want the txn to stay Ongoing.

    // `flush()` returning does not mean the coordinator has finished the
    // AddPartitionsToTxn round-trip that `enqueue` triggered, so its `TxnEntry`
    // can still read `Empty` with no partitions for a moment afterwards. Wait
    // for it here rather than in each caller: every test below asserts against
    // the `Ongoing` state this helper's name promises, and the one that did not
    // wait raced it and read `Empty` on CI. `DescribeTransactions` is the
    // strongest signal available, because it reports the state and the
    // registered partition set together, and there is no metadata-image event
    // to wait on instead. The deadline is generous because macOS runners are
    // measurably slower than linux on the in-process transactional path.
    let admin = admin_client(&bootstrap).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let r = admin
            .send(DescribeTransactionsRequest {
                transactional_ids: vec![tid.into()],
                ..Default::default()
            })
            .await
            .expect("DescribeTransactions");
        let ready = matches!(
            r.transaction_states.as_slice(),
            [row] if row.error_code == 0
                && row.transaction_state == "Ongoing"
                && !row.topics.is_empty()
        );
        if ready {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "txn {tid} never reached Ongoing with its partitions: {r:?}"
        );
        time::sleep(Duration::from_millis(100)).await;
    }

    OngoingTxn {
        broker,
        bootstrap,
        dir,
        producer,
        transaction,
    }
}

enum CoordinatorState {
    Idle {
        broker: BrokerHandle,
        bootstrap: String,
        directory: TempDir,
    },
    Active(OngoingTxn),
}

impl CoordinatorState {
    fn bootstrap(&self) -> &str {
        match self {
            Self::Idle { bootstrap, .. } => bootstrap,
            Self::Active(transaction) => &transaction.bootstrap,
        }
    }

    async fn shutdown(self) {
        match self {
            Self::Idle {
                broker,
                directory: _directory,
                ..
            } => broker.shutdown().await,
            Self::Active(OngoingTxn {
                broker,
                dir: _directory,
                producer,
                transaction,
                ..
            }) => teardown_test_txn(transaction, producer, broker).await,
        }
    }
}

struct TransactionInspection {
    coordinator: Option<CoordinatorState>,
    transactional_id: String,
    listed: Option<ListTransactionsResponse>,
    described: Vec<DescribedTransactionState>,
}

#[fixture]
fn inspection() -> TransactionInspection {
    // Start this scenario with fresh inputs and observations.

    TransactionInspection {
        coordinator: None,
        transactional_id: String::new(),
        listed: None,
        described: Vec::new(),
    }
}

#[given("transactional id {tid} has an ongoing transaction on topic {topic}")]
async fn ongoing_transaction(inspection: &mut TransactionInspection, tid: String, topic: String) {
    inspection.coordinator = Some(CoordinatorState::Active(
        boot_with_ongoing_txn(&tid, &topic).await,
    ));
    inspection.transactional_id = tid;
}

#[given("a broker has no initialized transactional ids")]
async fn empty_coordinator(inspection: &mut TransactionInspection) {
    let (broker, bootstrap, directory) = boot_single().await;
    inspection.coordinator = Some(CoordinatorState::Idle {
        broker,
        bootstrap,
        directory,
    });
}

async fn list_with_filters(inspection: &mut TransactionInspection, states: Vec<String>) {
    let bootstrap = inspection
        .coordinator
        .as_ref()
        .expect("a coordinator was started")
        .bootstrap()
        .to_owned();
    let client = admin_client(&bootstrap).await;
    inspection.listed = Some(
        list_transactions(&client, &ListTransactionsRequest {
            state_filters: states,
            ..Default::default()
        })
        .await,
    );
}

#[when("local transactions are listed without state filters")]
async fn list_all(inspection: &mut TransactionInspection) {
    list_with_filters(inspection, Vec::new()).await;
}

#[when("local transactions are listed with states {states}")]
async fn list_selected_states(inspection: &mut TransactionInspection, states: String) {
    list_with_filters(
        inspection,
        states
            .split(',')
            .map(|state| state.trim().to_owned())
            .collect(),
    )
    .await;
}

#[then("the listing contains exactly the ongoing transactional id with its allocated producer id")]
fn listed_transaction(inspection: &TransactionInspection) {
    let response = inspection
        .listed
        .as_ref()
        .expect("ListTransactions completed");
    let row = response
        .transaction_states
        .iter()
        .find(|row| row.transactional_id == inspection.transactional_id)
        .expect("the ongoing transaction is present");
    check!(row.producer_id >= 0);
    let expected = ListedTransactionState {
        transactional_id: inspection.transactional_id.clone(),
        producer_id: row.producer_id,
        transaction_state: "Ongoing".into(),
        ..Default::default()
    };
    assert!(response.transaction_states == [expected]);
}

#[then("the successful listing has no matching transactions")]
fn no_matching_transaction(inspection: &TransactionInspection) {
    assert!(
        inspection
            .listed
            .as_ref()
            .expect("ListTransactions completed")
            == &ListTransactionsResponse::default(),
        "an ongoing transaction must not match an Empty state filter"
    );
}

#[then("only state filter {state} is reported as unknown")]
fn unknown_state_filter(inspection: &TransactionInspection, state: String) {
    assert!(
        inspection
            .listed
            .as_ref()
            .expect("ListTransactions completed")
            == &ListTransactionsResponse {
                unknown_state_filters: vec![state],
                ..Default::default()
            }
    );
}

#[when("details of the ongoing transaction are requested")]
async fn describe_ongoing(inspection: &mut TransactionInspection) {
    let bootstrap = inspection
        .coordinator
        .as_ref()
        .expect("a coordinator was started")
        .bootstrap()
        .to_owned();
    let client = admin_client(&bootstrap).await;
    inspection.described = client
        .send(DescribeTransactionsRequest {
            transactional_ids: vec![inspection.transactional_id.clone()],
            ..Default::default()
        })
        .await
        .expect("DescribeTransactions")
        .transaction_states;
}

#[then(
    "transaction details show {tid} ongoing on {topic} partition {partition} with timeout {timeout}"
)]
fn described_transaction(
    inspection: &TransactionInspection,
    tid: String,
    topic: String,
    partition: i32,
    timeout: i32,
) {
    assert!(inspection.described.len() == 1);
    let row = &inspection.described[0];
    check!(row.producer_id >= 0);
    check!(row.transaction_start_time_ms > 0);
    assert!(
        *row == DescribedTransactionState {
            transactional_id: tid,
            transaction_state: "Ongoing".into(),
            transaction_timeout_ms: timeout,
            transaction_start_time_ms: row.transaction_start_time_ms,
            producer_id: row.producer_id,
            topics: vec![TopicData {
                topic,
                partitions: vec![partition],
                ..Default::default()
            }],
            ..Default::default()
        }
    );
}

#[when("an unknown transactional id {tid} is described")]
async fn describe_unknown(inspection: &mut TransactionInspection, tid: String) {
    let bootstrap = inspection
        .coordinator
        .as_ref()
        .expect("a coordinator was started")
        .bootstrap()
        .to_owned();
    let client = admin_client(&bootstrap).await;
    inspection.described = vec![describe_transaction(&client, &tid).await];
}

#[then("transactional id {tid} is reported missing")]
fn missing_transaction(inspection: &TransactionInspection, tid: String) {
    assert!(
        inspection.described
            == vec![DescribedTransactionState {
                error_code: codes::TRANSACTIONAL_ID_NOT_FOUND,
                transactional_id: tid,
                ..Default::default()
            }]
    );
}

async fn stop_inspection(inspection: &mut TransactionInspection) {
    inspection
        .coordinator
        .take()
        .expect("a coordinator was started")
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/list_describe_transactions.feature",
    name = "A local transaction listing exposes ongoing work"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_transactions_returns_ongoing_txn(inspection: TransactionInspection) {
    stop_inspection(&mut inspection).await;
    drop(inspection);
}

#[scenario(
    path = "tests/features/migrated/tests/list_describe_transactions.feature",
    name = "A state filter excludes an ongoing transaction from Empty results"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_transactions_state_filter_excludes_non_matching(inspection: TransactionInspection) {
    stop_inspection(&mut inspection).await;
    drop(inspection);
}

#[scenario(
    path = "tests/features/migrated/tests/list_describe_transactions.feature",
    name = "Unknown state filters are distinguished from valid filters"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_transactions_reports_unknown_state_filters(inspection: TransactionInspection) {
    stop_inspection(&mut inspection).await;
    drop(inspection);
}

#[scenario(
    path = "tests/features/migrated/tests/list_describe_transactions.feature",
    name = "Transaction details include its producer timing and registered partition"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_transactions_returns_full_state_for_known_tid(inspection: TransactionInspection) {
    stop_inspection(&mut inspection).await;
    drop(inspection);
}

#[scenario(
    path = "tests/features/migrated/tests/list_describe_transactions.feature",
    name = "Describing an unknown transactional id returns its own not-found row"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_transactions_returns_not_found_for_unknown_tid(
    inspection: TransactionInspection,
) {
    stop_inspection(&mut inspection).await;
    drop(inspection);
}

/// Abort before closing, proving the guard released its owning producer reference.
async fn abort_and_close(transaction: OwnedTransaction, producer: Arc<Producer>) {
    transaction.abort().await.unwrap();
    Arc::into_inner(producer)
        .expect("transaction guard released its producer reference")
        .close()
        .await
        .unwrap();
}

async fn teardown_test_txn(
    transaction: OwnedTransaction,
    producer: Arc<Producer>,
    broker: BrokerHandle,
) {
    abort_and_close(transaction, producer).await;
    broker.shutdown().await;
}
