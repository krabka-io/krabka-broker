//! The Kafka 4.x transactional client against a `transaction.version=2`
//! broker, and the `kafka-transactions` admin tool against the transaction it
//! leaves open.
//!
//! [`transactional_eos`](super::transactional_eos) runs the same JVM producer
//! source inside `cp-kafka:7.5.0`, whose Kafka 3.5 client predates KIP-890: it
//! registers every partition with `AddPartitionsToTxn` and never sees an epoch
//! in an `EndTxn` response. This case runs the *compiled* class under Kafka
//! 4.3.1's client jars instead, where the `TransactionManager` finalizes
//! `transaction.version=2` from `ApiVersions` and therefore:
//!
//! - sends no `AddPartitionsToTxn` at all -- Produce v12 carries the partition
//!   into the transaction, which is asserted from the broker's own
//!   `krabka_broker_api_requests_total{api_key="AddPartitionsToTxn"}` series
//!   rather than from a client-side debug trace; and
//! - takes the bumped `producer_epoch` out of the `EndTxn` v5 response, so the
//!   second transaction is written at a strictly higher epoch than the first.
//!
//! The class is compiled in [`KAFKA_IMAGE_TXN`], which is the only pinned
//! image with a `javac`, and run in [`KAFKA_IMAGE_ELR`], which is JRE-only.
//! `--release 11` is what that JDK can target -- `cp-kafka:7.5.0` ships Zulu
//! `OpenJDK` 11 -- and a class file at that level runs unchanged on the JRE 21
//! in the 4.3.1 image. The helper touches only `KafkaProducer`,
//! `ProducerRecord` and `ProducerConfig` constants, so compiling against the
//! 3.5 jars and running against the 4.3.1 ones resolves identically; what
//! decides the protocol is the 4.3.1 client that runs it.
//!
//! The second half drives `kafka-transactions.sh` (KIP-664:
//! `ListTransactions`, `DescribeTransactions`, `DescribeProducers` and the
//! `WriteTxnMarkers` behind `abort`) against a transaction that is deliberately
//! left open, and compares every rendered column with the answer the same
//! broker gives an in-process `krabka-client-core` admin client. The open
//! transaction is held by an in-process producer rather than by the JVM helper,
//! because the helper commits and aborts and then exits: nothing in it can be
//! made to hold a transaction open while a container runs beside it.

use {
    crate::{
        jvm_acceptance::{
            KAFKA_IMAGE_ELR, KAFKA_IMAGE_TXN, TRANSACTIONAL_PRODUCER_JAVA, broker0_advertised,
            docker_run_kafka_tool_with_image, docker_run_kafka_tool_with_image_and_mount,
            nc_check_connectivity, start_host_broker_with, tool_output,
        },
        support::client::connect_owned,
    },
    assert2::assert,
    bytes::Bytes,
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_client_producer::{OwnedTransaction, Producer, ProducerError, ProducerRecord},
    krabka_protocol::owned::{
        api_versions_request::ApiVersionsRequest,
        describe_producers_request::{DescribeProducersRequest, TopicRequest},
        describe_producers_response::ProducerState,
        describe_transactions_request::DescribeTransactionsRequest,
        describe_transactions_response::TransactionState,
        list_transactions_request::ListTransactionsRequest,
        list_transactions_response::ListTransactionsResponse,
    },
    std::{
        fs,
        net::SocketAddr,
        path::Path,
        process::Output,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpStream,
        time,
    },
};

/// The topic the JVM helper commits to and aborts on.
const TOPIC: &str = "krabka-tv2-itest";

/// The topic whose open transaction `kafka-transactions` is pointed at.
const HANGING_TOPIC: &str = "krabka-tv2-open";

/// The `transactional.id` [`TRANSACTIONAL_PRODUCER_JAVA`] hard-codes.
const EOS_TID: &str = "eos-tid";

/// The `transactional.id` of the transaction left open for the admin tool.
const OPEN_TID: &str = "tv2-open-tid";

/// The tools in [`KAFKA_IMAGE_ELR`] are not on `PATH`.
const KAFKA_TOPICS: &str = "/opt/kafka/bin/kafka-topics.sh";
const KAFKA_CONSOLE_CONSUMER: &str = "/opt/kafka/bin/kafka-console-consumer.sh";
const KAFKA_TRANSACTIONS: &str = "/opt/kafka/bin/kafka-transactions.sh";

/// Every container step is a JVM start-up plus a metadata round trip, and the
/// runners this suite shares are the slowest in the tree, so each wait below is
/// deliberately far longer than the operation needs.
const SETTLE_DEADLINE: Duration = Duration::from_secs(60);

/// Kafka's `COORDINATOR_LOAD_IN_PROGRESS`.
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;

/// The metric series that must stay absent: a 4.x client on `TV_2` registers
/// partitions through Produce, not through `AddPartitionsToTxn`.
const ADD_PARTITIONS_SERIES: &str =
    "krabka_broker_api_requests_total{api_key=\"AddPartitionsToTxn\"}";

/// The columns `kafka-transactions describe-producers` prints, in order. The
/// comparison reads them by position, so the list is the tool's contract.
const DESCRIBE_PRODUCERS_HEADERS: [&str; 6] = [
    "ProducerId",
    "ProducerEpoch",
    "LatestCoordinatorEpoch",
    "LastSequence",
    "LastTimestamp",
    "CurrentTransactionStartOffset",
];

/// One broker with a metrics listener, an in-process admin client, and the
/// topics a case names, created through the JVM `kafka-topics` so the
/// container path is exercised from the first request.
///
/// Both cases below need all of it, and each has to own its own broker: they
/// assert on cluster-wide state (a metric family's absence, the set of listed
/// transactions) that a shared broker would let the other case move.
struct Harness {
    broker: BrokerHandle,
    _dir: tempfile::TempDir,
    /// The advertised address a container reaches this broker on.
    bootstrap: &'static str,
    /// The same broker, as an in-process client dials it.
    host_bootstrap: String,
    metrics_addr: SocketAddr,
    admin: Client,
}

async fn harness(client_id: &str, topics: &[&str]) -> Harness {
    nc_check_connectivity();

    let (broker, dir) = start_host_broker_with(|config| {
        config.metrics_listen_addr = Some("127.0.0.1:0".parse().expect("static addr"));
    })
    .await;
    // The krabka-client-rs Producer does not retry a FindCoordinator that
    // answers COORDINATOR_NOT_AVAILABLE, so `__transaction_state` must exist
    // before `init_transactions`.
    broker.wait_until_transaction_coordinator_ready().await;
    let bootstrap = broker0_advertised();
    let metrics_addr = broker
        .metrics_addr()
        .expect("the broker was configured with a metrics listener");
    let host_bootstrap = broker.listen_addr().to_string();
    let admin = connect_owned(host_bootstrap.clone(), client_id, "in-process admin client").await;

    // Precondition. Everything in these cases reads as a KIP-890 claim only
    // because the cluster finalized `transaction.version` at level 2; at TV_0
    // or TV_1 the 4.x client falls back to `AddPartitionsToTxn` and the
    // absence asserted below would mean nothing.
    assert_transaction_version_2(&admin).await;

    for topic in topics {
        docker_run_kafka_tool_with_image(KAFKA_IMAGE_ELR, &[
            KAFKA_TOPICS,
            "--bootstrap-server",
            bootstrap,
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ]);
    }

    Harness {
        broker,
        _dir: dir,
        bootstrap,
        host_bootstrap,
        metrics_addr,
        admin,
    }
}

struct ToolSnapshot {
    transaction: TransactionState,
    listed_id: i64,
    listed_state: String,
    producers: Vec<ProducerState>,
    list_row: Vec<String>,
    transaction_row: Vec<String>,
    producer_row: Vec<String>,
    hanging: Vec<Vec<String>>,
}
struct Tv2 {
    harness: Option<Harness>,
    output: Option<Output>,
    consumed: Vec<String>,
    metrics: String,
    transaction_state: Option<TransactionState>,
    producers: Vec<ProducerState>,
    producer: Option<Arc<Producer>>,
    transaction: Option<OwnedTransaction>,
    snapshot: Option<ToolSnapshot>,
    producer_row: Vec<String>,
    final_row: Vec<String>,
}
#[rstest::fixture]
fn tv2() -> Tv2 {
    // Start this scenario with fresh inputs and observations.

    Tv2 {
        harness: None,
        output: None,
        consumed: Vec::new(),
        metrics: String::new(),
        transaction_state: None,
        producers: Vec::new(),
        producer: None,
        transaction: None,
        snapshot: None,
        producer_row: Vec::new(),
        final_row: Vec::new(),
    }
}
#[rstest_bdd_macros::given(
    "a transaction.version 2 broker exposes topic {topic} to admin client {client}"
)]
async fn version_two(tv2: &mut Tv2, topic: String, client: String) {
    tv2.harness = Some(harness(&client, &[&topic]).await);
}
#[rstest_bdd_macros::when(
    "the compiled Kafka 3.5 transactional helper runs under Kafka 4.3.1 jars"
)]
async fn modern_transactions(tv2: &mut Tv2) {
    let classes = compile_transactional_producer();
    let harness = tv2.harness.as_ref().expect("harness");
    tv2.output = Some(run_transactional_producer(
        classes.path(),
        harness.bootstrap,
        TOPIC,
    ));
    wait_for_transaction_state(&harness.admin, EOS_TID, "CompleteAbort").await;
}
#[rstest_bdd_macros::then("the transactional helper reports TXNPROBE OK")]
fn helper_success(tv2: &Tv2) {
    assert!(tool_output(tv2.output.as_ref().expect("helper ran")).contains("TXNPROBE OK"));
}
#[rstest_bdd_macros::when("the console reads {count} records using isolation {isolation}")]
fn isolated_read(tv2: &mut Tv2, count: usize, isolation: String) {
    tv2.consumed = consume(
        tv2.harness.as_ref().expect("harness").bootstrap,
        TOPIC,
        &isolation,
        count,
    );
}
#[rstest_bdd_macros::then("the transactional records are exactly these values in log order")]
fn ordered_records(tv2: &Tv2, datatable: Vec<Vec<String>>) {
    let expected: Vec<String> = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [value]: [String; 1] = row.try_into().expect("value");
            value
        })
        .collect();
    assert!(tv2.consumed == expected);
}
#[rstest_bdd_macros::when(
    "the administrator scrapes API counters and describes eos-tid and its partition producers"
)]
async fn protocol_observations(tv2: &mut Tv2) {
    let harness = tv2.harness.as_ref().expect("harness");
    tv2.metrics = scrape_metrics(harness.metrics_addr).await;
    tv2.transaction_state = Some(describe_transaction(&harness.admin, EOS_TID).await);
    tv2.producers = active_producers(&harness.admin, TOPIC, 0).await;
}
#[rstest_bdd_macros::then(
    "AddPartitionsToTxn has zero requests and both coordinator and written producer epochs are at least {minimum}"
)]
fn optimized_transactions(tv2: &Tv2, minimum: i16) {
    assert!(
        series_value(&tv2.metrics, ADD_PARTITIONS_SERIES).is_none_or(|count| count == 0.0),
        "{}",
        tv2.metrics
    );
    assert!(
        tv2.transaction_state
            .as_ref()
            .expect("transaction described")
            .producer_epoch
            >= minimum
    );
    assert!(
        tv2.producers
            .iter()
            .map(|state| state.producer_epoch)
            .max()
            .expect("partition producer state")
            >= i32::from(minimum)
    );
}
#[rstest_bdd_macros::given(
    "an initialized producer holds tv2-open-tid open after flushing value open to krabka-tv2-open"
)]
async fn open_transaction(tv2: &mut Tv2) {
    tv2.producer = Some(Arc::new(
        Producer::builder()
            .bootstrap(
                tv2.harness
                    .as_ref()
                    .expect("harness")
                    .host_bootstrap
                    .clone(),
            )
            .transactional_id(OPEN_TID)
            .build()
            .await
            .expect("producer"),
    ));
    tv2.producer
        .as_ref()
        .expect("producer")
        .init_transactions()
        .await
        .expect("init transactions");
    tv2.transaction = Some(
        Arc::clone(tv2.producer.as_ref().expect("producer"))
            .begin_transaction_owned()
            .await
            .expect("begin"),
    );
    drop(
        tv2.producer
            .as_ref()
            .expect("producer")
            .enqueue(ProducerRecord {
                topic: HANGING_TOPIC.into(),
                value: Some(Bytes::from_static(b"open")),
                ..Default::default()
            })
            .await
            .expect("queued"),
    );
    tv2.producer
        .as_ref()
        .expect("producer")
        .flush()
        .await
        .expect("flush");
    wait_for_transaction_state(
        &tv2.harness.as_ref().expect("harness").admin,
        OPEN_TID,
        "Ongoing",
    )
    .await;
}
#[rstest_bdd_macros::when(
    "the transactions tool and native administrator list, describe, inspect producers, and find hanging transactions"
)]
async fn inspect_open(tv2: &mut Tv2) {
    let harness = tv2.harness.as_ref().expect("harness");
    let transaction = describe_transaction(&harness.admin, OPEN_TID).await;
    let listed = list_transactions(&harness.admin)
        .await
        .transaction_states
        .into_iter()
        .find(|row| row.transactional_id == OPEN_TID)
        .expect("open transaction listed");
    let list_row = tool_row(
        &kafka_transactions(harness.bootstrap, &["list"]),
        &[
            "TransactionalId",
            "Coordinator",
            "ProducerId",
            "TransactionState",
        ],
        OPEN_TID,
    );
    let transaction_row = describe_open_transaction_row(harness.bootstrap);
    let producers = active_producers(&harness.admin, HANGING_TOPIC, 0).await;
    let producer_row = describe_open_producer_row(
        harness.bootstrap,
        producers.first().expect("open producer").producer_id,
    );
    let hanging = tool_rows(
        &kafka_transactions(harness.bootstrap, &["find-hanging", "--broker-id", "1"]),
        &[
            "Topic",
            "Partition",
            "ProducerId",
            "ProducerEpoch",
            "CoordinatorEpoch",
            "StartOffset",
            "LastTimestamp",
            "Duration(min)",
        ],
    );
    tv2.snapshot = Some(ToolSnapshot {
        transaction,
        listed_id: listed.producer_id,
        listed_state: listed.transaction_state,
        producers,
        list_row,
        transaction_row,
        producer_row,
        hanging,
    });
}
#[rstest_bdd_macros::then(
    "the tools agree with native transaction identity, epoch, state, coordinator 1, partition krabka-tv2-open-0, and producer start offset"
)]
fn transaction_parity(tv2: &Tv2) {
    let snapshot = tv2.snapshot.as_ref().expect("tools inspected");
    assert!(
        snapshot.list_row
            == vec![
                OPEN_TID.to_owned(),
                "1".to_owned(),
                snapshot.listed_id.to_string(),
                snapshot.listed_state.clone()
            ]
    );
    assert!(
        snapshot.transaction_row[1..5]
            == [
                OPEN_TID.to_owned(),
                snapshot.transaction.producer_id.to_string(),
                snapshot.transaction.producer_epoch.to_string(),
                snapshot.transaction.transaction_state.clone()
            ]
    );
    assert!(snapshot.transaction_row[8] == format!("{HANGING_TOPIC}-0"));
    assert!(snapshot.producers.len() == 1);
    let state = &snapshot.producers[0];
    assert!(
        snapshot.producer_row[..3]
            == [
                state.producer_id.to_string(),
                state.producer_epoch.to_string(),
                state.coordinator_epoch.to_string()
            ]
    );
    assert!(snapshot.producer_row[5] == state.current_txn_start_offset.to_string());
}
#[rstest_bdd_macros::then("the ongoing coordinator-owned transaction is not reported as hanging")]
fn not_hanging(tv2: &Tv2) {
    assert!(tv2.snapshot.as_ref().expect("snapshot").hanging.is_empty());
}
#[rstest_bdd_macros::when(
    "the operator aborts the open partition transaction through kafka-transactions"
)]
fn operator_abort(tv2: &mut Tv2) {
    let state = &tv2.snapshot.as_ref().expect("snapshot").producers[0];
    let harness = tv2.harness.as_ref().expect("harness");
    tv2.output = Some(kafka_transactions(harness.bootstrap, &[
        "abort",
        "--topic",
        HANGING_TOPIC,
        "--partition",
        "0",
        "--start-offset",
        &state.current_txn_start_offset.to_string(),
    ]));
    tv2.producer_row = describe_open_producer_row(harness.bootstrap, state.producer_id);
}
#[rstest_bdd_macros::then("the producer table reports transaction start offset None")]
fn partition_aborted(tv2: &Tv2) {
    assert!(tv2.producer_row[5] == "None");
}
#[rstest_bdd_macros::when(
    "the producer aborts its owned transaction and the transaction tool observes completion"
)]
async fn coordinator_abort(tv2: &mut Tv2) {
    tv2.transaction
        .take()
        .expect("open transaction")
        .abort()
        .await
        .expect("abort");
    let deadline = Instant::now() + SETTLE_DEADLINE;
    loop {
        tv2.final_row =
            describe_open_transaction_row(tv2.harness.as_ref().expect("harness").bootstrap);
        if tv2.final_row[4] == "CompleteAbort" {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "transaction never reached CompleteAbort: {:?}",
            tv2.final_row
        );
        time::sleep(Duration::from_millis(250)).await;
    }
}
#[rstest_bdd_macros::then("the transaction coordinator reports CompleteAbort")]
fn coordinator_complete(tv2: &Tv2) {
    assert!(tv2.final_row[4] == "CompleteAbort");
}
async fn shutdown(tv2: &mut Tv2) {
    if let Some(transaction) = tv2.transaction.take() {
        transaction
            .abort()
            .await
            .expect("abort remaining transaction");
    }
    if let Some(producer) = tv2.producer.take() {
        close_released(producer).await.expect("close producer");
    }
    tv2.harness.take().expect("harness").broker.shutdown().await;
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_durability/transactional_tv2.feature",
    name = "a kafka 4x client runs transactions without add partitions to txn"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kafka_4x_client_runs_transactions_without_add_partitions_to_txn(tv2: Tv2) {
    shutdown(&mut tv2).await;
    drop(tv2);
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_durability/transactional_tv2.feature",
    name = "the transactions tool agrees with the broker and can abort"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_transactions_tool_agrees_with_the_broker_and_can_abort(tv2: Tv2) {
    shutdown(&mut tv2).await;
    drop(tv2);
}

/// Close the in-process producer once its transaction guard has let go of it.
async fn close_released(producer: Arc<Producer>) -> Result<(), ProducerError> {
    Arc::into_inner(producer)
        .expect("the transaction guard released its producer reference")
        .close()
        .await
}

/// Fail unless the cluster finalized `transaction.version` at level 2.
///
/// The request carries `client_software_name` / `client_software_version`
/// because the broker validates them from `ApiVersions` v3 upwards, as Kafka's
/// own handler does: a v3+ request with either field empty is answered
/// `INVALID_REQUEST` and a response whose every other field is default. Read
/// without checking `error_code`, that refusal is indistinguishable from a
/// cluster that finalized nothing, which is what an earlier version of this
/// check misread -- the levels are there, and were the whole time.
async fn assert_transaction_version_2(admin: &Client) {
    let response = admin
        .send(ApiVersionsRequest {
            client_software_name: "krabka-tv2-acceptance".into(),
            client_software_version: env!("CARGO_PKG_VERSION").into(),
            ..Default::default()
        })
        .await
        .expect("ApiVersions");
    assert!(
        response.error_code == 0,
        "ApiVersions was refused with error {}; the feature levels below say \
         nothing until it is accepted",
        response.error_code,
    );
    let level = response
        .finalized_features
        .iter()
        .find(|feature| feature.name == "transaction.version")
        .map(|feature| feature.max_version_level);
    assert!(
        level == Some(2),
        "this case is only meaningful at transaction.version=2, and the \
         cluster finalized {level:?}: {:?}",
        response.finalized_features,
    );
}

/// Compile [`TRANSACTIONAL_PRODUCER_JAVA`] against the Kafka client jars in
/// [`KAFKA_IMAGE_TXN`], into a host directory the runner container mounts next.
///
/// The source is written from the host rather than piped in, because the
/// mount-and-run helper does not attach a stdin. The directory is world
/// writable because the compiling image runs as a non-root user whose uid does
/// not have to match the one that owns the temporary directory.
fn compile_transactional_producer() -> tempfile::TempDir {
    let work = tempfile::tempdir().expect("tempdir for the compiled helper");
    let source = work.path().join("TransactionalProducer.java");
    fs::write(&source, TRANSACTIONAL_PRODUCER_JAVA).expect("write the Java helper");
    #[cfg(unix)]
    {
        use crate::support::chmod_for_container;

        chmod_for_container(work.path(), 0o777, "chmod the work directory");
        chmod_for_container(&source, 0o644, "chmod the Java helper");
    }

    let out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &format!("{}:/work", work.path().display()),
        &[
            "bash",
            "-c",
            r#"set -e; CP=$(ls /usr/share/java/kafka/*.jar | tr '\n' ':'); \
               javac --release 11 -cp "$CP" -d /work /work/TransactionalProducer.java"#,
        ],
    );
    eprintln!(
        "KRABKA[test] javac status={} output={}",
        out.status,
        tool_output(&out)
    );
    assert!(
        work.path().join("TransactionalProducer.class").exists(),
        "javac produced no class file: {}",
        tool_output(&out),
    );
    work
}

/// Run the compiled class under the Kafka 4.3.1 client jars.
///
/// `kafka-run-class.sh` appends the image's own `libs/` to whatever `CLASSPATH`
/// it inherits, so the mounted class directory is all this has to add, and the
/// script rather than a bare `java` invocation is what finds the JRE.
fn run_transactional_producer(classes: &Path, bootstrap: &str, topic: &str) -> Output {
    let out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_ELR,
        &format!("{}:/work:ro", classes.display()),
        &[
            "bash",
            "-c",
            "CLASSPATH=/work exec /opt/kafka/bin/kafka-run-class.sh \
             TransactionalProducer \"$1\" \"$2\"",
            "--",
            bootstrap,
            topic,
        ],
    );
    eprintln!(
        "KRABKA[test] 4.x transactional producer status={} output={}",
        out.status,
        tool_output(&out)
    );
    out
}

/// Run one `kafka-transactions` subcommand from the 4.3.1 image.
fn kafka_transactions(bootstrap: &str, args: &[&str]) -> Output {
    let mut command = vec![KAFKA_TRANSACTIONS, "--bootstrap-server", bootstrap];
    command.extend_from_slice(args);
    docker_run_kafka_tool_with_image(KAFKA_IMAGE_ELR, &command)
}

/// Consume `count` records at `isolation` and return their values in log order.
fn consume(bootstrap: &str, topic: &str, isolation: &str, count: usize) -> Vec<String> {
    let out = docker_run_kafka_tool_with_image(KAFKA_IMAGE_ELR, &[
        KAFKA_CONSOLE_CONSUMER,
        "--bootstrap-server",
        bootstrap,
        "--topic",
        topic,
        "--isolation-level",
        isolation,
        "--from-beginning",
        "--max-messages",
        &count.to_string(),
        "--timeout-ms",
        "30000",
    ]);
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_owned)
        .collect()
}

/// Every data row of a `ToolsUtils.prettyPrintTable` table on the tool's
/// stdout, as whitespace-separated fields.
///
/// The table is a header line of `headers` padded to the widest cell, then one
/// line per row. No value any of these commands renders contains a space, so
/// splitting on whitespace recovers the columns. Requiring the header first is
/// what keeps a tool that printed a warning or nothing at all from parsing as
/// an empty table.
fn tool_rows(out: &Output, headers: &[&str]) -> Vec<Vec<String>> {
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let mut lines = stdout.lines().skip_while(|line| {
        let fields: Vec<&str> = line.split_whitespace().collect();
        fields.as_slice() != headers
    });
    assert!(
        lines.next().is_some(),
        "the tool printed no {headers:?} table: {}",
        tool_output(out),
    );
    lines
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            line.split_whitespace()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The one row of [`tool_rows`] whose first column is `key`.
fn tool_row(out: &Output, headers: &[&str], key: &str) -> Vec<String> {
    let rows = tool_rows(out, headers);
    let matched: Vec<Vec<String>> = rows
        .iter()
        .filter(|row| row.first().is_some_and(|first| first == key))
        .cloned()
        .collect();
    assert!(
        matched.len() == 1,
        "expected exactly one {headers:?} row keyed {key}: {rows:?}",
    );
    let mut row = matched[0].clone();
    // `kafka-transactions describe` renders `TopicPartitions` empty for a
    // transaction that holds none -- a completed one, which is exactly the
    // state this suite drives an abort into -- and whitespace splitting cannot
    // see a trailing empty column. Only the final column may be missing; a row
    // short anywhere else is a real divergence.
    assert!(
        row.len() == headers.len() || row.len() + 1 == headers.len(),
        "a {headers:?} row did not render every column: {row:?}",
    );
    if row.len() + 1 == headers.len() {
        row.push(String::new());
    }
    row
}

/// Poll `DescribeTransactions` until `tid` reads `state`.
async fn wait_for_transaction_state(admin: &Client, tid: &str, state: &str) {
    let deadline = Instant::now() + SETTLE_DEADLINE;
    loop {
        let described = describe_transaction(admin, tid).await;
        if described.transaction_state == state {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "{tid} never reached {state}: {described:?}",
        );
        time::sleep(Duration::from_millis(200)).await;
    }
}

/// `ListTransactions`, retried while the coordinator loads its state
/// partitions. Kafka answers `COORDINATOR_LOAD_IN_PROGRESS` until every
/// partition it leads is loaded, and its admin client retries; the other
/// state partitions can still be loading when the open transaction's is done.
async fn list_transactions(admin: &Client) -> ListTransactionsResponse {
    let deadline = Instant::now() + SETTLE_DEADLINE;
    loop {
        let listed = admin
            .send(ListTransactionsRequest::default())
            .await
            .expect("ListTransactions");
        if listed.error_code != COORDINATOR_LOAD_IN_PROGRESS {
            return listed;
        }
        assert!(
            Instant::now() <= deadline,
            "the coordinator never finished loading: {listed:?}",
        );
        time::sleep(Duration::from_millis(50)).await;
    }
}

/// The coordinator's row for `tid`.
async fn describe_transaction(admin: &Client, tid: &str) -> TransactionState {
    let resp = admin
        .send(DescribeTransactionsRequest {
            transactional_ids: vec![tid.into()],
            ..Default::default()
        })
        .await
        .expect("DescribeTransactions");
    let [row] = resp.transaction_states.as_slice() else {
        panic!("DescribeTransactions({tid}) answered {resp:?}");
    };
    assert!(row.error_code == 0, "DescribeTransactions({tid}): {row:?}");
    row.clone()
}

/// The producer-state snapshot the broker holds for one partition.
async fn active_producers(admin: &Client, topic: &str, partition: i32) -> Vec<ProducerState> {
    let resp = admin
        .send(DescribeProducersRequest {
            topics: vec![TopicRequest {
                name: topic.into(),
                partition_indexes: vec![partition],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DescribeProducers");
    let [topic_result] = resp.topics.as_slice() else {
        panic!("DescribeProducers({topic}) answered {resp:?}");
    };
    let [partition_result] = topic_result.partitions.as_slice() else {
        panic!("DescribeProducers({topic}-{partition}) answered {resp:?}");
    };
    assert!(
        partition_result.error_code == 0,
        "DescribeProducers({topic}-{partition}): {partition_result:?}",
    );
    partition_result.active_producers.clone()
}

/// The `OpenMetrics` body the broker serves on its metrics listener.
async fn scrape_metrics(addr: SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("connect to /metrics");
    let request = format!(
        "GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: */*\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send the scrape");
    stream.flush().await.expect("flush the scrape");
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .expect("read the scrape body");
    let text = String::from_utf8_lossy(&buf).into_owned();
    text.split_once("\r\n\r\n")
        .map_or(text.as_str(), |(_, body)| body)
        .to_owned()
}

/// The value of one fully-labelled `OpenMetrics` series, or `None` when the
/// scrape carries no such series at all.
fn series_value(body: &str, series: &str) -> Option<f64> {
    body.lines().find_map(|line| {
        let value = line.strip_prefix(series)?;
        Some(
            value
                .trim()
                .parse()
                .unwrap_or_else(|_| panic!("unparseable metric line: {line}")),
        )
    })
}

fn describe_open_transaction_row(bootstrap: &str) -> Vec<String> {
    tool_row(
        &kafka_transactions(bootstrap, &["describe", "--transactional-id", OPEN_TID]),
        &[
            "CoordinatorId",
            "TransactionalId",
            "ProducerId",
            "ProducerEpoch",
            "TransactionState",
            "TransactionTimeoutMs",
            "CurrentTransactionStartTimeMs",
            "TransactionDurationMs",
            "TopicPartitions",
        ],
        "1",
    )
}

fn describe_open_producer_row(bootstrap: &str, producer_id: i64) -> Vec<String> {
    tool_row(
        &kafka_transactions(bootstrap, &[
            "describe-producers",
            "--topic",
            HANGING_TOPIC,
            "--partition",
            "0",
        ]),
        &DESCRIBE_PRODUCERS_HEADERS,
        &producer_id.to_string(),
    )
}
