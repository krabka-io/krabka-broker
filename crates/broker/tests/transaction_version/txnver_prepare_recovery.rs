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

use std::time::{Duration, Instant};

use assert2::assert;
use bytes::Bytes;
use krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle, MarkerFanoutMode};
use krabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction},
        common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
        end_txn_request::EndTxnRequest,
        end_txn_response::EndTxnResponse,
        find_coordinator_request::FindCoordinatorRequest,
        init_producer_id_request::InitProducerIdRequest,
        init_producer_id_response::InitProducerIdResponse,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid,
    records::{Attributes, Record, RecordBatch},
};
use tempfile::TempDir;

use crate::txnver_harness::{admin_client, create_topic, downgrade_transaction_version};

const NOT_LEADER_OR_FOLLOWER: i16 = 6;
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const NOT_COORDINATOR: i16 = 16;
const CONCURRENT_TRANSACTIONS: i16 = 51;
const PRODUCER_FENCED: i16 = 90;
const UNKNOWN_TOPIC_ID: i16 = 100;

/// The time a retried request may take to see a transaction complete.
const SETTLE: Duration = Duration::from_secs(20);

fn config(log_dir: std::path::PathBuf, bootstrap_mode: Option<BootstrapMode>) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir);
    cfg.transaction_state_num_partitions = 1;
    cfg.transaction_state_replication_factor = 1;
    if let Some(mode) = bootstrap_mode {
        cfg.bootstrap_mode = mode;
    }
    cfg
}

#[derive(Debug, Clone, Copy)]
struct Identity {
    producer_id: i64,
    epoch: i16,
}

fn coordinator_is_loading(code: i16) -> bool {
    matches!(
        code,
        COORDINATOR_LOAD_IN_PROGRESS | COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR
    )
}

async fn find_coordinator(client: &Client, transactional_id: &str) {
    let _ = client
        .send(FindCoordinatorRequest {
            key: transactional_id.into(),
            key_type: 1,
            coordinator_keys: vec![transactional_id.into()],
            ..Default::default()
        })
        .await
        .expect("FindCoordinator");
}

async fn init_producer_id(
    client: &Client,
    transactional_id: &str,
    identity: (i64, i16),
) -> InitProducerIdResponse {
    client
        .send(InitProducerIdRequest {
            transactional_id: Some(transactional_id.into()),
            transaction_timeout_ms: 60_000,
            producer_id: identity.0,
            producer_epoch: identity.1,
            ..Default::default()
        })
        .await
        .expect("InitProducerId")
}

async fn init_producer(client: &Client, transactional_id: &str) -> Identity {
    find_coordinator(client, transactional_id).await;
    let deadline = Instant::now() + SETTLE;
    loop {
        let response = init_producer_id(client, transactional_id, (-1, -1)).await;
        if coordinator_is_loading(response.error_code) && Instant::now() < deadline {
            // intentional: coordinator load has no awaiter; the answer is the signal.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        assert!(response.error_code == 0, "InitProducerId: {response:?}");
        return Identity {
            producer_id: response.producer_id,
            epoch: response.producer_epoch,
        };
    }
}

async fn add_partition(client: &Client, transactional_id: &str, producer: Identity, topic: &str) {
    let added = AddPartitionsToTxnTopic {
        name: topic.into(),
        partitions: vec![0],
        ..Default::default()
    };
    let response = client
        .send(AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: transactional_id.into(),
                producer_id: producer.producer_id,
                producer_epoch: producer.epoch,
                topics: vec![added.clone()],
                ..Default::default()
            }],
            v3_and_below_transactional_id: transactional_id.into(),
            v3_and_below_producer_id: producer.producer_id,
            v3_and_below_producer_epoch: producer.epoch,
            v3_and_below_topics: vec![added],
            ..Default::default()
        })
        .await
        .expect("AddPartitionsToTxn");
    let code = response
        .results_by_transaction
        .first()
        .and_then(|transaction| transaction.topic_results.first())
        .and_then(|row| row.results_by_partition.first())
        .map_or(response.error_code, |row| row.partition_error_code);
    assert!(code == 0, "AddPartitionsToTxn: {response:?}");
}

async fn topic_id(client: &Client, topic: &str) -> Uuid {
    client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(topic.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata")
        .topics
        .iter()
        .find(|row| row.name.as_deref() == Some(topic))
        .map(|row| row.topic_id)
        .expect("topic in metadata")
}

/// Produce `values` in one batch. `producer` is `None` for a plain batch.
async fn produce(
    client: &Client,
    topic: &str,
    producer: Option<Identity>,
    values: &[&'static str],
) {
    let records = i32::try_from(values.len()).expect("record count");
    let batch = RecordBatch {
        attributes: Attributes::default().with_transactional(producer.is_some()),
        producer_id: producer.map_or(-1, |producer| producer.producer_id),
        producer_epoch: producer.map_or(-1, |producer| producer.epoch),
        base_sequence: if producer.is_some() { 0 } else { -1 },
        last_offset_delta: records - 1,
        max_timestamp: 1,
        records: values
            .iter()
            .zip(0..)
            .map(|(value, offset_delta)| Record {
                offset_delta,
                value: Some(Bytes::from_static(value.as_bytes())),
                ..Record::default()
            })
            .collect(),
        ..RecordBatch::default()
    };
    let request = ProduceRequest {
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.into(),
            topic_id: topic_id(client, topic).await,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(batch.into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let deadline = Instant::now() + SETTLE;
    loop {
        let response = client.send(request.clone()).await.expect("Produce");
        let code = response.responses[0].partition_responses[0].error_code;
        let settling = matches!(
            code,
            NOT_LEADER_OR_FOLLOWER | UNKNOWN_TOPIC_OR_PARTITION | UNKNOWN_TOPIC_ID
        );
        if settling && Instant::now() < deadline {
            // intentional: partition leadership after a start has no awaiter
            // reachable from this client; the produce answer is the signal.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        assert!(code == 0, "Produce: {response:?}");
        return;
    }
}

fn end_txn_request(transactional_id: &str, producer: Identity, committed: bool) -> EndTxnRequest {
    EndTxnRequest {
        transactional_id: transactional_id.into(),
        producer_id: producer.producer_id,
        producer_epoch: producer.epoch,
        committed,
        ..Default::default()
    }
}

/// Retry `EndTxn` the way a Kafka producer does after a lost answer, until
/// the answer is not retriable or the time is up.
async fn end_txn_until_answered(
    client: &Client,
    transactional_id: &str,
    producer: Identity,
    committed: bool,
) -> EndTxnResponse {
    find_coordinator(client, transactional_id).await;
    let deadline = Instant::now() + SETTLE;
    loop {
        let response = client
            .send(end_txn_request(transactional_id, producer, committed))
            .await
            .expect("EndTxn");
        let retriable = coordinator_is_loading(response.error_code)
            || response.error_code == CONCURRENT_TRANSACTIONS;
        if retriable && Instant::now() < deadline {
            // intentional: transaction completion has no awaiter reachable
            // from this client; the EndTxn answer is the signal.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        return response;
    }
}

/// The values a `read_committed` consumer reads, up to and including `last`.
async fn read_committed_through(bootstrap: &str, topic: &str, last: &str) -> Vec<String> {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.to_string())
        .group_id(format!("{topic}-reader"))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe([topic.to_string()])
        .build()
        .await
        .expect("consumer");
    let mut seen = Vec::new();
    let deadline = Instant::now() + SETTLE;
    while seen.last().map(String::as_str) != Some(last) && Instant::now() < deadline {
        for record in consumer
            .poll(krabka_units::millis(200))
            .await
            .expect("poll")
        {
            seen.push(String::from_utf8_lossy(record.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    consumer.close().await.expect("close consumer");
    seen
}

struct Started {
    broker: BrokerHandle,
    client: Client,
    bootstrap: String,
}

async fn start(log_dir: &std::path::Path, bootstrap_mode: Option<BootstrapMode>) -> Started {
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
    create_topic(client, topic, 1).await;
    if let Some(level) = downgrade_to {
        downgrade_transaction_version(client, level).await;
    }
    let producer = init_producer(client, transactional_id).await;
    add_partition(client, transactional_id, producer, topic).await;
    produce(client, topic, Some(producer), &["a", "b", "c"]).await;
    producer
}

/// The `EndTxn` v5 answer for a completed transaction. Transaction version 2
/// bumps the epoch on completion (`epoch_bump` 1). Lower versions keep it.
fn completed(producer: Identity, epoch_bump: i16) -> EndTxnResponse {
    EndTxnResponse {
        producer_id: producer.producer_id,
        producer_epoch: producer.epoch + epoch_bump,
        ..Default::default()
    }
}

struct CutCase {
    name: &'static str,
    topic: &'static str,
    committed: bool,
    downgrade_to: Option<i16>,
    epoch_bump: i16,
    /// What a `read_committed` consumer reads through the record that the
    /// test writes after the transaction completes.
    visible: &'static [&'static str],
}

/// Stop the broker while `EndTxn` waits between its `Prepare*` append and its
/// markers, start it again, and retry `EndTxn`.
async fn end_txn_cut_by_shutdown(case: &CutCase) -> (Identity, EndTxnResponse, Vec<String>) {
    let directory = TempDir::new().expect("tempdir");
    let first = start(directory.path(), None).await;
    let producer = open_transaction(&first.client, case.name, case.topic, case.downgrade_to).await;

    first
        .broker
        .set_transaction_marker_fanout_for_test(MarkerFanoutMode::Hold);
    let cut_client = admin_client(&first.bootstrap).await;
    let request = end_txn_request(case.name, producer, case.committed);
    let cut = tokio::spawn(async move { cut_client.send(request).await });
    first
        .broker
        .wait_for_transaction_marker_fanouts_for_test(1)
        .await;
    first.broker.shutdown().await;
    let cut_answer = cut.await.expect("cut EndTxn task");
    assert!(
        cut_answer.is_err(),
        "{}: a shutdown must cut the held EndTxn without an answer: {cut_answer:?}",
        case.name
    );

    let second = start(directory.path(), Some(BootstrapMode::Rejoin)).await;
    let retried = end_txn_until_answered(&second.client, case.name, producer, case.committed).await;
    produce(&second.client, case.topic, None, &["z"]).await;
    let seen = read_committed_through(&second.bootstrap, case.topic, "z").await;
    second.broker.shutdown().await;
    (producer, retried, seen)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_txn_cut_between_prepare_and_complete_completes_after_restart() {
    let cases = [
        CutCase {
            name: "cut-commit",
            topic: "cut-commit",
            committed: true,
            downgrade_to: None,
            epoch_bump: 1,
            visible: &["a", "b", "c", "z"],
        },
        CutCase {
            name: "cut-abort",
            topic: "cut-abort",
            committed: false,
            downgrade_to: None,
            epoch_bump: 1,
            visible: &["z"],
        },
        CutCase {
            name: "cut-commit-tv1",
            topic: "cut-commit-tv1",
            committed: true,
            downgrade_to: Some(1),
            epoch_bump: 0,
            visible: &["a", "b", "c", "z"],
        },
    ];
    for case in &cases {
        let (producer, retried, seen) = end_txn_cut_by_shutdown(case).await;
        assert!(
            retried == completed(producer, case.epoch_bump),
            "{}: retried EndTxn",
            case.name
        );
        assert!(seen == case.visible, "{}: read_committed", case.name);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn init_producer_id_during_prepare_commit_is_concurrent_transactions() {
    let transactional_id = "init-during-prepare";
    let topic = "init-during-prepare";
    let directory = TempDir::new().expect("tempdir");
    let started = start(directory.path(), None).await;
    let producer = open_transaction(&started.client, transactional_id, topic, None).await;

    started
        .broker
        .set_transaction_marker_fanout_for_test(MarkerFanoutMode::Hold);
    let end_client = admin_client(&started.bootstrap).await;
    let request = end_txn_request(transactional_id, producer, true);
    let end = tokio::spawn(async move { end_client.send(request).await });
    started
        .broker
        .wait_for_transaction_marker_fanouts_for_test(1)
        .await;

    // Kafka `prepareInitProducerIdTransit`: a producer ID that is not the
    // entry's is fenced, and any other request waits for the transition.
    let cases = [
        ("no identity", (-1, -1), CONCURRENT_TRANSACTIONS),
        (
            "the transaction's identity",
            (producer.producer_id, producer.epoch),
            CONCURRENT_TRANSACTIONS,
        ),
        (
            "another producer ID",
            (producer.producer_id + 1_000, 0),
            PRODUCER_FENCED,
        ),
    ];
    for (name, identity, error_code) in cases {
        let response = init_producer_id(&started.client, transactional_id, identity).await;
        let expected = InitProducerIdResponse {
            error_code,
            producer_id: -1,
            producer_epoch: -1,
            ..Default::default()
        };
        assert!(response == expected, "InitProducerId with {name}");
    }

    started
        .broker
        .set_transaction_marker_fanout_for_test(MarkerFanoutMode::Open);
    let answer = end.await.expect("EndTxn task").expect("EndTxn answer");
    assert!(answer == completed(producer, 1));
    produce(&started.client, topic, None, &["z"]).await;
    let seen = read_committed_through(&started.bootstrap, topic, "z").await;
    assert!(seen == ["a", "b", "c", "z"]);
    started.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_txn_with_a_failed_marker_fanout_answers_none_and_completes_later() {
    let transactional_id = "failed-fanout";
    let topic = "failed-fanout";
    let directory = TempDir::new().expect("tempdir");
    let started = start(directory.path(), None).await;
    let producer = open_transaction(&started.client, transactional_id, topic, None).await;

    started
        .broker
        .set_transaction_marker_fanout_for_test(MarkerFanoutMode::Fail);
    let answer = started
        .client
        .send(end_txn_request(transactional_id, producer, true))
        .await
        .expect("EndTxn");
    assert!(
        answer == completed(producer, 1),
        "EndTxn after a durable PrepareCommit"
    );

    started
        .broker
        .set_transaction_marker_fanout_for_test(MarkerFanoutMode::Open);
    let retried = end_txn_until_answered(&started.client, transactional_id, producer, true).await;
    assert!(retried == completed(producer, 1), "retried EndTxn");
    produce(&started.client, topic, None, &["z"]).await;
    let seen = read_committed_through(&started.bootstrap, topic, "z").await;
    assert!(seen == ["a", "b", "c", "z"]);
    started.broker.shutdown().await;
}
