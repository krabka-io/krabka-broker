//! The first sequence at a new producer epoch, before and after a restart.
//!
//! Kafka's `ProducerAppendInfo.checkSequence` answers
//! `OUT_OF_ORDER_SEQUENCE_NUMBER` (45) when a batch changes the producer epoch
//! and its first sequence is not 0. A transaction-version-2 end marker bumps
//! the epoch, so the next transaction must start at sequence 0. Each case
//! sends the rejected batch to the live broker, restarts the broker on the
//! same data directory, sends the rejected batch again, and then sends the
//! batch that Kafka accepts. The answer must not depend on the restart.

use std::time::{Duration, Instant};

use assert2::assert;
use krabka_broker::{BootstrapMode, Broker, BrokerConfig};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction},
        common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
        end_txn_request::EndTxnRequest,
        find_coordinator_request::FindCoordinatorRequest,
        init_producer_id_request::InitProducerIdRequest,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid,
    records::{Attributes, Record, RecordBatch},
};
use tempfile::TempDir;

use crate::txnver_harness::{admin_client, create_topic, downgrade_transaction_version};

const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
const NOT_LEADER_OR_FOLLOWER: i16 = 6;
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const NOT_COORDINATOR: i16 = 16;
const CONCURRENT_TRANSACTIONS: i16 = 51;
const UNKNOWN_TOPIC_ID: i16 = 100;

fn config(log_dir: std::path::PathBuf, bootstrap_mode: Option<BootstrapMode>) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir);
    cfg.transaction_state_num_partitions = 1;
    cfg.transaction_state_replication_factor = 1;
    if let Some(mode) = bootstrap_mode {
        cfg.bootstrap_mode = mode;
    }
    cfg
}

/// How the producer reaches the epoch that the probes use.
#[derive(Debug, Clone, Copy)]
enum Setup {
    /// A transactional producer commits one transaction of three records.
    /// `downgrade_to` selects the `transaction.version` level.
    Transaction { downgrade_to: Option<i16> },
    /// An idempotent producer writes three records at epoch 0. The probes use
    /// epoch 1, as the Java client does after a KIP-360 local epoch bump.
    Idempotent,
}

struct Case {
    name: &'static str,
    topic: &'static str,
    setup: Setup,
    /// `(epoch offset from the setup epoch, base sequence)` of the batch Kafka
    /// rejects.
    rejected: (i16, i32),
    /// `(epoch offset from the setup epoch, base sequence)` of the batch Kafka
    /// accepts.
    accepted: (i16, i32),
}

struct Identity {
    transactional_id: Option<&'static str>,
    producer_id: i64,
    epoch: i16,
}

async fn topic_id(client: &Client, topic: &str) -> Uuid {
    let response = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(topic.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata");
    response
        .topics
        .iter()
        .find(|row| row.name.as_deref() == Some(topic))
        .map(|row| row.topic_id)
        .expect("topic in metadata")
}

fn batch(producer: &Identity, epoch: i16, base_sequence: i32, records: i32) -> RecordBatch {
    RecordBatch {
        attributes: Attributes::default().with_transactional(producer.transactional_id.is_some()),
        producer_id: producer.producer_id,
        producer_epoch: epoch,
        base_sequence,
        last_offset_delta: records - 1,
        max_timestamp: 1,
        records: (0..records)
            .map(|offset_delta| Record {
                offset_delta,
                value: Some(bytes::Bytes::from_static(b"v")),
                ..Record::default()
            })
            .collect(),
        ..RecordBatch::default()
    }
}

/// Send one batch. Retry only while the partition or its leader is not ready
/// after a start, and return the final error code.
async fn produce(
    client: &Client,
    topic: &str,
    (batch_transactional_id, batch): (Option<&str>, RecordBatch),
) -> i16 {
    let id = topic_id(client, topic).await;
    let request = ProduceRequest {
        // A Kafka producer names its transactional id on every request.
        transactional_id: batch_transactional_id.map(str::to_owned),
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.into(),
            topic_id: id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(batch.into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let response = client.send(request.clone()).await.expect("Produce");
        let code = response.responses[0].partition_responses[0].error_code;
        let settling = matches!(
            code,
            NOT_LEADER_OR_FOLLOWER | UNKNOWN_TOPIC_OR_PARTITION | UNKNOWN_TOPIC_ID
        );
        if !settling || Instant::now() >= deadline {
            return code;
        }
        // intentional: partition leadership after a restart has no awaiter
        // reachable from this client; the produce answer is the signal.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Send a coordinator request until the coordinator has loaded its state.
async fn until_coordinator_ready<F, Fut>(mut send: F) -> i16
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = i16>,
{
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let code = send().await;
        let loading = matches!(
            code,
            COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR | CONCURRENT_TRANSACTIONS
        );
        if !loading || Instant::now() >= deadline {
            return code;
        }
        // intentional: coordinator load after a restart has no awaiter; the
        // coordinator answer is the signal.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn add_partition(client: &Client, producer: &Identity, topic: &str, epoch: i16) -> i16 {
    let transactional_id = producer.transactional_id.expect("transactional producer");
    find_coordinator(client, transactional_id).await;
    until_coordinator_ready(|| async {
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
                    producer_epoch: epoch,
                    topics: vec![added.clone()],
                    ..Default::default()
                }],
                v3_and_below_transactional_id: transactional_id.into(),
                v3_and_below_producer_id: producer.producer_id,
                v3_and_below_producer_epoch: epoch,
                v3_and_below_topics: vec![added],
                ..Default::default()
            })
            .await
            .expect("AddPartitionsToTxn");
        response
            .results_by_transaction
            .first()
            .and_then(|transaction| transaction.topic_results.first())
            .and_then(|topic| topic.results_by_partition.first())
            .map_or(response.error_code, |row| row.partition_error_code)
    })
    .await
}

/// `FindCoordinator` for a transactional id. It also starts the creation of
/// the transaction-state topic on a new cluster.
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

async fn init_transactional_producer(
    client: &Client,
    transactional_id: &str,
) -> krabka_protocol::owned::init_producer_id_response::InitProducerIdResponse {
    find_coordinator(client, transactional_id).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let response = client
            .send(InitProducerIdRequest {
                transactional_id: Some(transactional_id.into()),
                transaction_timeout_ms: 60_000,
                producer_id: -1,
                producer_epoch: -1,
                ..Default::default()
            })
            .await
            .expect("InitProducerId");
        let loading = matches!(
            response.error_code,
            COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR | CONCURRENT_TRANSACTIONS
        );
        if !loading || Instant::now() >= deadline {
            assert!(response.error_code == 0, "InitProducerId: {response:?}");
            return response;
        }
        // intentional: coordinator load has no awaiter; the answer is the signal.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Run the setup and return the producer at the epoch the probes start from.
async fn set_up(client: &Client, case: &Case) -> Identity {
    match case.setup {
        Setup::Idempotent => {
            let init = client
                .send(InitProducerIdRequest {
                    transactional_id: None,
                    producer_id: -1,
                    producer_epoch: -1,
                    ..Default::default()
                })
                .await
                .expect("InitProducerId");
            let producer = Identity {
                transactional_id: None,
                producer_id: init.producer_id,
                epoch: init.producer_epoch,
            };
            let code = produce(
                client,
                case.topic,
                (
                    producer.transactional_id,
                    batch(&producer, producer.epoch, 0, 3),
                ),
            )
            .await;
            assert!(code == 0, "{}: setup produce", case.name);
            Identity {
                epoch: producer.epoch + 1,
                ..producer
            }
        }
        Setup::Transaction { downgrade_to } => {
            if let Some(level) = downgrade_to {
                downgrade_transaction_version(client, level).await;
            }
            let transactional_id = case.name;
            let init = init_transactional_producer(client, transactional_id).await;
            let producer = Identity {
                transactional_id: Some(transactional_id),
                producer_id: init.producer_id,
                epoch: init.producer_epoch,
            };
            let added = add_partition(client, &producer, case.topic, producer.epoch).await;
            assert!(added == 0, "{}: AddPartitionsToTxn", case.name);
            let code = produce(
                client,
                case.topic,
                (
                    producer.transactional_id,
                    batch(&producer, producer.epoch, 0, 3),
                ),
            )
            .await;
            assert!(code == 0, "{}: setup produce", case.name);
            let end = client
                .send(EndTxnRequest {
                    transactional_id: transactional_id.into(),
                    producer_id: producer.producer_id,
                    producer_epoch: producer.epoch,
                    committed: true,
                    ..Default::default()
                })
                .await
                .expect("EndTxn");
            assert!(end.error_code == 0, "{}: EndTxn {end:?}", case.name);
            // EndTxn v5 carries the bumped epoch at transaction version 2.
            // Below version 2 the epoch does not change.
            let epoch = if end.producer_id >= 0 {
                end.producer_epoch
            } else {
                producer.epoch
            };
            Identity { epoch, ..producer }
        }
    }
}

async fn probe(
    client: &Client,
    case: &Case,
    producer: &Identity,
    (offset, sequence): (i16, i32),
) -> i16 {
    let epoch = producer.epoch + offset;
    if producer.transactional_id.is_some() {
        let added = add_partition(client, producer, case.topic, epoch).await;
        assert!(
            added == 0,
            "{}: AddPartitionsToTxn before a probe",
            case.name
        );
    }
    produce(
        client,
        case.topic,
        (
            producer.transactional_id,
            batch(producer, epoch, sequence, 1),
        ),
    )
    .await
}

/// Error codes: the rejected batch live, the rejected batch after a restart,
/// and the accepted batch after the restart.
async fn codes_across_restart(case: &Case) -> [i16; 3] {
    let directory = TempDir::new().expect("tempdir");
    let log_dir = directory.path().to_path_buf();

    let broker = Broker::start(config(log_dir.clone(), None))
        .await
        .expect("start broker");
    let client = admin_client(&broker.listen_addr().to_string()).await;
    create_topic(&client, case.topic, 1).await;
    let producer = set_up(&client, case).await;
    let live_rejected = probe(&client, case, &producer, case.rejected).await;
    broker.shutdown().await;

    let broker = Broker::start(config(log_dir, Some(BootstrapMode::Rejoin)))
        .await
        .expect("restart broker");
    let client = admin_client(&broker.listen_addr().to_string()).await;
    let recovered_rejected = probe(&client, case, &producer, case.rejected).await;
    let recovered_accepted = probe(&client, case, &producer, case.accepted).await;
    broker.shutdown().await;

    [live_rejected, recovered_rejected, recovered_accepted]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_sequence_at_a_new_epoch_gets_the_same_answer_before_and_after_restart() {
    let cases = [
        Case {
            name: "tv2-commit-bumps-the-epoch",
            topic: "seq-tv2",
            setup: Setup::Transaction { downgrade_to: None },
            rejected: (0, 3),
            accepted: (0, 0),
        },
        Case {
            name: "tv1-commit-keeps-the-epoch",
            topic: "seq-tv1",
            setup: Setup::Transaction {
                downgrade_to: Some(1),
            },
            // A gap at the kept epoch. The rejected batch must not start
            // below the accepted one: the KIP-890 verification of the rejected
            // batch keeps its first sequence as the lowest one, and Kafka's
            // `ProducerAppendInfo.checkSequence` then refuses any higher first
            // sequence until a transactional append clears that state.
            rejected: (0, 4),
            accepted: (0, 3),
        },
        Case {
            name: "idempotent-epoch-bump",
            topic: "seq-idempotent",
            setup: Setup::Idempotent,
            rejected: (0, 3),
            accepted: (0, 0),
        },
    ];
    for case in &cases {
        let codes = codes_across_restart(case).await;
        assert!(
            codes
                == [
                    OUT_OF_ORDER_SEQUENCE_NUMBER,
                    OUT_OF_ORDER_SEQUENCE_NUMBER,
                    0
                ],
            "{}: [live rejected, recovered rejected, recovered accepted]",
            case.name
        );
    }
}
