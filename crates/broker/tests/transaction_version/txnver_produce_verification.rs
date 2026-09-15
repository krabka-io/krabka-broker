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

use std::time::{Duration, Instant};

use assert2::assert;
use bytes::BufMut;
use krabka_client_core::Client;
use krabka_protocol::{
    Encode, ProtocolError, ProtocolRequest,
    owned::{
        add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction},
        common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
        end_txn_request::EndTxnRequest,
        find_coordinator_request::FindCoordinatorRequest,
        init_producer_id_request::InitProducerIdRequest,
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        produce_request::{self, PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::{PartitionProduceResponse, ProduceResponse},
    },
    records::{Attributes, Record, RecordBatch},
};

use crate::txnver_harness::{admin_client, boot_single, create_topic};

const NOT_COORDINATOR: i16 = 16;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const CONCURRENT_TRANSACTIONS: i16 = 51;
const INVALID_PRODUCER_EPOCH: i16 = 47;
const INVALID_TXN_STATE: i16 = 48;
const TRANSACTION_ABORTABLE: i16 = 120;

/// A `Produce` request sent at exactly version `V`.
#[derive(Clone, Debug)]
struct ProduceAt<const V: i16>(ProduceRequest);

impl<const V: i16> Encode for ProduceAt<V> {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl<const V: i16> ProtocolRequest for ProduceAt<V> {
    const API_KEY: i16 = produce_request::API_KEY;
    const MIN_VERSION: i16 = V;
    const MAX_VERSION: i16 = V;
    const FLEXIBLE_MIN: i16 = produce_request::FLEXIBLE_MIN;
    type Response = ProduceResponse;
}

/// One transactional producer.
#[derive(Clone, Copy, Debug)]
struct Producer {
    transactional_id: &'static str,
    id: i64,
    epoch: i16,
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

/// The batch a case sends.
#[derive(Clone, Copy, Debug)]
struct Probe {
    version: i16,
    with_transactional_id: bool,
    transactional: bool,
    epoch_offset: i16,
    base_sequence: i32,
}

#[derive(Clone, Copy, Debug)]
struct Case {
    name: &'static str,
    setup: Setup,
    probe: Probe,
    expected_code: i16,
    expected_message: Option<&'static str>,
    appends: bool,
}

fn batch(producer: Producer, epoch: i16, base_sequence: i32, transactional: bool) -> RecordBatch {
    RecordBatch {
        attributes: Attributes::default().with_transactional(transactional),
        producer_id: producer.id,
        producer_epoch: epoch,
        base_sequence,
        last_offset_delta: 0,
        max_timestamp: 1,
        records: vec![Record {
            offset_delta: 0,
            value: Some(bytes::Bytes::from_static(b"v")),
            ..Record::default()
        }],
        ..RecordBatch::default()
    }
}

async fn produce_at(
    client: &Client,
    version: i16,
    topic: &str,
    transactional_id: Option<&str>,
    batch: RecordBatch,
) -> PartitionProduceResponse {
    let request = ProduceRequest {
        transactional_id: transactional_id.map(str::to_owned),
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.into(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(batch.into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let response = match version {
        10 => client.send(ProduceAt::<10>(request)).await,
        11 => client.send(ProduceAt::<11>(request)).await,
        _ => client.send(ProduceAt::<12>(request)).await,
    }
    .expect("Produce");
    response.responses[0].partition_responses[0].clone()
}

async fn log_end(client: &Client, topic: &str) -> i64 {
    let response = client
        .send(ListOffsetsRequest {
            replica_id: -1,
            topics: vec![ListOffsetsTopic {
                name: topic.into(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: 0,
                    timestamp: -1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("ListOffsets");
    response.topics[0].partitions[0].offset
}

/// Send a coordinator request until the coordinator has loaded its state.
async fn until_ready<F, Fut, T>(mut send: F, code: impl Fn(&T) -> i16) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let answer = send().await;
        let loading = matches!(
            code(&answer),
            COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR | CONCURRENT_TRANSACTIONS
        );
        if !loading || Instant::now() >= deadline {
            return answer;
        }
        // intentional: coordinator load has no awaiter reachable from this
        // client; the coordinator answer is the signal.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn init(client: &Client, transactional_id: &'static str) -> Producer {
    let _ = client
        .send(FindCoordinatorRequest {
            key: transactional_id.into(),
            key_type: 1,
            coordinator_keys: vec![transactional_id.into()],
            ..Default::default()
        })
        .await;
    let response = until_ready(
        || async {
            client
                .send(InitProducerIdRequest {
                    transactional_id: Some(transactional_id.into()),
                    transaction_timeout_ms: 60_000,
                    producer_id: -1,
                    producer_epoch: -1,
                    ..Default::default()
                })
                .await
                .expect("InitProducerId")
        },
        |response| response.error_code,
    )
    .await;
    assert!(response.error_code == 0, "InitProducerId: {response:?}");
    Producer {
        transactional_id,
        id: response.producer_id,
        epoch: response.producer_epoch,
    }
}

async fn add_partition(client: &Client, producer: Producer, topic: &str) {
    let added = AddPartitionsToTxnTopic {
        name: topic.into(),
        partitions: vec![0],
        ..Default::default()
    };
    let code = until_ready(
        || async {
            let response = client
                .send(AddPartitionsToTxnRequest {
                    v3_and_below_transactional_id: producer.transactional_id.into(),
                    v3_and_below_producer_id: producer.id,
                    v3_and_below_producer_epoch: producer.epoch,
                    v3_and_below_topics: vec![added.clone()],
                    transactions: vec![AddPartitionsToTxnTransaction {
                        transactional_id: producer.transactional_id.into(),
                        producer_id: producer.id,
                        producer_epoch: producer.epoch,
                        topics: vec![added.clone()],
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .await
                .expect("AddPartitionsToTxn");
            response
                .results_by_transaction
                .first()
                .and_then(|transaction| transaction.topic_results.first())
                .or(response.results_by_topic_v3_and_below.first())
                .and_then(|topic| topic.results_by_partition.first())
                .map_or(response.error_code, |row| row.partition_error_code)
        },
        |code| *code,
    )
    .await;
    assert!(code == 0, "AddPartitionsToTxn: {code}");
}

async fn set_up(client: &Client, case: &Case) -> Producer {
    create_topic(client, case.name, 1).await;
    let mut producer = init(client, case.name).await;
    if case.probe.epoch_offset < 0 {
        // A second InitProducerId bumps the epoch, so a batch can carry an
        // epoch below the producer's one.
        producer = init(client, case.name).await;
    }
    if matches!(case.setup, Setup::Added | Setup::Open | Setup::Committed) {
        add_partition(client, producer, case.name).await;
    }
    if matches!(case.setup, Setup::Open | Setup::Committed) {
        let row = produce_at(
            client,
            11,
            case.name,
            Some(case.name),
            batch(producer, producer.epoch, 0, true),
        )
        .await;
        assert!(row.error_code == 0, "{}: setup produce {row:?}", case.name);
    }
    if matches!(case.setup, Setup::Committed) {
        let end = client
            .send(EndTxnRequest {
                transactional_id: case.name.into(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                committed: true,
                ..Default::default()
            })
            .await
            .expect("EndTxn");
        assert!(end.error_code == 0, "{}: EndTxn {end:?}", case.name);
    }
    producer
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transactional_produce_needs_a_verified_transaction() {
    let at = |version, transactional, base_sequence| Probe {
        version,
        with_transactional_id: true,
        transactional,
        epoch_offset: 0,
        base_sequence,
    };
    let cases = [
        Case {
            name: "v10-partition-not-added",
            setup: Setup::Nothing,
            probe: at(10, true, 0),
            expected_code: INVALID_TXN_STATE,
            expected_message: Some("Partition was not added to the transaction"),
            appends: false,
        },
        Case {
            name: "v11-partition-not-added",
            setup: Setup::Nothing,
            probe: at(11, true, 0),
            expected_code: TRANSACTION_ABORTABLE,
            expected_message: None,
            appends: false,
        },
        Case {
            name: "v11-partition-added",
            setup: Setup::Added,
            probe: at(11, true, 0),
            expected_code: 0,
            expected_message: None,
            appends: true,
        },
        Case {
            name: "v12-partition-not-added",
            setup: Setup::Nothing,
            probe: at(12, true, 0),
            expected_code: 0,
            expected_message: None,
            appends: true,
        },
        Case {
            name: "v11-no-transactional-id",
            setup: Setup::Added,
            probe: Probe {
                with_transactional_id: false,
                ..at(11, true, 0)
            },
            expected_code: INVALID_TXN_STATE,
            expected_message: None,
            appends: false,
        },
        Case {
            name: "open-transaction-stale-epoch",
            setup: Setup::Open,
            probe: Probe {
                epoch_offset: -1,
                ..at(11, true, 1)
            },
            expected_code: INVALID_PRODUCER_EPOCH,
            expected_message: None,
            appends: false,
        },
        Case {
            name: "open-transaction-non-transactional-batch",
            setup: Setup::Open,
            probe: at(11, false, 1),
            expected_code: INVALID_TXN_STATE,
            expected_message: None,
            appends: false,
        },
        // The broker runs at transaction version 2, so the commit marker
        // bumped the producer epoch. A replay at the old epoch is stale.
        Case {
            name: "committed-replay-of-the-last-batch",
            setup: Setup::Committed,
            probe: at(11, true, 0),
            expected_code: INVALID_PRODUCER_EPOCH,
            expected_message: None,
            appends: false,
        },
    ];

    let (broker, bootstrap, _dir) = boot_single().await;
    let client = admin_client(&bootstrap).await;
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for case in cases {
        let producer = set_up(&client, &case).await;
        let before = log_end(&client, case.name).await;
        let row = produce_at(
            &client,
            case.probe.version,
            case.name,
            case.probe.with_transactional_id.then_some(case.name),
            batch(
                producer,
                producer.epoch + case.probe.epoch_offset,
                case.probe.base_sequence,
                case.probe.transactional,
            ),
        )
        .await;
        let after = log_end(&client, case.name).await;
        actual.push((case.name, row, after - before));
        expected.push((
            case.name,
            PartitionProduceResponse {
                index: 0,
                error_code: case.expected_code,
                base_offset: if case.appends { before } else { -1 },
                log_append_time_ms: -1,
                log_start_offset: if case.appends { 0 } else { -1 },
                error_message: case.expected_message.map(str::to_owned),
                ..Default::default()
            },
            i64::from(case.appends),
        ));
    }
    broker.shutdown().await;

    assert!(actual == expected);
}
