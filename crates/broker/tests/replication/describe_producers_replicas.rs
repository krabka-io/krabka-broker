//! `DescribeProducers` on every replica of a partition.
//!
//! Kafka answers `DescribeProducers` from the producer state of the partition
//! log on every replica that hosts the partition:
//! `ReplicaManager.activeProducerState`, then `Partition.activeProducerState`,
//! then `UnifiedLog.activeProducers`. A follower updates that state for each
//! batch that it replicates (`UnifiedLog.appendAsFollower`). When a follower
//! has the log of its leader, it gives the same answer as its leader.
//!
//! The produce path of this broker keeps a second copy of the producer state.
//! A follower does not add its replicated data batches to that copy, so a
//! follower that answered from it reported only the producers whose
//! transaction markers it had replicated.

use {
    crate::{
        cluster_lock,
        support::{
            client::{connect_with_context, raw_connection},
            produce::{SinglePartitionProduceSetup, batch_request},
            shutdown_cluster,
            topics::CreateTopicRequestSetup,
            transactions::{self, ProducerEpoch, ProducerIdentity, TransactionOutcome},
            {self},
        },
    },
    assert2::assert,
    bytes::BytesMut,
    krabka_broker::{BrokerConfig, BrokerHandle, codes},
    krabka_client_core::{Connection},
    krabka_ids::ProducerId,
    krabka_protocol::{
        Decode, Encode,
        owned::{
            describe_producers_request::{DescribeProducersRequest, TopicRequest},
            describe_producers_response::{
                DescribeProducersResponse, PartitionResponse, ProducerState, TopicResponse,
            },
            produce_request::ProduceRequest,
            produce_response::ProduceResponse,
            write_txn_markers_request::WriteTxnMarkersRequest,
        },
        primitives::uuid::Uuid as WireUuid,
        records::RecordBatch,
    },
    rstest::fixture,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::{DataTableRow, given, scenario, then, when},
    std::{mem::take, net::SocketAddr, time::Duration},
    tempfile::TempDir,
    tokio::{sync::MutexGuard, time},
};

type Cluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;

const TOPIC: &str = "describe-producers-replicas";

/// The `transactional_id` of every transactional `Produce`.
const TRANSACTIONAL_ID: &str = "describe-producers-replicas";

/// The last `Produce` version before transaction version 2. With
/// `transaction.partition.verification.enable=false`, Kafka's leader appends
/// a transactional batch at this version without a call to the transaction
/// coordinator, so no coordinator takes part in this test.
const PRODUCE_V11: i16 = 11;

/// Kafka's `api_key` of `Produce`.
const PRODUCE_API_KEY: i16 = 0;

/// The producer with `id` at `epoch`.
const fn producer(id: i64, epoch: i16) -> ProducerIdentity {
    ProducerIdentity {
        id: ProducerId(id),
        epoch: ProducerEpoch(epoch),
    }
}

/// The batch fixture's view of `identity`.
const fn batch_producer(identity: ProducerIdentity) -> BatchProducer {
    BatchProducer {
        id: identity.id,
        epoch: BatchProducerEpoch(identity.epoch.0),
    }
}

/// The transaction fields of a `DescribeProducers` row.
#[derive(Clone, Copy)]
struct TransactionFields {
    coordinator_epoch: i32,
    current_txn_start_offset: i64,
}

use crate::support::records::now_ms;

/// One connection to the broker that binds `address`, so that a request
/// reaches that broker and no other.
async fn connect(address: SocketAddr) -> Connection {
    raw_connection(address, "describe-producers-replicas").await
}

/// Three brokers whose leaders append a transactional `Produce` below v12
/// without the transaction coordinator. A cluster start can split the vote
/// on a slow runner, so a failed start is tried again with new ports.
async fn start_cluster() -> Cluster {
    let mut last_error = None;
    for _ in 0..3 {
        match support::start_n_node_with(3, |_, config| {
            config.transaction_partition_verification_enable = false;
        })
        .await
        {
            Ok(cluster) => return cluster,
            Err(error) => {
                last_error = Some(error);
                // intentional: a new attempt needs the old ports released.
                time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!("cluster start failed after 3 attempts: {last_error:?}");
}

// A batch of `producer` with `records` records from
// `base_sequence`.
krabka_macros::producer_batch_fixture!(
    batch,
    ::bytes::Bytes::from(format!("{producer_id}-{offset_delta}"))
);

fn produce_request(
    topic_id: WireUuid,
    transactional_id: Option<&str>,
    batch: RecordBatch,
) -> ProduceRequest {
    ProduceRequest {
        transactional_id: transactional_id.map(Into::into),
        ..batch_request(batch, SinglePartitionProduceSetup {
            topic: (TOPIC).into(),
            topic_id,
            ..SinglePartitionProduceSetup::replicated_with_thirty_second_timeout()
        })
    }
}

/// Send the idempotent `batch` to the leader at the negotiated version, and
/// return the error code of the partition row.
async fn produce(leader: &Connection, topic_id: WireUuid, batch: RecordBatch) -> i16 {
    let response = leader
        .send(produce_request(topic_id, None, batch))
        .await
        .expect("Produce");
    response.responses[0].partition_responses[0].error_code
}

/// Send the transactional `batch` to the leader as `Produce` v11, and return
/// the error code of the partition row.
async fn produce_transactional(leader: &Connection, batch: RecordBatch) -> i16 {
    let request = produce_request(WireUuid::default(), Some(TRANSACTIONAL_ID), batch);
    let mut body = BytesMut::new();
    request
        .encode(&mut body, PRODUCE_V11)
        .expect("encode Produce v11");
    let mut answer = leader
        .raw_request(PRODUCE_API_KEY, PRODUCE_V11, body.freeze())
        .await
        .expect("Produce v11");
    let response = ProduceResponse::decode(&mut answer, PRODUCE_V11).expect("decode Produce v11");
    response.responses[0].partition_responses[0].error_code
}

/// Write the marker that ends the transaction of `producer` on the leader, and
/// return the error code of the partition row.
async fn end_transaction(
    leader: &Connection,
    producer: ProducerIdentity,
    commit: TransactionOutcome,
    transaction_version: i8,
    coordinator_epoch: i32,
) -> i16 {
    let response = leader
        .send(WriteTxnMarkersRequest {
            markers: vec![
                transactions::transaction_marker()
                    .producer(producer)
                    .outcome(commit)
                    .coordinator_epoch(coordinator_epoch)
                    .transaction_version(transaction_version)
                    .topics(vec![transactions::marker_topic(TOPIC.into(), vec![0])])
                    .call(),
            ],
            ..Default::default()
        })
        .await
        .expect("WriteTxnMarkers");
    response.markers[0].topics[0].partitions[0].error_code
}

async fn describe_producers(broker: &BrokerHandle) -> DescribeProducersResponse {
    connect(broker.listen_addr())
        .await
        .send(DescribeProducersRequest {
            topics: vec![TopicRequest {
                name: TOPIC.into(),
                partition_indexes: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DescribeProducers")
}

fn producer_row(
    producer: ProducerIdentity,
    last_sequence: i32,
    last_timestamp: i64,
    TransactionFields {
        coordinator_epoch,
        current_txn_start_offset,
    }: TransactionFields,
) -> ProducerState {
    ProducerState {
        producer_id: producer.id.0,
        producer_epoch: i32::from(producer.epoch.0),
        last_sequence,
        last_timestamp,
        coordinator_epoch,
        current_txn_start_offset,
        ..Default::default()
    }
}

/// The `last_timestamp` that `answer` reports for `producer_id`, or `-1`
/// when it has no row for that producer. After a marker it is the broker
/// clock at that marker, which a test cannot know in advance.
fn reported_timestamp(answer: &DescribeProducersResponse, producer_id: i64) -> i64 {
    answer.topics[0].partitions[0]
        .active_producers
        .iter()
        .find(|row| row.producer_id == producer_id)
        .map_or(-1, |row| row.last_timestamp)
}

#[derive(Default)]
struct ProducerReplicaScenario {
    cluster: Cluster,
    guard: Option<MutexGuard<'static, ()>>,
    leader: Option<Connection>,
    topic: Option<WireUuid>,
    codes: Vec<i16>,
    marker_before: Option<i64>,
    marker_after: Option<i64>,
    answers: Vec<DescribeProducersResponse>,
}

#[fixture]
fn producer_replicas() -> ProducerReplicaScenario {
    // Compare the actual log-derived producer rows on leader and both followers.
    ProducerReplicaScenario::default()
}

impl ProducerReplicaScenario {
    fn leader(&self) -> &Connection { self.leader.as_ref().expect("leader connection") }
}

#[derive(DataTableRow)]
struct ProducerAppend {
    id: i64,
    epoch: i16,
    sequence: i32,
    records: i32,
    timestamp: i64,
    transaction: String,
    marker: String,
    marker_epoch_delta: i16,
    transaction_version: i8,
    coordinator_epoch: i32,
}

#[derive(DataTableRow)]
struct ProducerExpectation {
    id: i64,
    epoch: i16,
    sequence: i32,
    timestamp: String,
    coordinator_epoch: i32,
    transaction_start: i64,
}

#[given("three replicas accept transactional Produce v11 without partition verification")]
async fn producer_state_replicas(producer_replicas: &mut ProducerReplicaScenario) {
    producer_replicas.guard = Some(cluster_lock().lock().await);
    producer_replicas.cluster = start_cluster().await;
    support::wait_for_all_brokers_registered(&producer_replicas.cluster, 3).await;
    let admin = connect_with_context(
        producer_replicas.cluster[0].1.listen_addr.to_string(),
        None,
        "admin client",
    )
    .await;
    producer_replicas.topic = Some(
        support::client::create_topic_spec(
            &admin,
            support::topic_on(TOPIC, &[&[1, 2, 3]]),
            CreateTopicRequestSetup::default(),
        )
        .await,
    );
    support::partitions::wait_for_partition_on_all(
        producer_replicas
            .cluster
            .iter()
            .map(|(broker, _, _)| broker),
        TOPIC,
        0,
    )
    .await;
    producer_replicas.cluster[0]
        .0
        .wait_until_local_partition_leader(TOPIC, 0, producer_replicas.cluster[0].1.node_id)
        .await;
    producer_replicas.leader = Some(connect(producer_replicas.cluster[0].0.listen_addr()).await);
}

#[when("the leader appends these producer batches and transaction markers in order")]
async fn append_replica_producers(
    producer_replicas: &mut ProducerReplicaScenario,
    #[datatable] rows: Rows<ProducerAppend>,
) {
    for row in rows {
        let batch = batch()
            .producer(batch_producer(producer(row.id, row.epoch)))
            .base_sequence(row.sequence)
            .records(row.records)
            .max_timestamp(row.timestamp)
            .transaction(if row.transaction == "transactional" {
                BatchTransactionMode::Transactional
            } else {
                BatchTransactionMode::Ordinary
            })
            .call();
        let code = if row.transaction == "transactional" {
            produce_transactional(producer_replicas.leader(), batch).await
        } else {
            produce(
                producer_replicas.leader(),
                producer_replicas.topic.expect("topic identity"),
                batch,
            )
            .await
        };
        producer_replicas.codes.push(code);
        if row.marker != "none" {
            if producer_replicas.marker_before.is_none() {
                producer_replicas.marker_before = Some(now_ms());
            }
            let outcome = match row.marker.as_str() {
                "commit" => TransactionOutcome::Commit,
                "abort" => TransactionOutcome::Abort,
                _ => panic!("unknown marker {}", row.marker),
            };
            let code = end_transaction(
                producer_replicas.leader(),
                producer(row.id, row.epoch + row.marker_epoch_delta),
                outcome,
                row.transaction_version,
                row.coordinator_epoch,
            )
            .await;
            producer_replicas.codes.push(code);
            producer_replicas.marker_after = Some(now_ms());
        }
    }
}

#[then("all seven producer append and marker operations succeed")]
fn producer_appends_replicated(producer_replicas: &ProducerReplicaScenario) {
    assert!(producer_replicas.codes == vec![codes::NONE; 7]);
}

#[when("every replica reaches log end twelve and answers DescribeProducers")]
async fn describe_replica_producers(producer_replicas: &mut ProducerReplicaScenario) {
    for (broker, _, _) in &producer_replicas.cluster {
        broker.wait_until_local_log_end_offset(TOPIC, 0, 12).await;
    }
    for (broker, _, _) in &producer_replicas.cluster {
        producer_replicas
            .answers
            .push(describe_producers(broker).await);
    }
}

#[then("all three complete DescribeProducers responses contain these ordered rows")]
fn equivalent_replica_producers(
    producer_replicas: &ProducerReplicaScenario,
    #[datatable] rows: Rows<ProducerExpectation>,
) {
    assert!(producer_replicas.answers.len() == 3);
    let rows: Vec<_> = rows.into_iter().collect();
    let committed = rows
        .iter()
        .find(|row| row.timestamp == "commit_marker_time")
        .expect("committed producer expectation")
        .id;
    let aborted = rows
        .iter()
        .find(|row| row.timestamp == "abort_marker_time")
        .expect("aborted producer expectation")
        .id;
    let committed_at = reported_timestamp(&producer_replicas.answers[0], committed);
    let aborted_at = reported_timestamp(&producer_replicas.answers[0], aborted);
    let expected = DescribeProducersResponse {
        throttle_time_ms: 0,
        topics: vec![TopicResponse {
            name: TOPIC.into(),
            partitions: vec![PartitionResponse {
                partition_index: 0,
                error_code: codes::NONE,
                error_message: None,
                active_producers: rows
                    .into_iter()
                    .map(|row| {
                        let timestamp = match row.timestamp.as_str() {
                            "commit_marker_time" => committed_at,
                            "abort_marker_time" => aborted_at,
                            value => value.parse().expect("producer timestamp"),
                        };
                        producer_row(
                            producer(row.id, row.epoch),
                            row.sequence,
                            timestamp,
                            TransactionFields {
                                coordinator_epoch: row.coordinator_epoch,
                                current_txn_start_offset: row.transaction_start,
                            },
                        )
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let window = producer_replicas
        .marker_before
        .expect("first marker instant")
        ..=producer_replicas.marker_after.expect("last marker instant");
    assert!(
        (
            &producer_replicas.answers,
            window.contains(&committed_at),
            window.contains(&aborted_at)
        ) == (&vec![expected; 3], true, true)
    );
}

#[scenario(
    path = "tests/features/migrated/tests/replication/describe_producers_replicas.feature",
    name = "every replica describes the producers of its log"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_replica_describes_the_producers_of_its_log(
    producer_replicas: ProducerReplicaScenario,
) {
    shutdown_cluster(take(&mut producer_replicas.cluster)).await;
    drop(producer_replicas);
}
