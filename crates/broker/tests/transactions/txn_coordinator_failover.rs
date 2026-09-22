//! The transaction coordinator moves with the leadership of
//! `__transaction_state`.
//!
//! Kafka's `TransactionCoordinator.onElection` loads a state partition on the
//! broker that becomes its leader, so a transaction that the old leader opened
//! commits through the new one. The test opens a transaction on the
//! coordinator of a three-broker cluster, stops that broker, commits the
//! transaction through the broker that is elected next, and reads the records
//! with `read_committed`.

use std::time::{Duration, Instant};

use assert2::assert;
use bytes::Bytes;
use krabka_broker::{BrokerConfig, BrokerHandle, NodeId};
use krabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction},
        common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        end_txn_request::EndTxnRequest,
        end_txn_response::EndTxnResponse,
        find_coordinator_request::FindCoordinatorRequest,
        init_producer_id_request::InitProducerIdRequest,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    records::{Attributes, Record, RecordBatch},
};
use tempfile::TempDir;

use crate::support;

const TID: &str = "txn-coordinator-failover";
const TOPIC: &str = "txn-coordinator-failover";
const STATE_TOPIC: &str = "__transaction_state";

const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const NOT_COORDINATOR: i16 = 16;
const CONCURRENT_TRANSACTIONS: i16 = 51;

/// The time a retried request may take to see the cluster settle.
const SETTLE: Duration = Duration::from_secs(60);

type Cluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;

fn retriable(code: i16) -> bool {
    matches!(
        code,
        COORDINATOR_LOAD_IN_PROGRESS | COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR
    )
}

async fn client(address: &str) -> Client {
    Client::builder()
        .bootstrap(address)
        .client_id("txn-coordinator-failover")
        .build()
        .await
        .expect("client")
}

fn address_of(cluster: &Cluster, node: u64) -> String {
    cluster
        .iter()
        .find(|(handle, _, _)| handle.node_id() == node)
        .map(|(handle, _, _)| handle.listen_addr().to_string())
        .expect("a broker of the cluster")
}

/// The leader of `topic-0` in the image of `handle`.
async fn leader_of(handle: &BrokerHandle, topic: &str) -> u64 {
    let deadline = Instant::now() + SETTLE;
    loop {
        if let Some(leader) = handle.partition_leader_for_test(topic, 0) {
            return leader;
        }
        assert!(Instant::now() < deadline, "{topic}-0 has no leader");
        // intentional: the image watch has no awaiter for "any leader".
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn create_topic(client: &Client) {
    let created = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: TOPIC.into(),
                num_partitions: 1,
                replication_factor: 3,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        created.topics[0].error_code == 0,
        "CreateTopics: {created:?}"
    );
}

/// Create `__transaction_state` with `FindCoordinator`.
async fn find_coordinator(client: &Client) {
    let deadline = Instant::now() + SETTLE;
    loop {
        let found = client
            .send(FindCoordinatorRequest {
                key: TID.into(),
                key_type: 1,
                coordinator_keys: vec![TID.into()],
                ..Default::default()
            })
            .await
            .expect("FindCoordinator");
        if found
            .coordinators
            .first()
            .is_some_and(|row| row.error_code == 0)
        {
            return;
        }
        assert!(Instant::now() < deadline, "FindCoordinator: {found:?}");
        // intentional: the topic creation has no awaiter from this client.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn init_producer(client: &Client) -> (i64, i16) {
    let deadline = Instant::now() + SETTLE;
    loop {
        let response = client
            .send(InitProducerIdRequest {
                transactional_id: Some(TID.into()),
                transaction_timeout_ms: 60_000,
                producer_id: -1,
                producer_epoch: -1,
                ..Default::default()
            })
            .await
            .expect("InitProducerId");
        if retriable(response.error_code) && Instant::now() < deadline {
            // intentional: the coordinator load has no awaiter; the answer is the signal.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        assert!(response.error_code == 0, "InitProducerId: {response:?}");
        return (response.producer_id, response.producer_epoch);
    }
}

async fn add_partition(client: &Client, (producer_id, epoch): (i64, i16)) {
    let topic = AddPartitionsToTxnTopic {
        name: TOPIC.into(),
        partitions: vec![0],
        ..Default::default()
    };
    let response = client
        .send(AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: TID.into(),
                producer_id,
                producer_epoch: epoch,
                topics: vec![topic.clone()],
                ..Default::default()
            }],
            v3_and_below_transactional_id: TID.into(),
            v3_and_below_producer_id: producer_id,
            v3_and_below_producer_epoch: epoch,
            v3_and_below_topics: vec![topic],
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

async fn produce(client: &Client, producer: Option<(i64, i16)>, values: &[&'static str]) {
    let topic_id = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(TOPIC.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata")
        .topics
        .iter()
        .find(|row| row.name.as_deref() == Some(TOPIC))
        .map(|row| row.topic_id)
        .expect("topic in metadata");
    let records = i32::try_from(values.len()).expect("record count");
    let batch = RecordBatch {
        attributes: Attributes::default().with_transactional(producer.is_some()),
        producer_id: producer.map_or(-1, |(producer_id, _)| producer_id),
        producer_epoch: producer.map_or(-1, |(_, epoch)| epoch),
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
    let response = client
        .send(ProduceRequest {
            // KIP-890: the partition leader asks the coordinator about a
            // transactional batch, and it needs the transactional id.
            transactional_id: producer.map(|_| TID.to_string()),
            acks: -1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: TOPIC.into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(batch.into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce");
    let code = response.responses[0].partition_responses[0].error_code;
    assert!(code == 0, "Produce: {response:?}");
}

async fn end_txn(client: &Client, (producer_id, epoch): (i64, i16)) -> EndTxnResponse {
    let deadline = Instant::now() + SETTLE;
    loop {
        let response = client
            .send(EndTxnRequest {
                transactional_id: TID.into(),
                producer_id,
                producer_epoch: epoch,
                committed: true,
                ..Default::default()
            })
            .await
            .expect("EndTxn");
        let again =
            retriable(response.error_code) || response.error_code == CONCURRENT_TRANSACTIONS;
        if again && Instant::now() < deadline {
            // intentional: the coordinator load has no awaiter; the answer is the signal.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        return response;
    }
}

async fn read_committed(bootstrap: &str, last: &str) -> Vec<String> {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.to_string())
        .group_id(format!("{TOPIC}-reader"))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe([TOPIC.to_string()])
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_commits_through_the_next_coordinator() {
    let mut cluster = support::start_n_node_with(3, |_, config| {
        config.transaction_state_num_partitions = 1;
        config.transaction_state_replication_factor = 3;
    })
    .await
    .expect("start the cluster");
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    let admin = client(&cluster[0].0.listen_addr().to_string()).await;
    create_topic(&admin).await;
    find_coordinator(&admin).await;
    let coordinator = leader_of(&cluster[0].0, STATE_TOPIC).await;
    cluster[0].0.wait_until_isr_len(STATE_TOPIC, 0, 3).await;
    cluster[0].0.wait_until_isr_len(TOPIC, 0, 3).await;

    // Open the transaction on the coordinator.
    let first = client(&address_of(&cluster, coordinator)).await;
    let producer = init_producer(&first).await;
    add_partition(&first, producer).await;
    let data_leader = client(&address_of(&cluster, leader_of(&cluster[0].0, TOPIC).await)).await;
    produce(&data_leader, Some(producer), &["a", "b", "c"]).await;
    first.close();
    data_leader.close();
    admin.close();

    // Stop the coordinator. A surviving broker leads the state partition next.
    let position = cluster
        .iter()
        .position(|(handle, _, _)| handle.node_id() == coordinator)
        .expect("the coordinator is a cluster member");
    let (stopped, _, stopped_dir) = cluster.remove(position);
    stopped.shutdown().await;
    cluster[0]
        .0
        .wait_until_partition_leader_changed(STATE_TOPIC, 0, NodeId(coordinator))
        .await;
    cluster[0]
        .0
        .wait_until_partition_leader_changed(TOPIC, 0, NodeId(coordinator))
        .await;
    let next = leader_of(&cluster[0].0, STATE_TOPIC).await;

    // Commit through the next coordinator. Transaction version 2 bumps the
    // epoch on completion.
    let second = client(&address_of(&cluster, next)).await;
    let committed = end_txn(&second, producer).await;
    assert!(
        committed
            == EndTxnResponse {
                producer_id: producer.0,
                producer_epoch: producer.1 + 1,
                ..Default::default()
            },
        "EndTxn through the next coordinator"
    );
    second.close();

    let data_leader = client(&address_of(&cluster, leader_of(&cluster[0].0, TOPIC).await)).await;
    produce(&data_leader, None, &["z"]).await;
    data_leader.close();
    let bootstrap = cluster[0].0.listen_addr().to_string();
    let seen = read_committed(&bootstrap, "z").await;
    assert!(seen == ["a", "b", "c", "z"]);

    for (handle, _, _) in cluster {
        handle.shutdown().await;
    }
    drop(stopped_dir);
}
