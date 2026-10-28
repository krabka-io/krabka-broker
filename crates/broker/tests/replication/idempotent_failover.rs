//! Idempotent-producer state across a leadership change.
//!
//! A Kafka follower appends every replicated batch through
//! `UnifiedLog.appendAsFollower`, and that append updates the producer state
//! exactly as a leader append does. A follower that becomes leader therefore
//! holds the last five batches of each producer up to its log end.
//! `ProducerStateEntry.findDuplicateBatch` finds a retry among them, and
//! `UnifiedLog.append` answers the retry with the offsets of the first append.
//!
//! The `ReplicationTest.test_replication_with_broker_failure` system test
//! (`clean_bounce`, `broker_type=leader`, `enable_idempotence=true`) found two
//! symptoms of one fault. The new leader appended a retry of a batch it had
//! already replicated, so the log held the batch twice. A broker that became
//! leader again refused the next batch with `OUT_OF_ORDER_SEQUENCE_NUMBER`,
//! because its producer state still held the sequence of its previous term.

use {
    crate::{
        cluster_lock,
        support::{
            self,
            client::connect_with_context,
            produce::{SinglePartitionProduceSetup, send_batch},
            records::{batch_from_records, value_record},
            registered_cluster, shutdown_cluster,
            topics::create_topic_request,
        },
    },
    assert2::assert,
    bytes::Bytes,
    krabka_broker::{BrokerConfig, BrokerHandle, codes},
    krabka_protocol::{
        owned::produce_response::{LeaderIdAndEpoch, PartitionProduceResponse},
        primitives::uuid::Uuid as WireUuid,
        records::RecordBatch,
    },
    std::time::Duration,
    tempfile::TempDir,
};

type Cluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;

struct ProducerBatchSpec {
    id: i64,
    epoch: i16,
    records: i32,
    timestamp: i64,
}

/// Construct the exact producer batch specified by the scenario.
fn idempotent_batch(spec: &ProducerBatchSpec, base_sequence: i32) -> RecordBatch {
    RecordBatch {
        last_offset_delta: spec.records - 1,
        base_timestamp: spec.timestamp,
        max_timestamp: spec.timestamp,
        producer_id: spec.id,
        producer_epoch: spec.epoch,
        base_sequence,
        ..batch_from_records(
            (0..spec.records)
                .map(|offset_delta| {
                    value_record(
                        offset_delta,
                        Some(Bytes::from(format!("seq-{}", base_sequence + offset_delta))),
                    )
                })
                .collect(),
        )
    }
}

/// The row of a batch that the leader appended at `base_offset`.
fn appended(base_offset: i64) -> PartitionProduceResponse {
    PartitionProduceResponse {
        index: 0,
        error_code: codes::NONE,
        base_offset,
        log_append_time_ms: -1,
        log_start_offset: 0,
        record_errors: vec![],
        error_message: None,
        current_leader: LeaderIdAndEpoch::default(),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    }
}

/// Create `topic` with one partition on `replicas`, the first one leading, and
/// wait until every broker of `cluster` has it.
async fn create_topic(cluster: &Cluster, topic: &str, replicas: &[i32]) -> WireUuid {
    let admin =
        connect_with_context(cluster[0].1.listen_addr.to_string(), None, "admin client").await;
    let response = admin
        .send(create_topic_request(support::topic_on(topic, &[replicas])))
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == codes::NONE);
    for (handle, _, _) in cluster {
        handle.wait_until_partition_present(topic, 0).await;
    }
    response.topics[0].topic_id
}

/// Send `batch` to partition 0 of `topic` with `acks=all`, and return the whole
/// partition row.
async fn produce(
    broker: &BrokerHandle,
    topic: &str,
    topic_id: WireUuid,
    batch: RecordBatch,
) -> PartitionProduceResponse {
    let client =
        connect_with_context(broker.listen_addr().to_string(), None, "producer client").await;
    let response = send_batch(&client, batch, SinglePartitionProduceSetup {
        topic: topic.to_owned(),
        topic_id,
        ..SinglePartitionProduceSetup::replicated_with_thirty_second_timeout()
    })
    .await;
    response.responses[0].partition_responses[0].clone()
}

/// Make broker `to` the leader of partition 0 of `topic` at the next leader
/// epoch, as an election does, and wait until it accepts produces.
async fn elect(cluster: &Cluster, topic: &str, to: u64) {
    let mut record = cluster[0]
        .0
        .partition_record_for_test(topic, 0)
        .expect("partition record");
    record.leader = krabka_metadata::NodeId(to);
    record.leader_epoch = record.leader_epoch.next();
    record.partition_epoch += 1;
    cluster[0]
        .0
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(record))
        .await
        .expect("elect a new leader");
    broker(cluster, to)
        .wait_until_local_partition_leader(topic, 0, krabka_raft::NodeId(to))
        .await;
}

/// The handle of broker `node_id`.
fn broker(cluster: &Cluster, node_id: u64) -> &BrokerHandle {
    &cluster
        .iter()
        .find(|(_, config, _)| config.node_id.0 == node_id)
        .expect("broker in cluster")
        .0
}

/// The base offset of every batch in the local log of each broker of
/// `cluster`, once each log ends at `log_end_offset`.
async fn batch_base_offsets(
    cluster: &Cluster,
    topic: &str,
    log_end_offset: i64,
) -> Vec<Option<Vec<i64>>> {
    let mut logs = Vec::new();
    for (handle, _, _) in cluster {
        handle
            .wait_until_local_log_end_offset(topic, 0, log_end_offset)
            .await;
        logs.push(handle.local_batch_base_offsets_for_test(topic, 0));
    }
    logs
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::mem::take,
    tokio::sync::MutexGuard,
};

#[derive(Default)]
struct IdempotentReplication {
    cluster: Cluster,
    guard: Option<MutexGuard<'static, ()>>,
    retired_directory: Option<TempDir>,
    topic: String,
    topic_id: Option<WireUuid>,
    leader: u64,
    batch: Option<ProducerBatchSpec>,
    response: Option<PartitionProduceResponse>,
    logs: Option<Vec<Option<Vec<i64>>>>,
}

#[fixture]
fn idempotent_replication() -> IdempotentReplication {
    // Retain the producer batch definition through the real leadership changes.
    IdempotentReplication::default()
}

#[given("three registered brokers replicate an idempotent producer's log")]
async fn idempotent_replica_cluster(idempotent_replication: &mut IdempotentReplication) {
    idempotent_replication.guard = Some(cluster_lock().lock().await);
    idempotent_replication.cluster = registered_cluster(3).await;
}

#[given(
    "producer {id} uses epoch {epoch} with {records} records per batch at timestamp {timestamp}"
)]
fn idempotent_batch_spec(
    idempotent_replication: &mut IdempotentReplication,
    id: i64,
    epoch: i16,
    records: i32,
    timestamp: i64,
) {
    idempotent_replication.batch = Some(ProducerBatchSpec {
        id,
        epoch,
        records,
        timestamp,
    });
}

impl IdempotentReplication {
    async fn install_topic(&mut self, topic: String, replicas: Vec<i32>) {
        self.leader = u64::try_from(replicas[0]).expect("leader node ID");
        self.topic_id = Some(create_topic(&self.cluster, &topic, &replicas).await);
        broker(&self.cluster, self.leader)
            .wait_until_local_partition_leader(&topic, 0, krabka_raft::NodeId(self.leader))
            .await;
        self.topic = topic;
    }

    async fn shutdown(&mut self) {
        shutdown_cluster(take(&mut self.cluster)).await;
        self.retired_directory.take();
        self.guard.take();
    }
}

#[given("topic {topic} pins broker one as leader with replicas one, two, and three")]
async fn initially_led_idempotent_topic(
    idempotent_replication: &mut IdempotentReplication,
    topic: String,
) {
    idempotent_replication
        .install_topic(topic, vec![1, 2, 3])
        .await;
}

#[given("topic {topic} chooses a data leader other than the active controller")]
async fn independently_led_idempotent_topic(
    idempotent_replication: &mut IdempotentReplication,
    topic: String,
) {
    let controller = idempotent_replication.cluster[0]
        .0
        .wait_until_controller_leader()
        .await
        .unwrap();
    let controller_index = idempotent_replication
        .cluster
        .iter()
        .position(|(_, config, _)| config.node_id == controller)
        .expect("controller member");
    let leader_index = (controller_index + 1) % idempotent_replication.cluster.len();
    let leader =
        i32::try_from(idempotent_replication.cluster[leader_index].1.node_id.0).expect("node ID");
    let mut replicas = vec![leader];
    replicas.extend(
        idempotent_replication
            .cluster
            .iter()
            .map(|(_, config, _)| i32::try_from(config.node_id.0).expect("node ID"))
            .filter(|node| *node != leader),
    );
    idempotent_replication.install_topic(topic, replicas).await;
}

#[when("the current leader receives the idempotent batch beginning at sequence {sequence}")]
async fn produce_idempotent_replica_batch(
    idempotent_replication: &mut IdempotentReplication,
    sequence: i32,
) {
    idempotent_replication.response = Some(
        produce(
            broker(
                &idempotent_replication.cluster,
                idempotent_replication.leader,
            ),
            &idempotent_replication.topic,
            idempotent_replication.topic_id.expect("topic identity"),
            idempotent_batch(
                idempotent_replication
                    .batch
                    .as_ref()
                    .expect("producer batch specification"),
                sequence,
            ),
        )
        .await,
    );
}

#[then(
    "the complete idempotent Produce response has base offset {base} and append timestamp {timestamp}"
)]
fn idempotent_replica_response(
    idempotent_replication: &IdempotentReplication,
    base: i64,
    timestamp: i64,
) {
    let expected = PartitionProduceResponse {
        log_append_time_ms: timestamp,
        ..appended(base)
    };
    assert!(
        idempotent_replication
            .response
            .as_ref()
            .expect("Produce answered")
            == &expected
    );
}

#[then("all idempotent replicas reach log end {offset}")]
async fn idempotent_replicas_caught_up(
    idempotent_replication: &IdempotentReplication,
    offset: i64,
) {
    batch_base_offsets(
        &idempotent_replication.cluster,
        &idempotent_replication.topic,
        offset,
    )
    .await;
}

#[when("the idempotent data leader completes controlled shutdown within thirty seconds")]
async fn stop_idempotent_replica_leader(idempotent_replication: &mut IdempotentReplication) {
    let index = idempotent_replication
        .cluster
        .iter()
        .position(|(_, config, _)| config.node_id.0 == idempotent_replication.leader)
        .expect("leader member");
    let (stopped, _, directory) = idempotent_replication.cluster.remove(index);
    idempotent_replication.retired_directory = Some(directory);
    stopped
        .controlled_shutdown(Duration::from_secs(30))
        .await
        .expect("controlled shutdown drains leader");
    let stopped_id = krabka_metadata::NodeId(idempotent_replication.leader);
    idempotent_replication.cluster[0]
        .0
        .wait_for_image(|image| {
            image
                .partition(&idempotent_replication.topic, 0)
                .is_some_and(|record| record.leader != stopped_id)
        })
        .await;
    idempotent_replication.leader = idempotent_replication.cluster[0]
        .0
        .partition_leader_for_test(&idempotent_replication.topic, 0)
        .expect("new leader");
    broker(
        &idempotent_replication.cluster,
        idempotent_replication.leader,
    )
    .wait_until_local_partition_leader(
        &idempotent_replication.topic,
        0,
        krabka_raft::NodeId(idempotent_replication.leader),
    )
    .await;
}

#[when("broker {leader} is elected as idempotent partition leader at the next epoch")]
async fn elect_idempotent_replica_leader(
    idempotent_replication: &mut IdempotentReplication,
    leader: u64,
) {
    elect(
        &idempotent_replication.cluster,
        &idempotent_replication.topic,
        leader,
    )
    .await;
    idempotent_replication.leader = leader;
}

#[when("the local idempotent batch bases are read after log end nine")]
async fn read_idempotent_replica_logs(idempotent_replication: &mut IdempotentReplication) {
    idempotent_replication.logs = Some(
        batch_base_offsets(
            &idempotent_replication.cluster,
            &idempotent_replication.topic,
            9,
        )
        .await,
    );
}

#[then("exactly {replicas} local logs contain batch bases zero, three, and six")]
fn idempotent_replica_logs(idempotent_replication: &IdempotentReplication, replicas: usize) {
    assert!(
        idempotent_replication
            .logs
            .as_ref()
            .expect("replica logs read")
            == &vec![Some(vec![0, 3, 6]); replicas]
    );
}

#[scenario(
    path = "tests/features/migrated/tests/replication/idempotent_failover.feature",
    name = "a leader elected by a controlled shutdown answers a replicated retry as a duplicate"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_elected_by_a_controlled_shutdown_answers_a_replicated_retry_as_a_duplicate(
    idempotent_replication: IdempotentReplication,
) {
    idempotent_replication.shutdown().await;
    drop(idempotent_replication);
}

#[scenario(
    path = "tests/features/migrated/tests/replication/idempotent_failover.feature",
    name = "a leader elected again continues from the batches it replicated"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_elected_again_continues_from_the_batches_it_replicated(
    idempotent_replication: IdempotentReplication,
) {
    idempotent_replication.shutdown().await;
    drop(idempotent_replication);
}
