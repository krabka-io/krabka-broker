//! Log compaction on every replica of a partition.
//!
//! Kafka's cleaner keeps the batch that holds the last sequence of each active
//! producer, as an empty batch when compaction removes all of its records
//! (`Cleaner.cleanInto`, `isBatchLastRecordOfProducer`). It reads the active
//! producers from the producer state of the log
//! (`UnifiedLog.lastRecordsOfActiveProducers`), which a follower updates for
//! each batch that it replicates. A follower that compacts its log therefore
//! keeps the same batches as its leader.
//!
//! The produce path of this broker keeps a second copy of the producer state,
//! and a follower does not add its replicated data batches to that copy. A
//! cleaner that read that copy removed the last batch of an idempotent
//! producer on each follower. A new leader then had no batch that held the
//! sequence of the producer.

use {
    crate::{
        Cluster, cluster_lock,
        support::{
            self,
            client::connect_with_context,
            produce::{SinglePartitionProduceSetup, send_batch},
            records::batch_from_records,
            registered_cluster, shutdown_cluster,
            topics::CreateTopicRequestSetup,
            transactions::{ProducerEpoch, ProducerIdentity},
        },
    },
    assert2::assert,
    bytes::Bytes,
    krabka_broker::{BrokerHandle, codes},
    krabka_client_core::{Connection, ConnectionOptions},
    krabka_ids::ProducerId,
    krabka_protocol::{
        owned::create_topics_request::CreatableTopicConfig,
        primitives::uuid::Uuid as WireUuid,
        records::{Record, RecordBatch},
    },
    rstest::fixture,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::{DataTableRow, given, scenario, then, when},
    std::{mem::take, net::SocketAddr},
    tokio::sync::MutexGuard,
};

const TOPIC: &str = "compaction-replicas";

/// The `internal.segment.bytes` of the topic. Each batch of this test is 72
/// bytes, so a segment holds one batch and not two. Kafka refuses a batch that
/// is larger than a segment with `RECORD_LIST_TOO_LARGE`.
const SEGMENT_BYTES: u32 = 100;

/// A producer identity and the sequence its batch starts at.
#[derive(Clone, Copy)]
struct SequencedProducer {
    identity: ProducerIdentity,
    base_sequence: i32,
}

/// What a batch from a client with no idempotence carries.
const NO_PRODUCER: SequencedProducer = SequencedProducer {
    identity: ProducerIdentity {
        id: ProducerId(-1),
        epoch: ProducerEpoch(-1),
    },
    base_sequence: -1,
};

// One batch of a local log after compaction. The compared fields are the
// ones that carry the state of a producer, and the records.
krabka_macros::compacted_batch!(Kept, plain);

use crate::support::records::now_ms;

/// One connection to the broker that binds `address`, so that a request
/// reaches that broker and no other.
async fn connect(address: SocketAddr) -> Connection {
    Connection::connect(address, ConnectionOptions {
        client_id: "compaction-replicas".to_owned(),
        ..ConnectionOptions::default()
    })
    .await
    .expect("connect")
}

/// A one-record batch of `key` and `value`. `producer` is the identity and
/// base sequence, or `None` for a client with no idempotence.
fn record(
    producer: Option<SequencedProducer>,
    key: &str,
    value: &str,
    timestamp: i64,
) -> RecordBatch {
    let SequencedProducer {
        identity,
        base_sequence,
    } = producer.unwrap_or(NO_PRODUCER);
    RecordBatch {
        base_timestamp: timestamp,
        max_timestamp: timestamp,
        producer_id: identity.id.0,
        producer_epoch: identity.epoch.0,
        base_sequence,
        ..batch_from_records(vec![Record {
            key: Some(Bytes::copy_from_slice(key.as_bytes())),
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            ..Record::default()
        }])
    }
}

/// Send `batch` to the leader with `acks=-1`, and return the error code of
/// the partition row.
async fn produce(leader: &Connection, topic_id: WireUuid, batch: RecordBatch) -> i16 {
    let response = send_batch(&leader, batch, SinglePartitionProduceSetup {
        topic: (TOPIC).into(),
        topic_id,
        ..SinglePartitionProduceSetup::replicated_with_thirty_second_timeout()
    })
    .await;
    response.responses[0].partition_responses[0].error_code
}

/// Wait until the topic configuration of [`TOPIC`] is in the log of the
/// partition on `broker`. A batch that arrives before it goes into one large
/// segment, which no compaction pass rewrites.
async fn wait_for_the_topic_config(broker: &BrokerHandle) {
    broker
        .wait_for_metrics("the topic config reaches the partition log", |_| {
            broker
                .partition_log_config_for_test(TOPIC, 0)
                .is_some_and(|config| {
                    config.cleanup_policy == krabka_log::CleanupPolicy::Compact
                        && config.segment_size == krabka_units::bytes(SEGMENT_BYTES)
                })
        })
        .await;
}

#[derive(Default)]
struct ReplicaCompaction {
    cluster: Cluster,
    guard: Option<MutexGuard<'static, ()>>,
    leader: Option<Connection>,
    topic: Option<WireUuid>,
    errors: Vec<i16>,
    kept: Vec<Vec<Kept>>,
}

#[fixture]
fn replica_compaction() -> ReplicaCompaction {
    // Observe each replica's actual compacted batches without sharing producer-state memory.
    ReplicaCompaction::default()
}

#[derive(DataTableRow)]
struct CompactionInput {
    id: i64,
    epoch: i16,
    sequence: i32,
    key: String,
    value: String,
}

#[derive(DataTableRow)]
struct CompactionExpectation {
    base: i64,
    last: i64,
    id: i64,
    epoch: i16,
    sequence: i32,
    records: String,
    key: String,
    value: String,
}

#[given("three compacted replicas use 100-byte segments so each record seals its own batch")]
async fn compacted_replica_logs(replica_compaction: &mut ReplicaCompaction) {
    replica_compaction.guard = Some(cluster_lock().lock().await);
    replica_compaction.cluster = registered_cluster(3).await;
    let admin = connect_with_context(
        replica_compaction.cluster[0].1.listen_addr.to_string(),
        None,
        "admin client",
    )
    .await;
    let mut topic = support::topic_on(TOPIC, &[&[1, 2, 3]]);
    topic.configs = [
        ("cleanup.policy", "compact".to_owned()),
        ("internal.segment.bytes", SEGMENT_BYTES.to_string()),
    ]
    .into_iter()
    .map(|(name, value)| CreatableTopicConfig {
        name: name.into(),
        value: Some(value),
        ..Default::default()
    })
    .collect();
    replica_compaction.topic = Some(
        support::client::create_topic_spec(&admin, topic, CreateTopicRequestSetup::default()).await,
    );
    support::partitions::wait_for_partition_on_all(
        replica_compaction
            .cluster
            .iter()
            .map(|(broker, _, _)| broker),
        TOPIC,
        0,
    )
    .await;
    for (broker, _, _) in &replica_compaction.cluster {
        wait_for_the_topic_config(broker).await;
    }
    replica_compaction.cluster[0]
        .0
        .wait_until_local_partition_leader(TOPIC, 0, replica_compaction.cluster[0].1.node_id)
        .await;
    replica_compaction.leader = Some(connect(replica_compaction.cluster[0].0.listen_addr()).await);
}

#[when("the leader appends these keyed batches at the same current timestamp")]
async fn append_compaction_records(
    replica_compaction: &mut ReplicaCompaction,
    #[datatable] rows: Rows<CompactionInput>,
) {
    let timestamp = now_ms();
    for row in rows {
        let identity = (row.id >= 0).then_some(SequencedProducer {
            identity: ProducerIdentity {
                id: ProducerId(row.id),
                epoch: ProducerEpoch(row.epoch),
            },
            base_sequence: row.sequence,
        });
        replica_compaction.errors.push(
            produce(
                replica_compaction
                    .leader
                    .as_ref()
                    .expect("leader connection"),
                replica_compaction.topic.expect("topic identity"),
                record(identity, &row.key, &row.value, timestamp),
            )
            .await,
        );
    }
}

#[then("all five compacted-topic appends succeed")]
fn compacted_replica_appends(replica_compaction: &ReplicaCompaction) {
    assert!(replica_compaction.errors == vec![codes::NONE; 5]);
}

#[when("every replica commits through offset five and compacts its own local log")]
async fn compact_replica_logs(replica_compaction: &mut ReplicaCompaction) {
    for (broker, _, _) in &replica_compaction.cluster {
        broker.wait_until_local_log_end_offset(TOPIC, 0, 5).await;
        broker.wait_until_high_watermark(TOPIC, 0, 5).await;
    }
    for (broker, _, _) in &replica_compaction.cluster {
        broker
            .compact_local_log_for_test(TOPIC, 0)
            .await
            .expect("compact");
        let batches = broker.local_batches_for_test(TOPIC, 0).expect("local log");
        replica_compaction
            .kept
            .push(batches.iter().map(Kept::of).collect());
    }
}

#[then("every compacted replica retains exactly these ordered producer headers and records")]
fn kept_replica_batches(
    replica_compaction: &ReplicaCompaction,
    #[datatable] rows: Rows<CompactionExpectation>,
) {
    let expected: Vec<_> = rows
        .into_iter()
        .map(|row| Kept {
            base_offset: row.base,
            last_offset: row.last,
            producer_id: row.id,
            producer_epoch: row.epoch,
            base_sequence: row.sequence,
            records: match row.records.as_str() {
                "empty" => vec![],
                "one" => vec![(Some(Bytes::from(row.key)), Some(Bytes::from(row.value)))],
                _ => panic!("unknown retained record count {}", row.records),
            },
        })
        .collect();
    assert!(replica_compaction.kept == vec![expected; 3]);
}

#[scenario(
    path = "tests/features/migrated/tests/replication/compaction_replicas.feature",
    name = "every replica keeps the last batch of an active producer"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_replica_keeps_the_last_batch_of_an_active_producer(
    replica_compaction: ReplicaCompaction,
) {
    shutdown_cluster(take(&mut replica_compaction.cluster)).await;
    drop(replica_compaction);
}
