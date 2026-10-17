//! A batching idempotent producer and the krabka consumer on a three-partition
//! diskless topic, with the flusher trimming behind the load.
//!
//! The other cases produce one record per request and read with a fetch that
//! carries no `last_fetched_epoch`. Real clients do neither. A batching
//! producer writes batches of many records, so the flusher's trim frontier --
//! the high watermark less the safety lag -- lands inside a batch, and the
//! krabka consumer sends Fetch v12 and later with the epoch of the last batch
//! it read. These cases run that traffic and require three things of it:
//!
//! - a consumer that starts from the earliest offset reads every record, each
//!   partition contiguously, although most of them are below the leader's
//!   trimmed local log start and only the object store holds them;
//! - every WAL voter catches up with its leader, although a voter that fell
//!   behind the trim restarts at a log start inside a batch;
//! - a follower that restarts while the load runs rejoins every partition's
//!   ISR, although the controller reserved offsets past its log end while it
//!   was down, and the WAL quorum of a partition it led keeps committing
//!   across the hole its lost reservation leaves in the new leader's log.

use {
    crate::{
        CLIENT_PRINCIPAL, PASSWORD, TOPIC, VOTERS,
        cluster::{DisklessCluster, start_diskless_cluster, wait_for},
        support::{
            sasl_client, sasl_plain_security, topic_id_for,
            topics::{creatable_topic, create_topic_request, topic_configs},
        },
        voter_dir::{durable_range, voter_dir},
    },
    assert2::assert,
    bytes::Bytes,
    krabka_broker::NodeId,
    krabka_client_consumer::{AutoOffsetReset, Consumer},
    krabka_client_core::security::ClientSecurity,
    krabka_client_producer::{Acks, Compression, Producer, ProducerRecord},
    krabka_protocol::primitives::uuid::Uuid as WireUuid,
    std::{collections::BTreeMap, time::Duration},
    tokio_util::sync::CancellationToken,
};

/// Partitions of the topic. More than one, so the consumer's fetch session
/// carries several diskless partitions at once.
const PARTITIONS: i32 = 3;

/// Records produced per partition before the read-back.
const RECORDS_PER_PARTITION: usize = 600;

/// Records enqueued per burst. Enough for one burst to fill several batches
/// of several records each.
const BURST: usize = 60;

/// A value large enough that a 16 KiB batch holds a few dozen records, as the
/// soak's 512-byte payloads do.
const VALUE_LEN: usize = 512;

/// Boot the cluster, flushing and trimming on a tight cadence so the trim
/// frontier keeps passing the batches the load writes.
async fn trimming_cluster() -> DisklessCluster {
    let cluster = Box::pin(start_diskless_cluster(|config| {
        config.diskless_wal_flush_interval = krabka_units::millis(50);
    }))
    .await;
    cluster.await_ready().await;
    cluster
}

/// Create the topic and wait until every partition's WAL quorum is running.
async fn create_topic(cluster: &DisklessCluster) -> uuid::Uuid {
    let admin = sasl_client(
        &cluster.bootstrap_for_node(cluster.node_ids()[0]),
        CLIENT_PRINCIPAL,
        PASSWORD,
    )
    .await;
    let response = admin
        .send(create_topic_request(
            creatable_topic()
                .name(TOPIC.to_owned())
                .partitions(PARTITIONS)
                .replicas(i16::try_from(VOTERS).expect("small cluster"))
                .configs(topic_configs([("krabka.diskless", "true")]))
                .call(),
        ))
        .await
        .expect("CreateTopics");
    assert!(
        response.topics[0].error_code == 0,
        "CreateTopics rejected the diskless topic: {response:?}"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let topic_id = loop {
        let topic_id = topic_id_for(&admin, TOPIC).await;
        if topic_id != WireUuid::default() {
            break topic_id;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "the metadata image never reported a topic id for {TOPIC}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    for partition in 0..PARTITIONS {
        await_partition_quorum(cluster, partition).await;
    }
    uuid::Uuid::from_bytes(topic_id.0)
}

async fn await_partition_quorum(cluster: &DisklessCluster, partition: i32) {
    for node in cluster.node_ids() {
        if let Some(broker) = cluster.handle_for_node(node) {
            broker.wait_until_partition_present(TOPIC, partition).await;
        }
    }
    let leader = partition_leader(cluster, partition);
    let broker = cluster.handle_for_node(leader).expect("the leader is up");
    wait_for(
        &format!("the WAL quorum of partition {partition}"),
        Duration::from_mins(1),
        || async { broker.diskless_wal_ready_for_test(TOPIC, partition, leader, VOTERS) },
    )
    .await;
}

fn partition_leader(cluster: &DisklessCluster, partition: i32) -> NodeId {
    cluster
        .node_ids()
        .into_iter()
        .find_map(|node| {
            cluster
                .handle_for_node(node)?
                .partition_leader_for_test(TOPIC, partition)
        })
        .map(NodeId)
        .expect("the partition has a leader")
}

fn security() -> ClientSecurity { sasl_plain_security(CLIENT_PRINCIPAL, PASSWORD) }

/// The soak's producer: idempotent, `acks=all`, lz4, a short linger and a
/// 16 KiB batch.
async fn batching_producer(bootstrap: String) -> Producer {
    Producer::builder()
        .bootstrap(bootstrap)
        .client_id("diskless-e2e-batched")
        .security(security())
        .acks(Acks::All)
        .enable_idempotence(true)
        .compression(Compression::Lz4)
        .linger(Duration::from_millis(5))
        .batch_size(16 * 1024)
        .build()
        .await
        .expect("build the batching producer")
}

fn value(partition: i32, index: usize) -> Bytes {
    let mut value = format!("p{partition}-{index:06}-").into_bytes();
    value.resize(VALUE_LEN, b'x');
    Bytes::from(value)
}

/// Produce `RECORDS_PER_PARTITION` records to every partition in bursts, and
/// require each one to be acknowledged. Returns the acknowledged count.
async fn produce_bursts(producer: &Producer, first_index: usize) -> usize {
    let mut acknowledged = 0;
    for burst in 0..RECORDS_PER_PARTITION.div_ceil(BURST) {
        let mut deliveries = Vec::new();
        for partition in 0..PARTITIONS {
            for slot in 0..BURST {
                let index = first_index + burst * BURST + slot;
                deliveries.push(
                    producer
                        .enqueue(ProducerRecord {
                            topic: TOPIC.to_owned(),
                            partition: Some(partition),
                            value: Some(value(partition, index)),
                            ..ProducerRecord::default()
                        })
                        .await
                        .expect("enqueue a record"),
                );
            }
        }
        for delivery in deliveries {
            delivery.await.expect("the record is acknowledged");
            acknowledged += 1;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    acknowledged
}

/// Read every partition from the earliest offset to `ends` with the krabka
/// consumer, as the soak executor's read-back does, and return the offsets it
/// read per partition.
async fn read_back(bootstrap: String, ends: &BTreeMap<i32, i64>) -> BTreeMap<i32, Vec<i64>> {
    let partitions: Vec<(String, i32)> = (0..PARTITIONS)
        .map(|partition| (TOPIC.to_owned(), partition))
        .collect();
    // `latest` is the client's default and the soak's: a fetch the broker
    // wrongly refuses as out of range then shows up as records skipped.
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .client_id("diskless-e2e-readback")
        .security(security())
        .auto_offset_reset(AutoOffsetReset::Latest)
        .build()
        .await
        .expect("build the consumer");
    consumer.assign(&partitions).await.expect("assign");
    consumer
        .seek_to_beginning(&partitions)
        .await
        .expect("seek to the beginning");
    let mut read: BTreeMap<i32, Vec<i64>> = BTreeMap::new();
    let deadline = std::time::Instant::now() + Duration::from_mins(2);
    loop {
        let done = ends.iter().all(|(partition, end)| {
            read.get(partition)
                .and_then(|offsets| offsets.last())
                .is_some_and(|last| last + 1 >= *end)
        });
        if done || consumer.at_log_end().await || std::time::Instant::now() > deadline {
            break;
        }
        for record in consumer
            .poll(krabka_units::secs(1))
            .await
            .expect("poll the consumer")
        {
            read.entry(record.partition)
                .or_default()
                .push(record.offset);
        }
    }
    let _ = consumer.close().await;
    read
}

/// Each partition's high watermark, read from its leader.
async fn high_watermarks(cluster: &DisklessCluster) -> BTreeMap<i32, i64> {
    let mut ends = BTreeMap::new();
    for partition in 0..PARTITIONS {
        let end = cluster
            .handle_for_node(partition_leader(cluster, partition))
            .expect("the leader is up")
            .high_watermark_for_test(TOPIC, partition)
            .await
            .expect("the leader holds the partition");
        ends.insert(partition, end);
    }
    ends
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_krabka_consumer_reads_every_batched_record_from_earliest() {
    let cluster = trimming_cluster().await;
    let topic_id = create_topic(&cluster).await;
    let producer = batching_producer(cluster.bootstrap_for_node(cluster.node_ids()[0])).await;

    let acknowledged = produce_bursts(&producer, 0).await;
    producer.close().await.expect("close the producer");
    let partitions = usize::try_from(PARTITIONS).expect("small topic");
    let total = RECORDS_PER_PARTITION.div_ceil(BURST) * BURST * partitions;
    assert!(acknowledged == total);

    // Wait until each leader has trimmed past its first batch, so the
    // read-back below starts in the object store and not in the local log.
    let ends = high_watermarks(&cluster).await;
    for partition in 0..PARTITIONS {
        let leader = cluster
            .handle_for_node(partition_leader(&cluster, partition))
            .expect("the leader is up");
        wait_for(
            &format!("the trim of partition {partition}"),
            Duration::from_mins(1),
            || async {
                leader
                    .partition_log_start_for_test(TOPIC, partition)
                    .is_some_and(|start| start > 0)
            },
        )
        .await;
    }

    let read = read_back(cluster.bootstrap_for_node(cluster.node_ids()[0]), &ends).await;
    for (partition, end) in &ends {
        let expected: Vec<i64> = (0..*end).collect();
        let observed = read.get(partition).cloned().unwrap_or_default();
        assert!(
            observed == expected,
            "partition {partition}: read {} of {end} offsets",
            observed.len()
        );
    }
    let observed_total: usize = read.values().map(Vec::len).sum();
    assert!(observed_total == total);

    // Every voter fsyncs every partition up to its leader's end.
    for partition in 0..PARTITIONS {
        let leader = partition_leader(&cluster, partition);
        for node in cluster
            .node_ids()
            .into_iter()
            .filter(|node| *node != leader)
        {
            let dir = voter_dir(cluster.log_dir_for_node(node), topic_id, partition, node);
            let end = ends[&partition];
            wait_for(
                &format!("voter {} of partition {partition} to reach {end}", node.0),
                Duration::from_mins(1),
                || async { durable_range(&dir).is_some_and(|range| range.end >= end) },
            )
            .await;
        }
    }

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_restarted_under_load_rejoins_the_isr() {
    let mut cluster = trimming_cluster().await;
    create_topic(&cluster).await;
    let leader = partition_leader(&cluster, 0);
    let follower = cluster
        .node_ids()
        .into_iter()
        .find(|node| *node != leader)
        .expect("a follower");
    let survivor = cluster
        .node_ids()
        .into_iter()
        .find(|node| *node != leader && *node != follower)
        .expect("a third broker");

    // Keep the load running across the restart, as the soak does, so the
    // controller reserves offsets past the follower's log end while it is down.
    let producer = batching_producer(cluster.bootstrap_for_node(leader)).await;
    let stop = CancellationToken::new();
    let load = tokio::spawn({
        let stop = stop.clone();
        async move {
            let mut first = 0;
            while !stop.is_cancelled() {
                first += produce_bursts(&producer, first).await;
            }
            producer.close().await.expect("close the producer");
        }
    });
    produce_settle().await;

    cluster.stop(follower).await;
    let observer = cluster
        .handle_for_node(survivor)
        .expect("the survivor is up");
    wait_for(
        "the stopped follower to leave the ISR",
        Duration::from_mins(1),
        || async {
            (0..PARTITIONS).any(|partition| {
                observer
                    .partition_isr_for_test(TOPIC, partition)
                    .is_some_and(|isr| !isr.contains(&follower.0))
            })
        },
    )
    .await;
    produce_settle().await;
    cluster.restart(follower).await;

    let observer = cluster
        .handle_for_node(survivor)
        .expect("the survivor is up");
    wait_for(
        "the restarted follower to rejoin every ISR",
        Duration::from_mins(1),
        || async {
            (0..PARTITIONS).all(|partition| {
                observer
                    .partition_isr_for_test(TOPIC, partition)
                    .is_some_and(|isr| isr.len() == VOTERS)
            })
        },
    )
    .await;

    stop.cancel();
    load.await.expect("the load task");
    cluster.shutdown().await;
}

/// Let the load run for a few flush ticks.
async fn produce_settle() { tokio::time::sleep(Duration::from_secs(3)).await; }
