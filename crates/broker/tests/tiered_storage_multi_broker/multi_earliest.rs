//! A tiered partition reads back from its earliest offset after its leader
//! restarts and leadership moves on.
//!
//! A broker that restarts on a tiered partition whose sealed segments local
//! retention already evicted opens a log that begins at its active segment.
//! That is where its *local* log starts. The global log start, the first offset
//! any tier still serves, is lower: nobody deleted the archived records, so the
//! remote tier still holds them. Kafka keeps the two apart across the restart
//! (`LogLoader.load` takes the log-start checkpoint, not the first local
//! segment, for a remote-storage-enabled log), so the restarted leader reports
//! the global start in every `Fetch` it answers, and a follower that raises its
//! own start to it (`ReplicaFetcherThread.processPartitionData` →
//! `maybeIncrementLogStartOffset`) raises it to nothing.
//!
//! This suite restarts the preferred leader of an RF=3 tiered partition, moves
//! leadership back to it, then away from it again, and reads the partition back
//! the way a consumer started with `auto.offset.reset=earliest` or
//! `seekToBeginning` does: `ListOffsets(EARLIEST)`, then `Fetch` from there. It
//! is the read-back of the sustained-load contract's tiered path. Every record
//! acknowledged must come back, at both isolation levels.

use {
    crate::{
        TOPIC,
        multi_cluster::{TieredCluster, ready_admin, start_tiered_cluster},
        multi_workload::local_segment_bases,
        support::{
            self,
            producer::{default_producer, producer_record},
            topics::AssignedTopicSetup,
        },
        topic_fixture::{await_tiered_replicas, create_assigned_topic},
    },
    assert2::assert,
    krabka_broker::{BootstrapMode, BrokerHandle},
    krabka_client_consumer::{Consumer, IsolationLevel},
    krabka_client_core::Client,
    krabka_metadata::NodeId,
    krabka_protocol::owned::elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
    std::{
        path::Path,
        time::{Duration, Instant},
    },
};

/// Records produced before the restart. Each is its own batch, so the 1 KiB
/// segments roll many times and local retention evicts most of them.
const ARCHIVED: usize = 120;
/// Records produced while the restarted broker leads again. Producing them with
/// `acks=all` is what makes both followers fetch from it.
const LED_BY_RESTARTED: usize = 20;
/// The payload each record carries.
const PAYLOAD: usize = 200;
/// Kafka's `ElectionType.PREFERRED`.
const PREFERRED: i8 = 0;

/// Broker indexes into [`TieredCluster`]: broker 2 is the preferred leader
/// that restarts, broker 3 the follower that leads while it is away. Broker 1
/// stays up throughout, because every broker's RLMM client bootstraps against
/// it.
const RESTARTED: usize = 1;
const STAND_IN: usize = 2;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tiered_partition_reads_back_from_earliest_after_its_leader_restarts() {
    let TieredCluster {
        brokers,
        configs,
        log_dirs,
        remote_dir: _remote_dir,
    } = start_tiered_cluster([krabka_units::kibibytes(1); 3]).await;
    let [b1, b2, b3] = brokers;
    let admin = ready_admin([&b1, &b2, &b3], "tiered-earliest-admin").await;
    create_assigned_topic()
        .client(&admin)
        .topic(AssignedTopicSetup::on_replicas(TOPIC, vec![
            NodeId(2),
            NodeId(3),
            NodeId(1),
        ]))
        .maybe_segment_bytes(Some("1024"))
        .call()
        .await;
    await_tiered_replicas(
        &[&b1, &b2, &b3],
        TOPIC,
        Some(krabka_units::kibibytes(1)),
        "tiered config did not propagate",
    )
    .await;
    await_leader(&b1, 2).await;
    let bootstrap = format!("127.0.0.1:{}", b1.listen_addr().port());

    produce(&bootstrap, 0..ARCHIVED).await;
    await_local_eviction(log_dirs[RESTARTED].path()).await;

    // Restart the preferred leader on the log it left: it opens a log whose
    // first local segment sits above everything the archive holds.
    let mut brokers = [Some(b1), Some(b2), Some(b3)];
    brokers[RESTARTED]
        .take()
        .expect("broker 2 runs")
        .shutdown()
        .await;
    let b1 = brokers[0].as_ref().expect("broker 1 runs");
    await_leader(b1, 3).await;
    let mut config = configs[RESTARTED].clone();
    config.bootstrap_mode = BootstrapMode::Rejoin;
    brokers[RESTARTED] = Some(support::start_reusing_addrs(&config, "restart broker 2").await);
    let b1 = brokers[0].as_ref().expect("broker 1 runs");
    b1.wait_for_image(|image| {
        image
            .partition(TOPIC, 0)
            .is_some_and(|partition| partition.isr.contains(&krabka_raft::NodeId(2)))
    })
    .await;

    // Lead from the restarted broker, and make both followers fetch from it.
    elect_preferred(&admin).await;
    await_leader(b1, 2).await;
    produce(&bootstrap, ARCHIVED..ARCHIVED + LED_BY_RESTARTED).await;
    let end = i64::try_from(ARCHIVED + LED_BY_RESTARTED).expect("small count");
    for broker in brokers.iter().flatten() {
        broker.wait_until_high_watermark(TOPIC, 0, end).await;
    }

    // Move leadership off the restarted broker again.
    brokers[RESTARTED]
        .take()
        .expect("broker 2 runs")
        .shutdown()
        .await;
    let b1 = brokers[0].as_ref().expect("broker 1 runs");
    await_leader(b1, 3).await;
    let stand_in = brokers[STAND_IN].as_ref().expect("broker 3 runs");
    stand_in.wait_until_high_watermark(TOPIC, 0, end).await;

    let expected: Vec<i64> = (0..end).collect();
    for isolation in [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
    ] {
        let offsets = read_from_earliest(&bootstrap, isolation, end).await;
        assert!(
            offsets == expected,
            "{isolation:?} read {} of {end} acknowledged records back from the earliest offset, \
             starting at {:?}",
            offsets.len(),
            offsets.first()
        );
    }

    drop(admin);
    for broker in brokers.into_iter().flatten() {
        broker.shutdown().await;
    }
}

/// Produce one acknowledged record per index, each its own batch.
async fn produce(bootstrap: &str, indexes: std::ops::Range<usize>) {
    let producer = default_producer(bootstrap).await;
    for index in indexes {
        let mut value = format!("record-{index}-").into_bytes();
        value.resize(PAYLOAD, b'x');
        producer
            .send(
                producer_record()
                    .topic(TOPIC.to_owned())
                    .partition(0)
                    .value(Some(bytes::Bytes::from(value)))
                    .call(),
            )
            .await
            .expect("acknowledged");
    }
    producer.close().await.expect("producer close");
}

/// Wait until `broker` sees `leader` lead the partition.
async fn await_leader(broker: &BrokerHandle, leader: u64) {
    broker
        .wait_for_image(move |image| {
            image
                .partition(TOPIC, 0)
                .is_some_and(|partition| partition.leader == krabka_raft::NodeId(leader))
        })
        .await;
}

/// Wait until local retention has taken the first archived segment off the
/// replica whose log directory is `log_dir`. The first local segment then
/// starts above offset zero, and nothing below it is on that disk.
async fn await_local_eviction(log_dir: &Path) {
    let partition_dir = log_dir.join(format!("{TOPIC}-0"));
    let deadline = Instant::now() + Duration::from_mins(1);
    // intentional: local retention runs on the remote-log manager's timer and
    // has no awaitable signal, so poll the directory (bounded loop).
    loop {
        if local_segment_bases(&partition_dir)
            .first()
            .is_some_and(|base| *base > 0)
        {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "{} never evicted its first segment: {:?}",
            partition_dir.display(),
            local_segment_bases(&partition_dir)
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Elect the preferred leader of the partition.
async fn elect_preferred(admin: &Client) {
    let response = admin
        .send(ElectLeadersRequest {
            election_type: PREFERRED,
            topic_partitions: Some(vec![TopicPartitions {
                topic: TOPIC.to_owned(),
                partitions: vec![0],
                ..Default::default()
            }]),
            timeout_ms: 30_000,
            ..Default::default()
        })
        .await
        .expect("ElectLeaders");
    assert!(
        response.error_code == 0,
        "ElectLeaders failed: {response:?}"
    );
}

/// Every offset a fresh, groupless consumer at `isolation` reads from the
/// earliest offset, as the soak contract's read-back reads: assign, seek to
/// the beginning, and poll until the high watermark `end` is reached or the
/// read stops making progress.
async fn read_from_earliest(bootstrap: &str, isolation: IsolationLevel, end: i64) -> Vec<i64> {
    let partitions = [(TOPIC.to_owned(), 0)];
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.to_owned())
        .client_id(format!("tiered-earliest-{isolation:?}"))
        .isolation_level(isolation)
        .build()
        .await
        .expect("consumer");
    consumer.assign(&partitions).await.expect("assign");
    consumer
        .seek_to_beginning(&partitions)
        .await
        .expect("seek to beginning");
    let mut offsets = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while offsets.last().is_none_or(|last| last + 1 < end) && Instant::now() < deadline {
        let records = consumer
            .poll(krabka_units::millis(500))
            .await
            .expect("poll");
        offsets.extend(records.iter().map(|record| record.offset));
    }
    consumer.close().await.expect("consumer close");
    offsets
}
