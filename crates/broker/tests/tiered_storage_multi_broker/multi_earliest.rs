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
//! Both cases read the partition back the way a consumer started with
//! `auto.offset.reset=earliest` or `seekToBeginning` does:
//! `ListOffsets(EARLIEST)`, then `Fetch` from there. It is the read-back of the
//! sustained-load contract's tiered path. Every record acknowledged must come
//! back, at both isolation levels.
//!
//! - [`a_tiered_partition_reads_back_from_earliest_after_its_leader_restarts`]
//!   restarts the preferred leader of an RF=3 tiered partition, moves
//!   leadership back to it, then away from it again.
//! - [`a_follower_left_with_a_raised_floor_takes_its_leaders_back`] starts a
//!   follower on the floor a broker without the fix for #1444 left behind, a
//!   restarted leader's local start established as the global one, and then
//!   makes it the leader. The leader it followed reported the real start, and
//!   a tiered follower takes it even when it is lower.
//! - [`a_compressed_archive_reads_back_from_earliest`] reads back an archive
//!   that an lz4 producer wrote, from a leader that evicted the start of it
//!   (#1448).

use {
    crate::{
        TOPIC,
        multi_cluster::{TieredCluster, ready_admin, start_tiered_cluster},
        multi_workload::local_segment_bases,
        support::{self, producer::producer_record, topics::AssignedTopicSetup},
        topic_fixture::{await_tiered_replicas, create_assigned_topic},
    },
    assert2::assert,
    krabka_broker::{BootstrapMode, BrokerConfig, BrokerHandle},
    krabka_client_consumer::{Consumer, IsolationLevel},
    krabka_client_core::Client,
    krabka_client_producer::{Compression, Producer},
    krabka_metadata::NodeId,
    krabka_protocol::owned::elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
    std::{
        ops::Range,
        path::Path,
        time::{Duration, Instant},
    },
    tokio::time,
};

/// Records produced before the restart. Each is its own batch, so the 1 KiB
/// segments roll many times and local retention evicts most of them.
const ARCHIVED: usize = 120;
/// Records produced once the case has set its cluster up. Producing them with
/// `acks=all` is what makes every follower fetch from the leader.
const LATER: usize = 20;
/// The payload each record carries.
const PAYLOAD: usize = 200;
/// Kafka's `ElectionType.PREFERRED`.
const PREFERRED: i8 = 0;

/// Broker index of the preferred leader, broker 2.
const LEADER: usize = 1;
/// Broker index of the follower that leads once broker 2 stops, broker 3.
const FOLLOWER: usize = 2;

/// The three brokers, the tiered partition every case reads back, and the
/// records it already archived and evicted from `evicted`'s disk.
struct Archived {
    brokers: [Option<BrokerHandle>; 3],
    configs: [BrokerConfig; 3],
    log_dirs: Vec<tempfile::TempDir>,
    _remote_dir: tempfile::TempDir,
    admin: Client,
    bootstrap: String,
    /// The codec every record the case produces is compressed with.
    compression: Compression,
}

#[bon::bon]
impl Archived {
    /// Boot the cluster, create the partition on replicas 2, 3 and 1 (broker
    /// 2 leads), produce [`ARCHIVED`] records, and wait until local retention
    /// has evicted the first of them from broker index `evicted`. Every
    /// record goes out compressed with `compression`.
    #[builder]
    async fn start(evicted: usize, #[builder(default)] compression: Compression) -> Self {
        let TieredCluster {
            brokers,
            configs,
            log_dirs,
            remote_dir,
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
        produce(&bootstrap, 0..ARCHIVED, compression).await;
        await_local_eviction(log_dirs[evicted].path()).await;
        Self {
            brokers: [Some(b1), Some(b2), Some(b3)],
            configs,
            log_dirs,
            _remote_dir: remote_dir,
            admin,
            bootstrap,
            compression,
        }
    }

    /// Broker 1, which stays up throughout because every broker's RLMM
    /// client bootstraps against it.
    fn anchor(&self) -> &BrokerHandle { self.brokers[0].as_ref().expect("broker 1 runs") }

    async fn stop(&mut self, index: usize) {
        self.brokers[index]
            .take()
            .expect("the broker runs")
            .shutdown()
            .await;
    }

    /// Start broker index `index` again on the log directory and addresses
    /// it left, and wait until it is back in the ISR.
    async fn restart(&mut self, index: usize) {
        let mut config = self.configs[index].clone();
        config.bootstrap_mode = BootstrapMode::Rejoin;
        self.brokers[index] = Some(support::start_reusing_addrs(&config, "restart a broker").await);
        let node = krabka_raft::NodeId(u64::try_from(index + 1).expect("small index"));
        self.anchor()
            .wait_for_image(move |image| {
                image
                    .partition(TOPIC, 0)
                    .is_some_and(|partition| partition.isr.contains(&node))
            })
            .await;
    }

    /// Produce `indexes` and wait until every running replica's high
    /// watermark covers them, so each has fetched from the leader since.
    async fn produce_and_replicate(&self, indexes: Range<usize>) -> i64 {
        produce(&self.bootstrap, indexes.clone(), self.compression).await;
        let end = i64::try_from(indexes.end).expect("small count");
        for broker in self.brokers.iter().flatten() {
            broker.wait_until_high_watermark(TOPIC, 0, end).await;
        }
        end
    }

    async fn shutdown(self) {
        drop(self.admin);
        for broker in self.brokers.into_iter().flatten() {
            broker.shutdown().await;
        }
    }
}

#[derive(Default)]
struct TieredEarliest {
    cluster: Option<Archived>,
    reads: Vec<(IsolationLevel, Vec<i64>)>,
}

#[rstest::fixture]
fn tiered_earliest() -> TieredEarliest {
    // Reads and leadership changes share one concrete acknowledged archive.
    TieredEarliest::default()
}

impl TieredEarliest {
    fn cluster(&self) -> &Archived { self.cluster.as_ref().expect("archived cluster") }

    async fn shutdown(&mut self) {
        Box::pin(self.cluster.take().expect("archived cluster").shutdown()).await;
    }
}

#[rstest_bdd_macros::given(
    "an RF3 tiered partition led by broker 2 archives 120 records of 200 bytes using {codec} and broker {evicted} evicts offset zero locally"
)]
async fn archived_earliest_workload(
    tiered_earliest: &mut TieredEarliest,
    codec: String,
    evicted: usize,
) {
    let compression = match codec.as_str() {
        "none" => Compression::default(),
        "lz4" => Compression::Lz4,
        _ => panic!("unsupported compression: {codec}"),
    };
    tiered_earliest.cluster = Some(
        Box::pin(
            Archived::start()
                .evicted(evicted.checked_sub(1).expect("positive broker id"))
                .compression(compression)
                .call(),
        )
        .await,
    );
}

#[rstest_bdd_macros::when(
    "broker 2 stops, broker 3 takes leadership and broker 2 restarts from its retained local log"
)]
async fn restart_archived_leader(tiered_earliest: &mut TieredEarliest) {
    tiered_earliest
        .cluster
        .as_mut()
        .expect("archived cluster")
        .stop(LEADER)
        .await;
    await_leader(tiered_earliest.cluster().anchor(), 3).await;
    tiered_earliest
        .cluster
        .as_mut()
        .expect("archived cluster")
        .restart(LEADER)
        .await;
}

#[rstest_bdd_macros::when("preferred election returns leadership to broker 2")]
async fn reelect_archived_leader(tiered_earliest: &TieredEarliest) {
    elect_preferred(&tiered_earliest.cluster().admin).await;
    await_leader(tiered_earliest.cluster().anchor(), 2).await;
}

#[rstest_bdd_macros::when(
    "twenty more records at offsets 120 through 139 are acknowledged and replicated everywhere"
)]
async fn append_archived_tail(tiered_earliest: &TieredEarliest) {
    tiered_earliest
        .cluster()
        .produce_and_replicate(ARCHIVED..ARCHIVED + LATER)
        .await;
}

#[rstest_bdd_macros::when("broker 2 stops again and broker 3 takes leadership")]
async fn follow_archived_leader(tiered_earliest: &mut TieredEarliest) {
    tiered_earliest
        .cluster
        .as_mut()
        .expect("archived cluster")
        .stop(LEADER)
        .await;
    await_leader(tiered_earliest.cluster().anchor(), 3).await;
}

#[rstest_bdd_macros::when(
    "broker 3 stops and its persisted global floor is raised to its first remaining local segment"
)]
async fn raise_archived_follower_floor(tiered_earliest: &mut TieredEarliest) {
    // Leave the follower's log on the floor an older broker established: the
    // first segment its disk still holds, which every archived offset below
    // it is under.
    tiered_earliest
        .cluster
        .as_mut()
        .expect("archived cluster")
        .stop(FOLLOWER)
        .await;
    let partition_dir = tiered_earliest.cluster().log_dirs[FOLLOWER]
        .path()
        .join(format!("{TOPIC}-0"));
    let raised = *local_segment_bases(&partition_dir)
        .first()
        .expect("the follower holds its active segment");
    let config = krabka_log::LogConfig {
        remote_storage_enable: true,
        ..tiered_earliest.cluster().configs[FOLLOWER]
            .log_config
            .clone()
    };
    let mut log = krabka_log::Log::open(&partition_dir, config.clone()).expect("open the log");
    log.set_log_start_offset(krabka_log::Offset(raised))
        .expect("raise the floor");
    drop(log);
    let reopened = krabka_log::Log::open(&partition_dir, config).expect("reopen the log");
    assert!(reopened.established_log_start() == Some(krabka_log::Offset(raised)));
    drop(reopened);
}

#[rstest_bdd_macros::when("broker 3 restarts and follows broker 2")]
async fn restart_archived_follower(tiered_earliest: &mut TieredEarliest) {
    tiered_earliest
        .cluster
        .as_mut()
        .expect("archived cluster")
        .restart(FOLLOWER)
        .await;
}

#[rstest_bdd_macros::then("broker 3 lowers its global log start back to 0")]
async fn restored_archived_floor(tiered_earliest: &TieredEarliest) {
    await_log_start(
        tiered_earliest.cluster().brokers[FOLLOWER]
            .as_ref()
            .expect("the follower runs"),
        0,
    )
    .await;
}

#[rstest_bdd_macros::when(
    "fresh consumers seek to earliest and read toward offset {end} at both isolation levels"
)]
async fn read_archived_earliest(tiered_earliest: &mut TieredEarliest, end: i64) {
    tiered_earliest.reads.clear();
    for isolation in [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
    ] {
        let offsets =
            read_from_earliest(&tiered_earliest.cluster().bootstrap, isolation, end).await;
        tiered_earliest.reads.push((isolation, offsets));
    }
}

#[rstest_bdd_macros::then("each consumer returns every offset from 0 through {last} exactly once")]
fn complete_archived_earliest(tiered_earliest: &TieredEarliest, last: i64) {
    assert!(tiered_earliest.reads.len() == 2);
    let expected: Vec<i64> = (0..=last).collect();
    for (isolation, offsets) in &tiered_earliest.reads {
        assert!(
            *offsets == expected,
            "{isolation:?} read {} of {} acknowledged records back from the earliest offset, starting at {:?}",
            offsets.len(),
            last + 1,
            offsets.first()
        );
    }
}

/// Produce one acknowledged record per index, each its own batch, compressed
/// with `compression`.
async fn produce(bootstrap: &str, indexes: Range<usize>, compression: Compression) {
    {
        let producer = Producer::builder()
            .bootstrap(bootstrap)
            .compression(compression)
            .build()
            .await
            .expect("producer");
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
        producer.close().await
    }
    .expect("producer close");
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
        time::sleep(Duration::from_millis(300)).await;
    }
}

/// Wait until `broker`'s replica of the partition starts at `start`.
async fn await_log_start(broker: &BrokerHandle, start: i64) {
    let deadline = Instant::now() + Duration::from_mins(2);
    // intentional: a follower takes its leader's log start on a fetch
    // response, once its RLMM has caught up after the restart, and neither has
    // an awaitable signal, so poll the partition (bounded loop).
    loop {
        let current = broker.partition_log_start_for_test(TOPIC, 0);
        if current == Some(start) {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "the replica's log start stayed at {current:?}, not {start}"
        );
        time::sleep(Duration::from_millis(300)).await;
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
/// the beginning, and poll until the high watermark `end` is reached or a
/// minute passes.
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
    let deadline = Instant::now() + Duration::from_mins(1);
    while offsets.last().is_none_or(|last| last + 1 < end) && Instant::now() < deadline {
        // A broker that just restarted answers a cold read with a retriable
        // error until its remote-log metadata has caught up, and the
        // consumer keeps its position across it, as the soak read-back does.
        match consumer.poll(krabka_units::millis(500)).await {
            Ok(records) => offsets.extend(records.iter().map(|record| record.offset)),
            Err(error) => {
                eprintln!("read-back at {isolation:?}: {error}");
                time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    consumer.close().await.expect("consumer close");
    offsets
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/tiered_storage_multi_broker/multi_earliest.feature",
    name = "a tiered partition reads back from earliest after its leader restarts"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tiered_partition_reads_back_from_earliest_after_its_leader_restarts(
    mut tiered_earliest: TieredEarliest,
) {
    tiered_earliest.shutdown().await;
    drop(tiered_earliest);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/tiered_storage_multi_broker/multi_earliest.feature",
    name = "a follower left with a raised floor takes its leaders back"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_left_with_a_raised_floor_takes_its_leaders_back(
    mut tiered_earliest: TieredEarliest,
) {
    tiered_earliest.shutdown().await;
    drop(tiered_earliest);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/tiered_storage_multi_broker/multi_earliest.feature",
    name = "a compressed archive reads back from earliest"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compressed_archive_reads_back_from_earliest(mut tiered_earliest: TieredEarliest) {
    tiered_earliest.shutdown().await;
    drop(tiered_earliest);
}
