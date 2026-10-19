//! `krabka-backup capture` against a live diskless broker.
//!
//! The broker enforces Kafka's internal-topic rule: a `Produce` to an internal
//! topic succeeds only for the `__admin_client` client id (and the broker's own
//! index writers), and anything else gets `INVALID_TOPIC_EXCEPTION` (17). A
//! capture that published into `__diskless_wal_index` therefore failed against
//! every cluster that had one. This suite boots a single diskless broker in
//! process, lets it flush a committed prefix into the index, and requires the
//! capture to freeze that prefix without writing anything.
//!
//! The same broker is then the source of a whole restore: many small lz4
//! batches from an idempotent producer, captured, materialized into a fresh
//! node by `krabka-restore`, and read back from the earliest offset by a
//! groupless consumer, the way the qualification disaster-recovery drill reads
//! its restored topics. The restored node flushes the materialized records into
//! its own WAL and trims its local log, so that read is a WAL cold read with
//! the consumer's KIP-320 `last_fetched_epoch` below the trimmed log start
//! (#1525).

use {
    assert2::{assert, check},
    bytes::Bytes,
    clap::Parser as _,
    krabka_backup::{
        archive::ArchiveArgs,
        capture::capture_key,
        manifest::{DISKLESS_WAL_INDEX, GROUP_OFFSETS, MANIFEST, Manifest},
        run,
    },
    krabka_broker::{
        Broker, BrokerConfig, BrokerHandle, KafkaRlmmConfig, NodeId, RemoteStorageBackend, RlmmKind,
    },
    krabka_client_admin::{AdminClient, CreateTopicSpec, TopicMutationOptions},
    krabka_client_consumer::Consumer,
    krabka_client_core::Client,
    krabka_compression::CompressionType,
    krabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord, TopicRecord},
    krabka_protocol::{
        owned::init_producer_id_request::InitProducerIdRequest,
        records::{Attributes, Record, RecordBatch},
    },
    krabka_remote_storage::diskless::{DisklessPartitionCapture, DisklessWalCapture},
    krabka_restore::{Cli, restore},
    std::{
        collections::BTreeMap,
        net::SocketAddr,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    uuid::Uuid,
};

krabka_macros::single_replica_partition_fixture!(single_replica_partition);

/// The diskless data topic the broker flushes into the index.
const TOPIC: &str = "events";

/// How many single-record `acks=all` produces the case commits.
const RECORDS: i64 = 3;

/// Kafka `NOT_LEADER_OR_FOLLOWER`, returned before any append.
const NOT_LEADER_OR_FOLLOWER: i16 = 6;

/// Kafka `UNKNOWN_TOPIC_OR_PARTITION`, also returned before any append.
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

/// A single-broker cluster that runs a diskless WAL and its topic-backed flush
/// index, and the directories that must outlive it.
struct DisklessBroker {
    handle: BrokerHandle,
    bootstrap: String,
    _log_dir: TempDir,
    object_store: TempDir,
}

/// Bind a loopback listener on an OS-assigned port.
async fn bind_loopback() -> tokio::net::TcpListener {
    tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback listener")
}

/// Boot one broker that can host a diskless topic on its own, on a fresh log
/// directory.
async fn start_diskless_broker() -> DisklessBroker {
    let log_dir = TempDir::new().expect("broker log dir");
    boot_diskless_broker(log_dir, bind_loopback().await).await
}

/// Boot one broker that can host a diskless topic on its own, on `log_dir`
/// and with its controller on `controller`.
///
/// It needs a rack, because WAL placement refuses a voter with none, a WAL
/// quorum of one, an object store to flush into, and a topic-backed metadata
/// log with one partition and one replica, because the flush index is a Kafka
/// topic this broker alone must host. The ports are bound up front: the index
/// client bootstraps against the data listener, and the controller address is
/// the static voter set, so neither can be `:0`. A restored node is formatted
/// with its controller address, so its caller binds that listener before the
/// restore runs.
async fn boot_diskless_broker(
    log_dir: TempDir,
    controller: tokio::net::TcpListener,
) -> DisklessBroker {
    let data = bind_loopback().await;
    let data_addr: SocketAddr = data.local_addr().expect("data address");
    let controller_addr: SocketAddr = controller.local_addr().expect("controller address");
    let object_store = TempDir::new().expect("object store dir");

    let mut config = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    config.listen_addr = data_addr;
    config.advertised_listener = data_addr.to_string();
    config.controller_listen_addr = controller_addr;
    config.controller_quorum_voters = vec![(NodeId(1), controller_addr.to_string())];
    config.rack = Some("rack-a".to_owned());
    config.diskless_wal_local_replica_count = 1;
    config.diskless_wal_flush_interval = krabka_units::millis(50);
    // The qualification drill's settings: small WAL objects, and a local log
    // trimmed as soon as its records are in one.
    config.diskless_wal_flush_max_size = krabka_units::kibibytes(1);
    config.diskless_wal_trim_safety_lag = 0;
    config.remote_storage_backend = Some(RemoteStorageBackend::Local {
        dir: object_store.path().to_path_buf(),
    });
    config.remote_log_metadata = RlmmKind::TopicBacked(KafkaRlmmConfig {
        bootstrap: data_addr.to_string(),
        num_partitions: 1,
        replication: 1,
        min_isr: 1,
        snapshot_interval: krabka_units::hours(1),
        ..KafkaRlmmConfig::default()
    });

    let handle = Broker::start_with_listeners(config, Some(controller), Some(data))
        .await
        .expect("the diskless broker starts");
    handle.wait_until_brokers_registered(1).await;
    handle.wait_until_diskless_flusher_ready().await;
    DisklessBroker {
        handle,
        bootstrap: data_addr.to_string(),
        _log_dir: log_dir,
        object_store,
    }
}

/// Create the diskless topic with `partitions` partitions and return its id.
async fn create_diskless_topic(bootstrap: &str, partitions: i32) -> Uuid {
    let mut admin = AdminClient::connect(&[bootstrap.to_owned()])
        .await
        .expect("admin client");
    let outcomes = admin
        .create_topics(
            &[CreateTopicSpec {
                name: TOPIC.to_owned(),
                partitions,
                replicas: 1,
                configs: BTreeMap::from([("krabka.diskless".to_owned(), "true".to_owned())]),
                replica_assignments: BTreeMap::new(),
            }],
            TopicMutationOptions::default(),
        )
        .await
        .expect("CreateTopics");
    let outcome = outcomes.first().expect("one CreateTopics outcome");
    assert!(outcome.error.is_none(), "CreateTopics failed: {outcome:?}");
    outcome.topic_id.expect("a created topic id")
}

/// Connect a client to `bootstrap`.
async fn client(bootstrap: &str) -> Client {
    Client::builder()
        .bootstrap(bootstrap.to_owned())
        .client_id("diskless-capture-producer")
        .build()
        .await
        .expect("client")
}

/// Produce `RECORDS` single-record batches with `acks=all` to partition 0.
async fn produce(bootstrap: &str, topic_id: Uuid) {
    let client = client(bootstrap).await;
    for index in 0..RECORDS {
        produce_batch(
            &client,
            topic_id,
            0,
            &[format!("record-{index}")],
            &RecordBatch::default(),
        )
        .await;
    }
}

/// Produce one batch holding `values` with `acks=all`, retrying only the
/// errors a broker returns before it appends. `shape` sets everything but the
/// records and their offsets and timestamps: the producer, its sequence and
/// the codec. Returns the base offset the broker assigned.
async fn produce_batch(
    client: &Client,
    topic_id: Uuid,
    partition: i32,
    values: &[String],
    shape: &RecordBatch,
) -> i64 {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        // The flusher expires batches older than `retention.ms` out of the
        // object store, so each batch carries the wall clock.
        let now_ms = test_wallclock_ms();
        let batch = RecordBatch {
            base_timestamp: now_ms,
            max_timestamp: now_ms,
            last_offset_delta: i32::try_from(values.len() - 1).expect("a small batch"),
            records: values
                .iter()
                .zip(0..)
                .map(|(value, offset_delta)| Record {
                    offset_delta,
                    value: Some(Bytes::from(value.clone())),
                    ..Record::default()
                })
                .collect(),
            ..shape.clone()
        };
        let response = client
            .send(krabka_broker::testing::single_partition_produce_request(
                TOPIC, topic_id, partition, batch, -1, 30_000,
            ))
            .await
            .expect("Produce");
        match response.responses[0].partition_responses[0].error_code {
            0 => return response.responses[0].partition_responses[0].base_offset,
            NOT_LEADER_OR_FOLLOWER | UNKNOWN_TOPIC_OR_PARTITION => {
                assert!(
                    Instant::now() <= deadline,
                    "{values:?} found no diskless leader: {response:?}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            error_code => panic!("produce of {values:?} failed with {error_code}"),
        }
    }
}

/// What `BrokerHandle::diskless_flush_state_for_test` reports for a partition:
/// whether it is diskless, then its index projection, log start, log end, high
/// watermark and flushed frontier.
type FlushState = (bool, u64, i64, i64, i64, Option<i64>);

/// Wait until the broker's own index projection says `partition` is flushed
/// through `end`, which is the state a capture must freeze.
async fn await_flushed(broker: &BrokerHandle, partition: i32, end: i64) {
    await_flush_state(
        broker,
        partition,
        "flushed",
        |(_, _, _, _, hw, frontier)| *hw == end && *frontier == Some(end),
    )
    .await;
}

/// Wait until `partition` is flushed through `end` and its local log trimmed
/// to it, so the broker serves every offset below `end` from its WAL objects.
async fn await_trimmed(broker: &BrokerHandle, partition: i32, end: i64) {
    await_flush_state(
        broker,
        partition,
        "flushed and trimmed",
        |(_, _, log_start, _, hw, frontier)| {
            *log_start == end && *hw == end && *frontier == Some(end)
        },
    )
    .await;
}

async fn await_flush_state(
    broker: &BrokerHandle,
    partition: i32,
    what: &str,
    done: impl Fn(&FlushState) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let state = broker.diskless_flush_state_for_test(TOPIC, partition).await;
        if let Some(state) = &state {
            assert!(state.0, "the topic must be diskless");
            if done(state) {
                return;
            }
        }
        assert!(
            Instant::now() <= deadline,
            "partition {partition} was never {what}; last state was {state:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Capture the cluster at `bootstrap` into a fresh local archive, and return
/// the archive and the capture id.
async fn capture(bootstrap: &str) -> (TempDir, String) {
    let backup_root = TempDir::new().expect("backup root");
    let archive = ArchiveArgs {
        local: Some(backup_root.path().to_path_buf()),
        ..ArchiveArgs::default()
    };
    let id = run::capture(None, Some(bootstrap), &archive)
        .await
        .expect("capture a cluster that has a diskless WAL index");
    (backup_root, id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capture_freezes_a_live_diskless_index_without_producing_to_it() {
    #[derive(Debug)]
    struct CoveredRange {
        first_offset: i64,
        last_offset: i64,
    }
    let broker = start_diskless_broker().await;
    let topic_id = create_diskless_topic(&broker.bootstrap, 1).await;
    produce(&broker.bootstrap, topic_id).await;
    await_flushed(&broker.handle, 0, RECORDS).await;
    let (backup_root, id) = capture(&broker.bootstrap).await;

    let manifest: Manifest = serde_json::from_slice(
        &std::fs::read(backup_root.path().join(capture_key(&id, MANIFEST))).expect("manifest"),
    )
    .expect("decode the manifest");
    let names: Vec<&str> = manifest
        .artifacts
        .iter()
        .map(|artifact| artifact.name.as_str())
        .collect();
    check!(names == vec![GROUP_OFFSETS, DISKLESS_WAL_INDEX]);
    let capture = DisklessWalCapture::from_slice(
        &std::fs::read(
            backup_root
                .path()
                .join(capture_key(&id, DISKLESS_WAL_INDEX)),
        )
        .expect("diskless capture"),
    )
    .expect("decode the diskless capture");
    check!(capture.source_cutoffs.len() == 1);
    check!(capture.source_cutoffs[0] > 0);
    // Every committed offset is covered once, in order, by the captured
    // ranges. The object keys are the flusher's to choose.
    let covered: Vec<CoveredRange> = capture
        .partitions
        .iter()
        .flat_map(|partition| &partition.ranges)
        .map(|range| CoveredRange {
            first_offset: range.entry.first_offset,
            last_offset: range.entry.last_offset,
        })
        .collect();
    check!(covered.first().map(|range| range.first_offset) == Some(0));
    check!(covered.last().map(|range| range.last_offset) == Some(RECORDS - 1));
    check!(
        covered
            .windows(2)
            .all(|pair| pair[1].first_offset == pair[0].last_offset + 1),
        "the captured ranges leave a gap: {covered:?}"
    );
    let partitions: Vec<DisklessPartitionCapture> = capture
        .partitions
        .into_iter()
        .map(|partition| DisklessPartitionCapture {
            ranges: Vec::new(),
            ..partition
        })
        .collect();
    assert!(
        partitions
            == vec![DisklessPartitionCapture {
                topic: TOPIC.to_owned(),
                topic_id,
                partition: 0,
                delete_floor: 0,
                recovery_cutoff: RECORDS,
                ranges: Vec::new(),
            }]
    );

    broker.handle.shutdown().await;
}

/// `ListOffsetsRequest.LATEST_TIMESTAMP`.
const LATEST_TIMESTAMP: i64 = -1;

/// How many partitions the restore case spreads its batches over.
const RESTORED_PARTITIONS: i32 = 3;

/// How many rounds of one small batch per partition the restore case produces.
const ROUNDS: usize = 40;

/// The values `partition` holds after round `round`, one small batch per
/// round, of one to three records.
fn round_values(partition: i32, round: usize) -> Vec<String> {
    let records = (round + usize::try_from(partition).expect("a partition index")) % 3 + 1;
    (0..records)
        .map(|record| {
            let tag = format!("p{partition}-r{round}-{record}-");
            format!("{tag}{}", noise(&tag, 480))
        })
        .collect()
}

/// `len` hex digits that lz4 cannot shrink, drawn from a generator seeded by
/// `seed`, so a record costs a WAL object about what the drill's random
/// payloads do.
fn noise(seed: &str, len: usize) -> String {
    let mut state = seed.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    });
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            char::from_digit(u32::try_from(state >> 60).expect("a nibble"), 16)
                .expect("a hex digit")
        })
        .collect()
}

/// The controller metadata checkpoint the source node holds: the topic, its
/// partitions on node 1, and the `krabka.diskless` setting only a checkpoint
/// can carry back to a restored cluster.
fn diskless_metadata_checkpoint(topic_id: Uuid) -> Vec<u8> {
    let mut image = MetadataImage::new(Uuid::new_v4());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: TOPIC.to_owned(),
        topic_id,
        partitions: RESTORED_PARTITIONS,
        replication_factor: 1,
    }));
    for partition in 0..RESTORED_PARTITIONS {
        image.apply(&MetadataRecord::V1Partition(single_replica_partition(
            TOPIC,
            krabka_ids::PartitionIndex(partition),
            krabka_metadata::NodeId(1),
        )));
    }
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: TOPIC.to_owned(),
        overrides: BTreeMap::from([("krabka.diskless".to_owned(), "true".to_owned())]),
    }));
    krabka_raft::serialize_metadata_snapshot(&image, 1_700_000_000_000)
        .expect("serialize the metadata checkpoint")
        .to_vec()
}

/// Every record value a groupless consumer reads from each partition of the
/// topic, assigned all of them and started from the beginning, until its
/// position reaches each of `ends` or the consumer says it is at the log end:
/// the qualification drill's read-back.
async fn consume_from_earliest(
    bootstrap: &str,
    ends: &BTreeMap<i32, i64>,
) -> BTreeMap<i32, Vec<String>> {
    let partitions: Vec<(String, i32)> = ends
        .keys()
        .map(|partition| (TOPIC.to_owned(), *partition))
        .collect();
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.to_owned())
        .client_id("diskless-restore-readback")
        .build()
        .await
        .expect("consumer");
    consumer.assign(&partitions).await.expect("assign");
    consumer
        .seek_to_beginning(&partitions)
        .await
        .expect("seek to the beginning");
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut next: BTreeMap<i32, i64> = BTreeMap::new();
    let mut values: BTreeMap<i32, Vec<String>> = BTreeMap::new();
    while Instant::now() <= deadline
        && !ends
            .iter()
            .all(|(partition, end)| next.get(partition).is_some_and(|next| next >= end))
        && !consumer.at_log_end().await
    {
        for record in consumer.poll(krabka_units::secs(1)).await.expect("poll") {
            values.entry(record.partition).or_default().push(
                String::from_utf8_lossy(record.value.as_deref().unwrap_or_default()).into_owned(),
            );
            next.insert(record.partition, record.offset + 1);
        }
    }
    let _ = consumer.close().await;
    values
}

/// An idempotent producer id, as `enable.idempotence` makes a producer ask for.
async fn init_producer_id(client: &Client) -> i64 {
    let response = client
        .send(InitProducerIdRequest {
            transactional_id: None,
            transaction_timeout_ms: -1,
            producer_id: -1,
            producer_epoch: -1,
            ..Default::default()
        })
        .await
        .expect("InitProducerId");
    check!(response.error_code == 0, "InitProducerId: {response:?}");
    response.producer_id
}

/// `ListOffsets(LATEST)` for `partition`: its high watermark.
async fn latest_offset(client: &Client, partition: i32) -> i64 {
    let mut response = client
        .send(
            krabka_broker::testing::list_offsets_request()
                .topic(TOPIC.to_owned())
                .partition_index(partition)
                .timestamp(LATEST_TIMESTAMP)
                .call(),
        )
        .await
        .expect("ListOffsets");
    let row = response.topics.remove(0).partitions.remove(0);
    check!(row.error_code == 0, "ListOffsets({partition}): {row:?}");
    row.offset
}

/// A restored diskless partition serves every record the restore materialized
/// into it (#1525).
///
/// The source takes many small batches on several partitions, flushed into
/// many WAL objects, so each partition's capture is many ranges. The restore
/// materializes them into the target's local logs, a broker boots there with
/// an empty WAL index and no WAL state of its own, and a reader from offset 0
/// of every partition must see every record, in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restored_diskless_partition_serves_every_materialized_record() {
    let source = start_diskless_broker().await;
    let topic_id = create_diskless_topic(&source.bootstrap, RESTORED_PARTITIONS).await;
    let producer = client(&source.bootstrap).await;
    let producer_id = init_producer_id(&producer).await;
    let mut sequences: BTreeMap<i32, i32> = BTreeMap::new();
    let mut expected: BTreeMap<i32, Vec<String>> = BTreeMap::new();
    for round in 0..ROUNDS {
        for partition in 0..RESTORED_PARTITIONS {
            let values = round_values(partition, round);
            let sequence = sequences.entry(partition).or_default();
            let shape = RecordBatch {
                attributes: Attributes::default().with_compression(CompressionType::Lz4),
                producer_id,
                producer_epoch: 0,
                base_sequence: *sequence,
                ..RecordBatch::default()
            };
            *sequence += i32::try_from(values.len()).expect("a small batch");
            produce_batch(&producer, topic_id, partition, &values, &shape).await;
            expected.entry(partition).or_default().extend(values);
        }
        // Let the flusher close a WAL object between rounds, so every
        // partition's history spans many objects.
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    for (partition, values) in &expected {
        let end = i64::try_from(values.len()).expect("a small partition");
        await_flushed(&source.handle, *partition, end).await;
    }
    let (backup_root, id) = capture(&source.bootstrap).await;
    let capture_path = backup_root
        .path()
        .join(capture_key(&id, DISKLESS_WAL_INDEX));
    let capture = DisklessWalCapture::from_slice(&std::fs::read(&capture_path).expect("capture"))
        .expect("decode the diskless capture");
    assert!(
        capture
            .partitions
            .iter()
            .all(|partition| partition.ranges.len() > 1),
        "every partition's history must span several WAL objects: {capture:?}"
    );
    drop(producer);
    source.handle.shutdown().await;

    // The source is lost; its object store is the archive.
    let checkpoint = backup_root.path().join("metadata.checkpoint");
    std::fs::write(&checkpoint, diskless_metadata_checkpoint(topic_id)).expect("write checkpoint");
    let target = TempDir::new().expect("restore target");
    let controller = bind_loopback().await;
    let controller_addr = controller.local_addr().expect("controller address");
    let report = restore(
        &Cli::parse_from([
            "restore".to_owned(),
            "--archive-local".to_owned(),
            source.object_store.path().display().to_string(),
            "--log-dir".to_owned(),
            target.path().display().to_string(),
            "--node-id".to_owned(),
            "1".to_owned(),
            "--standalone".to_owned(),
            "--controller-listener".to_owned(),
            controller_addr.to_string(),
            "--diskless-wal-capture".to_owned(),
            capture_path.display().to_string(),
            "--metadata-snapshot".to_owned(),
            checkpoint.display().to_string(),
        ])
        .args,
    )
    .await
    .expect("restore the diskless capture");
    let materialized: BTreeMap<i32, u64> = report
        .diskless
        .expect("a diskless restore report")
        .partitions
        .iter()
        .map(|partition| (partition.partition, partition.records))
        .collect();
    check!(
        materialized
            == expected
                .iter()
                .map(|(partition, values)| (*partition, values.len() as u64))
                .collect::<BTreeMap<_, _>>()
    );

    let restored = boot_diskless_broker(target, controller).await;
    let reader = client(&restored.bootstrap).await;
    let ends: BTreeMap<i32, i64> = expected
        .iter()
        .map(|(partition, values)| {
            (
                *partition,
                i64::try_from(values.len()).expect("a small partition"),
            )
        })
        .collect();
    let mut latest = BTreeMap::new();
    for (partition, end) in &ends {
        restored
            .handle
            .wait_until_partition_present(TOPIC, *partition)
            .await;
        // The qualification drill reads after the restored node has flushed
        // the materialized records into its own WAL and trimmed its local log
        // to them, so every read below the end is a WAL cold read.
        await_trimmed(&restored.handle, *partition, *end).await;
        latest.insert(*partition, latest_offset(&reader, *partition).await);
    }
    // A reader stops at the end `ListOffsets` gives it, as the qualification
    // drill's read-back and Kafka's `endOffsets` consumers do.
    check!(latest == ends);
    check!(consume_from_earliest(&restored.bootstrap, &ends).await == expected);

    // A restored partition takes new writes at its materialized end, as a
    // classic or tiered one does, and serves them after the restored records
    // (#1524). The drill's probe is this write: one batch, `acks=all`.
    let after_restore: BTreeMap<i32, i64> = {
        let mut assigned = BTreeMap::new();
        for partition in ends.keys() {
            let value = vec![format!("p{partition}-after-restore")];
            assigned.insert(
                *partition,
                produce_batch(
                    &reader,
                    topic_id,
                    *partition,
                    &value,
                    &RecordBatch::default(),
                )
                .await,
            );
            expected
                .get_mut(partition)
                .expect("a restored partition")
                .extend(value);
        }
        assigned
    };
    check!(after_restore == ends);
    let grown: BTreeMap<i32, i64> = ends
        .iter()
        .map(|(partition, end)| (*partition, end + 1))
        .collect();
    check!(consume_from_earliest(&restored.bootstrap, &grown).await == expected);

    drop(reader);
    restored.handle.shutdown().await;
}

krabka_macros::unix_millis_fixture!(
    test_wallclock_ms,
    "the test clock is after the epoch",
    "the test clock fits an i64"
);
