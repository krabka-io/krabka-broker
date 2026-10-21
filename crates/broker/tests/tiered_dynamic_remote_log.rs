//! KIP-956's remote-log quotas, altered on a running broker.
//!
//! Kafka 4.3.1 applies `remote.log.manager.fetch.max.bytes.per.second` and
//! `remote.log.manager.copy.max.bytes.per.second` without a restart, through
//! `DynamicRemoteLogConfig.reconfigure`. Each test boots one broker over a
//! local archive, alters the key through `IncrementalAlterConfigs`, and
//! watches the tier respond:
//!
//! * Over the fetch quota, a consumer fetch of an offset only the archive holds
//!   comes back as `ReplicaManager.handleOffsetOutOfRangeError`'s throttled
//!   row -- no error, no records, the live bounds -- and raising the quota
//!   serves the records again.
//! * Over the copy quota, `RLMCopyTask` uploads one segment and holds the rest
//!   back, and raising the quota lets the held segments through.

use assert2::{assert, check};
mod support;

use {
    krabka_broker::{BrokerHandle, RemoteStorageBackend, RlmmKind},
    krabka_client_core::Client,
    krabka_protocol::{owned::fetch_response::PartitionData, primitives::uuid::Uuid as WireUuid},
    std::{
        path::Path,
        time::{Duration, Instant},
    },
    support::{
        InProcess,
        configs::{ConfigMutation, ConfigOperation, broker_alter_request},
        fetch::{FetchLimits, RequestWaitMillis, fetch_partition, single_partition_fetch},
        start_configured,
        storage::{local_segment_bases, topic_remote_log_files},
        tiered::{await_tiered_replicas, create_configured_topic, tiered_configs},
        topic_id_for,
    },
    tempfile::TempDir,
};

/// Kafka's `remote.log.manager.fetch.max.bytes.per.second`.
const FETCH_QUOTA: &str = "remote.log.manager.fetch.max.bytes.per.second";
/// Kafka's `remote.log.manager.copy.max.bytes.per.second`.
const COPY_QUOTA: &str = "remote.log.manager.copy.max.bytes.per.second";
/// Records produced into each test's topic, one batch each. A 1 KiB segment
/// holds about a dozen of them, so the log seals several segments. Also the
/// high watermark once they are all committed.
const RECORDS: i64 = 80;

/// One broker over a local archive, and the directories behind both.
struct TieredBroker {
    broker: BrokerHandle,
    client: Client,
    remote_dir: TempDir,
    log_dir: TempDir,
}

async fn start_tiered_broker() -> TieredBroker {
    support::init_tracing();
    let remote_dir = tempfile::tempdir().expect("remote tempdir");
    let archive = remote_dir.path().to_path_buf();
    let InProcess {
        broker,
        client,
        _tempdir: log_dir,
    } = start_configured(move |config| {
        config.remote_storage_backend = Some(RemoteStorageBackend::Local { dir: archive });
        config.remote_log_metadata = RlmmKind::InMemory;
        // The copy and local-retention passes run on this tick.
        config.remote_log_manager_interval = krabka_units::secs(1);
    })
    .await;
    TieredBroker {
        broker,
        client,
        remote_dir,
        log_dir,
    }
}

/// Alters `key` on broker `broker`'s own resource: set to `value`, or deleted
/// back to its default when `value` is `None`.
async fn alter_broker_config(tiered: &TieredBroker, key: &str, value: Option<&str>) {
    let operation = if value.is_some() {
        ConfigOperation::Set
    } else {
        ConfigOperation::Delete
    };
    let response = tiered
        .client
        .send(broker_alter_request(
            tiered.broker.node_id().to_string(),
            vec![(key, operation, value)],
            ConfigMutation::Apply,
        ))
        .await
        .expect("IncrementalAlterConfigs");
    assert!(
        response.responses[0].error_code == 0,
        "altering {key} failed: {:?}",
        response.responses[0].error_message
    );
}

/// Creates a tiered topic with 1 KiB segments, waits for its config to reach
/// the partition, and produces [`RECORDS`] records into it.
async fn tiered_topic_with_records(tiered: &TieredBroker, topic: &str) -> WireUuid {
    create_configured_topic(&tiered.client, topic, tiered_configs(Some("1024"))).await;
    await_tiered_replicas(
        &[&tiered.broker],
        topic,
        Some(krabka_units::kibibytes(1)),
        "the tiered configuration never reached the partition",
    )
    .await;
    tiered
        .broker
        .produce_records_for_test(topic, 0, usize::try_from(RECORDS).expect("record count"))
        .await
        .expect("produce records");
    topic_id_for(&tiered.client, topic).await
}

/// Waits until local retention has evicted offset 0, so only the archive
/// holds it.
async fn await_offset_zero_only_in_the_archive(tiered: &TieredBroker, topic: &str) {
    let partition_dir = tiered.log_dir.path().join(format!("{topic}-0"));
    let deadline = Instant::now() + Duration::from_mins(1);
    // intentional: tiering and local retention run on the remote-log
    // manager's timer and have no awaitable signal, so poll the directory.
    loop {
        let bases = local_segment_bases(&partition_dir);
        if bases.first().is_some_and(|base| *base > 0) {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "offset 0 was never evicted to the archive: {bases:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// What a fetch of one partition answered.
#[derive(Debug, PartialEq, Eq)]
struct ColdRow {
    error_code: i16,
    high_watermark: i64,
    last_stable_offset: i64,
    log_start_offset: i64,
    aborted_transactions: Option<usize>,
    first_offset: Option<i64>,
}

impl ColdRow {
    fn of(partition: &PartitionData) -> Self {
        Self {
            error_code: partition.error_code,
            high_watermark: partition.high_watermark,
            last_stable_offset: partition.last_stable_offset,
            log_start_offset: partition.log_start_offset,
            aborted_transactions: partition.aborted_transactions.as_ref().map(Vec::len),
            first_offset: partition
                .records
                .as_ref()
                .and_then(|records| records.as_v2())
                .and_then(|batches| batches.first())
                .map(|batch| batch.base_offset),
        }
    }

    /// The archive served offset 0.
    const fn served() -> Self {
        Self {
            first_offset: Some(0),
            ..Self::throttled()
        }
    }

    /// `handleOffsetOutOfRangeError`'s throttled row: no error and no
    /// records, the live bounds, and no aborted-transaction list.
    const fn throttled() -> Self {
        Self {
            error_code: 0,
            high_watermark: RECORDS,
            last_stable_offset: RECORDS,
            log_start_offset: 0,
            aborted_transactions: None,
            first_offset: None,
        }
    }
}

/// Fetches offset 0 as a consumer.
async fn fetch_offset_zero(tiered: &TieredBroker, topic: &str, topic_id: WireUuid) -> ColdRow {
    let response = tiered
        .client
        .send(
            single_partition_fetch()
                .topic(topic.into())
                .topic_id(topic_id)
                .partition(fetch_partition().offset(0).call())
                .limits(FetchLimits::wait_for_data(RequestWaitMillis(100)))
                .call(),
        )
        .await
        .expect("Fetch");
    ColdRow::of(&response.responses[0].partitions[0])
}

/// Fetches offset 0 until the row is `want`.
async fn await_cold_row(tiered: &TieredBroker, topic: &str, topic_id: WireUuid, want: &ColdRow) {
    let deadline = Instant::now() + Duration::from_secs(30);
    // intentional: the alter reaches the broker's image asynchronously, and
    // the quota has no awaitable signal, so drive the fetch until it answers.
    loop {
        let row = fetch_offset_zero(tiered, topic, topic_id).await;
        if row == *want {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "offset 0 kept answering {row:?}, not {want:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_remote_fetch_quota_holds_back_cold_reads_until_it_is_raised() {
    const TOPIC: &str = "tiered-fetch-quota";
    let tiered = start_tiered_broker().await;
    let topic_id = tiered_topic_with_records(&tiered, TOPIC).await;
    await_offset_zero_only_in_the_archive(&tiered, TOPIC).await;
    check!(fetch_offset_zero(&tiered, TOPIC, topic_id).await == ColdRow::served());

    // One byte a second: the batch just read is far over it, so the next read
    // of the archive is skipped.
    alter_broker_config(&tiered, FETCH_QUOTA, Some("1")).await;
    await_cold_row(&tiered, TOPIC, topic_id, &ColdRow::throttled()).await;
    check!(fetch_offset_zero(&tiered, TOPIC, topic_id).await == ColdRow::throttled());

    // Back to Kafka's unlimited default: the same bytes are under it.
    alter_broker_config(&tiered, FETCH_QUOTA, None).await;
    await_cold_row(&tiered, TOPIC, topic_id, &ColdRow::served()).await;

    tiered.broker.shutdown().await;
}

/// How many segments of `topic` the archive under `root` holds.
fn archived_segments(root: &Path, topic: &str) -> usize {
    topic_remote_log_files(root, topic).len()
}

/// Waits until the local log has sealed at least `wanted` segments' worth of
/// records past the active one, so the copy pass has them to upload.
async fn await_archived_segments(root: &Path, topic: &str, wanted: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    // intentional: the copy pass runs on the remote-log manager's timer and the
    // archive is a directory, so poll it.
    while archived_segments(root, topic) < wanted {
        assert!(
            Instant::now() <= deadline,
            "the archive holds {} segment(s) of {topic}, wanted {wanted}",
            archived_segments(root, topic)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_remote_copy_quota_holds_back_uploads_until_it_is_raised() {
    const TOPIC: &str = "tiered-copy-quota";
    let tiered = start_tiered_broker().await;
    // One byte a second, set before the topic exists so the first copy pass
    // already runs under it.
    alter_broker_config(&tiered, COPY_QUOTA, Some("1")).await;
    tiered_topic_with_records(&tiered, TOPIC).await;

    // The first segment goes up at once and is counted; it alone is a rate
    // hundreds of times the bound, so every later segment waits. Five copy
    // ticks would upload every sealed segment without the quota.
    await_archived_segments(tiered.remote_dir.path(), TOPIC, 1).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    check!(archived_segments(tiered.remote_dir.path(), TOPIC) == 1);

    // Back to Kafka's unlimited default: the held segments go up.
    alter_broker_config(&tiered, COPY_QUOTA, None).await;
    await_archived_segments(tiered.remote_dir.path(), TOPIC, 3).await;

    tiered.broker.shutdown().await;
}
