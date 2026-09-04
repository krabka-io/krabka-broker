//! The tiered sweep against an object store that misbehaves.
//!
//! Every other suite in this module drives a backend that answers. A real
//! object store throttles, and it stalls, and the two fail differently: a
//! `503 SlowDown` surfaces as an error the copy path already knows how to
//! roll back, while a stall surfaces as nothing at all until something
//! bounds it. These cases run the production S3 backend --
//! [`krabka_remote_storage::S3RemoteStorage`] over a real
//! `ObjectStoreClient` -- on top of an in-memory store wrapped in
//! [`krabka_object_store::fault::FaultInjectingStore`], so the copy travels
//! the whole path it travels in production and only the store misbehaves.
//!
//! What they pin is the three things an operator is promised when
//! `docs/operations/alert-rules.yaml` fires `KrabkaRemoteCopyErrors` or
//! `KrabkaRemoteCopyLagGrowing`: the failing copy is counted, the lag gauge
//! keeps saying the segment is still owed, and the local copy of a segment
//! whose remote copy did not finish is not deleted.
//!
//! Every case runs on a multi-threaded runtime, because the backend bridges
//! its async object-store calls with `block_in_place`, which a current-thread
//! runtime refuses.

use std::sync::Arc;

use assert2::{assert, check};
use krabka_ids::{LeaderEpoch, PartitionIndex};
use krabka_object_store::fault::{FaultInjectingStore, FaultKind, FaultPolicy, OpFault, StoreOp};
use krabka_remote_storage::{
    InmemoryRemoteLogMetadataManager, RemoteLogMetadataManager, RemoteLogSegmentState,
    RemoteStorageManager, S3RemoteStorage, TopicIdPartition,
};

use super::*;
use crate::{
    metrics::{BrokerMetrics, TopicLabel},
    remote_log_manager::{
        RemoteTier, copy_eligible, local_retention_pass,
        test_support::rolled_tiered_partition_with_config,
    },
};

/// The `orders` topic with `partitions` partitions, so a sweep can walk more
/// than one of them.
fn image_with_orders_partitions(partitions: i32) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::from_u128(9));
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: "orders".into(),
        topic_id: tp().topic_id,
        partitions,
        replication_factor: 1,
    }));
    image
}

/// How long a stalled store holds a call. Long enough that a copy under the
/// deadline below can only end at that deadline, short enough that the
/// abandoned blocking task drains while the test tears down.
const STALL: std::time::Duration = std::time::Duration::from_secs(2);

/// The copy deadline the stall cases run under.
const SHORT_COPY_DEADLINE: Time = millis(150);

/// The production S3 backend over `store`, so a fault drill exercises the
/// same `put_from_path` / multipart / key-layout code a real copy does.
fn backend(store: Arc<FaultInjectingStore>) -> Arc<dyn RemoteStorageManager> {
    Arc::new(S3RemoteStorage::with_store(store, None))
}

/// An in-memory store wrapped in `policy`.
fn faulty_store(policy: FaultPolicy) -> Arc<FaultInjectingStore> {
    krabka_object_store::fault::build_faulty_object_store(
        &krabka_object_store::ObjectStoreConfig::InMemory,
        policy,
    )
    .expect("the in-memory store always builds")
}

/// A store that answers every upload with `503 SlowDown`.
fn throttling_store() -> Arc<FaultInjectingStore> {
    faulty_store(FaultPolicy::none().with(StoreOp::Put, OpFault::always(FaultKind::Throttled)))
}

/// A store that accepts every upload and answers it [`STALL`] late.
fn stalling_store() -> Arc<FaultInjectingStore> {
    faulty_store(FaultPolicy::none().with(StoreOp::Put, OpFault::stall(STALL)))
}

/// A tiered `orders-0` with segments already sealed on disk, plus its
/// exports and log config.
fn tiered_partition(
    log_dir: &std::path::Path,
) -> (Arc<Partition>, Vec<krabka_log::SegmentExport>, LogConfig) {
    let partition = rolled_tiered_partition_with_config(
        log_dir,
        LogConfig {
            segment_size: bytes(256),
            remote_storage_enable: true,
            local_retention: Some(millis(1)),
            ..LogConfig::default()
        },
    );
    let (exports, config) = {
        let log = partition.log.lock().expect("partition log mutex poisoned");
        (log.tierable_segments(), log.config_snapshot())
    };
    assert!(exports.len() >= 2, "the fixture needs sealed segments");
    (partition, exports, config)
}

fn orders_label() -> TopicLabel {
    TopicLabel {
        topic: Arc::from(tp().topic.as_str()),
    }
}

/// A tier over `rsm` with its own metrics, so each case reads counters only
/// it moved.
fn faulty_tier<'a>(
    rsm: &'a Arc<dyn RemoteStorageManager>,
    rlmm: &'a Arc<dyn RemoteLogMetadataManager>,
    metrics: &'a BrokerMetrics,
    index_cache: &'a Arc<krabka_remote_storage::RemoteIndexCache>,
    copy_timeout: Time,
) -> RemoteTier<'a> {
    RemoteTier {
        archive: ArchiveMode::Mutable,
        rsm,
        rlmm,
        metrics,
        index_cache,
        copy_timeout,
    }
}

/// The control. The same fixture over a store with no faults copies every
/// segment and finishes it, so the refusals below are the store's doing and
/// not the fixture's.
#[tokio::test(flavor = "multi_thread")]
async fn a_healthy_store_finishes_every_copy() {
    let log_dir = tempfile::tempdir().unwrap();
    let (_partition, exports, _config) = tiered_partition(log_dir.path());
    let rsm = backend(faulty_store(FaultPolicy::none()));
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    let metrics = BrokerMetrics::new();
    let index_cache = Arc::new(krabka_remote_storage::RemoteIndexCache::disabled());

    let copied = copy_eligible(
        &faulty_tier(
            &rsm,
            &rlmm,
            &metrics,
            &index_cache,
            crate::remote_log_manager::test_support::TEST_COPY_TIMEOUT,
        ),
        &tp(),
        1,
        LeaderEpoch(0),
        exports.clone(),
    )
    .await;

    check!(copied == exports.len());
    let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
    check!(
        listed
            .iter()
            .all(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
    );
    check!(
        metrics
            .remote_copy_errors_total
            .get_or_create(&orders_label())
            .get()
            == 0
    );
}

/// A store answering `503 SlowDown` must leave nothing finished, count every
/// attempt as an error, and leave the copy lag standing at the full backlog.
/// Those are the three series `KrabkaRemoteCopyErrors` and
/// `KrabkaRemoteCopyLagGrowing` read.
#[tokio::test(flavor = "multi_thread")]
async fn a_throttling_store_finishes_nothing_and_moves_the_error_and_lag_series() {
    let log_dir = tempfile::tempdir().unwrap();
    let (_partition, exports, _config) = tiered_partition(log_dir.path());
    let store = throttling_store();
    let rsm = backend(Arc::clone(&store));
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    let metrics = BrokerMetrics::new();
    let index_cache = Arc::new(krabka_remote_storage::RemoteIndexCache::disabled());
    let tier = faulty_tier(
        &rsm,
        &rlmm,
        &metrics,
        &index_cache,
        crate::remote_log_manager::test_support::TEST_COPY_TIMEOUT,
    );

    let copied = copy_eligible(&tier, &tp(), 1, LeaderEpoch(0), exports.clone()).await;

    check!(copied == 0);
    check!(
        store.attempts(StoreOp::Put) > 0,
        "the store was never asked"
    );
    let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
    check!(
        listed
            .iter()
            .all(|md| md.state() != RemoteLogSegmentState::CopySegmentFinished),
        "a throttled copy reached CopySegmentFinished: {listed:?}"
    );
    let label = orders_label();
    let want = u64::try_from(exports.len()).unwrap();
    check!(metrics.remote_copy_errors_total.get_or_create(&label).get() == want);
    check!(metrics.remote_copy_bytes_total.get_or_create(&label).get() == 0);
    // The lag is what the round found waiting. Nothing was copied, so a
    // second round must still find all of it: this is the gauge whose
    // derivative `KrabkaRemoteCopyLagGrowing` alerts on.
    check!(
        metrics.remote_copy_lag_segments.get_or_create(&label).get()
            == i64::try_from(exports.len()).unwrap()
    );
    copy_eligible(&tier, &tp(), 1, LeaderEpoch(0), exports.clone()).await;
    check!(
        metrics.remote_copy_lag_segments.get_or_create(&label).get()
            == i64::try_from(exports.len()).unwrap()
    );
}

/// A store that accepts the upload and then answers [`STALL`] late must not
/// hold the sweep for that long: the copy deadline abandons it, and it is
/// counted as a failure like any other.
#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_copy_is_abandoned_at_its_deadline() {
    let log_dir = tempfile::tempdir().unwrap();
    let (_partition, exports, _config) = tiered_partition(log_dir.path());
    let store = stalling_store();
    let rsm = backend(Arc::clone(&store));
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    let metrics = BrokerMetrics::new();
    let index_cache = Arc::new(krabka_remote_storage::RemoteIndexCache::disabled());

    let started = std::time::Instant::now();
    let copied = copy_eligible(
        &faulty_tier(&rsm, &rlmm, &metrics, &index_cache, SHORT_COPY_DEADLINE),
        &tp(),
        1,
        LeaderEpoch(0),
        vec![exports[0].clone()],
    )
    .await;
    let elapsed = started.elapsed();

    check!(copied == 0);
    check!(
        elapsed < STALL,
        "a {SHORT_COPY_DEADLINE:?} deadline let a stalled copy run for {elapsed:?}"
    );
    check!(
        metrics
            .remote_copy_errors_total
            .get_or_create(&orders_label())
            .get()
            == 1
    );
    // Abandoned, not rolled back: the upload the deadline walked away from is
    // still running against this segment id, so the metadata stays in
    // `CopySegmentStarted` and the next tick re-copies under a fresh id.
    let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
    check!(listed.len() == 1);
    check!(listed[0].state() == RemoteLogSegmentState::CopySegmentStarted);
}

/// The copy pass is serial, so a partition whose copy stalls must not stop
/// the sweep from reaching the partitions behind it. Two tiered partitions,
/// one stalled store, one tick: the tick has to return, and the second
/// partition has to have been offered to the store.
#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_partition_does_not_stop_the_sweep_reaching_the_next() {
    let log_dir = tempfile::tempdir().unwrap();
    let partitions = Arc::new(PartitionRegistry::new());
    let first = tiered_partition(log_dir.path()).0;
    partitions.insert("orders".into(), PartitionIndex(0), first);
    let second_dir = tempfile::tempdir().unwrap();
    let second = tiered_partition(second_dir.path()).0;
    second.current_leader.store(1, Ordering::Relaxed);
    partitions.insert("orders".into(), PartitionIndex(1), second);

    let store = stalling_store();
    let rsm = backend(Arc::clone(&store));
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    let metrics = BrokerMetrics::new();
    let index_cache = Arc::new(krabka_remote_storage::RemoteIndexCache::disabled());
    let controller: Arc<dyn crate::metadata_source::MetadataSource> =
        Arc::new(fixed_source(image_with_orders_partitions(2)));

    let started = std::time::Instant::now();
    tick_all(
        &partitions,
        &*controller,
        &faulty_tier(&rsm, &rlmm, &metrics, &index_cache, SHORT_COPY_DEADLINE),
        NodeId(1),
        1,
    )
    .await;
    let elapsed = started.elapsed();

    check!(
        elapsed < STALL,
        "one stalled partition held the whole sweep for {elapsed:?}"
    );
    // Both partitions reached the store: had the sweep stopped at the first,
    // the second would have contributed no attempt at all.
    check!(
        store.attempts(StoreOp::Put) >= 2,
        "the sweep offered only {} upload(s) to the store",
        store.attempts(StoreOp::Put)
    );
    for index in [0, 1] {
        let listed = rlmm
            .list_remote_log_segments(&TopicIdPartition::new(tp().topic_id, "orders", index))
            .unwrap();
        check!(
            listed
                .iter()
                .all(|md| md.state() != RemoteLogSegmentState::CopySegmentFinished),
            "partition {index} finished a copy the store never completed"
        );
    }
}

/// The failure this issue exists to prevent: local retention deleting a
/// segment whose remote copy is not actually complete. Both misbehaving
/// stores are driven through the same assertion, against a retention clock
/// far enough ahead that every segment is otherwise expired.
#[tokio::test(flavor = "multi_thread")]
async fn local_retention_keeps_segments_whose_copy_never_finished() {
    /// One misbehaving store and the copy deadline it is driven under.
    type Case = (&'static str, fn() -> Arc<FaultInjectingStore>, Time);

    let cases: [Case; 2] = [
        (
            "throttled",
            throttling_store,
            crate::remote_log_manager::test_support::TEST_COPY_TIMEOUT,
        ),
        ("stalled", stalling_store, SHORT_COPY_DEADLINE),
    ];
    for (case, store, copy_timeout) in cases {
        let log_dir = tempfile::tempdir().unwrap();
        let (partition, exports, config) = tiered_partition(log_dir.path());
        let rsm = backend(store());
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let metrics = BrokerMetrics::new();
        let index_cache = Arc::new(krabka_remote_storage::RemoteIndexCache::disabled());

        let copied = copy_eligible(
            &faulty_tier(&rsm, &rlmm, &metrics, &index_cache, copy_timeout),
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
        )
        .await;
        check!(copied == 0, "{case}");

        // Far past every segment's `local.retention.ms`, so nothing but the
        // missing remote copy can be holding these bytes on disk.
        let removed = local_retention_pass(
            &tp(),
            &partition,
            &exports,
            &config,
            &rlmm,
            now_ms() + 1_000_000,
        );

        check!(removed == 0, "{case}");
        let log = partition.log.lock().expect("partition log mutex poisoned");
        check!(
            log.local_log_start_offset() == exports[0].base_offset,
            "{case}"
        );
        check!(log.tierable_segments().len() == exports.len(), "{case}");
    }
}
