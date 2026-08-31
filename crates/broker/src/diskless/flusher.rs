//! Diskless WAL object-store flusher.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use futures_util::TryStreamExt as _;
use krabka_log::Offset;
use krabka_metadata::{MetadataImage, NodeId};
use object_store::{ObjectStore, ObjectStoreExt as _};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{index_log::DisklessIndexLog, wal_index::WalFlushRecord};
use crate::{partition::Partition, partition_registry::PartitionRegistry};

mod config;
mod object_flush;
#[cfg(test)]
mod test_support;

#[cfg(any(test, feature = "test-helpers"))]
pub(crate) use self::object_flush::put_failure_count;
pub(crate) use self::{config::FlushConfig, object_flush::flush_once};

/// Every broker sweeps the shared prefix, so objects from removed brokers are
/// covered too. The observation grace lets independent index consumers apply
/// replacements before any broker deletes the old object.
const RECLAIM_INTERVAL: Duration = Duration::from_secs(30);
const RECLAIM_GRACE: Duration = Duration::from_mins(5);

pub(crate) struct FlushPartition {
    pub(crate) topic_id: Uuid,
    pub(crate) handle: Arc<Partition>,
    pub(crate) high_watermark: Offset,
}

/// Dependencies owned by the broker's diskless flush task.
pub(crate) struct FlusherContext {
    pub(crate) partitions: Arc<PartitionRegistry>,
    pub(crate) image_rx: tokio::sync::watch::Receiver<Arc<MetadataImage>>,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) index_log: DisklessIndexLog,
    pub(crate) node_id: NodeId,
    pub(crate) broker_id: i32,
    /// Set once the first tick is allowed to fire, which is after the index
    /// projection has replayed the index topic.
    pub(crate) ready: Arc<AtomicBool>,
}

/// Why [`run`] returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlusherExit {
    /// Shutdown was requested. The flusher is done.
    ShutDown,
    /// The index projection never replayed the index topic, so no tick ever
    /// fired. This is not recoverable on the index log the flusher was handed:
    /// the caller has to rebuild it. See
    /// [`DisklessIndexLog::wait_until_caught_up`].
    ReplayStalled,
}

/// Flush committed tails until broker shutdown. A failed tick does not move
/// the durable index frontier, so the next tick safely retries the same tail.
///
/// The first tick waits for the index projection to replay the index topic.
/// A tick that fires before that reads a frontier that is behind what object
/// storage already holds, re-uploads the flushed prefix under a fresh key,
/// and — because [`WalIndexCache::apply`] keys on `first_offset` — leaves the
/// object the previous incarnation wrote unreferenced forever.
///
/// [`WalIndexCache::apply`]: crate::diskless::wal_index::WalIndexCache::apply
pub(crate) async fn run(
    context: FlusherContext,
    config: FlushConfig,
    shutdown: CancellationToken,
) -> FlusherExit {
    let caught_up = tokio::select! {
        biased;
        () = shutdown.cancelled() => return FlusherExit::ShutDown,
        caught_up = context
            .index_log
            .wait_until_caught_up(config.index_projection_timeout) => caught_up,
    };
    if !caught_up {
        tracing::warn!(
            stall_timeout_ms = config.index_projection_timeout.as_millis(),
            "diskless WAL index replay made no progress; no flush has run"
        );
        return FlusherExit::ReplayStalled;
    }
    let mut reclaimer = Reclaimer::new(RECLAIM_GRACE);
    reclaimer.sweep(&context).await;
    context.ready.store(true, Ordering::Release);
    let mut ticker = tokio::time::interval(config.interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut reclaim_ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + RECLAIM_INTERVAL,
        RECLAIM_INTERVAL,
    );
    reclaim_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut rotation = 0usize;
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Err(error) = flush_tick(&context, &config, rotation).await {
                    tracing::warn!(%error, "diskless WAL flush failed; retrying");
                }
                rotation = rotation.wrapping_add(1);
            }
            _ = reclaim_ticker.tick() => reclaimer.sweep(&context).await,
            () = shutdown.cancelled() => {
                tracing::debug!("diskless WAL flusher shutting down");
                return FlusherExit::ShutDown;
            }
        }
    }
}

async fn flush_tick(
    context: &FlusherContext,
    config: &FlushConfig,
    rotation: usize,
) -> Result<Option<WalFlushRecord>, crate::error::BrokerError> {
    let image = context.image_rx.borrow().clone();
    tombstone_deleted_topics(context, &image).await?;
    let mut partitions = flushable_partitions(&context.partitions, &image, context.node_id).await;
    if !partitions.is_empty() {
        let start = rotation % partitions.len();
        partitions.rotate_left(start);
    }
    flush_once(
        Arc::clone(&context.object_store),
        context.broker_id,
        &context.index_log,
        context.index_log.cache(),
        &partitions,
        config,
    )
    .await
}

async fn tombstone_deleted_topics(
    context: &FlusherContext,
    image: &MetadataImage,
) -> Result<(), crate::error::BrokerError> {
    let topic_ids = context.index_log.cache().lock().await.topic_ids();
    for topic_id in topic_ids {
        if image.topic_by_id(&topic_id).is_none() {
            context.index_log.tombstone_topic(topic_id).await?;
        }
    }
    Ok(())
}

struct Reclaimer {
    first_seen: HashMap<String, Instant>,
    grace: Duration,
}

impl Reclaimer {
    fn new(grace: Duration) -> Self {
        Self {
            first_seen: HashMap::new(),
            grace,
        }
    }

    async fn sweep(&mut self, context: &FlusherContext) {
        self.sweep_at(context, Instant::now()).await;
    }

    async fn sweep_at(&mut self, context: &FlusherContext, now: Instant) {
        let referenced = context.index_log.cache().lock().await.referenced_objects();
        let prefix = object_store::path::Path::from("diskless-wal");
        let objects = match context
            .object_store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
        {
            Ok(objects) => objects,
            Err(error) => {
                tracing::warn!(%error, "diskless WAL reclaim listing failed; retrying");
                return;
            }
        };
        let mut unreferenced = HashSet::new();
        for object in objects {
            let object_key = object.location.to_string();
            if referenced.contains(&object_key) {
                self.first_seen.remove(&object_key);
                continue;
            }
            unreferenced.insert(object_key.clone());
            let first_seen = *self.first_seen.entry(object_key.clone()).or_insert(now);
            if now.duration_since(first_seen) < self.grace {
                continue;
            }
            match context.object_store.delete(&object.location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                    self.first_seen.remove(&object_key);
                }
                Err(error) => {
                    tracing::warn!(%object_key, %error, "diskless WAL reclaim failed; retrying");
                }
            }
        }
        self.first_seen
            .retain(|object_key, _| unreferenced.contains(object_key));
    }
}

async fn flushable_partitions(
    registry: &PartitionRegistry,
    image: &MetadataImage,
    node_id: NodeId,
) -> Vec<FlushPartition> {
    // Snapshot registry handles before awaiting any partition state.
    let mut out = Vec::new();
    for handle in registry.arcs() {
        if !handle.diskless || handle.current_leader.load(Ordering::Relaxed) != node_id {
            continue;
        }
        let Some(topic_id) = image.topic(&handle.topic).map(|topic| topic.topic_id) else {
            continue;
        };
        let high_watermark = handle.high_watermark().await;
        out.push(FlushPartition {
            topic_id,
            handle,
            high_watermark,
        });
    }
    out.sort_unstable_by(|left, right| {
        left.handle
            .topic
            .cmp(&right.handle.topic)
            .then_with(|| left.handle.index.cmp(&right.handle.index))
    });
    out
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;
    use krabka_metadata::{MetadataRecord, TopicRecord};
    use krabka_units::{ByteSize, convert::ByteSizeExt as _};
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path};
    use tempfile::tempdir;

    use super::{test_support::test_partition, *};
    use crate::diskless::index_log::test_support::{PacedReplayLog, ReplayPace};

    #[tokio::test]
    async fn tick_rotates_size_limited_flush_start() {
        let dir = tempdir().unwrap();
        let first = test_partition(dir.path(), "orders", 0, true, NodeId(1));
        let second = test_partition(dir.path(), "orders", 1, true, NodeId(1));
        let partitions = Arc::new(PartitionRegistry::new());
        partitions.insert("orders".into(), krabka_ids::PartitionIndex(0), first);
        partitions.insert("orders".into(), krabka_ids::PartitionIndex(1), second);

        let topic_id = Uuid::from_u128(11);
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id,
            partitions: 2,
            replication_factor: 1,
        }));
        let (_, image_rx) = tokio::sync::watch::channel(Arc::new(image));
        let index = DisklessIndexLog::start(
            krabka_remote_storage_topic::InProcessMetadataEventLog::new(1),
        )
        .await
        .unwrap();
        let cache = index.cache();
        let context = FlusherContext {
            partitions,
            image_rx,
            object_store: Arc::new(InMemory::new()),
            index_log: index,
            node_id: NodeId(1),
            broker_id: 7,
            ready: Arc::new(AtomicBool::new(false)),
        };

        flush_tick(
            &context,
            &FlushConfig {
                max_size: ByteSize::from_bytes(1),
                trim_safety_lag: None,
                ..FlushConfig::default()
            },
            1,
        )
        .await
        .unwrap();

        let cache = cache.lock().await;
        assert!(cache.flushed_frontier(topic_id, 0).is_none());
        assert!(cache.flushed_frontier(topic_id, 1) == Some(3));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_rotates_size_limited_flushes_without_starvation() {
        let dir = tempdir().unwrap();
        let first = test_partition(dir.path(), "orders", 0, true, NodeId(1));
        let second = test_partition(dir.path(), "orders", 1, true, NodeId(1));
        let partitions = Arc::new(PartitionRegistry::new());
        partitions.insert("orders".into(), krabka_ids::PartitionIndex(0), first);
        partitions.insert("orders".into(), krabka_ids::PartitionIndex(1), second);

        let topic_id = Uuid::from_u128(11);
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id,
            partitions: 2,
            replication_factor: 1,
        }));
        let (_, image_rx) = tokio::sync::watch::channel(Arc::new(image));
        let index = DisklessIndexLog::start(
            krabka_remote_storage_topic::InProcessMetadataEventLog::new(1),
        )
        .await
        .unwrap();
        let cache = index.cache();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            FlusherContext {
                partitions,
                image_rx,
                object_store: Arc::new(InMemory::new()),
                index_log: index,
                node_id: NodeId(1),
                broker_id: 7,
                ready: Arc::new(AtomicBool::new(false)),
            },
            FlushConfig {
                interval: Duration::from_millis(1),
                max_size: ByteSize::from_bytes(1),
                trim_safety_lag: None,
                ..FlushConfig::default()
            },
            shutdown.clone(),
        ));

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let cache = cache.lock().await;
                if cache.flushed_frontier(topic_id, 0) == Some(3)
                    && cache.flushed_frontier(topic_id, 1) == Some(3)
                {
                    break;
                }
                drop(cache);
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        shutdown.cancel();
        assert!(task.await.unwrap() == FlusherExit::ShutDown);
    }

    /// Every object key the store holds, sorted.
    async fn object_keys(store: &Arc<dyn ObjectStore>) -> Vec<String> {
        use futures_util::TryStreamExt as _;

        let mut keys: Vec<String> = store
            .list(None)
            .map_ok(|meta| meta.location.to_string())
            .try_collect()
            .await
            .unwrap();
        keys.sort();
        keys
    }

    async fn wait_for_object(
        cache: &Arc<tokio::sync::Mutex<crate::diskless::wal_index::WalIndexCache>>,
        topic_id: Uuid,
        object_key: &str,
    ) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if cache
                    .lock()
                    .await
                    .lookup(topic_id, 0, 0)
                    .is_some_and(|(key, _, _)| key == object_key)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn reclaim_grace_then_deletes_a_superseded_object_from_another_broker() {
        let topic_id = Uuid::from_u128(11);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for key in ["diskless-wal/9/old.ckwl", "diskless-wal/7/new.ckwl"] {
            store
                .put(&Path::from(key), PutPayload::from_static(b"object"))
                .await
                .unwrap();
        }
        let index = DisklessIndexLog::start(
            krabka_remote_storage_topic::InProcessMetadataEventLog::new(1),
        )
        .await
        .unwrap();
        let cache = index.cache();
        for key in ["diskless-wal/9/old.ckwl", "diskless-wal/7/new.ckwl"] {
            index
                .publish_flush(&WalFlushRecord {
                    object_key: key.into(),
                    format_version: 1,
                    entries: vec![crate::diskless::wal_index::WalIndexEntry {
                        topic_id,
                        partition: 0,
                        first_offset: 0,
                        last_offset: 2,
                        byte_start: 0,
                        byte_len: 6,
                    }],
                })
                .await
                .unwrap();
            wait_for_object(&cache, topic_id, key).await;
        }
        let (_, image_rx) = tokio::sync::watch::channel(Arc::new(MetadataImage::new(Uuid::nil())));
        let context = FlusherContext {
            partitions: Arc::new(PartitionRegistry::new()),
            image_rx,
            object_store: Arc::clone(&store),
            index_log: index,
            node_id: NodeId(1),
            broker_id: 7,
            ready: Arc::new(AtomicBool::new(false)),
        };

        let mut reclaimer = Reclaimer::new(Duration::from_mins(1));
        let observed = Instant::now();
        reclaimer.sweep_at(&context, observed).await;
        assert!(
            store
                .head(&Path::from("diskless-wal/9/old.ckwl"))
                .await
                .is_ok(),
            "the grace period protects lagging projections"
        );
        reclaimer
            .sweep_at(&context, observed + Duration::from_mins(1))
            .await;

        assert!(
            store
                .head(&Path::from("diskless-wal/9/old.ckwl"))
                .await
                .is_err()
        );
        assert!(
            store
                .head(&Path::from("diskless-wal/7/new.ckwl"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn deleted_topic_leaves_the_cache_and_object_store() {
        let topic_id = Uuid::from_u128(11);
        let object_key = "diskless-wal/7/deleted.ckwl";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(&Path::from(object_key), PutPayload::from_static(b"object"))
            .await
            .unwrap();
        let index = DisklessIndexLog::start(
            krabka_remote_storage_topic::InProcessMetadataEventLog::new(1),
        )
        .await
        .unwrap();
        let cache = index.cache();
        index
            .publish_flush(&WalFlushRecord {
                object_key: object_key.into(),
                format_version: 1,
                entries: vec![crate::diskless::wal_index::WalIndexEntry {
                    topic_id,
                    partition: 0,
                    first_offset: 0,
                    last_offset: 2,
                    byte_start: 0,
                    byte_len: 6,
                }],
            })
            .await
            .unwrap();
        wait_for_object(&cache, topic_id, object_key).await;
        let image = MetadataImage::new(Uuid::nil());
        let (_, image_rx) = tokio::sync::watch::channel(Arc::new(image.clone()));
        let context = FlusherContext {
            partitions: Arc::new(PartitionRegistry::new()),
            image_rx,
            object_store: Arc::clone(&store),
            index_log: index,
            node_id: NodeId(1),
            broker_id: 7,
            ready: Arc::new(AtomicBool::new(false)),
        };

        tombstone_deleted_topics(&context, &image).await.unwrap();
        Reclaimer::new(Duration::ZERO).sweep(&context).await;

        assert!(cache.lock().await.lookup(topic_id, 0, 0).is_none());
        assert!(store.head(&Path::from(object_key)).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restarted_worker_does_not_reupload_an_already_flushed_range() {
        let dir = tempdir().unwrap();
        let partitions = Arc::new(PartitionRegistry::new());
        partitions.insert(
            "orders".into(),
            krabka_ids::PartitionIndex(0),
            test_partition(dir.path(), "orders", 0, true, NodeId(1)),
        );

        let topic_id = Uuid::from_u128(11);
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id,
            partitions: 1,
            replication_factor: 1,
        }));
        let (_, image_rx) = tokio::sync::watch::channel(Arc::new(image));
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let event_log: Arc<dyn krabka_remote_storage_topic::MetadataEventLog> =
            krabka_remote_storage_topic::InProcessMetadataEventLog::new(1);
        // Retain the flushed prefix locally, so a projection that has not
        // caught up reads offset 0 as the flush start and re-uploads it.
        let config = FlushConfig {
            interval: Duration::from_millis(1),
            trim_safety_lag: None,
            ..FlushConfig::default()
        };

        let index = DisklessIndexLog::start(Arc::clone(&event_log))
            .await
            .unwrap();
        let cache = index.cache();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            FlusherContext {
                partitions: Arc::clone(&partitions),
                image_rx: image_rx.clone(),
                object_store: Arc::clone(&store),
                index_log: index,
                node_id: NodeId(1),
                broker_id: 7,
                ready: Arc::new(AtomicBool::new(false)),
            },
            config.clone(),
            shutdown.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if cache.lock().await.flushed_frontier(topic_id, 0) == Some(3) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        shutdown.cancel();
        assert!(task.await.unwrap() == FlusherExit::ShutDown);
        let flushed = object_keys(&store).await;
        assert!(flushed.len() == 1);

        // Restart: a fresh projection over the already-populated index topic,
        // replaying far more slowly than the flush interval.
        let index = DisklessIndexLog::start(PacedReplayLog::new(
            event_log,
            ReplayPace::OneEvery(Duration::from_millis(200)),
        ))
        .await
        .unwrap();
        let cache = index.cache();
        let ready = Arc::new(AtomicBool::new(false));
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            FlusherContext {
                partitions,
                image_rx,
                object_store: Arc::clone(&store),
                index_log: index,
                node_id: NodeId(1),
                broker_id: 7,
                ready: Arc::clone(&ready),
            },
            config,
            shutdown.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !ready.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the restarted flusher becomes ready once the replay finishes");
        // Well inside the replay delay, so a flusher that ticked without
        // waiting has already landed its object by now.
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.cancel();
        assert!(task.await.unwrap() == FlusherExit::ShutDown);

        // A tick during the replay re-uploads offsets 0..=2 under a second
        // key, orphaning the object the first incarnation wrote.
        assert!(object_keys(&store).await == flushed);
        // Readiness means the replay is complete, so the first tick sees
        // everything object storage holds.
        assert!(cache.lock().await.flushed_frontier(topic_id, 0) == Some(3));
    }

    /// A flusher over one led diskless partition whose index topic already
    /// holds a flush record, but whose replay never delivers -- the shape a
    /// dead partition fetch loop leaves behind, with the stream open and
    /// silent. Returns the context and the object store behind it.
    async fn silent_replay_flusher(
        dir: &std::path::Path,
        ready: Arc<AtomicBool>,
    ) -> (FlusherContext, Arc<dyn ObjectStore>) {
        let partitions = Arc::new(PartitionRegistry::new());
        partitions.insert(
            "orders".into(),
            krabka_ids::PartitionIndex(0),
            test_partition(dir, "orders", 0, true, NodeId(1)),
        );

        let topic_id = Uuid::from_u128(11);
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id,
            partitions: 1,
            replication_factor: 1,
        }));
        let (_, image_rx) = tokio::sync::watch::channel(Arc::new(image));
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let event_log: Arc<dyn krabka_remote_storage_topic::MetadataEventLog> =
            krabka_remote_storage_topic::InProcessMetadataEventLog::new(1);
        let seed = DisklessIndexLog::start(Arc::clone(&event_log))
            .await
            .unwrap();
        seed.publish_flush(&WalFlushRecord {
            object_key: "diskless-wal/7/seed.ckwl".into(),
            format_version: 1,
            entries: vec![crate::diskless::wal_index::WalIndexEntry {
                topic_id,
                partition: 0,
                first_offset: 0,
                last_offset: 2,
                byte_start: 0,
                byte_len: 10,
            }],
        })
        .await
        .unwrap();

        let index = DisklessIndexLog::start(PacedReplayLog::new(event_log, ReplayPace::Never))
            .await
            .unwrap();
        (
            FlusherContext {
                partitions,
                image_rx,
                object_store: Arc::clone(&store),
                index_log: index,
                node_id: NodeId(1),
                broker_id: 7,
                ready,
            },
            store,
        )
    }

    fn stall_after(timeout: Duration) -> FlushConfig {
        FlushConfig {
            interval: Duration::from_millis(1),
            index_projection_timeout: timeout,
            trim_safety_lag: None,
            ..FlushConfig::default()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_reports_a_stalled_replay_instead_of_flushing_or_hanging() {
        let dir = tempdir().unwrap();
        let ready = Arc::new(AtomicBool::new(false));
        let (context, store) = silent_replay_flusher(dir.path(), Arc::clone(&ready)).await;

        let exit = tokio::time::timeout(
            Duration::from_secs(5),
            run(
                context,
                stall_after(Duration::from_millis(50)),
                CancellationToken::new(),
            ),
        )
        .await
        .expect("a stalled replay must return, not hang the flusher forever");

        // The bootstrap needs this to rebuild the index log; a fetch loop that
        // died on connect never recovers on its own subscription.
        assert!(exit == FlusherExit::ReplayStalled);
        assert!(!ready.load(Ordering::Acquire));
        assert!(object_keys(&store).await.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_stops_at_shutdown_without_finishing_the_replay() {
        let dir = tempdir().unwrap();
        let ready = Arc::new(AtomicBool::new(false));
        let (context, store) = silent_replay_flusher(dir.path(), Arc::clone(&ready)).await;

        // Shutdown during a replay must not wait it out: the stall window is
        // far longer than this test would tolerate.
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let exit = tokio::time::timeout(
            Duration::from_secs(1),
            run(context, stall_after(Duration::from_mins(1)), shutdown),
        )
        .await
        .expect("shutdown during a replay returns promptly");

        assert!(exit == FlusherExit::ShutDown);
        assert!(!ready.load(Ordering::Acquire));
        assert!(object_keys(&store).await.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_flushes_only_led_diskless_partitions_and_stops() {
        let dir = tempdir().unwrap();
        let led = test_partition(dir.path(), "orders", 0, true, NodeId(1));
        let follower = test_partition(dir.path(), "orders", 1, true, NodeId(2));
        let local = test_partition(dir.path(), "orders", 2, false, NodeId(1));
        let partitions = Arc::new(PartitionRegistry::new());
        partitions.insert("orders".into(), krabka_ids::PartitionIndex(0), led);
        partitions.insert("orders".into(), krabka_ids::PartitionIndex(1), follower);
        partitions.insert("orders".into(), krabka_ids::PartitionIndex(2), local);

        let topic_id = Uuid::from_u128(11);
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id,
            partitions: 3,
            replication_factor: 1,
        }));
        let (_, image_rx) = tokio::sync::watch::channel(Arc::new(image));
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let index = DisklessIndexLog::start(
            krabka_remote_storage_topic::InProcessMetadataEventLog::new(1),
        )
        .await
        .unwrap();
        let cache = index.cache();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            FlusherContext {
                partitions,
                image_rx,
                object_store: Arc::clone(&store),
                index_log: index,
                node_id: NodeId(1),
                broker_id: 7,
                ready: Arc::new(AtomicBool::new(false)),
            },
            FlushConfig {
                interval: Duration::from_millis(1),
                trim_safety_lag: None,
                ..FlushConfig::default()
            },
            shutdown.clone(),
        ));

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if cache.lock().await.flushed_frontier(topic_id, 0) == Some(3) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        shutdown.cancel();
        assert!(task.await.unwrap() == FlusherExit::ShutDown);

        assert!(cache.lock().await.flushed_frontier(topic_id, 1).is_none());
        assert!(cache.lock().await.flushed_frontier(topic_id, 2).is_none());
        let object_key = cache.lock().await.lookup(topic_id, 0, 0).unwrap().0;
        assert!(object_key.starts_with("diskless-wal/7/"));
        assert!(store.head(&Path::from(object_key)).await.is_ok());
    }
}
