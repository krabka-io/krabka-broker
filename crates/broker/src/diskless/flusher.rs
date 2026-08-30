//! Diskless WAL object-store flusher.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use krabka_log::Offset;
use krabka_metadata::{MetadataImage, NodeId};
use object_store::ObjectStore;
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
pub(crate) async fn run(context: FlusherContext, config: FlushConfig, shutdown: CancellationToken) {
    let caught_up = tokio::select! {
        biased;
        () = shutdown.cancelled() => return,
        caught_up = context.index_log.wait_until_caught_up() => caught_up,
    };
    if !caught_up {
        tracing::error!(
            "diskless WAL index projection stopped before it caught up; flusher not starting"
        );
        return;
    }
    context.ready.store(true, Ordering::Release);
    let mut ticker = tokio::time::interval(config.interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut rotation = 0usize;
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            () = shutdown.cancelled() => {
                tracing::debug!("diskless WAL flusher shutting down");
                return;
            }
        }
        if let Err(error) = flush_tick(&context, &config, rotation).await {
            tracing::warn!(%error, "diskless WAL flush failed; retrying");
        }
        rotation = rotation.wrapping_add(1);
    }
}

async fn flush_tick(
    context: &FlusherContext,
    config: &FlushConfig,
    rotation: usize,
) -> Result<Option<WalFlushRecord>, crate::error::BrokerError> {
    let image = context.image_rx.borrow().clone();
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
    use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
    use tempfile::tempdir;

    use super::{test_support::test_partition, *};

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
        task.await.unwrap();
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

    /// A [`MetadataEventLog`] whose replay delivers one record per
    /// `replay_delay`. It stands in for the real gap between a restart and a
    /// caught-up projection, which on a populated index topic is many flush
    /// intervals wide.
    ///
    /// [`MetadataEventLog`]: krabka_remote_storage_topic::MetadataEventLog
    struct SlowReplayLog {
        inner: Arc<dyn krabka_remote_storage_topic::MetadataEventLog>,
        replay_delay: Duration,
    }

    #[async_trait::async_trait]
    impl krabka_remote_storage_topic::MetadataEventLog for SlowReplayLog {
        fn partition_count(&self) -> i32 {
            self.inner.partition_count()
        }

        async fn publish(
            &self,
            partition: i32,
            event: bytes::Bytes,
        ) -> Result<i64, krabka_remote_storage_topic::MetadataLogError> {
            self.inner.publish(partition, event).await
        }

        fn subscribe(
            &self,
            assignment: Vec<krabka_remote_storage_topic::PartitionStart>,
        ) -> (
            krabka_remote_storage_topic::MetadataEventStream,
            Arc<dyn krabka_remote_storage_topic::AssignmentHandle>,
        ) {
            use futures_util::StreamExt as _;

            let (stream, handle) = self.inner.subscribe(assignment);
            let replay_delay = self.replay_delay;
            let slow = stream.then(move |event| async move {
                tokio::time::sleep(replay_delay).await;
                event
            });
            (Box::pin(slow), handle)
        }

        async fn high_water_marks(
            &self,
        ) -> Result<Vec<i64>, krabka_remote_storage_topic::MetadataLogError> {
            self.inner.high_water_marks().await
        }
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
        task.await.unwrap();
        let flushed = object_keys(&store).await;
        assert!(flushed.len() == 1);

        // Restart: a fresh projection over the already-populated index topic,
        // replaying far more slowly than the flush interval.
        let index = DisklessIndexLog::start(Arc::new(SlowReplayLog {
            inner: event_log,
            replay_delay: Duration::from_millis(200),
        }))
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
        task.await.unwrap();

        // A tick during the replay re-uploads offsets 0..=2 under a second
        // key, orphaning the object the first incarnation wrote.
        assert!(object_keys(&store).await == flushed);
        // Readiness means the replay is complete, so the first tick sees
        // everything object storage holds.
        assert!(cache.lock().await.flushed_frontier(topic_id, 0) == Some(3));
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
        task.await.unwrap();

        assert!(cache.lock().await.flushed_frontier(topic_id, 1).is_none());
        assert!(cache.lock().await.flushed_frontier(topic_id, 2).is_none());
        let object_key = cache.lock().await.lookup(topic_id, 0, 0).unwrap().0;
        assert!(object_key.starts_with("diskless-wal/7/"));
        assert!(store.head(&Path::from(object_key)).await.is_ok());
    }
}
