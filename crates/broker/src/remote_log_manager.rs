//! `RemoteLogManager`: the KIP-405 tiered-storage copy path.
//!
//! Every `interval`, the manager walks the partition registry. For each
//! partition where this broker is the leader and the topic has
//! `remote.storage.enable=true`, it copies the partition's sealed log
//! segments that are not yet in the remote tier to a
//! [`RemoteStorageManager`]. It records each copy in a
//! [`RemoteLogMetadataManager`] (`CopySegmentStarted` →
//! `CopySegmentFinished`).
//!
//! This is the copy path. Their own modules implement local-retention deletion
//! of copied segments and the remote read path on `Fetch`. The
//! remote-storage SPIs are blocking, so each copy and each delete
//! runs on the `tokio` blocking pool.
//!
//! A KFC-9 write freeze splits the sweep in two. The copy runs on a frozen
//! topic, because it adds a replica and takes nothing away, and tiering a
//! frozen topic is what a migration wants. Both retention passes stop, because
//! each one removes data from the topic's log.

use std::{
    sync::{Arc, atomic::Ordering},
    time::{Duration, SystemTime},
};

use krabka_metadata::NodeId;
use krabka_remote_storage::{RemoteLogMetadataManager, RemoteStorageManager, TopicIdPartition};
use krabka_units::{ByteSize, Time, bytes, convert::TimeExt as _, secs};
use krabka_verified::FreezeMutationKind;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{
    freeze::resolve::{FreezeMutationResolution, resolve_freeze_mutation},
    partition::Partition,
    partition_registry::PartitionRegistry,
};

mod archive;
mod copy;
mod copy_segment;
mod delete;
mod leader_epoch;
mod local_retention;
mod remote_retention;
mod rlmm;
#[cfg(test)]
mod test_support;

pub(crate) use self::{
    archive::ArchiveMode,
    copy::copy_eligible,
    delete::cascade_remote_partition_delete,
    local_retention::local_retention_pass,
    remote_retention::{RemoteRetentionBounds, remote_retention_pass},
};

/// Default cadence of the tiered-storage sweep (copy and retention passes).
const DEFAULT_TIERING_INTERVAL: Time = secs(30);

/// The floor of every size-budget walk in this module.
const NO_BYTES: ByteSize = bytes(0);

/// Tunables for [`run`].
#[derive(Debug, Clone)]
pub(crate) struct RemoteLogManagerConfig {
    pub interval: Time,
}

impl Default for RemoteLogManagerConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_TIERING_INTERVAL,
        }
    }
}

pub(crate) struct RemoteLogManagerContext {
    pub partitions: Arc<PartitionRegistry>,
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
    /// Whether `rsm` is a write-once archive. It gates every delete this
    /// module would otherwise issue, and turns on manifest chaining.
    pub archive: ArchiveMode,
    pub rsm: Arc<dyn RemoteStorageManager>,
    pub rlmm: Arc<dyn RemoteLogMetadataManager>,
    pub node_id: NodeId,
    pub broker_id: i32,
}

/// Spawned task entry point. Ticks every `cfg.interval` until `shutdown`.
// task dependencies; bundling would obscure them
pub(crate) async fn run(
    context: RemoteLogManagerContext,
    cfg: RemoteLogManagerConfig,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(cfg.interval.to_std());
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            () = shutdown.cancelled() => {
                debug!("remote-log-manager task shutting down");
                return;
            }
        }
        tick_all(
            &context.partitions,
            &*context.controller,
            context.archive,
            &context.rsm,
            &context.rlmm,
            context.node_id,
            context.broker_id,
        )
        .await;
    }
}

async fn tick_all(
    partitions: &PartitionRegistry,
    controller: &dyn crate::metadata_source::MetadataSource,
    archive: ArchiveMode,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    node_id: NodeId,
    broker_id: i32,
) {
    // Snapshot first to avoid holding any registry guard across an await.
    let snapshot: Vec<Arc<Partition>> = partitions.arcs();
    let image = controller.current_image();
    for partition in snapshot {
        if partition.current_leader.load(Ordering::Relaxed) != node_id {
            continue;
        }
        // Read config, the global log start and the sealed-segment list under
        // one hold of the log lock, then drop it. The floor rides along with
        // the config because remote retention measures a segment against it,
        // and a value read under a second lock could describe a different
        // `DeleteRecords` than the segment list does.
        let (log_config, log_start_offset, exports) = {
            let log = partition.log.lock().expect("log mutex poisoned");
            let cfg = log.config_snapshot();
            if !cfg.remote_storage_enable {
                continue;
            }
            (cfg, log.log_start_offset(), log.tierable_segments())
        };
        let Some(topic_id) = image.topic(&partition.topic).map(|t| t.topic_id) else {
            // Topic vanished from the metadata image between snapshots; skip.
            continue;
        };
        // Atomic stores the raw epoch; wrap for the remote-storage metadata seam.
        let leader_epoch =
            krabka_ids::LeaderEpoch(partition.current_leader_epoch.load(Ordering::Acquire));
        let tp = TopicIdPartition::new(topic_id, partition.topic.clone(), partition.index.get());
        // Only the copy pass has nothing to do without sealed local segments.
        // The retention passes below still do: a partition whose whole local
        // log has already been evicted is exactly the one whose remote
        // segments age out, or fall below a `DeleteRecords` floor, with no
        // local segment left to notice it.
        if !exports.is_empty() {
            copy_eligible(
                &tp,
                broker_id,
                leader_epoch,
                exports.clone(),
                archive,
                rsm,
                rlmm,
            )
            .await;
        }
        // KFC-9: the copy above stays allowed on a frozen topic, and both
        // retention passes below stop. A freeze refuses every operation that
        // removes data from the topic's log, and a copy removes none: it adds
        // a replica, which is exactly what a migration out of a frozen topic
        // needs.
        //
        // This is a different question from `archive`, and it does not
        // contradict the reason that gate gives below. `archive` says the
        // remote tier cannot accept a delete, which leaves the local eviction
        // free precisely because it deletes nothing remote. A freeze says this
        // topic's log must not lose bytes anywhere, so it stops the local
        // eviction too.
        if matches!(
            resolve_freeze_mutation(
                &image,
                &partition.topic,
                true,
                FreezeMutationKind::Retention,
            ),
            FreezeMutationResolution::Frozen(_)
        ) {
            debug!(topic = %partition.topic, partition = tp.partition,
                   "remote-log-manager: a write freeze holds both retention passes");
            continue;
        }
        // Local retention is deliberately not gated on `archive`: evicting a
        // local segment that the archive already holds is the whole point of
        // tiering, and it deletes nothing from the remote tier.
        local_retention_pass(&tp, &partition, &exports, &log_config, rlmm, now_ms());
        let outcome = remote_retention_pass(
            &tp,
            broker_id,
            RemoteRetentionBounds {
                log_config: &log_config,
                archive,
                log_start_offset,
                now_ms: now_ms(),
            },
            rsm,
            rlmm,
        )
        .await;
        // The records the pass deleted are now in no tier at all, so the
        // partition's global floor follows them (Kafka's
        // `handleLogStartOffsetUpdate`). `set_log_start_offset` only moves
        // forward, so a `DeleteRecords` that landed while the pass ran keeps
        // the higher of the two floors.
        if let Some(new_start) = outcome.log_start {
            let mut log = partition.log.lock().expect("log mutex poisoned");
            if let Err(error) = log.set_log_start_offset(new_start) {
                debug!(topic = %partition.topic, partition = tp.partition, %error,
                       "remote-log-manager: could not advance the log start after a remote delete");
            }
        }
    }
}

fn now_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_ids::PartitionIndex;
    use krabka_log::LogConfig;
    use krabka_metadata::{MetadataImage, MetadataRecord, TopicRecord};
    use krabka_remote_storage::{
        InmemoryRemoteLogMetadataManager, LocalTieredStorage, RemoteLogSegmentState,
    };
    use krabka_units::millis;
    use uuid::Uuid;

    use super::{
        test_support::{fixed_source, rolled_tiered_partition_with_config, tp},
        *,
    };

    mod freeze;

    fn image_with_orders_topic() -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::from_u128(9));
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: tp().topic_id,
            partitions: 1,
            replication_factor: 1,
        }));
        image
    }

    fn rolled_tiered_partition(log_dir: &std::path::Path) -> Arc<Partition> {
        rolled_tiered_partition_with_config(
            log_dir,
            LogConfig {
                segment_size: bytes(256),
                remote_storage_enable: true,
                retention: None,
                retention_size: None,
                ..LogConfig::default()
            },
        )
    }

    async fn wait_for_remote_segments(rlmm: &Arc<dyn RemoteLogMetadataManager>, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
                if listed.len() >= expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("remote-log-manager run loop did not copy expected segments");
    }

    #[tokio::test]
    async fn run_ticks_and_copies_eligible_segments() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let partitions = Arc::new(PartitionRegistry::new());
        let partition = rolled_tiered_partition(log_dir.path());
        let export_count = partition
            .log
            .lock()
            .expect("partition log mutex poisoned")
            .tierable_segments()
            .len();
        assert!(export_count >= 2, "test needs multiple sealed segments");
        partitions.insert("orders".into(), PartitionIndex(0), partition);

        let controller: Arc<dyn crate::metadata_source::MetadataSource> =
            Arc::new(fixed_source(image_with_orders_topic()));
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            RemoteLogManagerContext {
                partitions,
                controller,
                archive: ArchiveMode::Mutable,
                rsm,
                rlmm: rlmm.clone(),
                node_id: NodeId(1),
                broker_id: 1,
            },
            RemoteLogManagerConfig {
                interval: millis(10),
            },
            shutdown.clone(),
        ));

        wait_for_remote_segments(&rlmm, export_count).await;
        shutdown.cancel();
        task.await.expect("remote-log-manager task panicked");

        let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
        assert!(listed.len() == export_count);
        assert!(
            listed
                .iter()
                .all(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
        );
    }

    #[tokio::test]
    async fn tick_all_copies_local_leader_remote_enabled_partition() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let partitions = PartitionRegistry::new();
        let partition = rolled_tiered_partition(log_dir.path());
        let export_count = partition
            .log
            .lock()
            .expect("partition log mutex poisoned")
            .tierable_segments()
            .len();
        assert!(export_count >= 2, "test needs multiple sealed segments");
        partitions.insert("orders".into(), PartitionIndex(0), partition);

        let controller = fixed_source(image_with_orders_topic());
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        tick_all(
            &partitions,
            &controller,
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
            NodeId(1),
            1,
        )
        .await;

        let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
        assert!(listed.len() == export_count);
        assert!(
            listed
                .iter()
                .all(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
        );
    }

    #[tokio::test]
    async fn tick_all_skips_partition_led_by_other_node() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let partitions = PartitionRegistry::new();
        let partition = rolled_tiered_partition(log_dir.path());
        partition.current_leader.store(2, Ordering::Relaxed);
        partitions.insert("orders".into(), PartitionIndex(0), partition);

        let controller = fixed_source(image_with_orders_topic());
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        tick_all(
            &partitions,
            &controller,
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
            NodeId(1),
            1,
        )
        .await;

        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
    }

    #[tokio::test]
    async fn tick_all_skips_remote_storage_disabled_partition() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let partitions = PartitionRegistry::new();
        let partition = rolled_tiered_partition_with_config(
            log_dir.path(),
            LogConfig {
                segment_size: bytes(256),
                remote_storage_enable: false,
                retention: None,
                retention_size: None,
                ..LogConfig::default()
            },
        );
        partitions.insert("orders".into(), PartitionIndex(0), partition);

        let controller = fixed_source(image_with_orders_topic());
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        tick_all(
            &partitions,
            &controller,
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
            NodeId(1),
            1,
        )
        .await;

        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
    }

    #[test]
    fn now_ms_tracks_current_unix_epoch_millis() {
        let before = i64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        let observed = now_ms();
        let after = i64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();

        assert!(observed >= before);
        assert!(observed <= after);
    }
}
