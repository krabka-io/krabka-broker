//! `RemoteLogManager`: the KIP-405 tiered-storage sweep.
//!
//! Every `interval`, the manager walks the partition registry and sweeps each
//! partition whose topic has `remote.storage.enable=true`. Leadership splits
//! the sweep, the way Kafka splits it between `RemoteLogManager`'s leader and
//! follower tasks:
//!
//! - Where this broker leads the partition, it copies the sealed log segments
//!   that are not yet in the remote tier to a [`RemoteStorageManager`],
//!   recording each copy in a [`RemoteLogMetadataManager`]
//!   (`CopySegmentStarted` → `CopySegmentFinished`), and it enforces the
//!   topic's remote retention against that tier. Both are the leader's alone:
//!   one writer per partition owns the remote tier.
//! - **Every** replica, leader and follower alike, enforces local retention on
//!   its own disk. A follower's sealed segment is droppable for the same
//!   reason the leader's is -- the RLMM says the leader finished copying it --
//!   and the RLMM is shared, so the follower reads the same
//!   `CopySegmentFinished` set. Without this a follower would hold every
//!   segment it ever fetched until it was elected, and its disk would grow to
//!   the full `retention.ms` footprint rather than the `local.retention.ms`
//!   one.
//!
//! This is the copy path. Their own modules implement local-retention deletion
//! of copied segments and the remote read path on `Fetch`. The
//! remote-storage SPIs are blocking, so each copy and each delete
//! runs on the `tokio` blocking pool.
//!
//! A KFC-9 write freeze splits the sweep in two. The copy runs on a frozen
//! topic, because it adds a replica and takes nothing away, and tiering a
//! frozen topic is what a migration wants. Both retention passes stop, on
//! every replica, because each one removes data from the topic's log.

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
        // Leadership decides which halves of the sweep run, not whether it
        // runs at all: the copy and the remote-retention pass are the
        // leader's, local retention is every replica's.
        let is_leader = partition.current_leader.load(Ordering::Relaxed) == node_id;
        // Read config, both readings of the global log start and the
        // sealed-segment list under one hold of the log lock, then drop it.
        // The floors ride along with the config because remote retention
        // measures a segment against them, and a value read under a second
        // lock could describe a different `DeleteRecords` than the segment
        // list does.
        let (log_config, log_start_offset, deleted_below, exports) = {
            let log = partition.log.lock().expect("log mutex poisoned");
            let cfg = log.config_snapshot();
            if !cfg.remote_storage_enable {
                continue;
            }
            (
                cfg,
                log.log_start_offset(),
                log.established_log_start(),
                log.tierable_segments(),
            )
        };
        // A sealed segment that ends below the global floor is deleted data,
        // whatever its file is still doing on disk. Kafka's copy task starts
        // at `max(logStartOffset, lastCopiedOffset)` for the same reason:
        // without this the log-start breach in `remote_retention_pass` would
        // delete the remote copy, the next tick would upload it again off the
        // local file, and the two would cycle for as long as the file sat
        // there.
        let exports: Vec<krabka_log::SegmentExport> = exports
            .into_iter()
            .filter(|export| export.last_offset >= log_start_offset)
            .collect();
        let Some(topic_id) = image.topic(&partition.topic).map(|t| t.topic_id) else {
            // Topic vanished from the metadata image between snapshots; skip.
            continue;
        };
        let tp = TopicIdPartition::new(topic_id, partition.topic.clone(), partition.index.get());
        // Only the copy pass has nothing to do without sealed local segments.
        // The retention passes below still do: a partition whose whole local
        // log has already been evicted is exactly the one whose remote
        // segments age out, or fall below a `DeleteRecords` floor, with no
        // local segment left to notice it.
        if is_leader && !exports.is_empty() {
            // Atomic stores the raw epoch; wrap for the remote-storage
            // metadata seam.
            let leader_epoch =
                krabka_ids::LeaderEpoch(partition.current_leader_epoch.load(Ordering::Acquire));
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
        if is_leader {
            let outcome = remote_retention_pass(
                &tp,
                broker_id,
                RemoteRetentionBounds {
                    log_config: &log_config,
                    archive,
                    log_start_offset,
                    deleted_below,
                    now_ms: now_ms(),
                },
                rsm,
                rlmm,
            )
            .await;
            // The records the pass deleted are now in no tier at all, so the
            // partition's global floor follows them (Kafka's
            // `handleLogStartOffsetUpdate`). `set_log_start_offset` only moves
            // forward, so a `DeleteRecords` that landed while the pass ran
            // keeps the higher of the two floors.
            if let Some(new_start) = outcome.log_start {
                let mut log = partition.log.lock().expect("log mutex poisoned");
                if let Err(error) = log.set_log_start_offset(new_start) {
                    debug!(topic = %partition.topic, partition = tp.partition, %error,
                           "remote-log-manager: could not advance the log start after a remote delete");
                }
                // The local files under the new floor go with it. No reader
                // may ask for those offsets any more and no tier answers for
                // them, so leaving the files behind would only hold disk and
                // keep the copy filter above working around them on every
                // tick.
                if let Err(error) = log.delete_local_segments_through(new_start) {
                    debug!(topic = %partition.topic, partition = tp.partition, %error,
                           "remote-log-manager: could not drop the local segments under the new floor");
                }
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
    use assert2::{assert, check};
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

    /// A `DeleteRecords` floor takes the segments under it out of the copy
    /// pass, and keeps them out on every later tick.
    ///
    /// Without the filter the log-start breach in `remote_retention_pass`
    /// deletes the remote copy of a below-floor segment, the next tick uploads
    /// it again off the local file the floor has not removed, and the two
    /// cycle for as long as the file is there -- unbounded copies, deletes and
    /// metadata for records nobody may read.
    #[tokio::test]
    async fn tick_all_never_copies_a_segment_under_the_log_start() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let partitions = PartitionRegistry::new();
        let partition = rolled_tiered_partition(log_dir.path());
        // A floor at the second sealed segment's base, with every local file
        // still on disk: `set_log_start_offset` moves the pointer and deletes
        // nothing, which is the state a remote-retention advance leaves.
        let (floor, export_count) = {
            let mut log = partition.log.lock().expect("partition log mutex poisoned");
            let exports = log.tierable_segments();
            assert!(exports.len() >= 3, "test needs several sealed segments");
            let floor = exports[1].base_offset;
            log.set_log_start_offset(floor).expect("move the log start");
            (floor, exports.len())
        };
        partitions.insert("orders".into(), PartitionIndex(0), Arc::clone(&partition));

        let controller = fixed_source(image_with_orders_topic());
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        // Two sweeps: the second is where a copy/delete cycle would show.
        for sweep in 1..=2 {
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
            assert!(
                listed.len() == export_count - 1,
                "sweep {sweep}: every segment but the one under the floor"
            );
            assert!(
                listed.iter().all(|md| md.end_offset() >= floor.0),
                "sweep {sweep}: nothing under the floor was copied"
            );
        }
    }

    /// What one [`tick_all`] over a follower replica of `orders` left behind:
    /// what the remote tier holds, and what is still sealed on local disk.
    struct FollowerSweep {
        sealed_before: usize,
        remote_finished: usize,
        local_sealed_after: usize,
    }

    /// Drive one sweep over an `orders` partition that node 2 leads, with this
    /// broker (node 1) hosting a follower replica of it. The topic's local
    /// budget is zero, so every segment the RLMM reports copied is past the
    /// local-retention window the moment the sweep sees it.
    ///
    /// When `leader_already_copied`, the copy the real leader would have run
    /// is run first against the same RSM and RLMM. That is what a follower
    /// meets in a cluster: the metadata is shared, so the follower reads the
    /// leader's `CopySegmentFinished` set off `__remote_log_metadata` without
    /// ever having copied a byte itself.
    async fn follower_sweep(leader_already_copied: bool) -> FollowerSweep {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let partitions = PartitionRegistry::new();
        let partition = rolled_tiered_partition_with_config(
            log_dir.path(),
            LogConfig {
                segment_size: bytes(256),
                remote_storage_enable: true,
                local_retention_size: Some(NO_BYTES),
                retention: None,
                retention_size: None,
                ..LogConfig::default()
            },
        );
        partition.current_leader.store(2, Ordering::Relaxed);
        let exports = partition
            .log
            .lock()
            .expect("partition log mutex poisoned")
            .tierable_segments();
        let sealed_before = exports.len();
        partitions.insert("orders".into(), PartitionIndex(0), Arc::clone(&partition));

        let controller = fixed_source(image_with_orders_topic());
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        if leader_already_copied {
            copy_eligible(
                &tp(),
                2,
                krabka_ids::LeaderEpoch(0),
                exports,
                ArchiveMode::Mutable,
                &rsm,
                &rlmm,
            )
            .await;
        }

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

        let remote_finished = rlmm
            .list_remote_log_segments(&tp())
            .unwrap()
            .iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .count();
        let local_sealed_after = partition
            .log
            .lock()
            .expect("partition log mutex poisoned")
            .tierable_segments()
            .len();
        FollowerSweep {
            sealed_before,
            remote_finished,
            local_sealed_after,
        }
    }

    #[tokio::test]
    async fn tick_all_on_a_follower_evicts_locally_and_never_copies() {
        // KIP-405 splits the sweep by leadership, not by replica: the copy is
        // the leader's alone, and local retention runs on every replica. A
        // follower that skipped it would hold `retention.ms` worth of disk
        // while the leader held `local.retention.ms` worth.
        let cases = [
            ("a follower whose leader has not copied yet", false),
            ("a follower whose leader already copied", true),
        ];
        for (label, leader_already_copied) in cases {
            let outcome = follower_sweep(leader_already_copied).await;

            check!(
                outcome.sealed_before >= 2,
                "{label}: the fixture needs multiple sealed segments"
            );
            // The follower never adds to the remote tier: whatever is there
            // is what the leader's copy put there.
            let want_remote = if leader_already_copied {
                outcome.sealed_before
            } else {
                0
            };
            check!(
                outcome.remote_finished == want_remote,
                "{label}: segments in the remote tier after the sweep"
            );
            // ...but it does drop its own copy of what the leader finished.
            let want_local = if leader_already_copied {
                0
            } else {
                outcome.sealed_before
            };
            check!(
                outcome.local_sealed_after == want_local,
                "{label}: sealed segments still on the follower's disk"
            );
        }
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
