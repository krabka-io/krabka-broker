//! Remote-segment and remote-partition deletion: the shared
//! `DeleteSegmentStarted` to `DeleteSegmentFinished` lifecycle, and the
//! partition-wide cascade that a `DeleteTopics` request starts.

use std::sync::Arc;

use krabka_remote_storage::{
    RemoteLogMetadataManager, RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate,
    RemoteLogSegmentState, RemotePartitionDeleteMetadata, RemotePartitionDeleteState,
    RemoteStorageManager, TopicIdPartition,
};
use tracing::{debug, warn};

use super::{archive::ArchiveMode, now_ms, rlmm::rlmm_mutate};

/// KIP-405: cascade the
/// [`DeletePartitionMarked` → `DeletePartitionStarted` →
/// `DeletePartitionFinished`] lifecycle for `tp`, and delete every remote
/// segment along the way. The `DeleteTopics` handler runs this as a detached
/// task, so the response does not wait on remote-tier I/O. A failure logs
/// at WARN. Leftover `DeleteSegmentStarted` segments are harmless in the
/// in-memory RLMM, because a `DeleteTopics`-recreate combination regenerates
/// the topic id and the new partition is a fresh `TopicIdPartition`.
///
/// # A write-once archive keeps every byte
///
/// Under [`ArchiveMode::WriteOnce`] the cascade still walks
/// `DeletePartitionMarked` → `DeletePartitionStarted` →
/// `DeletePartitionFinished`, and still clears the partition's segment
/// metadata, but it removes nothing from the archive. Deleting a Kafka topic
/// is a cluster operation; it is not, and must not become, an instruction to
/// erase a compliance archive. The archived segments and their manifests
/// outlive the topic, and the verifier reads them without any broker.
pub(crate) async fn cascade_remote_partition_delete(
    tp: TopicIdPartition,
    broker_id: i32,
    archive: ArchiveMode,
    rsm: Arc<dyn RemoteStorageManager>,
    rlmm: Arc<dyn RemoteLogMetadataManager>,
) {
    if let Err(e) = put_partition_state(
        &rlmm,
        &tp,
        RemotePartitionDeleteState::DeletePartitionMarked,
        broker_id,
    )
    .await
    {
        warn!(topic = %tp.topic, partition = tp.partition, error = %e,
              "remote-log-manager: failed to mark partition deleted");
        return;
    }
    if let Err(e) = put_partition_state(
        &rlmm,
        &tp,
        RemotePartitionDeleteState::DeletePartitionStarted,
        broker_id,
    )
    .await
    {
        warn!(topic = %tp.topic, partition = tp.partition, error = %e,
              "remote-log-manager: failed to start partition delete");
        return;
    }

    let segments = match rlmm.list_remote_log_segments(&tp) {
        Ok(list) => list,
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to list segments for partition delete");
            return;
        }
    };
    for md in segments {
        // Skip segments already past `DeleteSegmentStarted` (no-op delete).
        if md.state() == RemoteLogSegmentState::DeleteSegmentFinished {
            continue;
        }
        let _ = delete_one_segment(&tp, broker_id, &md, archive, &rsm, &rlmm).await;
    }

    if let Err(e) = put_partition_state(
        &rlmm,
        &tp,
        RemotePartitionDeleteState::DeletePartitionFinished,
        broker_id,
    )
    .await
    {
        warn!(topic = %tp.topic, partition = tp.partition, error = %e,
              "remote-log-manager: failed to finish partition delete");
    }
}

async fn put_partition_state(
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    tp: &TopicIdPartition,
    state: RemotePartitionDeleteState,
    broker_id: i32,
) -> Result<(), krabka_remote_storage::RemoteStorageError> {
    let md = RemotePartitionDeleteMetadata {
        topic_id_partition: tp.clone(),
        state,
        event_timestamp_ms: now_ms(),
        broker_id,
    };
    rlmm_mutate(rlmm, move |m| m.put_remote_partition_delete_metadata(md)).await
}

/// Drive one `CopySegmentFinished` (or in-flight) segment through the
/// `DeleteSegmentStarted` → RSM delete → `DeleteSegmentFinished` chain.
/// Returns `true` when the lifecycle completes cleanly. Shared by
/// [`remote_retention_pass`](super::remote_retention::remote_retention_pass)
/// and [`cascade_remote_partition_delete`].
///
/// Under [`ArchiveMode::WriteOnce`] the RSM delete is skipped outright and
/// only the metadata lifecycle advances. Calling it would fail — the backend
/// refuses every delete, and the bucket's object-lock policy refuses it under
/// that — so the skip is what keeps a routine pass from logging an error every
/// tick.
pub(super) async fn delete_one_segment(
    tp: &TopicIdPartition,
    broker_id: i32,
    md: &RemoteLogSegmentMetadata,
    archive: ArchiveMode,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
) -> bool {
    let id = md.remote_log_segment_id().clone();
    // Transition to DeleteSegmentStarted unless the segment is already
    // there (cascade may retry against a partially-cleaned partition).
    if md.state() == RemoteLogSegmentState::CopySegmentFinished {
        let upd = RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: id.clone(),
            event_timestamp_ms: now_ms(),
            custom_metadata: None,
            state: RemoteLogSegmentState::DeleteSegmentStarted,
            broker_id,
        };
        if let Err(e) = rlmm_mutate(rlmm, move |m| m.update_remote_log_segment_metadata(upd)).await
        {
            warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
                  error = %e,
                  "remote-log-manager: failed to record DeleteSegmentStarted");
            return false;
        }
    }

    match archive {
        ArchiveMode::Mutable => {
            // RSM delete (blocking).
            let rsm_del = rsm.clone();
            let md_del = md.clone();
            let delete_result =
                tokio::task::spawn_blocking(move || rsm_del.delete_log_segment_data(&md_del)).await;
            match delete_result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
                          error = %e, "remote-log-manager: RSM delete failed");
                    return false;
                }
                Err(e) => {
                    warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
                          error = %e, "remote-log-manager: RSM delete task panicked");
                    return false;
                }
            }
        }
        // DEBUG and not WARN on purpose: deleting a topic with ten thousand
        // archived segments would otherwise emit ten thousand warnings for
        // behavior that is working exactly as configured.
        ArchiveMode::WriteOnce => {
            debug!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
                   worm_retained = true,
                   "remote-log-manager: retaining remote segment data; the tier is a \
                    write-once archive");
        }
    }

    let upd = RemoteLogSegmentMetadataUpdate {
        remote_log_segment_id: id,
        event_timestamp_ms: now_ms(),
        custom_metadata: None,
        state: RemoteLogSegmentState::DeleteSegmentFinished,
        broker_id,
    };
    if let Err(e) = rlmm_mutate(rlmm, move |m| m.update_remote_log_segment_metadata(upd)).await {
        warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
              error = %e, "remote-log-manager: failed to record DeleteSegmentFinished");
        return false;
    }
    debug!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
           worm_retained = archive == ArchiveMode::WriteOnce,
           "remote-log-manager: remote segment reached DeleteSegmentFinished");
    true
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_ids::LeaderEpoch;
    use krabka_remote_storage::{InmemoryRemoteLogMetadataManager, LocalTieredStorage};

    use super::*;
    use crate::remote_log_manager::{
        copy_eligible,
        test_support::{FakeWormArchive, rolled_log, synth_export, tp},
    };

    #[tokio::test]
    async fn cascade_remote_partition_delete_drops_every_segment() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm_impl = Arc::new(InmemoryRemoteLogMetadataManager::new());
        let rlmm: Arc<dyn RemoteLogMetadataManager> = rlmm_impl.clone();
        let copied = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
        )
        .await;
        assert!(copied == exports.len());

        cascade_remote_partition_delete(tp(), 1, ArchiveMode::Mutable, rsm.clone(), rlmm.clone())
            .await;

        // All segments are gone from the cache.
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
        // The remote directory for this partition is empty (or absent).
        // Kafka LocalTieredStorage layout:
        // <remote_dir>/<topic>-<partition>-<topic_id_base64>/.
        let part_dir = remote_dir.path().join("orders-0-AAAAAAAAAAAAAAAAAAAAAQ");
        let entries: Vec<_> = std::fs::read_dir(&part_dir).unwrap().collect();
        assert!(entries.is_empty(), "stray remote files: {entries:?}");
        let dump = rlmm_impl.export();
        let partition = dump
            .partitions
            .iter()
            .find(|partition| partition.topic_id_partition == tp())
            .expect("partition delete state should be dumped");
        assert!(
            partition.delete_state == Some(RemotePartitionDeleteState::DeletePartitionFinished)
        );
    }

    #[tokio::test]
    async fn cascade_remote_partition_delete_is_noop_on_empty_partition() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        // No add — partition has no segments. Cascade still walks the
        // three partition-delete states without error.
        cascade_remote_partition_delete(tp(), 1, ArchiveMode::Mutable, rsm, rlmm.clone()).await;
        // No segments after, no panics; that's the test.
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
    }

    #[tokio::test]
    async fn cascade_partition_delete_retains_archive_objects_but_finishes_the_lifecycle() {
        let archive = Arc::new(FakeWormArchive::new());
        let rsm: Arc<dyn RemoteStorageManager> = archive.clone();
        let rlmm_impl = Arc::new(InmemoryRemoteLogMetadataManager::new());
        let rlmm: Arc<dyn RemoteLogMetadataManager> = rlmm_impl.clone();
        let copied = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            vec![synth_export(0, 9, 100, 64), synth_export(10, 19, 200, 64)],
            ArchiveMode::WriteOnce,
            &rsm,
            &rlmm,
        )
        .await;
        check!(copied == 2);

        // The RSM panics on delete, so reaching one fails this test.
        cascade_remote_partition_delete(tp(), 1, ArchiveMode::WriteOnce, rsm.clone(), rlmm.clone())
            .await;

        check!(
            rlmm.list_remote_log_segments(&tp()).unwrap().is_empty(),
            "the broker's own metadata is still cleared"
        );
        let dump = rlmm_impl.export();
        let partition = dump
            .partitions
            .iter()
            .find(|partition| partition.topic_id_partition == tp())
            .expect("partition delete state should be dumped");
        check!(partition.delete_state == Some(RemotePartitionDeleteState::DeletePartitionFinished));
        check!(
            archive.archived_segments() == 2,
            "deleting a topic must not erase a compliance archive"
        );
    }
}
