//! The lifecycle of one segment copy: `CopySegmentStarted`, the blocking RSM
//! upload, then `CopySegmentFinished`, together with the rollback that runs
//! when any step of it fails.

use std::{collections::BTreeMap, sync::Arc};

use krabka_log::SegmentExport;
use krabka_remote_storage::{
    LogSegmentData, RemoteLogMetadataManager, RemoteLogSegmentId, RemoteLogSegmentMetadata,
    RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, RemoteStorageManager, TopicIdPartition,
    WormChainRecord,
};
use krabka_units::convert::ByteSizeExt as _;
use tracing::{debug, error, warn};
use uuid::Uuid;

use super::{
    archive::{ArchiveMode, ChainPosition},
    leader_epoch::leader_epoch_index_bytes,
    now_ms,
    rlmm::rlmm_mutate,
};

/// What one [`copy_one`] attempt left behind.
#[derive(Debug)]
pub(super) enum CopyOutcome {
    /// The segment reached `CopySegmentFinished`.
    Copied {
        /// Where the next copy in this tick joins the chain. Carried out of
        /// the copy so consecutive segments chain without re-listing the RLMM.
        next: ChainPosition,
    },
    /// The attempt did not reach `CopySegmentFinished`. The caller keeps its
    /// chain position and the next tick retries the segment.
    Failed,
}

/// Copy one sealed segment through the full `Started` → `Finished`
/// lifecycle. On any failure, this function deletes the partial remote data
/// and drops the metadata (`DeleteSegmentStarted` → `DeleteSegmentFinished`),
/// so the next tick retries the segment; see [`rollback`] for the part of
/// that a write-once archive cannot do.
///
/// `chain` decides whether the copy is stamped for a WORM manifest. When it
/// is, the stamp goes on before the copy, so the `CopySegmentStarted` record
/// already says where the manifest was meant to sit, and a durable metadata
/// manager shows that even if the broker dies mid-copy.
pub(super) async fn copy_one(
    tp: &TopicIdPartition,
    broker_id: i32,
    leader_epoch: krabka_ids::LeaderEpoch,
    ex: &SegmentExport,
    chain: ChainPosition,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
) -> CopyOutcome {
    let id = RemoteLogSegmentId::new(tp.clone(), Uuid::new_v4());
    // Unwrap the log-layer `Offset`s into the remote-storage metadata's `i64`
    // world at the seam; the epoch map keeps its `LeaderEpoch` keys, which
    // `RemoteLogSegmentMetadata` carries verbatim.
    let epochs: BTreeMap<krabka_ids::LeaderEpoch, i64> = if ex.leader_epochs.is_empty() {
        BTreeMap::from([(
            krabka_ids::LeaderEpoch(leader_epoch.0.max(0)),
            ex.base_offset.0,
        )])
    } else {
        ex.leader_epochs
            .iter()
            .map(|&(epoch, off)| (epoch, off.0))
            .collect()
    };
    let size = ex.size.bytes_i32();

    let metadata = match RemoteLogSegmentMetadata::new(
        id.clone(),
        ex.base_offset.0,
        ex.last_offset.0,
        ex.max_timestamp,
        broker_id,
        now_ms(),
        krabka_remote_storage::RemoteLogSegmentDetails::new(
            size,
            RemoteLogSegmentState::CopySegmentStarted,
            epochs.clone(),
        ),
    ) {
        Ok(m) => m,
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                  error = %e, "remote-log-manager: skipping segment with invalid metadata");
            return CopyOutcome::Failed;
        }
    };
    // KIP-405 txnIndexEmpty: set true when the log segment has no transaction
    // index file (non-transactional topics or segments written before txn support).
    let metadata = if ex.transaction_index_path.is_none() {
        metadata.with_txn_index_empty(true)
    } else {
        metadata
    };
    // The chain stamp goes on before the copy: a WORM backend refuses to seal
    // an unstamped manifest, and it refuses only *after* uploading every
    // object, which would leave orphans in a bucket that takes nothing back.
    let metadata = match chain {
        ChainPosition::Unchained => metadata,
        ChainPosition::At(stamp) => {
            metadata.with_custom_metadata(WormChainRecord::request(stamp).to_custom_metadata())
        }
    };

    let md_started = metadata.clone();
    if let Err(e) = rlmm_mutate(rlmm, move |m| m.add_remote_log_segment_metadata(md_started)).await
    {
        warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
              error = %e, "remote-log-manager: failed to record CopySegmentStarted");
        return CopyOutcome::Failed;
    }

    let data = LogSegmentData {
        log_segment: ex.log_path.clone(),
        offset_index: ex.offset_index_path.clone(),
        time_index: ex.time_index_path.clone(),
        transaction_index: ex.transaction_index_path.clone(),
        producer_snapshot_index: Some(ex.producer_snapshot_path.clone()),
        leader_epoch_index: leader_epoch_index_bytes(&epochs),
    };

    // The RSM is a blocking SPI — run the copy on the blocking pool.
    let rsm_copy = rsm.clone();
    let md_copy = metadata.clone();
    let copy_result =
        tokio::task::spawn_blocking(move || rsm_copy.copy_log_segment_data(&md_copy, &data)).await;

    // Copy failed (or the blocking task panicked): clean up so the segment
    // is retried next tick.
    let returned = match copy_result {
        Ok(Ok(returned)) => returned,
        Ok(Err(e)) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                  error = %e, "remote-log-manager: segment copy failed");
            rollback(&metadata, broker_id, chain.archive(), rsm, rlmm).await;
            return CopyOutcome::Failed;
        }
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                  error = %e, "remote-log-manager: segment copy task panicked");
            rollback(&metadata, broker_id, chain.archive(), rsm, rlmm).await;
            return CopyOutcome::Failed;
        }
    };

    // A write-once copy is only complete once the backend hands back a receipt
    // carrying the head its manifest produced. Without one the objects landed
    // with no verifiable manifest over them, so the segment must not be marked
    // finished: the read path serves every finished segment, and it would then
    // be serving unattested data. Leaving it in `CopySegmentStarted` is what
    // makes the next tick retry it under a fresh segment id.
    let next = match chain {
        ChainPosition::Unchained => ChainPosition::Unchained,
        ChainPosition::At(_) => {
            let Some(stamp) = returned
                .as_ref()
                .and_then(|custom| WormChainRecord::from_custom_metadata(custom).ok())
                .and_then(|receipt| receipt.next_stamp())
            else {
                error!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                       "remote-log-manager: write-once copy returned no chain receipt; \
                        leaving the segment in CopySegmentStarted rather than serving \
                        unattested data");
                return CopyOutcome::Failed;
            };
            ChainPosition::At(stamp)
        }
    };

    let upd = RemoteLogSegmentMetadataUpdate {
        remote_log_segment_id: id,
        event_timestamp_ms: now_ms(),
        // The backend's receipt is the chain position a restart reads back, so
        // it has to be durable alongside the segment, not dropped here.
        custom_metadata: returned,
        state: RemoteLogSegmentState::CopySegmentFinished,
        broker_id,
    };
    if let Err(e) = rlmm_mutate(rlmm, move |m| m.update_remote_log_segment_metadata(upd)).await {
        warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
              error = %e, "remote-log-manager: failed to record CopySegmentFinished");
        return CopyOutcome::Failed;
    }
    debug!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
           end = ex.last_offset.0, "remote-log-manager: copied segment to remote tier");
    CopyOutcome::Copied { next }
}

/// Delete partial remote data and drop the metadata after a failed copy.
///
/// # A write-once archive keeps its partial objects
///
/// Under [`ArchiveMode::WriteOnce`] the RSM delete is skipped and only the
/// metadata is dropped. Whatever objects the failed copy managed to write stay
/// in the archive for good, because the backend refuses every delete and the
/// bucket policy refuses it under that. They are inert — the copy never sealed
/// a manifest, so no chain references them — and the retry runs under a fresh
/// segment UUID, so its keys cannot collide with theirs. This residue is
/// exactly what the WORM verifier reports as `orphan_objects`: unreferenced
/// bytes are the standing cost of a tier that can take nothing back.
async fn rollback(
    metadata: &RemoteLogSegmentMetadata,
    broker_id: i32,
    archive: ArchiveMode,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
) {
    let id = metadata.remote_log_segment_id().clone();
    match archive {
        ArchiveMode::Mutable => {
            let rsm_del = rsm.clone();
            let md_del = metadata.clone();
            let _ =
                tokio::task::spawn_blocking(move || rsm_del.delete_log_segment_data(&md_del)).await;
        }
        ArchiveMode::WriteOnce => {
            debug!(topic = %id.topic_id_partition.topic,
                   partition = id.topic_id_partition.partition,
                   base = metadata.start_offset(), worm_retained = true,
                   "remote-log-manager: leaving a failed copy's objects in the write-once \
                    archive; the verifier reports them as orphans");
        }
    }
    for state in [
        RemoteLogSegmentState::DeleteSegmentStarted,
        RemoteLogSegmentState::DeleteSegmentFinished,
    ] {
        let upd = RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: id.clone(),
            event_timestamp_ms: now_ms(),
            custom_metadata: None,
            state,
            broker_id,
        };
        let _ = rlmm_mutate(rlmm, move |m| m.update_remote_log_segment_metadata(upd)).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use assert2::{assert, check};
    use krabka_ids::LeaderEpoch;
    use krabka_log::Offset;
    use krabka_remote_storage::{
        ChainHead, ChainStamp, CustomMetadata, EpochId, IndexType,
        InmemoryRemoteLogMetadataManager, LocalTieredStorage, ManifestSeq, RemoteStorageError,
    };
    use krabka_units::bytes;

    use super::*;
    use crate::remote_log_manager::{
        copy_eligible,
        test_support::{rolled_log, synth_export, tp},
    };

    /// An RSM whose copy always fails, but whose delete succeeds. The tests
    /// use it to exercise the failure rollback path.
    struct AlwaysFailRsm;

    impl RemoteStorageManager for AlwaysFailRsm {
        fn copy_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
            _data: &LogSegmentData,
        ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
            Err(RemoteStorageError::InvalidArgument("boom".into()))
        }
        fn fetch_log_segment(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _start: u32,
            _end: Option<u32>,
        ) -> Result<Vec<u8>, RemoteStorageError> {
            Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
        fn fetch_index(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _index_type: IndexType,
        ) -> Result<Vec<u8>, RemoteStorageError> {
            Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
        fn delete_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }
    }

    /// Records the metadata every copy hands the backend, then fails the copy.
    /// Tests use it to see what the broker stamped on a segment before the
    /// upload, which is the only moment that stamp is observable.
    #[derive(Default)]
    struct CapturingRsm {
        seen: Mutex<Vec<RemoteLogSegmentMetadata>>,
    }

    impl RemoteStorageManager for CapturingRsm {
        fn copy_log_segment_data(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _data: &LogSegmentData,
        ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
            self.seen
                .lock()
                .expect("captured-metadata mutex poisoned")
                .push(metadata.clone());
            Err(RemoteStorageError::InvalidArgument("captured".into()))
        }
        fn fetch_log_segment(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _start: u32,
            _end: Option<u32>,
        ) -> Result<Vec<u8>, RemoteStorageError> {
            Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
        fn fetch_index(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _index_type: IndexType,
        ) -> Result<Vec<u8>, RemoteStorageError> {
            Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
        fn delete_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn copy_failure_rolls_back_and_leaves_no_metadata() {
        let log_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();
        assert!(!exports.is_empty());

        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(AlwaysFailRsm);
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

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
        assert!(copied == 0, "every copy failed");
        // Rollback (delete + DeleteSegmentStarted -> DeleteSegmentFinished)
        // drops the started metadata, so nothing is left behind and a later
        // run with a healthy store can retry the same segments.
        assert!(
            rlmm.list_remote_log_segments(&tp()).unwrap().is_empty(),
            "failed copies must not leave dangling metadata"
        );
    }

    #[tokio::test]
    async fn fallback_leader_epoch_when_export_has_none() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        // Hand-build an export with no leader epochs but real files on disk.
        let src = tempfile::tempdir().unwrap();
        let write = |name: &str, bytes: &[u8]| {
            let p = src.path().join(name);
            std::fs::write(&p, bytes).unwrap();
            p
        };
        let export = SegmentExport {
            base_offset: Offset(0),
            last_offset: Offset(9),
            max_timestamp: 42,
            size: bytes(10),
            log_path: write("00.log", b"0123456789"),
            offset_index_path: write("00.index", b"i"),
            time_index_path: write("00.timeindex", b"t"),
            transaction_index_path: None,
            producer_snapshot_path: write("10.snapshot", b"snapshot"),
            leader_epochs: Vec::new(),
        };

        let copied = copy_eligible(
            &tp(),
            7,
            LeaderEpoch(3),
            vec![export],
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
        )
        .await;
        assert!(copied == 1);
        let md = &rlmm.list_remote_log_segments(&tp()).unwrap()[0];
        // The fallback recorded the partition's current leader epoch (3).
        assert!(md.segment_leader_epochs().get(&LeaderEpoch(3)) == Some(&0));
    }

    #[tokio::test]
    async fn copy_one_stamps_a_chain_request_on_the_started_metadata() {
        let stamp = ChainStamp {
            epoch_id: EpochId(Uuid::from_u128(0x5eed)),
            seq: ManifestSeq(4),
            prev_head: ChainHead([7; 32]),
        };
        let cases = [
            (
                "mutable tier stamps nothing",
                ChainPosition::Unchained,
                None,
            ),
            (
                "write-once stamps the request form",
                ChainPosition::At(stamp),
                Some(WormChainRecord::request(stamp).to_custom_metadata()),
            ),
        ];
        for (name, chain, expected) in cases {
            let rsm_impl = Arc::new(CapturingRsm::default());
            let rsm: Arc<dyn RemoteStorageManager> = rsm_impl.clone();
            let rlmm: Arc<dyn RemoteLogMetadataManager> =
                Arc::new(InmemoryRemoteLogMetadataManager::new());
            let export = synth_export(0, 9, 100, 64);

            let outcome = copy_one(&tp(), 1, LeaderEpoch(0), &export, chain, &rsm, &rlmm).await;

            check!(matches!(outcome, CopyOutcome::Failed), "case {name}");
            let seen = rsm_impl
                .seen
                .lock()
                .expect("captured-metadata mutex poisoned");
            check!(seen.len() == 1, "case {name}");
            // The stamp is on the metadata the backend sees, which is the
            // same value the RLMM recorded as `CopySegmentStarted`.
            check!(
                seen[0].custom_metadata() == expected.as_ref(),
                "case {name}"
            );
        }
    }
}
