//! The lifecycle of one segment copy: `CopySegmentStarted`, the blocking RSM
//! upload, then `CopySegmentFinished`, together with the rollback that runs
//! when any step of it fails.

#[cfg(test)]
use krabka_remote_storage::LogSegmentData;
use {
    super::{
        RemoteTier,
        archive::{ArchiveMode, ChainPosition},
        copy::CopyIdentity,
        delete::delete_one_segment,
        leader_epoch::leader_epoch_index_bytes,
        rlmm::rlmm_mutate,
    },
    crate::{blocking::spawn_blocking, metrics::RemoteTierPath, time_util::now_ms},
    krabka_log::{Offset, SegmentExport},
    krabka_remote_storage::{
        RemoteLogMetadataManager, RemoteLogSegmentId, RemoteLogSegmentMetadata,
        RemoteLogSegmentState, RemoteStorageError, RemoteStorageManager, TopicIdPartition,
        WormChainRecord,
    },
    krabka_units::convert::{ByteSizeExt as _, TimeExt as _},
    std::{collections::BTreeMap, sync::Arc},
    tracing::{debug, error, warn},
    uuid::Uuid,
};

/// What one [`copy_one`] attempt left behind.
#[derive(Debug, PartialEq, Eq)]
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
    /// The copy uploaded, but the segment was deleted under it: a
    /// `DeleteRecords` marked it for deletion before `CopySegmentFinished`
    /// went in, or it finished wholly below the partition's established log
    /// start and was deleted straight after. Nothing is retried, since the
    /// offsets are gone, but a write-once copy did seal its manifest, so the
    /// chain moves on to `next` all the same.
    Withdrawn {
        /// Where the next copy in this tick joins the chain.
        next: ChainPosition,
    },
    /// The broker stopped leading the partition, or began shutting down,
    /// while the copy ran. The copy was rolled back rather than published, and
    /// the pass stops: the tier is the new leader's to write.
    Fenced,
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
///
/// The copy asks `identity`'s fence twice more: once `CopySegmentStarted` is
/// recorded, before any byte is uploaded, and once the upload returns, before
/// `CopySegmentFinished` is published. Either write can wait on the metadata
/// manager for seconds, and a broker that lost the partition in that time must
/// not publish a copy over offsets the new leader is copying under its own
/// segment boundaries; both copies would stay in the tier.
pub(super) async fn copy_one(
    tier: &RemoteTier<'_>,
    tp: &TopicIdPartition,
    identity: CopyIdentity<'_>,
    ex: &SegmentExport,
    chain: ChainPosition,
) -> CopyOutcome {
    // KIP-405's `RemoteCopyRequestsPerSec` and its errors and bytes twins.
    // The attempt is counted here rather than at each of the copy's many
    // return points, so the error count is exactly the attempts that did not
    // finish and the two divide into a failure rate.
    tier.metrics
        .record_remote_request(RemoteTierPath::Copy, &tp.topic);
    let outcome = copy_one_inner(tier, tp, identity, ex, chain).await;
    match outcome {
        // The bytes are counted on a *finished* copy, not on the backend PUT
        // returning: a copy whose metadata never reached `CopySegmentFinished`
        // moved bytes no consumer can read, and counting those would say the
        // tier is keeping up when it is not.
        CopyOutcome::Copied { .. } => {
            tier.metrics
                .record_remote_bytes(RemoteTierPath::Copy, &tp.topic, ex.size.bytes_u64());
        }
        CopyOutcome::Failed => {
            tier.metrics
                .record_remote_error(RemoteTierPath::Copy, &tp.topic);
        }
        // Losing the partition, or the offsets, is not a backend error.
        CopyOutcome::Fenced | CopyOutcome::Withdrawn { .. } => {}
    }
    outcome
}

async fn copy_one_inner(
    tier: &RemoteTier<'_>,
    tp: &TopicIdPartition,
    CopyIdentity {
        broker_id,
        leader_epoch,
        fence,
    }: CopyIdentity<'_>,
    ex: &SegmentExport,
    chain: ChainPosition,
) -> CopyOutcome {
    let (rsm, rlmm) = (tier.rsm, tier.rlmm);
    // Only a chained copy is trying to seal a manifest, so only a chained copy
    // can fail to. An unchained copy failing on a mutable tier is an ordinary
    // copy failure and says nothing about the archive's attestation.
    let seal_failed = || {
        if matches!(chain, ChainPosition::At(_)) {
            tier.metrics.worm_manifest_seal_failures_total.inc();
        }
    };
    if chain == ChainPosition::Exhausted {
        error!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
               "remote-log-manager: refusing a WORM copy after chain sequence exhaustion");
        return CopyOutcome::Failed;
    }
    let id = RemoteLogSegmentId::new(tp.clone(), Uuid::new_v4());
    // Unwrap the log-layer `Offset`s into the remote-storage metadata's `i64`
    // world at the seam; the epoch map keeps its `LeaderEpoch` keys, which
    // `RemoteLogSegmentMetadata` carries verbatim.
    let epochs = export_epoch_map(ex, krabka_ids::LeaderEpoch(leader_epoch.0.max(0)));
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
        ChainPosition::Exhausted => return CopyOutcome::Failed,
    };

    let md_started = metadata.clone();
    if let Err(e) = rlmm_mutate(rlmm, move |m| m.add_remote_log_segment_metadata(md_started)).await
    {
        warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
              error = %e, "remote-log-manager: failed to record CopySegmentStarted");
        return CopyOutcome::Failed;
    }

    // Recording `CopySegmentStarted` waits for the metadata manager to apply
    // it. Nothing is uploaded yet, so a broker that lost the partition in
    // that wait withdraws the record and leaves the tier untouched.
    if !fence.holds() {
        fenced(tp, ex, "before the upload");
        rollback(&metadata, broker_id, chain.archive(), rsm, rlmm).await;
        return CopyOutcome::Fenced;
    }

    let data = export_segment_data(ex, ProducerSnapshotExport::Include, || {
        leader_epoch_index_bytes(&epochs)
    });

    // The RSM is a blocking SPI — run the copy on the blocking pool.
    //
    // Bounded, because the pass is serial: an object store that accepts the
    // upload and then answers nothing would otherwise hold this partition,
    // and every partition queued behind it, for as long as the store cares
    // to stall. `spawn_blocking` cannot be cancelled, so past the deadline
    // the upload is abandoned rather than stopped — it keeps running on the
    // blocking pool against a segment id no metadata will ever finish, and
    // its objects are what the next tick's fresh id copies again.
    let rsm_copy = rsm.clone();
    let md_copy = metadata.clone();
    let copy_result = tokio::time::timeout(
        tier.copy_timeout.to_std(),
        spawn_blocking(move || rsm_copy.copy_log_segment_data(&md_copy, &data)),
    )
    .await;

    // Copy failed (or the blocking task panicked): clean up so the segment
    // is retried next tick.
    let returned = match copy_result {
        Ok(Ok(Ok(returned))) => returned,
        // The deadline expired. Nothing is rolled back: the abandoned upload
        // is still writing under this metadata's segment id, and a delete
        // racing it could remove an object the upload then re-creates,
        // leaving the tier holding a partial segment that no metadata
        // records. The segment stays in `CopySegmentStarted` instead, which
        // is what makes local retention refuse to drop its local copy and
        // the next tick re-copy it under a fresh id.
        Err(_) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                  timeout_ms = tier.copy_timeout.millis_i64(),
                  "remote-log-manager: segment copy exceeded its deadline; abandoning it and \
                   moving on to the next partition");
            seal_failed();
            return CopyOutcome::Failed;
        }
        Ok(Ok(Err(e))) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                  error = %e, "remote-log-manager: segment copy failed");
            seal_failed();
            rollback(&metadata, broker_id, chain.archive(), rsm, rlmm).await;
            return CopyOutcome::Failed;
        }
        Ok(Err(e)) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                  error = %e, "remote-log-manager: segment copy task panicked");
            seal_failed();
            rollback(&metadata, broker_id, chain.archive(), rsm, rlmm).await;
            return CopyOutcome::Failed;
        }
    };

    // The upload may have outlived this broker's leadership. On a mutable
    // tier the objects are withdrawn before anything references them, so the
    // new leader's copy of these offsets is the only one the tier keeps. A
    // write-once copy is past that point: the backend has sealed a manifest
    // over the objects, they cannot be deleted, and the chain the next copy
    // extends already runs through it, so it is published as usual.
    if chain == ChainPosition::Unchained && !fence.holds() {
        fenced(tp, ex, "after the upload");
        rollback(&metadata, broker_id, chain.archive(), rsm, rlmm).await;
        return CopyOutcome::Fenced;
    }

    // A write-once copy is only complete once the backend hands back a receipt
    // carrying the head its manifest produced. Without one the objects landed
    // with no verifiable manifest over them, so the segment must not be marked
    // finished: the read path serves every finished segment, and it would then
    // be serving unattested data. Leaving it in `CopySegmentStarted` is what
    // makes the next tick retry it under a fresh segment id.
    let next = match chain {
        ChainPosition::Unchained => ChainPosition::Unchained,
        ChainPosition::At(requested) => {
            let Some(receipt) = returned
                .as_ref()
                .and_then(|custom| WormChainRecord::from_custom_metadata(custom).ok())
            else {
                error!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                       "remote-log-manager: write-once copy returned no chain receipt; \
                        leaving the segment in CopySegmentStarted rather than serving \
                        unattested data");
                seal_failed();
                return CopyOutcome::Failed;
            };
            if receipt.head.is_none()
                || receipt.epoch_id != requested.epoch_id
                || receipt.seq != requested.seq
                || receipt.prev_head != requested.prev_head
            {
                error!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                       "remote-log-manager: write-once copy returned a mismatched chain receipt; \
                        leaving the segment in CopySegmentStarted rather than serving \
                        unattested data");
                seal_failed();
                return CopyOutcome::Failed;
            }
            // The receipt is the archive's word that a manifest over these
            // objects is sealed and chained where the copy asked for it, so
            // this is the one point at which a seal is known to have happened.
            tier.metrics.worm_manifests_sealed_total.inc();
            match receipt.next_stamp() {
                Some(stamp) => ChainPosition::At(stamp),
                None if receipt.seq.0 == u64::MAX => ChainPosition::Exhausted,
                None => return CopyOutcome::Failed,
            }
        }
        ChainPosition::Exhausted => return CopyOutcome::Failed,
    };

    // The backend's receipt is the chain position a restart reads back, so
    // it has to be durable alongside the segment, not dropped here.
    match super::rlmm::update_segment(
        rlmm,
        id,
        returned,
        RemoteLogSegmentState::CopySegmentFinished,
        broker_id,
    )
    .await
    {
        Ok(()) => {}
        Err(e) if deleted_under_the_copy(&e) => {
            debug!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                   end = ex.last_offset.0, error = %e,
                   "remote-log-manager: the segment was deleted while it was copied; \
                    withdrawing the copy");
            rollback(&metadata, broker_id, chain.archive(), rsm, rlmm).await;
            return CopyOutcome::Withdrawn { next };
        }
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                  error = %e, "remote-log-manager: failed to record CopySegmentFinished");
            return CopyOutcome::Failed;
        }
    }
    debug!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
           end = ex.last_offset.0, "remote-log-manager: copied segment to remote tier");
    match fence.established_log_start() {
        Some(floor) if ex.last_offset < floor => {
            if delete_below_floor(tier, tp, broker_id, &metadata, floor).await {
                CopyOutcome::Withdrawn { next }
            } else {
                CopyOutcome::Copied { next }
            }
        }
        _ => CopyOutcome::Copied { next },
    }
}

/// Whether a refused `CopySegmentFinished` means somebody deleted the segment
/// while it was being copied: a `DeleteRecords` that marked it
/// `DeleteSegmentStarted` under the floor it raised, or a retention pass that
/// went on to finish that delete. The copy then rolls back, and its bytes go
/// with it.
fn deleted_under_the_copy(error: &RemoteStorageError) -> bool {
    matches!(
        error,
        RemoteStorageError::SegmentNotFound(_)
            | RemoteStorageError::InvalidSegmentTransition {
                from: RemoteLogSegmentState::DeleteSegmentStarted
                    | RemoteLogSegmentState::DeleteSegmentFinished,
                ..
            }
    )
}

/// Delete a copy that has just finished wholly below `floor`, the partition's
/// established log start, and return whether it went.
///
/// The segment is read back from the metadata manager first, so a
/// `DeleteRecords` that marked it `DeleteSegmentStarted` in the meantime is
/// picked up where it left off. A delete that fails leaves a finished segment
/// below the floor, and the next retention pass's log-start breach removes it.
async fn delete_below_floor(
    tier: &RemoteTier<'_>,
    tp: &TopicIdPartition,
    broker_id: i32,
    copied: &RemoteLogSegmentMetadata,
    floor: Offset,
) -> bool {
    let id = copied.remote_log_segment_id();
    let current = match tier.rlmm.list_remote_log_segments(tp) {
        Ok(listed) => listed
            .into_iter()
            .find(|md| md.remote_log_segment_id() == id),
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = copied.start_offset(),
                  error = %e,
                  "remote-log-manager: failed to re-read a copy that finished below the log \
                   start; leaving it to remote retention");
            return false;
        }
    };
    // Somebody else finished the delete already.
    let Some(current) = current else {
        return true;
    };
    debug!(topic = %tp.topic, partition = tp.partition, base = copied.start_offset(),
           end = copied.end_offset(), floor = floor.0,
           "remote-log-manager: copy finished below the established log start; deleting it");
    tier.metrics
        .record_remote_request(RemoteTierPath::Delete, &tp.topic);
    let deleted = delete_one_segment(
        tp,
        broker_id,
        &current,
        tier.archive,
        tier.rsm,
        tier.rlmm,
        tier.index_cache,
    )
    .await;
    if !deleted {
        tier.metrics
            .record_remote_error(RemoteTierPath::Delete, &tp.topic);
    }
    deleted
}

/// Log a copy withdrawn because the broker lost the partition `when`.
fn fenced(tp: &TopicIdPartition, ex: &SegmentExport, when: &str) {
    debug!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
           end = ex.last_offset.0, when,
           "remote-log-manager: no longer the partition's leader in this epoch; withdrawing \
            the copy instead of publishing it");
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
            let _ = spawn_blocking(move || rsm_del.delete_log_segment_data(&md_del)).await;
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
        let _ = super::rlmm::update_segment(rlmm, id.clone(), None, state, broker_id).await;
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::remote_log_manager::{
            copy::CopyFence,
            copy_eligible,
            test_support::{
                self as fixtures, FakeWormArchive, copy_exports, local_backends, rolled_log,
                synth_export, tier, tp,
            },
        },
        assert2::{assert, check},
        krabka_ids::LeaderEpoch,
        krabka_log::Offset,
        krabka_remote_storage::{
            ChainHead, ChainStamp, CustomMetadata, EpochId, ManifestSeq, RemoteStorageError,
        },
        krabka_units::bytes,
        std::sync::Mutex,
    };

    /// An RSM whose copy always fails, but whose delete succeeds. The tests
    /// use it to exercise the failure rollback path.
    struct AlwaysFailRsm;

    #[krabka_macros::missing_remote_reads(delete_ok)]

    impl RemoteStorageManager for AlwaysFailRsm {
        fn copy_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
            _data: &LogSegmentData,
        ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
            Err(RemoteStorageError::InvalidArgument("boom".into()))
        }
    }

    /// Returns caller-selected metadata without touching storage.
    struct ReturningRsm(Option<CustomMetadata>);

    #[krabka_macros::missing_remote_reads(delete_ok)]

    impl RemoteStorageManager for ReturningRsm {
        fn copy_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
            _data: &LogSegmentData,
        ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
            Ok(self.0.clone())
        }
    }

    /// Records the metadata every copy hands the backend, then fails the copy.
    /// Tests use it to see what the broker stamped on a segment before the
    /// upload, which is the only moment that stamp is observable.
    #[derive(Default)]
    struct CapturingRsm {
        seen: Mutex<Vec<RemoteLogSegmentMetadata>>,
    }

    #[krabka_macros::missing_remote_reads(delete_ok)]

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
    }

    /// A fence that always holds, over a partition whose established log
    /// start reads `floor`.
    struct FloorFence(Option<Offset>);

    impl CopyFence for FloorFence {
        fn holds(&self) -> bool { true }

        fn established_log_start(&self) -> Option<Offset> { self.0 }
    }

    /// Accepts every copy and records the segment each delete removes. With
    /// `marking` set, a copy also moves its own segment to
    /// `DeleteSegmentStarted` before it answers, which is what a
    /// `DeleteRecords` that raises the floor over a copy in flight does.
    #[derive(Default)]
    struct DeletingRsm {
        marking: Option<Arc<dyn RemoteLogMetadataManager>>,
        deleted: Mutex<Vec<Uuid>>,
    }

    #[krabka_macros::missing_remote_reads]
    impl RemoteStorageManager for DeletingRsm {
        fn copy_log_segment_data(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _data: &LogSegmentData,
        ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
            if let Some(rlmm) = &self.marking {
                rlmm.update_remote_log_segment_metadata(
                    krabka_remote_storage::RemoteLogSegmentMetadataUpdate {
                        remote_log_segment_id: metadata.remote_log_segment_id().clone(),
                        event_timestamp_ms: 100,
                        custom_metadata: None,
                        state: RemoteLogSegmentState::DeleteSegmentStarted,
                        broker_id: 1,
                    },
                )?;
            }
            Ok(None)
        }

        fn delete_log_segment_data(
            &self,
            metadata: &RemoteLogSegmentMetadata,
        ) -> Result<(), RemoteStorageError> {
            self.deleted
                .lock()
                .expect("deleted-segments mutex poisoned")
                .push(metadata.remote_log_segment_id().id);
            Ok(())
        }
    }

    /// What one copy of the fixture segment, offsets 0 to 9, left: its
    /// outcome, the segment states still listed, and how many segments the
    /// backend deleted.
    #[derive(Debug, PartialEq, Eq)]
    struct CopyResidue {
        outcome: CopyOutcome,
        states: Vec<RemoteLogSegmentState>,
        deletes: usize,
    }

    async fn copy_under(
        rsm_impl: Arc<DeletingRsm>,
        rlmm: &Arc<dyn RemoteLogMetadataManager>,
        floor: Option<Offset>,
    ) -> CopyResidue {
        let rsm: Arc<dyn RemoteStorageManager> = rsm_impl.clone();
        let outcome = copy_one(
            &tier(ArchiveMode::Mutable, &rsm, rlmm),
            &tp(),
            CopyIdentity {
                broker_id: 1,
                leader_epoch: LeaderEpoch(0),
                fence: &FloorFence(floor),
            },
            &synth_export().call(),
            ChainPosition::Unchained,
        )
        .await;
        let states = rlmm
            .list_remote_log_segments(&tp())
            .unwrap()
            .iter()
            .map(RemoteLogSegmentMetadata::state)
            .collect();
        let deletes = rsm_impl
            .deleted
            .lock()
            .expect("deleted-segments mutex poisoned")
            .len();
        CopyResidue {
            outcome,
            states,
            deletes,
        }
    }

    /// A copy that finishes wholly below the partition's established log
    /// start is deleted straight away, so no finished segment is left below
    /// the floor for a follower to read as an unenforced one.
    #[yare::parameterized(
        no_established_floor = { None, false },
        floor_at_the_last_offset = { Some(9), false },
        floor_past_the_segment = { Some(10), true },
    )]
    #[test_macro(tokio::test)]
    async fn a_copy_that_finishes_below_the_floor_is_deleted(floor: Option<i64>, deleted: bool) {
        let rlmm = fixtures::in_memory_metadata();

        let residue = copy_under(Arc::default(), &rlmm, floor.map(Offset)).await;

        let next = ChainPosition::Unchained;
        let expected = if deleted {
            CopyResidue {
                outcome: CopyOutcome::Withdrawn { next },
                states: Vec::new(),
                deletes: 1,
            }
        } else {
            CopyResidue {
                outcome: CopyOutcome::Copied { next },
                states: vec![RemoteLogSegmentState::CopySegmentFinished],
                deletes: 0,
            }
        };
        check!(residue == expected);
    }

    /// A `DeleteRecords` that marks a segment `DeleteSegmentStarted` while
    /// its copy uploads makes `CopySegmentFinished` an invalid transition.
    /// The copy then rolls back: its bytes are deleted, and its metadata ends
    /// in `DeleteSegmentFinished`, which the metadata manager lists no more.
    #[tokio::test]
    async fn a_copy_marked_for_deletion_mid_copy_rolls_back() {
        let rlmm = fixtures::in_memory_metadata();
        let rsm_impl = Arc::new(DeletingRsm {
            marking: Some(rlmm.clone()),
            ..DeletingRsm::default()
        });

        let residue = copy_under(rsm_impl, &rlmm, None).await;

        check!(
            residue
                == CopyResidue {
                    outcome: CopyOutcome::Withdrawn {
                        next: ChainPosition::Unchained,
                    },
                    states: Vec::new(),
                    deletes: 1,
                }
        );
    }

    #[tokio::test]
    async fn copy_failure_rolls_back_and_leaves_no_metadata() {
        let log_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();
        assert!(!exports.is_empty());

        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(AlwaysFailRsm);
        let rlmm = fixtures::in_memory_metadata();

        let copied = copy_exports(&tier(ArchiveMode::Mutable, &rsm, &rlmm), exports.clone()).await;
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
        let fixtures::Backends { rsm, rlmm } = local_backends(remote_dir.path());

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
            last_modified_ms: 42,
            size: bytes(10),
            log_path: write("00.log", b"0123456789"),
            offset_index_path: write("00.index", b"i"),
            time_index_path: write("00.timeindex", b"t"),
            transaction_index_path: None,
            producer_snapshot_path: write("10.snapshot", b"snapshot"),
            leader_epochs: Vec::new(),
        };

        let copied = copy_eligible(
            &tier(ArchiveMode::Mutable, &rsm, &rlmm),
            &tp(),
            7,
            LeaderEpoch(3),
            vec![export],
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
            let rlmm = fixtures::in_memory_metadata();
            let export = synth_export().call();

            let outcome = copy_one(
                &tier(chain.archive(), &rsm, &rlmm),
                &tp(),
                CopyIdentity::unfenced(1, LeaderEpoch(0)),
                &export,
                chain,
            )
            .await;

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

    #[tokio::test]
    async fn copy_one_rejects_unattested_or_mismatched_chain_receipts() {
        let requested = ChainStamp {
            epoch_id: EpochId(Uuid::from_u128(0x5eed)),
            seq: ManifestSeq(4),
            prev_head: ChainHead([7; 32]),
        };
        let cases = [
            WormChainRecord::request(requested),
            WormChainRecord::request(ChainStamp {
                seq: ManifestSeq(5),
                ..requested
            })
            .with_head(ChainHead([8; 32])),
        ];
        for receipt in cases {
            let rsm: Arc<dyn RemoteStorageManager> =
                Arc::new(ReturningRsm(Some(receipt.to_custom_metadata())));
            let rlmm = fixtures::in_memory_metadata();

            let outcome = copy_one(
                &tier(ArchiveMode::WriteOnce, &rsm, &rlmm),
                &tp(),
                CopyIdentity::unfenced(1, LeaderEpoch(0)),
                &synth_export().call(),
                ChainPosition::At(requested),
            )
            .await;

            check!(matches!(outcome, CopyOutcome::Failed));
            check!(
                rlmm.list_remote_log_segments(&tp())
                    .unwrap()
                    .iter()
                    .all(|md| md.state() != RemoteLogSegmentState::CopySegmentFinished)
            );
        }
    }
    /// One seal case: what it is called, the backend it runs against, where
    /// the copy sits in the chain, and the counter values it must leave.
    struct SealCase {
        name: &'static str,
        rsm: Arc<dyn RemoteStorageManager>,
        chain: ChainPosition,
        sealed: u64,
        failed: u64,
    }

    /// The two KFC-5 seal counters, over every outcome one copy can reach.
    ///
    /// A seal is only attempted on a chained copy, so an unchained copy that
    /// fails must move neither counter: a mutable tier's copy failure says
    /// nothing about a WORM archive's attestation, and counting it would make
    /// the failure alert fire on clusters that hold no archive at all.
    #[tokio::test]
    async fn the_seal_counters_follow_the_chain_receipt() {
        let stamp = ChainStamp {
            epoch_id: EpochId(Uuid::from_u128(0x00c0_ffee)),
            seq: ManifestSeq(0),
            prev_head: ChainHead::GENESIS,
        };
        let cases: [SealCase; 4] = [
            SealCase {
                name: "a sealed manifest with a matching receipt",
                rsm: Arc::new(FakeWormArchive::new()),
                chain: ChainPosition::At(stamp),
                sealed: 1,
                failed: 0,
            },
            SealCase {
                name: "a copy the backend refused",
                rsm: Arc::new(AlwaysFailRsm),
                chain: ChainPosition::At(stamp),
                sealed: 0,
                failed: 1,
            },
            SealCase {
                name: "a copy that came back with no receipt",
                rsm: Arc::new(ReturningRsm(None)),
                chain: ChainPosition::At(stamp),
                sealed: 0,
                failed: 1,
            },
            SealCase {
                name: "an unchained copy that failed",
                rsm: Arc::new(AlwaysFailRsm),
                chain: ChainPosition::Unchained,
                sealed: 0,
                failed: 0,
            },
        ];
        for SealCase {
            name,
            rsm,
            chain,
            sealed,
            failed,
        } in cases
        {
            let fixtures::OwnedTierResources {
                rlmm,
                metrics,
                index_cache,
            } = fixtures::owned_tier_resources();
            let tier = fixtures::configured_tier()
                .rsm(&rsm)
                .rlmm(&rlmm)
                .archive(chain.archive())
                .metrics(&metrics)
                .index_cache(&index_cache)
                .call();

            copy_one(
                &tier,
                &tp(),
                CopyIdentity::unfenced(1, LeaderEpoch(0)),
                &synth_export().call(),
                chain,
            )
            .await;

            check!(
                metrics.worm_manifests_sealed_total.get() == sealed,
                "case {name}"
            );
            check!(
                metrics.worm_manifest_seal_failures_total.get() == failed,
                "case {name}"
            );
        }
    }
}

/// The export's epoch map, using the caller's fallback only when no checkpoint exists.
pub(crate) fn export_epoch_map(
    export: &SegmentExport,
    fallback: krabka_ids::LeaderEpoch,
) -> BTreeMap<krabka_ids::LeaderEpoch, i64> {
    if export.leader_epochs.is_empty() {
        maplit::btreemap! { fallback => export.base_offset.0 }
    } else {
        export
            .leader_epochs
            .iter()
            .map(|&(epoch, offset)| (epoch, offset.0))
            .collect()
    }
}

// Preserve export paths and optional snapshot policy before constructing the epoch bytes.
krabka_macros::export_segment_data_fixture!(export_segment_data);
