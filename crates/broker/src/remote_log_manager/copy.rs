//! The per-partition copy pass: which sealed segments the remote tier still
//! lacks, and how one tick's copies thread a write-once archive's chain.

use std::collections::HashSet;

use krabka_log::SegmentExport;
use krabka_remote_storage::{RemoteLogSegmentState, TopicIdPartition};
use krabka_units::convert::ByteSizeExt as _;
use tracing::warn;

use super::{
    RemoteTier,
    archive::ChainPosition,
    copy_segment::{CopyOutcome, copy_one},
};

/// Copy every sealed segment in `exports` that the metadata store does not
/// already know about. Returns the number of segments newly copied to
/// `CopySegmentFinished`. This is a separate function from
/// [`tick_all`](super::tick_all) so
/// that tests can drive it directly against a real `Log` and a reference
/// RSM/RLMM.
pub(crate) async fn copy_eligible(
    tier: &RemoteTier<'_>,
    tp: &TopicIdPartition,
    broker_id: i32,
    leader_epoch: krabka_ids::LeaderEpoch,
    exports: Vec<SegmentExport>,
) -> usize {
    let listed = match tier.rlmm.list_remote_log_segments(tp) {
        Ok(list) => list,
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to list remote segments");
            return 0;
        }
    };

    // Only a *finished* copy claims a base offset.
    //
    // This skip set used to key on every state. A segment left in
    // `CopySegmentStarted` by a failed copy therefore claimed its offset
    // forever and was never retried. On a mutable tier `rollback` erased that
    // metadata, so the bug stayed hidden; a write-once archive keeps it, and
    // tiering for that offset would stop silently and permanently. A `Delete*`
    // segment does not claim its offset either: its bytes are on the way out,
    // so a still-local segment at the same base is copyable again.
    let mut known: HashSet<i64> = HashSet::new();
    for md in &listed {
        match md.state() {
            RemoteLogSegmentState::CopySegmentFinished => {
                known.insert(md.start_offset());
            }
            // This listing is taken once per tick, so anything already in
            // `CopySegmentStarted` was left there by an earlier tick.
            RemoteLogSegmentState::CopySegmentStarted => {
                warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
                      segment = %md.remote_log_segment_id().id,
                      "remote-log-manager: segment still in CopySegmentStarted after an \
                       earlier tick; re-copying it under a fresh segment id");
            }
            RemoteLogSegmentState::DeleteSegmentStarted
            | RemoteLogSegmentState::DeleteSegmentFinished => {}
        }
    }

    // KIP-405's `RemoteCopyLagSegments` / `RemoteCopyLagBytes`, recorded
    // before the round the way `RLMCopyTask.copyLogSegmentsToRemote` does:
    // the sealed local segments this partition has not finished copying, and
    // their total size. A tier that has stopped keeping up shows a lag that
    // climbs, where a rate would merely stop.
    let pending: Vec<&SegmentExport> = exports
        .iter()
        .filter(|ex| !known.contains(&ex.base_offset.0))
        .collect();
    tier.metrics.set_remote_copy_lag(
        &tp.topic,
        u64::try_from(pending.len()).unwrap_or(u64::MAX),
        pending.iter().map(|ex| ex.size.bytes_u64()).sum(),
    );

    let mut chain = ChainPosition::seed(tier.archive, &listed);
    let mut copied = 0;
    for ex in exports {
        if known.contains(&ex.base_offset.0) {
            continue;
        }
        if chain == ChainPosition::Exhausted {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                  "remote-log-manager: WORM chain sequence exhausted; refusing to copy a \
                   segment without a distinct chain position");
            break;
        }
        // Each success hands back the next chain position, so a run of
        // consecutive segments chains inside one tick with no further listing.
        if let CopyOutcome::Copied { next } =
            copy_one(tier, tp, broker_id, leader_epoch, &ex, chain).await
        {
            copied += 1;
            chain = next;
        }
    }
    copied
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_ids::LeaderEpoch;
    use krabka_remote_storage::{
        ChainHead, ChainStamp, CustomMetadata, EpochId, IndexType,
        InmemoryRemoteLogMetadataManager, LocalTieredStorage, LogSegmentData, ManifestSeq,
        RemoteLogMetadataManager, RemoteLogSegmentDetails, RemoteLogSegmentId,
        RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate, RemoteStorageError,
        RemoteStorageManager, WormChainRecord,
    };
    use uuid::Uuid;

    use super::*;
    use crate::{
        metrics::BrokerMetrics,
        remote_log_manager::{
            ArchiveMode, RemoteTier,
            test_support::{
                FakeWormArchive, rolled_log, stuck_started_segment, synth_export, tier, tp,
            },
        },
    };

    /// KIP-405's `RemoteCopyRequestsPerSec`, `RemoteCopyBytesPerSec` and the
    /// two copy-lag gauges. An operator whose object store starts refusing
    /// writes learns about it from these; before them, a stalled tier was
    /// visible only as consumer lag and a filling disk.
    #[tokio::test]
    async fn a_copy_round_records_its_requests_bytes_and_lag() {
        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(AcceptingRsm { receipt: None });
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let metrics = BrokerMetrics::new();
        let index_cache = Arc::new(krabka_remote_storage::RemoteIndexCache::disabled());
        let tier = RemoteTier {
            archive: ArchiveMode::Mutable,
            rsm: &rsm,
            rlmm: &rlmm,
            metrics: &metrics,
            index_cache: &index_cache,
        };
        let exports = vec![synth_export(0, 9, 100, 64), synth_export(10, 19, 200, 64)];

        let copied = copy_eligible(&tier, &tp(), 1, LeaderEpoch(0), exports).await;

        check!(copied == 2);
        let topic = crate::metrics::TopicLabel {
            topic: std::sync::Arc::from(tp().topic.as_str()),
        };
        check!(metrics.remote_copy_requests_total.get_or_create(&topic).get() == 2);
        check!(metrics.remote_copy_errors_total.get_or_create(&topic).get() == 0);
        check!(metrics.remote_copy_bytes_total.get_or_create(&topic).get() == 128);
        // The lag is what the round found waiting, recorded before it ran.
        check!(metrics.remote_copy_lag_segments.get_or_create(&topic).get() == 2);
        check!(metrics.remote_copy_lag_bytes.get_or_create(&topic).get() == 128);
    }

    /// A copy the backend refuses counts as an error and moves no bytes, so
    /// the ratio of the two counters is a failure rate an alert can read.
    #[tokio::test]
    async fn a_refused_copy_counts_an_error_and_no_bytes() {
        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(RefusingRsm);
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let metrics = BrokerMetrics::new();
        let index_cache = Arc::new(krabka_remote_storage::RemoteIndexCache::disabled());
        let tier = RemoteTier {
            archive: ArchiveMode::Mutable,
            rsm: &rsm,
            rlmm: &rlmm,
            metrics: &metrics,
            index_cache: &index_cache,
        };

        let copied = copy_eligible(
            &tier,
            &tp(),
            1,
            LeaderEpoch(0),
            vec![synth_export(0, 9, 100, 64)],
        )
        .await;

        check!(copied == 0);
        let topic = crate::metrics::TopicLabel {
            topic: std::sync::Arc::from(tp().topic.as_str()),
        };
        check!(metrics.remote_copy_requests_total.get_or_create(&topic).get() == 1);
        check!(metrics.remote_copy_errors_total.get_or_create(&topic).get() == 1);
        check!(metrics.remote_copy_bytes_total.get_or_create(&topic).get() == 0);
    }

    /// An RSM that refuses every copy, so the error path is the only one a
    /// round over it can take.
    struct RefusingRsm;

    impl RemoteStorageManager for RefusingRsm {
        fn copy_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
            _data: &LogSegmentData,
        ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
            Err(RemoteStorageError::Io(std::io::Error::other(
                "the backend refused the copy",
            )))
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

    /// An RSM whose copy always succeeds, handing back `receipt` verbatim,
    /// and whose delete always succeeds. It touches no files, so tests can
    /// drive it with synthetic exports.
    struct AcceptingRsm {
        receipt: Option<CustomMetadata>,
    }

    impl RemoteStorageManager for AcceptingRsm {
        fn copy_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
            _data: &LogSegmentData,
        ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
            Ok(self.receipt.clone())
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

    /// Every WORM receipt the metadata manager holds for `tp()`, oldest
    /// segment first.
    fn chain_records(rlmm: &Arc<dyn RemoteLogMetadataManager>) -> Vec<WormChainRecord> {
        let mut listed = rlmm.list_remote_log_segments(&tp()).unwrap();
        listed.sort_by_key(RemoteLogSegmentMetadata::start_offset);
        listed
            .iter()
            .map(|md| {
                WormChainRecord::from_custom_metadata(
                    md.custom_metadata()
                        .expect("an archived segment carries a chain receipt"),
                )
                .expect("the chain receipt decodes")
            })
            .collect()
    }

    #[tokio::test]
    async fn copies_all_sealed_segments_and_records_finished() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();
        assert!(exports.len() >= 2, "test needs multiple sealed segments");

        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        let copied = copy_eligible(
            &tier(ArchiveMode::Mutable, &rsm, &rlmm),
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
        )
        .await;
        assert!(copied == exports.len());

        let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
        assert!(listed.len() == exports.len());
        for md in &listed {
            // The data + offset/leader-epoch indexes are fetchable (non-empty)
            // from the remote store.
            check!(md.state() == RemoteLogSegmentState::CopySegmentFinished);
            check!(!rsm.fetch_log_segment(md, 0, None).unwrap().is_empty());
            check!(!rsm.fetch_index(md, IndexType::Offset).unwrap().is_empty());
            check!(
                !rsm.fetch_index(md, IndexType::ProducerSnapshot)
                    .unwrap()
                    .is_empty()
            );
            check!(
                !rsm.fetch_index(md, IndexType::LeaderEpoch)
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn re_running_is_idempotent() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();

        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        let first = copy_eligible(
            &tier(ArchiveMode::Mutable, &rsm, &rlmm),
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
        )
        .await;
        assert!(first == exports.len());
        // Second pass: everything is already known → nothing re-copied.
        let second = copy_eligible(
            &tier(ArchiveMode::Mutable, &rsm, &rlmm),
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
        )
        .await;
        assert!(second == 0);
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().len() == exports.len());
    }

    #[tokio::test]
    async fn empty_exports_copies_nothing() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let copied = copy_eligible(
            &tier(ArchiveMode::Mutable, &rsm, &rlmm),
            &tp(),
            1,
            LeaderEpoch(0),
            Vec::new(),
        )
        .await;
        assert!(copied == 0);
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
    }

    #[tokio::test]
    async fn copy_eligible_records_the_rsm_receipt_on_copy_segment_finished() {
        let receipt = CustomMetadata(b"backend-receipt-42".to_vec());
        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(AcceptingRsm {
            receipt: Some(receipt.clone()),
        });
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        let copied = copy_eligible(
            &tier(ArchiveMode::Mutable, &rsm, &rlmm),
            &tp(),
            1,
            LeaderEpoch(0),
            vec![synth_export(0, 9, 100, 64)],
        )
        .await;

        check!(copied == 1);
        let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
        check!(listed.len() == 1);
        check!(listed[0].state() == RemoteLogSegmentState::CopySegmentFinished);
        check!(listed[0].custom_metadata() == Some(&receipt));
    }

    #[tokio::test]
    async fn copy_eligible_chains_consecutive_segments() {
        let archive = Arc::new(FakeWormArchive::new());
        let rsm: Arc<dyn RemoteStorageManager> = archive.clone();
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let exports = vec![
            synth_export(0, 9, 100, 64),
            synth_export(10, 19, 200, 64),
            synth_export(20, 29, 300, 64),
        ];

        let copied = copy_eligible(
            &tier(ArchiveMode::WriteOnce, &rsm, &rlmm),
            &tp(),
            1,
            LeaderEpoch(0),
            exports,
        )
        .await;

        check!(copied == 3);
        check!(archive.archived_segments() == 3);
        let records = chain_records(&rlmm);
        check!(records.len() == 3);
        check!(
            records.iter().map(|r| r.seq).collect::<Vec<_>>()
                == vec![ManifestSeq(0), ManifestSeq(1), ManifestSeq(2)]
        );
        // One chain run, and each manifest hashes onto the one before it.
        check!(records.iter().all(|r| r.epoch_id == records[0].epoch_id));
        check!(records[0].prev_head == ChainHead::GENESIS);
        check!(records[1].prev_head == records[0].head.unwrap());
        check!(records[2].prev_head == records[1].head.unwrap());
    }

    #[tokio::test]
    async fn copy_eligible_finishes_the_last_sequence_then_stops() {
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let segment_id = RemoteLogSegmentId::new(tp(), Uuid::from_u128(0xdead));
        let started = RemoteLogSegmentMetadata::new(
            segment_id.clone(),
            0,
            9,
            100,
            1,
            100,
            RemoteLogSegmentDetails::new(
                64,
                RemoteLogSegmentState::CopySegmentStarted,
                maplit::btreemap! {LeaderEpoch(0) => 0},
            ),
        )
        .unwrap();
        rlmm.add_remote_log_segment_metadata(started).unwrap();
        let receipt = WormChainRecord::request(ChainStamp {
            epoch_id: EpochId(Uuid::from_u128(7)),
            seq: ManifestSeq(u64::MAX - 1),
            prev_head: ChainHead([0xaa; 32]),
        })
        .with_head(ChainHead([0xbb; 32]))
        .to_custom_metadata();
        rlmm.update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: segment_id,
            event_timestamp_ms: 101,
            custom_metadata: Some(receipt),
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        })
        .unwrap();

        let archive = Arc::new(FakeWormArchive::new());
        let rsm: Arc<dyn RemoteStorageManager> = archive.clone();
        let copied = copy_eligible(
            &tier(ArchiveMode::WriteOnce, &rsm, &rlmm),
            &tp(),
            1,
            LeaderEpoch(0),
            vec![
                synth_export(0, 9, 100, 64),
                synth_export(10, 19, 200, 64),
                synth_export(20, 29, 300, 64),
            ],
        )
        .await;

        check!(copied == 1);
        check!(archive.archived_segments() == 1);
        let records = chain_records(&rlmm);
        check!(records.len() == 2);
        check!(records[1].seq == ManifestSeq(u64::MAX));
    }

    #[tokio::test]
    async fn copy_eligible_resumes_the_chain_from_the_rlmm_after_a_restart() {
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let first_rsm: Arc<dyn RemoteStorageManager> = Arc::new(FakeWormArchive::new());
        let copied = copy_eligible(
            &tier(ArchiveMode::WriteOnce, &first_rsm, &rlmm),
            &tp(),
            1,
            LeaderEpoch(0),
            vec![synth_export(0, 9, 100, 64), synth_export(10, 19, 200, 64)],
        )
        .await;
        check!(copied == 2);
        let before = chain_records(&rlmm);

        // A restart: a brand-new backend and a brand-new copy pass, sharing
        // only the metadata manager. The chain continues from the receipts.
        let second_rsm: Arc<dyn RemoteStorageManager> = Arc::new(FakeWormArchive::new());
        let copied = copy_eligible(
            &tier(ArchiveMode::WriteOnce, &second_rsm, &rlmm),
            &tp(),
            1,
            LeaderEpoch(0),
            vec![
                synth_export(0, 9, 100, 64),
                synth_export(10, 19, 200, 64),
                synth_export(20, 29, 300, 64),
            ],
        )
        .await;

        check!(copied == 1, "only the segment the archive lacks is copied");
        let after = chain_records(&rlmm);
        check!(after.len() == 3);
        check!(after[..2] == before[..]);
        check!(after[2].epoch_id == before[0].epoch_id);
        check!(after[2].seq == ManifestSeq(2));
        check!(after[2].prev_head == before[1].head.unwrap());
    }

    #[tokio::test]
    async fn copy_eligible_starts_a_new_epoch_when_the_rlmm_is_empty() {
        let mut genesis = Vec::new();
        for _ in 0..2 {
            let rsm: Arc<dyn RemoteStorageManager> = Arc::new(FakeWormArchive::new());
            let rlmm: Arc<dyn RemoteLogMetadataManager> =
                Arc::new(InmemoryRemoteLogMetadataManager::new());
            let copied = copy_eligible(
                &tier(ArchiveMode::WriteOnce, &rsm, &rlmm),
                &tp(),
                1,
                LeaderEpoch(0),
                vec![synth_export(0, 9, 100, 64)],
            )
            .await;
            check!(copied == 1);
            genesis.push(chain_records(&rlmm).remove(0));
        }

        // A metadata manager holding no receipt cannot continue the old
        // chain, so each run says so with a fresh epoch instead of restarting
        // the old one at sequence zero and looking like a rewrite.
        check!(genesis[0].epoch_id != genesis[1].epoch_id);
        for record in &genesis {
            check!((record.seq, record.prev_head) == (ManifestSeq(0), ChainHead::GENESIS));
        }
    }

    #[tokio::test]
    async fn copy_eligible_retries_a_segment_stuck_in_copy_segment_started() {
        let cases: [(&str, ArchiveMode, Arc<dyn RemoteStorageManager>); 2] = [
            (
                "mutable tier",
                ArchiveMode::Mutable,
                Arc::new(AcceptingRsm { receipt: None }),
            ),
            (
                "write-once archive",
                ArchiveMode::WriteOnce,
                Arc::new(FakeWormArchive::new()),
            ),
        ];
        for (name, archive, rsm) in cases {
            let rlmm: Arc<dyn RemoteLogMetadataManager> =
                Arc::new(InmemoryRemoteLogMetadataManager::new());
            let abandoned = stuck_started_segment(&rlmm, 0x57c, 0);

            let copied = copy_eligible(
                &tier(archive, &rsm, &rlmm),
                &tp(),
                1,
                LeaderEpoch(0),
                vec![synth_export(0, 9, 100, 64)],
            )
            .await;

            check!(copied == 1, "case {name}");
            let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
            let finished: Vec<&RemoteLogSegmentMetadata> = listed
                .iter()
                .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
                .collect();
            check!(finished.len() == 1, "case {name}");
            check!(finished[0].start_offset() == 0, "case {name}");
            check!(
                finished[0].remote_log_segment_id().id != abandoned,
                "case {name}: the retry runs under a fresh segment id"
            );
        }
    }
}
