//! Fixtures shared by the `DeleteRecords` tests: the KFC-9 gated broker
//! configuration, and archived remote segments for the tiered trim.
//!
//! The gate's unit tests and the end-to-end refusal test both build a broker
//! that names an approver set, so the fixture lives beside neither and is
//! reachable from both. The approved proposal and the image that holds it are
//! the crate-wide break-glass fixtures in `break_glass::gate::tests`.
//!
//! The segment fixture is shared with the remote-log-manager tests of the
//! marking the tiered trim runs, which seed the same archive shapes.

pub(super) use crate::test_support::gated_break_glass_config as gated_config;
use {
    krabka_ids::LeaderEpoch,
    krabka_remote_storage::{
        RemoteLogMetadataManager, RemoteLogSegmentDetails, RemoteLogSegmentId,
        RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState,
        TopicIdPartition,
    },
    std::{ops::RangeInclusive, sync::Arc},
    uuid::Uuid,
};

/// One archived segment of a partition, by id, offset range and state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ArchivedSegment {
    pub(crate) id: u128,
    pub(crate) offsets: RangeInclusive<i64>,
    pub(crate) state: RemoteLogSegmentState,
}

/// Add `segment` to `rlmm` under `tp` and walk it to its state, the way a
/// copy, and then a delete, would.
pub(crate) fn seed_segment(
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    tp: &TopicIdPartition,
    segment: &ArchivedSegment,
) {
    let segment_id = RemoteLogSegmentId::new(tp.clone(), Uuid::from_u128(segment.id));
    let (start, end) = (*segment.offsets.start(), *segment.offsets.end());
    let md = RemoteLogSegmentMetadata::new(
        segment_id.clone(),
        start,
        end,
        100,
        1,
        100,
        RemoteLogSegmentDetails::new(
            100,
            RemoteLogSegmentState::CopySegmentStarted,
            maplit::btreemap! {LeaderEpoch(0) => start},
        ),
    )
    .expect("a valid segment");
    rlmm.add_remote_log_segment_metadata(md)
        .expect("add the segment");
    let path: &[RemoteLogSegmentState] = match segment.state {
        RemoteLogSegmentState::CopySegmentStarted => &[],
        RemoteLogSegmentState::CopySegmentFinished => &[RemoteLogSegmentState::CopySegmentFinished],
        RemoteLogSegmentState::DeleteSegmentStarted
        | RemoteLogSegmentState::DeleteSegmentFinished => &[
            RemoteLogSegmentState::CopySegmentFinished,
            RemoteLogSegmentState::DeleteSegmentStarted,
        ],
    };
    for &state in path {
        rlmm.update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: segment_id.clone(),
            event_timestamp_ms: 100,
            custom_metadata: None,
            state,
            broker_id: 1,
        })
        .expect("advance the segment");
    }
}

/// Every segment `rlmm` lists for `tp`, in the shape [`seed_segment`] takes.
pub(crate) fn archived_segments(
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    tp: &TopicIdPartition,
) -> Vec<ArchivedSegment> {
    let mut listed: Vec<ArchivedSegment> = rlmm
        .list_remote_log_segments(tp)
        .expect("list the segments")
        .iter()
        .map(|md| ArchivedSegment {
            id: md.remote_log_segment_id().id.as_u128(),
            offsets: md.start_offset()..=md.end_offset(),
            state: md.state(),
        })
        .collect();
    listed.sort_by_key(|segment| segment.id);
    listed
}
