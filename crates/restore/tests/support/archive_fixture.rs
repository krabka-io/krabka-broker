//! Copy one sealed real log segment into a KIP-405 test archive.

use bytes::Bytes;
use krabka_ids::LeaderEpoch;
use krabka_log::SegmentExport;
use krabka_remote_storage::{
    LogSegmentData, RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadata,
    RemoteLogSegmentState, RemoteStorageManager, TopicIdPartition,
};
use uuid::Uuid;

pub fn archive_segment(
    storage: &impl RemoteStorageManager,
    partition: TopicIdPartition,
    id: Uuid,
    export: &SegmentExport,
) -> RemoteLogSegmentMetadata {
    let metadata = RemoteLogSegmentMetadata::new(
        RemoteLogSegmentId::new(partition, id),
        export.base_offset.0,
        export.last_offset.0,
        export.max_timestamp,
        1,
        0,
        RemoteLogSegmentDetails::new(
            i32::try_from(
                std::fs::metadata(&export.log_path)
                    .expect("log metadata")
                    .len(),
            )
            .expect("fixture segment fits i32"),
            RemoteLogSegmentState::CopySegmentFinished,
            maplit::btreemap! {LeaderEpoch(0) => export.base_offset.0},
        ),
    )
    .expect("valid remote metadata");
    storage
        .copy_log_segment_data(
            &metadata,
            &LogSegmentData {
                log_segment: export.log_path.clone(),
                offset_index: export.offset_index_path.clone(),
                time_index: export.time_index_path.clone(),
                transaction_index: export.transaction_index_path.clone(),
                producer_snapshot_index: Some(export.producer_snapshot_path.clone()),
                leader_epoch_index: Bytes::from(
                    format!("0\n1\n0 {}\n", export.base_offset.0).into_bytes(),
                ),
            },
        )
        .expect("archive the segment");
    metadata
}
