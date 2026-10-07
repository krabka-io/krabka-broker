//! Segment fixtures shared by the filesystem and object-store backend tests.

use std::path::{Path, PathBuf};

use bytes::Bytes;
use krabka_ids::LeaderEpoch;
use uuid::Uuid;

use crate::{
    metadata::{
        RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentState, TopicIdPartition,
    },
    storage_manager::LogSegmentData,
};

pub fn sample_metadata(id: u128) -> RemoteLogSegmentMetadata {
    RemoteLogSegmentMetadata::new(
        RemoteLogSegmentId::new(
            TopicIdPartition::new(Uuid::from_u128(1), "orders", 0),
            Uuid::from_u128(id),
        ),
        0,
        99,
        123,
        1,
        456,
        crate::metadata::RemoteLogSegmentDetails::new(
            8,
            RemoteLogSegmentState::CopySegmentStarted,
            maplit::btreemap! {LeaderEpoch(0) => 0},
        ),
    )
    .unwrap()
}

pub fn write_file(dir: &Path, name: &str, contents: &[u8]) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, contents).unwrap();
    p
}

pub fn sample_data(src: &Path, with_txn: bool) -> LogSegmentData {
    LogSegmentData {
        log_segment: write_file(src, "00.log", b"0123456789"),
        offset_index: write_file(src, "00.index", b"OFFSET-IDX"),
        time_index: write_file(src, "00.timeindex", b"TIME-IDX"),
        transaction_index: with_txn.then(|| write_file(src, "00.txnindex", b"TXN-IDX")),
        producer_snapshot_index: Some(write_file(src, "00.snapshot", b"SNAP")),
        leader_epoch_index: Bytes::from_static(b"EPOCH-BYTES"),
    }
}
