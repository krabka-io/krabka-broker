//! Unit tests for reconciliation, driven through the scan the way an operator
//! reaches it: a snapshot that agrees, a deletion still in flight that is
//! dropped in silence, and the four disagreements that stop a restore.

use assert2::check;
use krabka_remote_storage::{RlmmCacheDump, TopicIdPartition};

use super::*;
use crate::{
    backend::open_archive,
    discover::{
        inventory,
        test_support::{args_from, snapshot_segment, write_full_segment, write_snapshot},
    },
};

#[tokio::test]
async fn a_snapshot_that_agrees_keeps_every_live_segment() {
    let archive = tempfile::tempdir().expect("temp dir");
    let topic_id = Uuid::from_u128(1);
    let seg_a = Uuid::from_u128(10);
    let seg_b = Uuid::from_u128(11);
    write_full_segment(archive.path(), "orders", 0, topic_id, 0, seg_a);
    write_full_segment(archive.path(), "orders", 0, topic_id, 100, seg_b);

    let snap_dir = tempfile::tempdir().expect("temp dir");
    let snap_path = snap_dir.path().join("snapshot");
    write_snapshot(
        &snap_path,
        RlmmCacheDump {
            partitions: vec![PartitionDump {
                topic_id_partition: TopicIdPartition::new(topic_id, "orders", 0),
                segments: vec![
                    snapshot_segment(
                        "orders",
                        0,
                        topic_id,
                        seg_a,
                        0,
                        RemoteLogSegmentState::CopySegmentFinished,
                    ),
                    snapshot_segment(
                        "orders",
                        0,
                        topic_id,
                        seg_b,
                        100,
                        RemoteLogSegmentState::CopySegmentFinished,
                    ),
                ],
                delete_state: None,
            }],
        },
    );

    let args = args_from(
        archive.path(),
        &["--rlmm-snapshot", &snap_path.display().to_string()],
    );
    let store = open_archive(&args).expect("store");
    let result = inventory(&store, &args).await.expect("inventory");

    check!(result.partitions.len() == 1);
    check!(result.partitions[0].segments.len() == 2);
}

#[tokio::test]
async fn a_delete_started_segment_is_excluded_from_the_inventory_without_an_error() {
    let archive = tempfile::tempdir().expect("temp dir");
    let topic_id = Uuid::from_u128(1);
    let seg_a = Uuid::from_u128(10);
    let seg_b = Uuid::from_u128(11);
    write_full_segment(archive.path(), "orders", 0, topic_id, 0, seg_a);
    write_full_segment(archive.path(), "orders", 0, topic_id, 100, seg_b);

    let snap_dir = tempfile::tempdir().expect("temp dir");
    let snap_path = snap_dir.path().join("snapshot");
    write_snapshot(
        &snap_path,
        RlmmCacheDump {
            partitions: vec![PartitionDump {
                topic_id_partition: TopicIdPartition::new(topic_id, "orders", 0),
                segments: vec![
                    snapshot_segment(
                        "orders",
                        0,
                        topic_id,
                        seg_a,
                        0,
                        RemoteLogSegmentState::CopySegmentFinished,
                    ),
                    // Deletion is in flight: the remote tier may not
                    // have caught up yet, so leftover bytes are
                    // expected and this must not be an error.
                    snapshot_segment(
                        "orders",
                        0,
                        topic_id,
                        seg_b,
                        100,
                        RemoteLogSegmentState::DeleteSegmentStarted,
                    ),
                ],
                delete_state: None,
            }],
        },
    );

    let args = args_from(
        archive.path(),
        &["--rlmm-snapshot", &snap_path.display().to_string()],
    );
    let store = open_archive(&args).expect("store");
    let result = inventory(&store, &args).await.expect("inventory");

    check!(result.partitions.len() == 1);
    check!(result.partitions[0].segments.len() == 1);
    check!(result.partitions[0].segments[0].segment_id == seg_a);
}

#[tokio::test]
async fn a_segment_the_snapshot_does_not_mention_is_a_disagreement() {
    let archive = tempfile::tempdir().expect("temp dir");
    let topic_id = Uuid::from_u128(1);
    write_full_segment(
        archive.path(),
        "orders",
        0,
        topic_id,
        0,
        Uuid::from_u128(10),
    );

    let snap_dir = tempfile::tempdir().expect("temp dir");
    let snap_path = snap_dir.path().join("snapshot");
    write_snapshot(
        &snap_path,
        RlmmCacheDump {
            partitions: vec![PartitionDump {
                topic_id_partition: TopicIdPartition::new(topic_id, "orders", 0),
                // The snapshot knows nothing about this partition's
                // segment at all.
                segments: vec![],
                delete_state: None,
            }],
        },
    );

    let args = args_from(
        archive.path(),
        &["--rlmm-snapshot", &snap_path.display().to_string()],
    );
    let store = open_archive(&args).expect("store");
    let err = inventory(&store, &args).await.unwrap_err();
    check!(matches!(
        err,
        RestoreError::MetadataDisagreement { topic, partition, .. }
            if topic == "orders" && partition == 0
    ));
}

#[tokio::test]
async fn a_delete_finished_segment_with_bytes_still_present_is_a_disagreement() {
    // `DeleteSegmentFinished` says the bytes should be gone. Bytes still
    // being in the archive is a real inconsistency, unlike
    // `DeleteSegmentStarted`, where a deletion still in flight leaving
    // bytes behind is routine and gets dropped silently instead.
    let archive = tempfile::tempdir().expect("temp dir");
    let topic_id = Uuid::from_u128(1);
    let seg = Uuid::from_u128(10);
    write_full_segment(archive.path(), "orders", 0, topic_id, 0, seg);

    let snap_dir = tempfile::tempdir().expect("temp dir");
    let snap_path = snap_dir.path().join("snapshot");
    write_snapshot(
        &snap_path,
        RlmmCacheDump {
            partitions: vec![PartitionDump {
                topic_id_partition: TopicIdPartition::new(topic_id, "orders", 0),
                segments: vec![snapshot_segment(
                    "orders",
                    0,
                    topic_id,
                    seg,
                    0,
                    RemoteLogSegmentState::DeleteSegmentFinished,
                )],
                delete_state: None,
            }],
        },
    );

    let args = args_from(
        archive.path(),
        &["--rlmm-snapshot", &snap_path.display().to_string()],
    );
    let store = open_archive(&args).expect("store");
    let err = inventory(&store, &args).await.unwrap_err();
    check!(matches!(err, RestoreError::MetadataDisagreement { .. }));
}

#[tokio::test]
async fn a_live_partition_missing_from_the_scan_entirely_is_a_disagreement() {
    let archive = tempfile::tempdir().expect("temp dir");
    let payments_id = Uuid::from_u128(2);
    let payments_seg = Uuid::from_u128(20);
    write_full_segment(archive.path(), "payments", 0, payments_id, 0, payments_seg);

    let orders_id = Uuid::from_u128(1);
    let snap_dir = tempfile::tempdir().expect("temp dir");
    let snap_path = snap_dir.path().join("snapshot");
    write_snapshot(
        &snap_path,
        RlmmCacheDump {
            partitions: vec![
                // Matches the scan exactly: no disagreement from this one.
                PartitionDump {
                    topic_id_partition: TopicIdPartition::new(payments_id, "payments", 0),
                    segments: vec![snapshot_segment(
                        "payments",
                        0,
                        payments_id,
                        payments_seg,
                        0,
                        RemoteLogSegmentState::CopySegmentFinished,
                    )],
                    delete_state: None,
                },
                // Live in the snapshot, but the scan never found this
                // partition at all.
                PartitionDump {
                    topic_id_partition: TopicIdPartition::new(orders_id, "orders", 0),
                    segments: vec![snapshot_segment(
                        "orders",
                        0,
                        orders_id,
                        Uuid::from_u128(10),
                        0,
                        RemoteLogSegmentState::CopySegmentFinished,
                    )],
                    delete_state: None,
                },
            ],
        },
    );

    let args = args_from(
        archive.path(),
        &["--rlmm-snapshot", &snap_path.display().to_string()],
    );
    let store = open_archive(&args).expect("store");
    let err = inventory(&store, &args).await.unwrap_err();
    check!(matches!(
        err,
        RestoreError::MetadataDisagreement { topic, partition, .. }
            if topic == "orders" && partition == 0
    ));
}

#[tokio::test]
async fn a_missing_snapshot_file_is_reported_as_io_not_found() {
    let archive = tempfile::tempdir().expect("temp dir");
    let topic_id = Uuid::from_u128(1);
    write_full_segment(
        archive.path(),
        "orders",
        0,
        topic_id,
        0,
        Uuid::from_u128(10),
    );

    let missing = archive.path().join("does-not-exist-snapshot");
    let args = args_from(
        archive.path(),
        &["--rlmm-snapshot", &missing.display().to_string()],
    );
    let store = open_archive(&args).expect("store");
    let err = inventory(&store, &args).await.unwrap_err();
    check!(matches!(
        err,
        RestoreError::Io(io_error) if io_error.kind() == std::io::ErrorKind::NotFound
    ));
}
