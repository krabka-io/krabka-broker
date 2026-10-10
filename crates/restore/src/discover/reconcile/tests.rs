//! Unit tests for reconciliation, driven through the scan the way an operator
//! reaches it: a snapshot that agrees, a deletion still in flight that is
//! dropped in silence, and the four disagreements that stop a restore.

use assert2::check;
use krabka_remote_storage::{RlmmCacheDump, TopicIdPartition};

use super::*;
use crate::{
    args::RestoreArgs,
    backend::open_archive,
    discover::{
        inventory,
        test_support::{
            args_from, single_segment_archive, snapshot_segment, write_full_segment, write_snapshot,
        },
    },
};

/// Partition 0 of `topic` as the snapshot records it, one segment per
/// `(segment_id, base_offset, state)`.
fn partition_dump(
    topic: &str,
    topic_id: Uuid,
    segments: &[(Uuid, i64, RemoteLogSegmentState)],
) -> PartitionDump {
    PartitionDump {
        topic_id_partition: TopicIdPartition::new(topic_id, topic, 0),
        segments: segments
            .iter()
            .map(|&(segment_id, base_offset, state)| {
                snapshot_segment(topic, 0, topic_id, segment_id, base_offset, state)
            })
            .collect(),
        delete_state: None,
    }
}

/// Write a snapshot at `path` that holds the one [`partition_dump`].
fn snapshot_of(
    path: &std::path::Path,
    topic: &str,
    topic_id: Uuid,
    segments: &[(Uuid, i64, RemoteLogSegmentState)],
) {
    write_snapshot(
        path,
        RlmmCacheDump {
            partitions: vec![partition_dump(topic, topic_id, segments)],
        },
    );
}

fn two_segment_archive() -> (tempfile::TempDir, Uuid, Uuid, Uuid) {
    let archive = tempfile::tempdir().expect("temp dir");
    let (topic, first, second) = (Uuid::from_u128(1), Uuid::from_u128(10), Uuid::from_u128(11));
    write_full_segment(archive.path(), "orders", 0, topic, 0, first);
    write_full_segment(archive.path(), "orders", 0, topic, 100, second);
    (archive, topic, first, second)
}

fn two_segment_snapshot(
    second_state: RemoteLogSegmentState,
) -> (
    tempfile::TempDir,
    tempfile::TempDir,
    std::path::PathBuf,
    Uuid,
) {
    let (archive, topic_id, first, second) = two_segment_archive();
    let snapshot_dir = tempfile::tempdir().expect("temp dir");
    let snapshot = snapshot_dir.path().join("snapshot");
    snapshot_of(
        &snapshot,
        "orders",
        topic_id,
        &[
            (first, 0, RemoteLogSegmentState::CopySegmentFinished),
            (second, 100, second_state),
        ],
    );
    (archive, snapshot_dir, snapshot, first)
}

fn authenticated_snapshot(
    state: RemoteLogSegmentState,
) -> (tempfile::TempDir, std::path::PathBuf, RestoreArgs) {
    let (archive, topic_id, segment_id) = single_segment_archive(0);
    let snapshot = archive.path().join("snapshot");
    snapshot_of(&snapshot, "orders", topic_id, &[(segment_id, 0, state)]);
    let mut args = args_from(
        archive.path(),
        &["--rlmm-snapshot", &snapshot.display().to_string()],
    );
    args.archive.worm_key_id.push("trusted".into());
    args.archive.worm_public_key.push("unused.pub".into());
    (archive, snapshot, args)
}

async fn snapshot_inventory(
    archive: &std::path::Path,
    snapshot: &std::path::Path,
) -> Result<crate::discover::ArchiveInventory, RestoreError> {
    let args = args_from(
        archive,
        &["--rlmm-snapshot", &snapshot.display().to_string()],
    );
    let store = open_archive(&args).expect("store");
    inventory(&store, &args).await
}

fn check_orders_disagreement(error: RestoreError) {
    check!(
        matches!(error, RestoreError::MetadataDisagreement { topic, partition, .. } if topic == "orders" && partition == 0)
    );
}

#[tokio::test]
async fn a_snapshot_that_agrees_keeps_every_live_segment() {
    let (archive, _snapshot_dir, snap_path, _) =
        two_segment_snapshot(RemoteLogSegmentState::CopySegmentFinished);

    let result = snapshot_inventory(archive.path(), &snap_path)
        .await
        .expect("inventory");

    check!(result.partitions.len() == 1);
    check!(result.partitions[0].segments.len() == 2);
}

#[tokio::test]
async fn a_delete_started_segment_is_excluded_from_the_inventory_without_an_error() {
    // Deletion is in flight: leftover remote bytes are expected and must not
    // make inventory fail while the remote tier catches up.
    let (archive, _snapshot_dir, snap_path, seg_a) =
        two_segment_snapshot(RemoteLogSegmentState::DeleteSegmentStarted);

    let result = snapshot_inventory(archive.path(), &snap_path)
        .await
        .expect("inventory");

    check!(result.partitions.len() == 1);
    check!(result.partitions[0].segments.len() == 1);
    check!(result.partitions[0].segments[0].segment_id == seg_a);
}

#[tokio::test]
async fn authenticated_inventory_does_not_let_unsigned_rlmm_remove_a_segment() {
    let (_archive, _snapshot, args) =
        authenticated_snapshot(RemoteLogSegmentState::DeleteSegmentStarted);
    let store = open_archive(&args).expect("store");

    let result = inventory(&store, &args).await.expect("inventory");
    check!(result.partitions[0].segments.len() == 1);
}

#[tokio::test]
async fn authenticated_rlmm_excludes_objects_retained_after_completed_deletion() {
    let (_archive, snap_path, args) =
        authenticated_snapshot(RemoteLogSegmentState::DeleteSegmentFinished);
    let store = open_archive(&args).expect("store");
    let mut result = inventory(&store, &args).await.expect("inventory");

    reconcile_authenticated_with_snapshot(&mut result.partitions, &args, &snap_path)
        .expect("authenticated reconciliation");
    check!(result.partitions.is_empty());
}

#[tokio::test]
async fn a_segment_the_snapshot_does_not_mention_is_a_disagreement() {
    let (archive, topic_id, _) = single_segment_archive(0);

    let snap_dir = tempfile::tempdir().expect("temp dir");
    let snap_path = snap_dir.path().join("snapshot");
    // The snapshot knows nothing about this partition's segment at all.
    snapshot_of(&snap_path, "orders", topic_id, &[]);

    let err = snapshot_inventory(archive.path(), &snap_path)
        .await
        .unwrap_err();
    check_orders_disagreement(err);
}

#[tokio::test]
async fn a_delete_finished_segment_with_bytes_still_present_is_a_disagreement() {
    // `DeleteSegmentFinished` says the bytes should be gone. Bytes still
    // being in the archive is a real inconsistency, unlike
    // `DeleteSegmentStarted`, where a deletion still in flight leaving
    // bytes behind is routine and gets dropped silently instead.
    let (archive, topic_id, seg) = single_segment_archive(0);

    let snap_dir = tempfile::tempdir().expect("temp dir");
    let snap_path = snap_dir.path().join("snapshot");
    snapshot_of(
        &snap_path,
        "orders",
        topic_id,
        &[(seg, 0, RemoteLogSegmentState::DeleteSegmentFinished)],
    );

    let err = snapshot_inventory(archive.path(), &snap_path)
        .await
        .unwrap_err();
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
                partition_dump(
                    "payments",
                    payments_id,
                    &[(payments_seg, 0, RemoteLogSegmentState::CopySegmentFinished)],
                ),
                // Live in the snapshot, but the scan never found this
                // partition at all.
                partition_dump(
                    "orders",
                    orders_id,
                    &[(
                        Uuid::from_u128(10),
                        0,
                        RemoteLogSegmentState::CopySegmentFinished,
                    )],
                ),
            ],
        },
    );

    let err = snapshot_inventory(archive.path(), &snap_path)
        .await
        .unwrap_err();
    check_orders_disagreement(err);
}

#[tokio::test]
async fn a_missing_snapshot_file_is_reported_as_io_not_found() {
    let (archive, _topic_id, _) = single_segment_archive(0);

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

#[tokio::test]
async fn duplicate_segment_keys_in_the_snapshot_are_a_disagreement() {
    let (archive, topic_id, segment_id) = single_segment_archive(0);

    let snap_dir = tempfile::tempdir().expect("temp dir");
    let snap_path = snap_dir.path().join("snapshot");
    snapshot_of(
        &snap_path,
        "orders",
        topic_id,
        &[
            (segment_id, 0, RemoteLogSegmentState::CopySegmentFinished),
            (segment_id, 0, RemoteLogSegmentState::CopySegmentFinished),
        ],
    );

    let err = snapshot_inventory(archive.path(), &snap_path)
        .await
        .unwrap_err();
    check!(matches!(err, RestoreError::MetadataDisagreement { .. }));
}

#[tokio::test]
async fn duplicate_partition_keys_in_the_snapshot_are_a_disagreement() {
    let (archive, topic_id, segment_id) = single_segment_archive(0);

    let snap_dir = tempfile::tempdir().expect("temp dir");
    let snap_path = snap_dir.path().join("snapshot");
    let duplicate = partition_dump(
        "orders",
        topic_id,
        &[(segment_id, 0, RemoteLogSegmentState::CopySegmentFinished)],
    );
    write_snapshot(
        &snap_path,
        RlmmCacheDump {
            partitions: vec![duplicate.clone(), duplicate],
        },
    );

    let err = snapshot_inventory(archive.path(), &snap_path)
        .await
        .unwrap_err();
    check!(matches!(err, RestoreError::MetadataDisagreement { .. }));
}

#[tokio::test]
async fn maximum_offset_reconciliation_is_stable_across_retry() {
    let (archive, topic_id, segment_id) = single_segment_archive(i64::MAX);

    let snap_dir = tempfile::tempdir().expect("temp dir");
    let snap_path = snap_dir.path().join("snapshot");
    snapshot_of(
        &snap_path,
        "orders",
        topic_id,
        &[(
            segment_id,
            i64::MAX,
            RemoteLogSegmentState::CopySegmentFinished,
        )],
    );

    let args = args_from(
        archive.path(),
        &["--rlmm-snapshot", &snap_path.display().to_string()],
    );
    let store = open_archive(&args).expect("store");
    let first = inventory(&store, &args).await.expect("first inventory");
    let retry = inventory(&store, &args).await.expect("retry inventory");

    check!(first == retry);
    check!(first.partitions[0].segments[0].base_offset.get() == i64::MAX);
}

#[tokio::test]
async fn a_corrupt_snapshot_file_is_reported_as_io_invalid_data() {
    let archive = tempfile::tempdir().expect("temp dir");
    let snap_path = archive.path().join("snapshot");
    std::fs::write(&snap_path, b"not an RLMM snapshot").expect("write corrupt snapshot");

    let err = snapshot_inventory(archive.path(), &snap_path)
        .await
        .unwrap_err();
    check!(matches!(
        err,
        RestoreError::Io(io_error) if io_error.kind() == std::io::ErrorKind::InvalidData
    ));
}
