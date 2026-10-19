//! A restore from a capture of a cluster that kept writing after it.
//!
//! A capture reads the RLMM snapshot once. The source keeps running, so by the
//! time a rehearsal or a disaster-day restore reads the archive, two things
//! have moved on. Remote copy has uploaded segments the snapshot never saw,
//! and remote retention has deleted, oldest first, segments the snapshot
//! still names as live. Both are the archive moving forward in time at its
//! two ends. Anything else the scan and the snapshot disagree about still
//! stops the restore.
//!
//! Compaction does not appear here: krabka refuses `remote.storage.enable`
//! on a compacted topic, so compaction never changes the archive.

use {
    crate::{
        args::{RestoreOptions, restore_args},
        fixture::{Fixture, build_fixture},
    },
    assert2::check,
    krabka_remote_storage::{LocalTieredStorage, RemoteStorageManager},
    krabka_restore::{RestoreError, restore},
    uuid::Uuid,
};

/// What the source did to the archive between the capture and the restore.
#[derive(Clone, Copy, Debug)]
enum AfterTheCapture {
    /// Remote copy uploaded the newest segment of `orders-0`.
    Uploaded,
    /// Remote retention deleted the oldest segment of `payments-1`.
    RetentionDeleted,
    /// Both of the above.
    UploadedAndRetentionDeleted,
    /// The newest segment of `orders-0` is gone and its older one is not.
    /// Retention never deletes in that order.
    NewestDeleted,
    /// Every segment of `orders-1` is gone, so the scan finds no partition
    /// that the snapshot names.
    PartitionEmptied,
}

/// A segment of the fixture, by partition and position.
#[derive(Clone, Copy, PartialEq, Eq)]
struct SegmentAt {
    topic: &'static str,
    partition: i32,
    index: usize,
}

const ORDERS_0_NEWEST: SegmentAt = SegmentAt {
    topic: "orders",
    partition: 0,
    index: 1,
};
const PAYMENTS_1_OLDEST: SegmentAt = SegmentAt {
    topic: "payments",
    partition: 1,
    index: 0,
};
const ORDERS_1_ONLY: SegmentAt = SegmentAt {
    topic: "orders",
    partition: 1,
    index: 0,
};

impl AfterTheCapture {
    /// The segments uploaded after the capture, which its snapshot omits.
    fn uploaded(self) -> &'static [SegmentAt] {
        match self {
            Self::Uploaded | Self::UploadedAndRetentionDeleted => &[ORDERS_0_NEWEST],
            Self::RetentionDeleted | Self::NewestDeleted | Self::PartitionEmptied => &[],
        }
    }

    /// The segments deleted from the archive after the capture, which its
    /// snapshot still names as live.
    fn deleted(self) -> &'static [SegmentAt] {
        match self {
            Self::Uploaded => &[],
            Self::RetentionDeleted | Self::UploadedAndRetentionDeleted => &[PAYMENTS_1_OLDEST],
            Self::NewestDeleted => &[ORDERS_0_NEWEST],
            Self::PartitionEmptied => &[ORDERS_1_ONLY],
        }
    }
}

/// The segment ids a restore of the moved-on archive must write, per
/// partition: everything the archive still holds.
fn surviving_segments(fixture: &Fixture, deleted: &[SegmentAt]) -> Vec<(String, i32, Vec<Uuid>)> {
    fixture
        .partitions()
        .into_iter()
        .map(|partition| {
            let ids = partition
                .segments
                .iter()
                .enumerate()
                .filter(|(index, _)| {
                    !deleted.contains(&SegmentAt {
                        topic: partition.topic,
                        partition: partition.partition,
                        index: *index,
                    })
                })
                .map(|(_, segment)| segment.segment_id)
                .collect();
            (partition.topic.to_owned(), partition.partition, ids)
        })
        .collect()
}

/// The partition a restore must refuse, for a change the source cannot have made.
#[derive(Debug, PartialEq, Eq)]
struct Disagreement {
    topic: String,
    partition: i32,
}

#[yare::parameterized(
    uploaded_after_the_capture = { AfterTheCapture::Uploaded, None },
    deleted_by_retention_after_the_capture = { AfterTheCapture::RetentionDeleted, None },
    uploaded_and_deleted_after_the_capture = { AfterTheCapture::UploadedAndRetentionDeleted, None },
    newest_gone_before_the_oldest = { AfterTheCapture::NewestDeleted, Some(("orders", 0)) },
    every_segment_of_a_partition_gone = { AfterTheCapture::PartitionEmptied, Some(("orders", 1)) },
)]
#[test_macro(tokio::test)]
async fn a_capture_restores_from_the_archive_its_source_kept_writing(
    after: AfterTheCapture,
    refused: Option<(&str, i32)>,
) {
    let fixture = build_fixture();
    let capture = tempfile::tempdir().unwrap();
    let snapshot = capture.path().join("rlmm-snapshot");
    // The capture names every archived segment but those uploaded after it.
    let captured = fixture.rlmm_snapshot(|partition, index| {
        !after.uploaded().contains(&SegmentAt {
            topic: partition.topic,
            partition: partition.partition,
            index,
        })
    });
    std::fs::write(&snapshot, captured).unwrap();

    let storage = LocalTieredStorage::new(fixture.archive_root.path());
    for at in after.deleted() {
        let segment = &fixture.partition(at.topic, at.partition).segments[at.index];
        storage
            .delete_log_segment_data(&segment.metadata)
            .expect("retention deletes the segment");
    }

    let target = tempfile::tempdir().unwrap();
    let snapshot_arg = snapshot.display().to_string();
    let args = restore_args(fixture.archive_root.path(), target.path(), RestoreOptions {
        extra: &["--rlmm-snapshot", &snapshot_arg],
        ..Default::default()
    });
    let outcome = restore(&args).await;

    match refused {
        None => {
            let report = outcome.expect("the restore accepts the moved-on archive");
            let restored: Vec<(String, i32, Vec<Uuid>)> = report
                .partitions
                .iter()
                .map(|partition| {
                    let ids = partition
                        .segments
                        .iter()
                        .map(|segment| segment.segment_id)
                        .collect();
                    (partition.topic.clone(), partition.partition, ids)
                })
                .collect();
            check!(restored == surviving_segments(&fixture, after.deleted()));
        }
        Some((topic, partition)) => {
            let error = outcome.expect_err("the restore refuses the archive");
            let disagreement = match error {
                RestoreError::MetadataDisagreement {
                    topic, partition, ..
                } => Some(Disagreement { topic, partition }),
                _ => None,
            };
            check!(
                disagreement
                    == Some(Disagreement {
                        topic: topic.to_owned(),
                        partition,
                    })
            );
        }
    }
}
