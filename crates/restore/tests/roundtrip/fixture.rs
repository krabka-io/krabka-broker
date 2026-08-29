//! The KIP-405 archive the round-trip tests restore from, and the batches a
//! correct restore must reproduce for every partition it holds.
//!
//! Each fixture partition is a real `krabka_log::Log` whose sealed segments
//! are copied through the real `LocalTieredStorage`, so building the archive
//! is the half of every test that has nothing to do with what the restore
//! then makes of it.

use std::collections::BTreeMap;

use assert2::assert;
use bytes::Bytes;
use krabka_ids::LeaderEpoch;
use krabka_log::Log;
use krabka_protocol::records::RecordBatch;
use krabka_remote_storage::{
    LocalTieredStorage, LogSegmentData, RemoteLogSegmentDetails, RemoteLogSegmentId,
    RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageManager as _, TopicIdPartition,
};
use tempfile::TempDir;
use uuid::Uuid;

use crate::batches::{text_batch, tiny_segment_config};

/// One archived segment: the id it was archived under, and the exact batch
/// (with its real, log-assigned `base_offset`) a correct restore must
/// reproduce verbatim.
pub(crate) struct SegmentFixture {
    pub(crate) segment_id: Uuid,
    pub(crate) batch: RecordBatch,
}

/// One archived partition: its identity, and every segment archived for it,
/// in base-offset order.
pub(crate) struct PartitionFixture {
    pub(crate) topic: &'static str,
    pub(crate) topic_id: Uuid,
    pub(crate) partition: i32,
    pub(crate) segments: Vec<SegmentFixture>,
}

impl PartitionFixture {
    /// The batches a correct restore must reproduce, in offset order.
    pub(crate) fn expected_batches(&self) -> Vec<RecordBatch> {
        self.segments.iter().map(|s| s.batch.clone()).collect()
    }
}

/// What one partition's fixture is built from: its identity, and its record
/// values grouped into batches so that every group but the last seals into
/// its own archived segment (see [`build_partition`]). The last group keeps
/// the log's active segment non-empty but is never archived: a real
/// tiered-storage copy never tiers the still-open active segment either.
#[derive(Clone, Copy)]
struct PartitionSpec<'a> {
    topic: &'static str,
    topic_id: Uuid,
    partition: i32,
    groups: &'a [&'a [&'a str]],
}

/// Build a real, multi-batch `krabka_log::Log` for one partition, roll it so
/// every group but the last seals into its own segment, and archive each
/// sealed segment through the real `LocalTieredStorage::copy_log_segment_data`
/// -- the same call the broker's remote-log-manager makes.
fn build_partition(storage: &LocalTieredStorage, spec: PartitionSpec<'_>) -> PartitionFixture {
    let local = tempfile::tempdir().expect("local log tempdir");
    let mut log = Log::open(local.path(), tiny_segment_config()).expect("open local log");

    let mut appended: Vec<RecordBatch> = Vec::with_capacity(spec.groups.len());
    for values in spec.groups {
        let mut batch = text_batch(values);
        log.append(&mut batch).expect("append batch");
        appended.push(batch);
    }

    let exports = log.tierable_segments();
    assert!(
        exports.len() == spec.groups.len() - 1,
        "{}-{}: every append after the first should roll exactly one segment",
        spec.topic,
        spec.partition,
    );

    let segments = exports
        .iter()
        .zip(appended.iter())
        .map(|(export, batch)| {
            let segment_id = Uuid::new_v4();
            let partition_id = TopicIdPartition::new(spec.topic_id, spec.topic, spec.partition);
            let metadata = RemoteLogSegmentMetadata::new(
                RemoteLogSegmentId::new(partition_id, segment_id),
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
                        leader_epoch_index: Bytes::from_static(b"0\n1\n0 0\n"),
                    },
                )
                .expect("archive the segment");

            SegmentFixture {
                segment_id,
                batch: batch.clone(),
            }
        })
        .collect();

    PartitionFixture {
        topic: spec.topic,
        topic_id: spec.topic_id,
        partition: spec.partition,
        segments,
    }
}

/// The whole archive this file tests against: two topics ("orders" with two
/// partitions, "payments" with one), and one partition ("orders-0") with two
/// archived segments.
pub(crate) struct Fixture {
    pub(crate) archive_root: TempDir,
    pub(crate) orders_id: Uuid,
    pub(crate) payments_id: Uuid,
    orders_0: PartitionFixture,
    orders_1: PartitionFixture,
    payments_0: PartitionFixture,
}

impl Fixture {
    /// Every partition, in the (topic, partition) order discovery sorts to.
    pub(crate) fn partitions(&self) -> [&PartitionFixture; 3] {
        [&self.orders_0, &self.orders_1, &self.payments_0]
    }

    /// Distinct topics the archive holds.
    pub(crate) fn topic_count() -> usize {
        2
    }
}

pub(crate) fn build_fixture() -> Fixture {
    let archive_root = tempfile::tempdir().expect("archive root");
    let storage = LocalTieredStorage::new(archive_root.path());
    let orders_id = Uuid::new_v4();
    let payments_id = Uuid::new_v4();

    let orders_0 = build_partition(
        &storage,
        PartitionSpec {
            topic: "orders",
            topic_id: orders_id,
            partition: 0,
            groups: &[&["o0-0", "o0-1"], &["o0-2"], &["o0-3", "o0-4"]],
        },
    );
    let orders_1 = build_partition(
        &storage,
        PartitionSpec {
            topic: "orders",
            topic_id: orders_id,
            partition: 1,
            groups: &[&["o1-0", "o1-1"], &["o1-2"]],
        },
    );
    let payments_0 = build_partition(
        &storage,
        PartitionSpec {
            topic: "payments",
            topic_id: payments_id,
            partition: 0,
            groups: &[&["p0-0", "p0-1"], &["p0-2"]],
        },
    );

    Fixture {
        archive_root,
        orders_id,
        payments_id,
        orders_0,
        orders_1,
        payments_0,
    }
}
