//! Unit tests for the archive scan: how a clean archive groups and sorts, what
//! `--topic` and `--archive-prefix` change about the selection, where a key the
//! codec cannot attribute ends up, and which archives are empty enough to be an
//! error.

use assert2::check;
use krabka_remote_storage::kafka_uuid;

use super::{
    test_support::{
        FULL_SEGMENT_SUFFIXES, PartitionKey, args_from, expected_full_segment, write_artifact,
        write_full_segment,
    },
    *,
};
use crate::backend::open_archive;

#[tokio::test]
async fn a_clean_archive_groups_and_sorts_by_topic_partition_and_base_offset() {
    let archive = tempfile::tempdir().expect("temp dir");
    let orders_id = Uuid::from_u128(1);
    let alerts_id = Uuid::from_u128(2);
    let seg_a = Uuid::from_u128(10);
    let seg_b = Uuid::from_u128(11);
    let seg_c = Uuid::from_u128(12);
    let seg_d = Uuid::from_u128(20);
    let seg_e = Uuid::from_u128(30);

    // orders-0: three segments, written out of base-offset order.
    write_full_segment(archive.path(), "orders", 0, orders_id, 100, seg_b);
    write_full_segment(archive.path(), "orders", 0, orders_id, 0, seg_a);
    write_full_segment(archive.path(), "orders", 0, orders_id, 250, seg_c);
    // orders-1: one segment.
    write_full_segment(archive.path(), "orders", 1, orders_id, 0, seg_d);
    // alerts-0: one segment. "alerts" sorts before "orders".
    write_full_segment(archive.path(), "alerts", 0, alerts_id, 0, seg_e);

    let args = args_from(archive.path(), &[]);
    let store = open_archive(&args).expect("store");
    let result = inventory(&store, &args).await.expect("inventory");

    let expected = ArchiveInventory {
        partitions: vec![
            PartitionInventory {
                partition: TopicIdPartition::new(alerts_id, "alerts", 0),
                segments: vec![expected_full_segment(
                    &store, "alerts", 0, alerts_id, 0, seg_e,
                )],
            },
            PartitionInventory {
                partition: TopicIdPartition::new(orders_id, "orders", 0),
                segments: vec![
                    expected_full_segment(&store, "orders", 0, orders_id, 0, seg_a),
                    expected_full_segment(&store, "orders", 0, orders_id, 100, seg_b),
                    expected_full_segment(&store, "orders", 0, orders_id, 250, seg_c),
                ],
            },
            PartitionInventory {
                partition: TopicIdPartition::new(orders_id, "orders", 1),
                segments: vec![expected_full_segment(
                    &store, "orders", 1, orders_id, 0, seg_d,
                )],
            },
        ],
        unrecognized: vec![],
    };
    check!(result == expected);

    // `TopicIdPartition` equality ignores the topic name, so pin the
    // names too: a bug that mixed up which name goes with which id would
    // not otherwise be caught by the struct comparison above.
    let names: Vec<(&str, i32)> = result
        .partitions
        .iter()
        .map(|p| (p.partition.topic.as_str(), p.partition.partition))
        .collect();
    check!(names == vec![("alerts", 0), ("orders", 0), ("orders", 1)]);
}

#[tokio::test]
async fn a_topic_filter_narrows_the_selection_and_the_rest_lands_in_unrecognized() {
    let archive = tempfile::tempdir().expect("temp dir");
    let orders_id = Uuid::from_u128(1);
    let payments_id = Uuid::from_u128(2);
    let orders_seg = Uuid::from_u128(10);
    let payments_seg = Uuid::from_u128(20);
    write_full_segment(archive.path(), "orders", 0, orders_id, 0, orders_seg);
    write_full_segment(archive.path(), "payments", 0, payments_id, 0, payments_seg);

    let args = args_from(archive.path(), &["--topic", "orders"]);
    let store = open_archive(&args).expect("store");
    let result = inventory(&store, &args).await.expect("inventory");

    check!(result.partitions.len() == 1);
    check!(result.partitions[0].partition.topic == "orders");
    // Every key of the topic `--topic` excluded is kept, not dropped.
    check!(result.unrecognized.len() == FULL_SEGMENT_SUFFIXES.len());
    let payments_dir = format!("payments-0-{}", kafka_uuid(payments_id));
    check!(
        result
            .unrecognized
            .iter()
            .all(|key| key.to_string().contains(&payments_dir))
    );
}

#[tokio::test]
async fn malformed_keys_land_in_unrecognized_not_dropped_and_not_an_error() {
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
    // Wrong number of path components: a key directly under the root.
    std::fs::write(archive.path().join("not-a-valid-key.log"), b"junk").expect("write");
    // Two components, but the directory name does not decode.
    std::fs::create_dir_all(archive.path().join("weird-dir")).expect("mkdir");
    std::fs::write(
        archive
            .path()
            .join("weird-dir")
            .join("00000000000000000005-not-base64.log"),
        b"junk",
    )
    .expect("write");

    let args = args_from(archive.path(), &[]);
    let store = open_archive(&args).expect("store");
    let result = inventory(&store, &args).await.expect("inventory");

    check!(result.partitions.len() == 1);
    check!(result.unrecognized.len() == 2);
    check!(
        result
            .unrecognized
            .iter()
            .any(|key| key.to_string().contains("not-a-valid-key.log"))
    );
    check!(
        result
            .unrecognized
            .iter()
            .any(|key| key.to_string().contains("weird-dir"))
    );
}

#[tokio::test]
async fn a_torn_copy_still_appears_with_the_missing_artifact_as_none() {
    let archive = tempfile::tempdir().expect("temp dir");
    let topic_id = Uuid::from_u128(1);
    let seg = Uuid::from_u128(10);
    let key = PartitionKey {
        topic: "orders",
        partition: 0,
        topic_id,
    };
    // Only `.log` and `.index` land; the rest of the copy never arrives.
    // Discovery reports presence and leaves judging completeness to
    // `verify`, so this must not become a `TornCopy` error here.
    write_artifact(archive.path(), None, key, 0, seg, ".log");
    write_artifact(archive.path(), None, key, 0, seg, ".index");

    let args = args_from(archive.path(), &[]);
    let store = open_archive(&args).expect("store");
    let result = inventory(&store, &args).await.expect("inventory");

    check!(result.partitions.len() == 1);
    let segment = &result.partitions[0].segments[0];
    check!(segment.log.is_some());
    check!(segment.offset_index.is_some());
    check!(segment.time_index.is_none());
    check!(segment.producer_snapshot.is_none());
    check!(segment.leader_epoch.is_none());
    check!(segment.transaction_index.is_none());
}

#[tokio::test]
async fn a_key_prefix_does_not_confuse_the_relative_path_split() {
    let archive = tempfile::tempdir().expect("temp dir");
    let topic_id = Uuid::from_u128(1);
    let seg = Uuid::from_u128(10);
    let key = PartitionKey {
        topic: "orders",
        partition: 0,
        topic_id,
    };
    write_artifact(archive.path(), Some("tier"), key, 0, seg, ".log");
    write_artifact(archive.path(), Some("tier"), key, 0, seg, ".index");

    let args = args_from(archive.path(), &["--archive-prefix", "tier"]);
    let store = open_archive(&args).expect("store");
    let result = inventory(&store, &args).await.expect("inventory");

    check!(result.unrecognized.is_empty());
    check!(result.partitions.len() == 1);
    check!(result.partitions[0].segments.len() == 1);
}

#[tokio::test]
async fn an_archive_with_nothing_in_it_is_an_empty_archive_error() {
    let archive = tempfile::tempdir().expect("temp dir");
    let args = args_from(archive.path(), &[]);
    let store = open_archive(&args).expect("store");
    let err = inventory(&store, &args).await.unwrap_err();
    check!(matches!(err, RestoreError::EmptyArchive { prefix } if prefix.is_empty()));
}

#[tokio::test]
async fn a_topic_filter_that_selects_nothing_is_also_an_empty_archive_error() {
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

    let args = args_from(archive.path(), &["--topic", "bogus"]);
    let store = open_archive(&args).expect("store");
    let err = inventory(&store, &args).await.unwrap_err();
    check!(matches!(err, RestoreError::EmptyArchive { .. }));
}
