//! Unit tests for the share-state machine: Kafka's validation and fencing
//! order, and the records each operation appends.

use assert2::{assert, check};
use tempfile::tempdir;

use super::*;
use crate::share_coordinator::{
    config::ShareCoordinatorConfig,
    coordinator::test_support::{
        Logged, NOW_MS, batch, configured_coordinator, coordinator, image_with_topic, lead_all,
        logged_records, share_write,
    },
};

const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([5; 16]);

type WriteOutcome = Result<(), ShareStateError>;

/// `(partition, (state_epoch, leader_epoch), (start_offset, dcc), drop the
/// state partition log, expected result, expected summary)`.
type WriteRow = (
    i32,
    (i32, i32),
    (i64, i32),
    bool,
    WriteOutcome,
    Option<ShareStateSummary>,
);

/// `(partition, leader_epoch, expected read as (state_epoch, start),
/// expected write with leader epoch 3, stored leader epoch after reload)`.
type ReadRow = (
    i32,
    i32,
    Result<(i32, i64), ShareStateError>,
    Option<WriteOutcome>,
    i32,
);

fn refused(code: ShareErrorCode, message: &'static str) -> ShareStateError {
    ShareStateError::Refused { code, message }
}

#[tokio::test]
async fn initialize_then_summary() {
    let dir = tempdir().unwrap();
    let (coord, _reg) = coordinator(dir.path());
    lead_all(&coord).await;

    coord
        .initialize(&image_with_topic(TOPIC, 1), "g", TOPIC, 0, 5, Offset(100))
        .await
        .unwrap();

    let summary = coord.read_summary("g", TOPIC, 0).await;
    assert!(summary == Ok(Some((5, 0, Offset(100), 0))));
}

/// Stored state for the read and write tables, on topic `TOPIC` with two
/// partitions: partition 0 at state epoch 2, leader epoch 3, start offset
/// 10 and delivery complete count 4, and partition 2 (not in the image of
/// two partitions) at state epoch 2.
async fn seeded(dir: &std::path::Path) -> (ShareCoordinator, MetadataImage) {
    let (coord, _reg) = coordinator(dir);
    lead_all(&coord).await;
    let image = image_with_topic(TOPIC, 2);
    let wide = image_with_topic(TOPIC, 3);
    coord
        .initialize(&wide, "g", TOPIC, 0, 2, Offset(10))
        .await
        .unwrap();
    coord
        .initialize(&wide, "g", TOPIC, 2, 2, Offset(10))
        .await
        .unwrap();
    coord.read(&image, "g", TOPIC, 0, 3).await.unwrap();
    coord
        .write(&image, "g", TOPIC, 0, share_write((2, 3), (10, 4), vec![]))
        .await
        .unwrap();
    assert!(coord.read_summary("g", TOPIC, 0).await == Ok(Some((2, 3, Offset(10), 4))));
    (coord, image)
}

fn write_rows() -> [WriteRow; 13] {
    let unchanged = Some((2, 3, Offset(10), 4));
    [
        (0, (2, 3), (10, 2), false, Ok(()), unchanged),
        (0, (2, 3), (5, 9), false, Ok(()), unchanged),
        (
            0,
            (2, 3),
            (20, 1),
            false,
            Ok(()),
            Some((2, 3, Offset(20), 1)),
        ),
        (0, (7, 3), (10, 4), false, Ok(()), unchanged),
        (0, (2, 9), (10, 4), false, Ok(()), unchanged),
        (0, (2, 3), (10, -1), false, Ok(()), unchanged),
        (
            0,
            (1, 3),
            (10, 4),
            false,
            Err(refused(
                codes::FENCED_STATE_EPOCH,
                message::FENCED_STATE_EPOCH,
            )),
            unchanged,
        ),
        (
            0,
            (2, 2),
            (10, 4),
            false,
            Err(refused(
                codes::FENCED_LEADER_EPOCH,
                message::FENCED_LEADER_EPOCH,
            )),
            unchanged,
        ),
        (
            1,
            (0, 0),
            (0, 0),
            false,
            Err(refused(
                codes::INVALID_REQUEST,
                message::WRITE_UNINITIALIZED_SHARE_PARTITION,
            )),
            None,
        ),
        (
            -1,
            (2, 3),
            (10, 4),
            false,
            Err(refused(
                codes::INVALID_REQUEST,
                message::NEGATIVE_PARTITION_ID,
            )),
            None,
        ),
        (
            0,
            (2, -1),
            (10, 4),
            false,
            Err(refused(
                codes::INVALID_REQUEST,
                message::NEGATIVE_LEADER_EPOCH,
            )),
            unchanged,
        ),
        (
            2,
            (2, 3),
            (10, 4),
            false,
            Err(refused(
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                message::UNKNOWN_TOPIC_OR_PARTITION,
            )),
            Some((2, 0, Offset(10), 0)),
        ),
        (
            0,
            (2, 3),
            (30, 0),
            true,
            Err(ShareStateError::Operation {
                code: codes::COORDINATOR_NOT_AVAILABLE,
                message: message::UNKNOWN_TOPIC_OR_PARTITION,
            }),
            unchanged,
        ),
    ]
}

/// `WriteShareGroupState` as Kafka's `ShareCoordinatorShard.writeState`
/// answers it, and the stored summary after each write.
#[tokio::test]
async fn write_matches_kafka_checks_and_record_rules() {
    for (index, (partition, epochs, progress, drop_log, expected, summary)) in
        write_rows().into_iter().enumerate()
    {
        let dir = tempdir().unwrap();
        let (coord, image) = seeded(dir.path()).await;
        if drop_log {
            let state_partition = coord.state_partition_for("g", &TOPIC, partition);
            coord
                .partitions
                .remove(crate::share_coordinator::bootstrap::TOPIC, state_partition);
        }
        let result = coord
            .write(
                &image,
                "g",
                TOPIC,
                partition,
                share_write(epochs, progress, vec![batch(progress.0, progress.0 + 9)]),
            )
            .await;
        check!(result == expected, "row {index}");
        check!(
            coord.read_summary("g", TOPIC, partition).await == Ok(summary),
            "row {index}"
        );
    }
}

#[tokio::test]
async fn write_refuses_a_negative_state_epoch() {
    let dir = tempdir().unwrap();
    let (coord, image) = seeded(dir.path()).await;
    let result = coord
        .write(&image, "g", TOPIC, 0, share_write((-1, 3), (10, 4), vec![]))
        .await;
    assert!(
        result
            == Err(refused(
                codes::INVALID_REQUEST,
                message::NEGATIVE_STATE_EPOCH
            ))
    );
}

/// `ReadShareGroupState` as Kafka's `readStateAndMaybeUpdateLeaderEpoch`
/// answers it, then a write with leader epoch 3, and the stored leader epoch
/// after a reload of the log.
#[tokio::test]
async fn read_fences_and_persists_the_leader_epoch() {
    let fenced = || refused(codes::FENCED_LEADER_EPOCH, message::FENCED_LEADER_EPOCH);
    let rows: [ReadRow; 7] = [
        (0, 3, Ok((2, 10)), Some(Ok(())), 3),
        (0, 4, Ok((2, 10)), Some(Err(fenced())), 4),
        (0, 2, Err(fenced()), Some(Ok(())), 3),
        (
            1,
            0,
            Err(refused(
                codes::INVALID_REQUEST,
                message::READ_UNINITIALIZED_SHARE_PARTITION,
            )),
            None,
            3,
        ),
        (
            -1,
            0,
            Err(refused(
                codes::INVALID_REQUEST,
                message::NEGATIVE_PARTITION_ID,
            )),
            None,
            3,
        ),
        (
            0,
            -1,
            Err(refused(
                codes::INVALID_REQUEST,
                message::NEGATIVE_LEADER_EPOCH,
            )),
            None,
            3,
        ),
        (
            2,
            0,
            Err(refused(
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                message::UNKNOWN_TOPIC_OR_PARTITION,
            )),
            None,
            3,
        ),
    ];

    for (index, (partition, leader_epoch, expected, write, reloaded_leader_epoch)) in
        rows.into_iter().enumerate()
    {
        let dir = tempdir().unwrap();
        let (coord, image) = seeded(dir.path()).await;
        let read = coord
            .read(&image, "g", TOPIC, partition, leader_epoch)
            .await
            .map(|st| (st.state_epoch, st.start_offset.0));
        check!(read == expected, "row {index}");
        if let Some(expected_write) = write {
            let written = coord
                .write(
                    &image,
                    "g",
                    TOPIC,
                    partition,
                    share_write((2, 3), (10, 4), vec![]),
                )
                .await;
            check!(written == expected_write, "row {index}");
        }
        coord.reload_all_partitions_for_test().await;
        let summary = coord.read_summary("g", TOPIC, 0).await;
        check!(
            summary.map(|s| s.map(|(_, leader, ..)| leader)) == Ok(Some(reloaded_leader_epoch)),
            "row {index}"
        );
    }
}

/// A failed append leaves the in-memory state as it was.
#[tokio::test]
async fn failed_read_append_changes_no_state() {
    let dir = tempdir().unwrap();
    let (coord, image) = seeded(dir.path()).await;
    let state_partition = coord.state_partition_for("g", &TOPIC, 0);
    coord
        .partitions
        .remove(crate::share_coordinator::bootstrap::TOPIC, state_partition);

    let read = coord.read(&image, "g", TOPIC, 0, 8).await;
    assert!(
        read == Err(ShareStateError::Operation {
            code: codes::COORDINATOR_NOT_AVAILABLE,
            message: message::UNKNOWN_TOPIC_OR_PARTITION,
        })
    );
    assert!(coord.read_summary("g", TOPIC, 0).await == Ok(Some((2, 3, Offset(10), 4))));
}

/// A re-initialize writes leader epoch 0, but the recorded leader epoch
/// stays, as Kafka's `leaderEpochMap` only grows: a share-partition leader
/// with an older epoch is still fenced.
#[tokio::test]
async fn reinitialize_keeps_the_recorded_leader_epoch() {
    let dir = tempdir().unwrap();
    let (coord, image) = seeded(dir.path()).await;
    coord
        .initialize(&image, "g", TOPIC, 0, 3, Offset(40))
        .await
        .unwrap();
    check!(coord.read_summary("g", TOPIC, 0).await == Ok(Some((3, 0, Offset(40), 0))));
    let rows = [
        (
            2,
            Err(refused(
                codes::FENCED_LEADER_EPOCH,
                message::FENCED_LEADER_EPOCH,
            )),
        ),
        (3, Ok(())),
    ];
    for (leader_epoch, expected) in rows {
        let written = coord
            .write(
                &image,
                "g",
                TOPIC,
                0,
                share_write((3, leader_epoch), (40, 0), vec![]),
            )
            .await;
        check!(written == expected, "leader epoch {leader_epoch}");
    }
}

fn update(start_offset: i64, delivery_complete_count: i32, batches: Vec<StateBatch>) -> Logged {
    Logged::Update(ShareUpdateValue {
        snapshot_epoch: 0,
        leader_epoch: 0,
        start_offset: Offset(start_offset),
        delivery_complete_count,
        state_batches: batches,
    })
}

/// With a threshold of two updates, the third write is one `ShareSnapshot`
/// in place of the update, as Kafka's `generateShareStateRecord` picks it.
/// An update holds only the written batches, combined among themselves and
/// clipped at the start offset. The snapshot folds the stored batches in.
#[tokio::test]
async fn writes_append_updates_then_one_snapshot_at_the_threshold() {
    let dir = tempdir().unwrap();
    let config = ShareCoordinatorConfig {
        snapshot_update_records_per_snapshot: 2,
        ..ShareCoordinatorConfig::default()
    };
    let (coord, _reg, _clock) = configured_coordinator(dir.path(), config);
    lead_all(&coord).await;
    let image = image_with_topic(TOPIC, 1);
    coord
        .initialize(&image, "g", TOPIC, 0, 1, Offset(0))
        .await
        .unwrap();
    let writes = [
        (0, 1, vec![batch(0, 9), batch(5, 14)]),
        (10, 2, vec![batch(0, 19)]),
        (12, 3, vec![batch(20, 29)]),
        (12, 3, vec![batch(30, 39)]),
    ];
    for (start, dcc, batches) in writes {
        coord
            .write(
                &image,
                "g",
                TOPIC,
                0,
                share_write((1, 0), (start, dcc), batches),
            )
            .await
            .unwrap();
    }

    let state_partition = coord.state_partition_for("g", &TOPIC, 0);
    let logged: Vec<Logged> = logged_records(&coord, state_partition)
        .into_iter()
        .map(|(_, logged)| logged)
        .collect();
    let expected = vec![
        Logged::Snapshot(ShareSnapshotValue {
            snapshot_epoch: 0,
            state_epoch: 1,
            leader_epoch: 0,
            start_offset: Offset(0),
            delivery_complete_count: 0,
            create_timestamp: NOW_MS,
            write_timestamp: NOW_MS,
            state_batches: vec![],
        }),
        update(0, 1, vec![batch(0, 14)]),
        update(10, 2, vec![batch(10, 19)]),
        Logged::Snapshot(ShareSnapshotValue {
            snapshot_epoch: 1,
            state_epoch: 1,
            leader_epoch: 0,
            start_offset: Offset(12),
            delivery_complete_count: 3,
            create_timestamp: NOW_MS,
            write_timestamp: NOW_MS,
            state_batches: vec![batch(12, 29)],
        }),
        Logged::Update(ShareUpdateValue {
            snapshot_epoch: 1,
            leader_epoch: 0,
            start_offset: Offset(12),
            delivery_complete_count: 3,
            state_batches: vec![batch(30, 39)],
        }),
    ];
    assert!(logged == expected);
    let state = coord.state_for_test("g", TOPIC, 0).await.unwrap();
    check!(state.state_batches == vec![batch(12, 39)]);
    check!(state.updates_since_snapshot == 1);
}
