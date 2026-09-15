//! Unit tests for the `__share_group_state` replay: the recovery read bound,
//! the round trip from a written state back into memory, and the per-record
//! and inter-batch offset arithmetic that the replay cursor depends on.

use assert2::{assert, check};
use krabka_protocol::records::{Record, RecordBatch};
use tempfile::tempdir;

use super::*;
use crate::{
    partition_registry::PartitionRegistry,
    share_coordinator::{
        config::ShareCoordinatorConfig,
        coordinator::test_support::{batch, coordinator, lead_all, open_state_partition},
        persistence::{StateBatch, encode_state_key},
    },
};

#[tokio::test]
async fn recover_honors_nondefault_read_bound() {
    let dir = tempdir().unwrap();
    let registry = Arc::new(PartitionRegistry::new());
    let topic_id = uuid::Uuid::from_bytes([42; 16]);
    let state_partition = crate::share_coordinator::partitioner::partition_for_share_key(
        "bounded",
        &topic_id,
        0,
        ShareCoordinatorConfig::default().state_topic_num_partitions,
    );
    open_state_partition(&registry, dir.path(), state_partition);
    let partition = registry
        .get(bootstrap::TOPIC, PartitionIndex(state_partition))
        .expect("state partition open");
    let key = ShareStateKey {
        record_type: KEY_SHARE_SNAPSHOT,
        group_id: "bounded".to_string(),
        topic_id,
        partition: 0,
    };
    let snapshot = ShareSnapshotValue {
        snapshot_epoch: 0,
        state_epoch: 3,
        leader_epoch: 4,
        start_offset: Offset(5),
        delivery_complete_count: 6,
        state_batches: vec![],
    };
    let mut batch = RecordBatch::default();
    batch.records.push(Record {
        key: Some(encode_state_key(&key)),
        value: Some(snapshot.encode()),
        ..Record::default()
    });
    batch.records.push(Record {
        value: Some(Bytes::from(vec![0; 2_048])),
        ..Record::default()
    });
    batch.last_offset_delta = 1;
    partition.produce_batch(batch).await.unwrap();

    let image = state_partition_image(state_partition, 1, 0);
    let bounded = Arc::new(ShareCoordinator::new(
        krabka_audit::NodeId(1),
        Arc::clone(&registry),
        ShareCoordinatorConfig {
            recovery_read_max: krabka_units::bytes(700),
            ..ShareCoordinatorConfig::default()
        },
    ));
    bounded.recover(&image).await.unwrap();
    assert!(bounded.read("bounded", topic_id, 0).await == Ok(None));

    let unbounded = Arc::new(ShareCoordinator::new(
        krabka_audit::NodeId(1),
        registry,
        ShareCoordinatorConfig {
            recovery_read_max: krabka_units::kibibytes(4),
            ..ShareCoordinatorConfig::default()
        },
    ));
    unbounded.recover(&image).await.unwrap();
    assert!(unbounded.read_summary("bounded", topic_id, 0).await == Ok(Some((3, 4, Offset(5), 6))));
}

#[tokio::test]
async fn write_persists_and_recovers() {
    let dir = tempdir().unwrap();
    let reg = Arc::new(PartitionRegistry::new());
    for p in 0..ShareCoordinatorConfig::default().state_topic_num_partitions {
        open_state_partition(&reg, dir.path(), p);
    }
    let tid = uuid::Uuid::from_bytes([10; 16]);
    {
        let coord = ShareCoordinator::new(
            krabka_audit::NodeId(1),
            reg.clone(),
            ShareCoordinatorConfig::default(),
        );
        lead_all(&coord).await;
        coord.initialize("g", tid, 0, 2, Offset(0)).await.unwrap();
        coord
            .write("g", tid, 0, (2, 3), (Offset(20), 4), vec![batch(20, 29)])
            .await
            .unwrap();
    }

    // New coordinator over the SAME registry (same open logs); recover
    // replays the records written above.
    let recovered = ShareCoordinator::new(
        krabka_audit::NodeId(1),
        reg.clone(),
        ShareCoordinatorConfig::default(),
    );
    // `recover` derives leadership from a MetadataImage. Here every state
    // partition starts a new term directly, and each log is replayed.
    recovered.reload_all_partitions_for_test().await;

    let st = recovered
        .read("g", tid, 0)
        .await
        .unwrap()
        .expect("recovered");
    check!(st.state_epoch == 2);
    check!(st.leader_epoch == 3);
    check!(st.start_offset == 20);
    check!(st.delivery_complete_count == 4);
    check!(st.state_batches == vec![batch(20, 29)]);
}

// The replay must derive each record's offset as
// `base_offset + offset_delta` and advance the inter-batch cursor as
// `base_offset + last_offset_delta + 1`. A hand-crafted TWO-record batch
// (an update at offset_delta 0, then a snapshot at offset_delta 1) pins
// both: after replay, the snapshot's recorded `last_snapshot_offset` must
// be `base_offset + 1`, and a second batch appended after it must also be
// replayed (only reachable when the cursor advances by
// `last_offset_delta + 1`).
#[tokio::test]
async fn replay_uses_per_record_and_inter_batch_offsets() {
    let dir = tempdir().unwrap();
    let (coord, reg) = coordinator(dir.path());
    lead_all(&coord).await;
    let tid = uuid::Uuid::from_bytes([11; 16]);
    let state_partition = coord.state_partition_for("g", &tid, 0);
    let part = reg
        .get(bootstrap::TOPIC, state_partition)
        .expect("state partition open");

    let snap_key = encode_state_key(&ShareStateKey {
        record_type: KEY_SHARE_SNAPSHOT,
        group_id: "g".to_string(),
        topic_id: tid,
        partition: 0,
    });
    let upd_key = encode_state_key(&ShareStateKey {
        record_type: KEY_SHARE_UPDATE,
        group_id: "g".to_string(),
        topic_id: tid,
        partition: 0,
    });

    // Batch A (base_offset 0): an UPDATE at delta 0, then a SNAPSHOT at
    // delta 1 (last_offset_delta = 1). The snapshot's rec_offset is
    // `base_offset + 1 == 1`.
    let mut batch_a = RecordBatch {
        last_offset_delta: 1,
        ..RecordBatch::default()
    };
    batch_a.records.push(Record {
        offset_delta: 0,
        key: Some(upd_key.clone()),
        value: Some(
            ShareUpdateValue {
                snapshot_epoch: 0,
                leader_epoch: 1,
                start_offset: Offset(0),
                delivery_complete_count: 0,
                state_batches: vec![],
            }
            .encode(),
        ),
        ..Default::default()
    });
    batch_a.records.push(Record {
        offset_delta: 1,
        key: Some(snap_key.clone()),
        value: Some(
            ShareSnapshotValue {
                snapshot_epoch: 5,
                state_epoch: 2,
                leader_epoch: 3,
                start_offset: Offset(20),
                delivery_complete_count: 4,
                state_batches: vec![batch(20, 29)],
            }
            .encode(),
        ),
        ..Default::default()
    });
    part.produce_batch(batch_a).await.unwrap();

    // Batch B (base_offset 2): a later SNAPSHOT. Only reached if the cursor
    // advanced past batch A by `last_offset_delta + 1`.
    let mut batch_b = RecordBatch::default();
    batch_b.records.push(Record {
        offset_delta: 0,
        key: Some(snap_key.clone()),
        value: Some(
            ShareSnapshotValue {
                snapshot_epoch: 6,
                state_epoch: 2,
                leader_epoch: 9,
                start_offset: Offset(50),
                delivery_complete_count: 8,
                state_batches: vec![batch(50, 59)],
            }
            .encode(),
        ),
        ..Default::default()
    });
    part.produce_batch(batch_b).await.unwrap();

    coord.reload_all_partitions_for_test().await;

    let st = coord.read("g", tid, 0).await.unwrap().expect("recovered");
    // Batch B is the final snapshot — proves the inter-batch cursor advanced
    // past batch A (base_offset + last_offset_delta + 1 == 2).
    check!(st.leader_epoch == 9);
    check!(st.start_offset == 50);
    check!(st.delivery_complete_count == 8);
    check!(st.state_batches == vec![batch(50, 59)]);
    // Batch B's snapshot sits at base_offset 2 (single record, delta 0).
    check!(st.last_snapshot_offset == 2);
}

/// A replay of ONLY batch A pins the per-record offset arithmetic.
///
/// The snapshot at `offset_delta 1` over `base_offset 0` records
/// `last_snapshot_offset == 1`, not `Offset(-1)`.
#[tokio::test]
async fn replay_snapshot_offset_is_base_plus_delta() {
    let dir = tempdir().unwrap();
    let (coord, reg) = coordinator(dir.path());
    lead_all(&coord).await;
    let tid = uuid::Uuid::from_bytes([12; 16]);
    let state_partition = coord.state_partition_for("g", &tid, 0);
    let part = reg
        .get(bootstrap::TOPIC, state_partition)
        .expect("state partition open");

    let snap_key = encode_state_key(&ShareStateKey {
        record_type: KEY_SHARE_SNAPSHOT,
        group_id: "g".to_string(),
        topic_id: tid,
        partition: 0,
    });
    let upd_key = encode_state_key(&ShareStateKey {
        record_type: KEY_SHARE_UPDATE,
        group_id: "g".to_string(),
        topic_id: tid,
        partition: 0,
    });

    // Single batch, base_offset 0: an UPDATE at delta 0 then a SNAPSHOT at
    // delta 1. The snapshot's rec_offset is `0 + 1 == 1`.
    let mut batch_a = RecordBatch {
        last_offset_delta: 1,
        ..RecordBatch::default()
    };
    batch_a.records.push(Record {
        offset_delta: 0,
        key: Some(upd_key),
        value: Some(
            ShareUpdateValue {
                snapshot_epoch: 0,
                leader_epoch: 1,
                start_offset: Offset(0),
                delivery_complete_count: 0,
                state_batches: vec![],
            }
            .encode(),
        ),
        ..Default::default()
    });
    batch_a.records.push(Record {
        offset_delta: 1,
        key: Some(snap_key),
        value: Some(
            ShareSnapshotValue {
                snapshot_epoch: 5,
                state_epoch: 2,
                leader_epoch: 3,
                start_offset: Offset(20),
                delivery_complete_count: 4,
                state_batches: vec![batch(20, 29)],
            }
            .encode(),
        ),
        ..Default::default()
    });
    part.produce_batch(batch_a).await.unwrap();

    coord.reload_all_partitions_for_test().await;

    let st = coord.read("g", tid, 0).await.unwrap().expect("recovered");
    check!(st.leader_epoch == 3);
    check!(st.start_offset == 20);
    // The snapshot record sits at base_offset(0) + offset_delta(1) == 1.
    check!(st.last_snapshot_offset == 1);
}

/// A metadata image with one `__share_group_state` partition, led by `leader`
/// at `leader_epoch`.
fn state_partition_image(partition: i32, leader: u64, leader_epoch: i32) -> MetadataImage {
    let node = krabka_metadata::NodeId(leader);
    MetadataImage::from_records(
        uuid::Uuid::nil(),
        &[
            krabka_metadata::MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
                name: bootstrap::TOPIC.to_string(),
                topic_id: uuid::Uuid::from_bytes([44; 16]),
                partitions: ShareCoordinatorConfig::default().state_topic_num_partitions,
                replication_factor: 2,
            }),
            krabka_metadata::MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
                topic: bootstrap::TOPIC.to_string(),
                partition,
                leader: node,
                replicas: vec![krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)],
                isr: vec![krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)],
                leader_epoch: krabka_metadata::LeaderEpoch(leader_epoch),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: leader_epoch,
            }),
        ],
    )
}

#[derive(Debug, Clone, Copy)]
enum Broker {
    A,
    B,
}

#[derive(Debug)]
enum Step {
    /// Apply an image where `leader` leads the state partition at `epoch`.
    /// `wait` waits for the loads that the refresh started.
    Refresh {
        on: Broker,
        leader: u64,
        epoch: i32,
        wait: bool,
    },
    /// Wait until the load that an earlier refresh started has ended.
    AwaitActive { on: Broker },
    /// Advance the start offset and write one batch.
    Write {
        on: Broker,
        start: i64,
        expected: Result<(), i16>,
    },
    /// Read the state: `Ok(Some((start_offset, batches)))`, `Ok(None)` for no
    /// state, or the error code.
    Read {
        on: Broker,
        expected: Result<Option<(i64, Vec<StateBatch>)>, i16>,
    },
}

/// Leadership of a `__share_group_state` partition moves between two brokers
/// that share one log (the replicated log of the partition). Each broker
/// loads the log when it becomes the leader, answers
/// `COORDINATOR_LOAD_IN_PROGRESS` until the load ends, and drops its state and
/// answers `NOT_COORDINATOR` when it resigns (Kafka's
/// `ShareCoordinatorService.onElection` and `onResignation`).
#[tokio::test]
async fn leadership_change_loads_and_unloads_the_state_partition() {
    use crate::codes::{COORDINATOR_LOAD_IN_PROGRESS, NOT_COORDINATOR};

    let dir = tempdir().unwrap();
    let registry = Arc::new(PartitionRegistry::new());
    let topic_id = uuid::Uuid::from_bytes([45; 16]);
    let coordinator_a = Arc::new(ShareCoordinator::new(
        krabka_audit::NodeId(1),
        Arc::clone(&registry),
        ShareCoordinatorConfig::default(),
    ));
    let coordinator_b = Arc::new(ShareCoordinator::new(
        krabka_audit::NodeId(2),
        Arc::clone(&registry),
        ShareCoordinatorConfig::default(),
    ));
    let state_partition = coordinator_a.state_partition_for("g", &topic_id, 0);
    open_state_partition(&registry, dir.path(), state_partition.get());

    // Broker A leads at epoch 0 and initializes the key at offset 10.
    coordinator_a
        .refresh_leader_partitions(&state_partition_image(state_partition.get(), 1, 0))
        .await
        .finished()
        .await;
    coordinator_a
        .initialize("g", topic_id, 0, 1, Offset(10))
        .await
        .unwrap();

    let steps = [
        Step::Write {
            on: Broker::A,
            start: 20,
            expected: Ok(()),
        },
        Step::Read {
            on: Broker::A,
            expected: Ok(Some((20, vec![batch(20, 29)]))),
        },
        // Leadership moves to B. B has not run its load yet.
        Step::Refresh {
            on: Broker::A,
            leader: 2,
            epoch: 1,
            wait: true,
        },
        Step::Refresh {
            on: Broker::B,
            leader: 2,
            epoch: 1,
            wait: false,
        },
        Step::Read {
            on: Broker::B,
            expected: Err(COORDINATOR_LOAD_IN_PROGRESS),
        },
        Step::Write {
            on: Broker::B,
            start: 30,
            expected: Err(COORDINATOR_LOAD_IN_PROGRESS),
        },
        Step::Read {
            on: Broker::A,
            expected: Err(NOT_COORDINATOR),
        },
        Step::Write {
            on: Broker::A,
            start: 30,
            expected: Err(NOT_COORDINATOR),
        },
        // B finished its load: it serves the state that A wrote.
        Step::AwaitActive { on: Broker::B },
        Step::Read {
            on: Broker::B,
            expected: Ok(Some((20, vec![batch(20, 29)]))),
        },
        Step::Write {
            on: Broker::B,
            start: 30,
            expected: Ok(()),
        },
        // Leadership moves back to A: A serves the state that B wrote, not
        // the state that it held in its earlier term.
        Step::Refresh {
            on: Broker::B,
            leader: 1,
            epoch: 2,
            wait: true,
        },
        Step::Refresh {
            on: Broker::A,
            leader: 1,
            epoch: 2,
            wait: true,
        },
        Step::Read {
            on: Broker::A,
            expected: Ok(Some((30, vec![batch(30, 39)]))),
        },
        Step::Read {
            on: Broker::B,
            expected: Err(NOT_COORDINATOR),
        },
    ];

    for (index, step) in steps.into_iter().enumerate() {
        let on = |broker| match broker {
            Broker::A => &coordinator_a,
            Broker::B => &coordinator_b,
        };
        match step {
            Step::Refresh {
                on: broker,
                leader,
                epoch,
                wait,
            } => {
                let loads = on(broker)
                    .refresh_leader_partitions(&state_partition_image(
                        state_partition.get(),
                        leader,
                        epoch,
                    ))
                    .await;
                if wait {
                    loads.finished().await;
                }
            }
            Step::AwaitActive { on: broker } => {
                let coordinator = on(broker);
                tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    while coordinator.load_status(state_partition).await
                        != Some(super::LoadStatus::Active)
                    {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("the load ends");
            }
            Step::Write {
                on: broker,
                start,
                expected,
            } => {
                let written = on(broker)
                    .write(
                        "g",
                        topic_id,
                        0,
                        (1, 0),
                        (Offset(start), 0),
                        vec![batch(start, start + 9)],
                    )
                    .await;
                assert!(written == expected, "step {index}");
            }
            Step::Read {
                on: broker,
                expected,
            } => {
                let read = on(broker)
                    .read("g", topic_id, 0)
                    .await
                    .map(|state| state.map(|st| (st.start_offset.0, st.state_batches)));
                assert!(read == expected, "step {index}");
            }
        }
    }
}

/// A broker that the image names as leader, but whose state partition log is
/// not open yet, answers `COORDINATOR_LOAD_IN_PROGRESS`. The refresh after the
/// log opens loads the partition.
#[tokio::test]
async fn led_partition_without_a_local_log_loads_once_the_log_opens() {
    let dir = tempdir().unwrap();
    let registry = Arc::new(PartitionRegistry::new());
    let topic_id = uuid::Uuid::from_bytes([46; 16]);
    let coordinator = Arc::new(ShareCoordinator::new(
        krabka_audit::NodeId(1),
        Arc::clone(&registry),
        ShareCoordinatorConfig::default(),
    ));
    let state_partition = coordinator.state_partition_for("g", &topic_id, 0);
    let image = state_partition_image(state_partition.get(), 1, 0);

    coordinator
        .refresh_leader_partitions(&image)
        .await
        .finished()
        .await;
    check!(coordinator.load_status(state_partition).await == Some(super::LoadStatus::Pending));
    check!(
        coordinator.read("g", topic_id, 0).await == Err(crate::codes::COORDINATOR_LOAD_IN_PROGRESS)
    );

    open_state_partition(&registry, dir.path(), state_partition.get());
    coordinator
        .refresh_leader_partitions(&image)
        .await
        .finished()
        .await;
    check!(coordinator.load_status(state_partition).await == Some(super::LoadStatus::Active));
    check!(coordinator.read("g", topic_id, 0).await == Ok(None));
}

/// A failed replay installs no partial state: the partition answers
/// `NOT_COORDINATOR`, as Kafka's runtime answers for a `FAILED` shard, and the
/// next refresh loads it again.
#[tokio::test]
async fn failed_load_serves_nothing_and_the_next_refresh_loads_again() {
    let dir = tempdir().unwrap();
    let registry = Arc::new(PartitionRegistry::new());
    let topic_id = uuid::Uuid::from_bytes([47; 16]);
    let coordinator = Arc::new(ShareCoordinator::new(
        krabka_audit::NodeId(1),
        Arc::clone(&registry),
        ShareCoordinatorConfig::default(),
    ));
    let state_partition = coordinator.state_partition_for("g", &topic_id, 0);
    open_state_partition(&registry, dir.path(), state_partition.get());
    let image = state_partition_image(state_partition.get(), 1, 0);

    // The spawned load does not run before this task yields, so the failure
    // below ends the term first.
    let loads = coordinator.refresh_leader_partitions(&image).await;
    let generation = coordinator.leader_partitions.read().await[&state_partition].generation;
    coordinator
        .install_load(
            state_partition,
            generation,
            Err(BrokerError::Share("injected read error".into())),
        )
        .await;
    loads.finished().await;
    check!(coordinator.load_status(state_partition).await == Some(super::LoadStatus::Failed));
    check!(coordinator.read_summary("g", topic_id, 0).await == Err(crate::codes::NOT_COORDINATOR));

    coordinator
        .refresh_leader_partitions(&image)
        .await
        .finished()
        .await;
    check!(coordinator.load_status(state_partition).await == Some(super::LoadStatus::Active));
    check!(coordinator.read_summary("g", topic_id, 0).await == Ok(None));
}
