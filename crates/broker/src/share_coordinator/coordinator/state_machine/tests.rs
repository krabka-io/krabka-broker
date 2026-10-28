//! Unit tests for the share-state machine: Kafka's validation and fencing
//! order, and the records each operation appends.

use {
    super::*,
    crate::{
        coordinator::unified::persistence::MAX_STRING_BYTES,
        share_coordinator::{
            bootstrap::TOPIC as SHARE_STATE_TOPIC,
            config::ShareCoordinatorConfig,
            coordinator::test_support::{
                ConfiguredCoordinator, CoordinatorWithRegistry, Logged, NOW_MS, batch,
                configured_coordinator, coordinator, image_with_topic, initialize_led_group,
                lead_all, logged_records, share_write,
            },
        },
    },
    assert2::{assert, check},
    std::{
        path::Path,
        sync::Arc,
        time::{Duration, Instant},
    },
    tempfile::tempdir,
    tokio::{task::yield_now, time::sleep},
};

const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([5; 16]);

type WriteOutcome = Result<(), ShareStateError>;

/// The state epoch and start offset that a read returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadSummary {
    state_epoch: i32,
    start_offset: i64,
}

/// What a read leaves behind.
#[derive(Clone, Copy)]
struct ReadResult {
    /// The expected read.
    read: Result<ReadSummary, ShareStateError>,
    /// The expected write with leader epoch 3.
    write: Option<WriteOutcome>,
    /// The stored leader epoch after a reload.
    reloaded_leader_epoch: i32,
}

/// A read of one partition at a leader epoch.
#[derive(Clone, Copy)]
struct ReadRequest {
    partition: i32,
    leader_epoch: i32,
}

/// One read row: the request, and what Kafka 4.3.1 and Kafka trunk leave
/// behind.
struct ReadRow {
    request: ReadRequest,
    kafka_4_3_1: ReadResult,
    trunk: ReadResult,
}

impl ReadRow {
    fn same(request: ReadRequest, result: ReadResult) -> Self {
        Self {
            request,
            kafka_4_3_1: result,
            trunk: result,
        }
    }
}

/// A coordinator with Kafka trunk's rules on or off.
fn rules(trunk: bool) -> ShareCoordinatorConfig {
    ShareCoordinatorConfig {
        trunk_rules: trunk,
        ..ShareCoordinatorConfig::default()
    }
}

fn refused(code: ShareErrorCode, message: &'static str) -> ShareStateError {
    ShareStateError::Refused { code, message }
}

#[tokio::test]
async fn initialize_then_summary() {
    let dir = tempdir().unwrap();
    let CoordinatorWithRegistry {
        coord,
        registry: _reg,
    } = coordinator(dir.path());
    lead_all(&coord).await;

    coord
        .initialize(&image_with_topic(TOPIC, 1), "g", TOPIC, 0, 5, Offset(100))
        .await
        .unwrap();

    let summary = coord.read_summary("g", TOPIC, 0).await;
    assert!(summary == Ok(Some((5, 0, Offset(100), 0))));
}

/// A shard serializes first initialization through its append, even before a
/// state cell exists. A queued initialization must validate the installed
/// epoch and use the next snapshot epoch, as replay does.
#[tokio::test]
async fn concurrent_first_initializes_preserve_epoch_fencing_and_replay() {
    for trunk in [false, true] {
        for next_epoch in [1, 3] {
            let dir = tempdir().unwrap();
            let ConfiguredCoordinator {
                coord,
                registry,
                clock: _clock,
            } = configured_coordinator(dir.path(), ShareCoordinatorConfig {
                state_topic_num_partitions: 1,
                ..rules(trunk)
            });
            lead_all(&coord).await;
            let image = image_with_topic(TOPIC, 1);
            let partition = registry
                .get(
                    SHARE_STATE_TOPIC,
                    coord.state_partition_for("g", &TOPIC, 0).unwrap(),
                )
                .expect("state partition is open");
            // The first request reaches its append while the logical key is
            // still absent. Pause that append at the metadata transition seam.
            let transition = partition.replication_target.write().await;
            let first = coord.initialize(&image, "g", TOPIC, 0, 2, Offset(10));
            let next = coord.initialize(&image, "g", TOPIC, 0, next_epoch, Offset(20));
            tokio::pin!(first, next);
            tokio::select! {
                biased;
                result = &mut first => panic!("paused append completed: {result:?}"),
                () = yield_now() => {}
            }
            assert!(coord.entry("g", TOPIC, 0).is_none());
            tokio::select! {
                biased;
                result = &mut next => panic!("queued initialize completed: {result:?}"),
                () = yield_now() => {}
            }
            drop(transition);
            let (first, next) = tokio::join!(first, next);
            let expected_next = if next_epoch == 1 {
                Err(refused(
                    codes::FENCED_STATE_EPOCH,
                    message::FENCED_STATE_EPOCH,
                ))
            } else {
                Ok(())
            };
            assert!((first, next) == (Ok(()), expected_next));
            let expected = if next_epoch == 1 {
                (2, 0, Offset(10), 0)
            } else {
                (3, 0, Offset(20), 0)
            };
            assert!(coord.read_summary("g", TOPIC, 0).await == Ok(Some(expected)));
            let snapshots: Vec<_> =
                logged_records(&coord, coord.state_partition_for("g", &TOPIC, 0).unwrap())
                    .into_iter()
                    .filter_map(|record| match record.logged {
                        Logged::Snapshot(snapshot) => {
                            Some((snapshot.snapshot_epoch, snapshot.state_epoch))
                        }
                        _ => None,
                    })
                    .collect();
            let expected_snapshots = if next_epoch == 1 {
                vec![(0, 2)]
            } else {
                vec![(0, 2), (1, 3)]
            };
            assert!(snapshots == expected_snapshots);
            coord.reload_all_partitions_for_test().await;
            assert!(coord.read_summary("g", TOPIC, 0).await == Ok(Some(expected)));
            let entry = coord.entry("g", TOPIC, 0).expect("state replayed");
            assert!(entry.lock().await.fence_state_epoch == expected.0);
        }
    }
}

/// `InitializeShareGroupState` creates the state key, which writes its group id
/// with an `INT16` length. A group id of 32767 bytes is initialized and deleted.
/// A longer one is refused with the internal error, as Kafka answers a record
/// that its writer cannot serialize, and the coordinator keeps no state for it.
#[tokio::test]
async fn a_group_id_over_32767_bytes_is_not_initialized() {
    let dir = tempdir().unwrap();
    let CoordinatorWithRegistry {
        coord,
        registry: _reg,
    } = coordinator(dir.path());
    lead_all(&coord).await;
    let image = image_with_topic(TOPIC, 1);
    let limit = MAX_STRING_BYTES;
    let longest = "g".repeat(limit);
    let too_long = "g".repeat(limit + 1);

    check!(
        coord
            .initialize(&image, &longest, TOPIC, 0, 5, Offset(100))
            .await
            == Ok(())
    );
    check!(coord.read_summary(&longest, TOPIC, 0).await == Ok(Some((5, 0, Offset(100), 0))));
    check!(coord.delete(&image, &longest, TOPIC, 0).await == Ok(()));

    let refused = coord
        .initialize(&image, &too_long, TOPIC, 0, 5, Offset(100))
        .await;
    check!(
        refused
            == Err(ShareStateError::Operation {
                code: codes::UNKNOWN_SERVER_ERROR,
                message: message::UNKNOWN_SERVER_ERROR,
            })
    );
    check!(coord.read_summary(&too_long, TOPIC, 0).await == Ok(None));
}

/// A coordinator with stored state, and the image it runs over.
struct Seeded {
    coord: ShareCoordinator,
    image: MetadataImage,
}

/// Stored state for the read and write tables, on topic `TOPIC` with two
/// partitions: partition 0 at state epoch 2, leader epoch 3, start offset
/// 10 and delivery complete count 4, and partition 2 (not in the image of
/// two partitions) at state epoch 2.
async fn seeded(dir: &Path, trunk: bool) -> Seeded {
    let ConfiguredCoordinator {
        coord,
        registry: _reg,
        clock: _clock,
    } = configured_coordinator(dir, rules(trunk));
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
    Seeded { coord, image }
}

/// `ReadShareGroupState` as Kafka's `readStateAndMaybeUpdateLeaderEpoch`
/// answers it, then a write with leader epoch 3, and the stored leader epoch
/// after a reload of the log.
#[tokio::test]
async fn read_fences_and_persists_the_leader_epoch() {
    let fenced = || refused(codes::FENCED_LEADER_EPOCH, message::FENCED_LEADER_EPOCH);
    let rows = [
        ReadRow::same(
            ReadRequest {
                partition: 0,
                leader_epoch: 3,
            },
            ReadResult {
                read: Ok(ReadSummary {
                    state_epoch: 2,
                    start_offset: 10,
                }),
                write: Some(Ok(())),
                reloaded_leader_epoch: 3,
            },
        ),
        ReadRow::same(
            ReadRequest {
                partition: 0,
                leader_epoch: 4,
            },
            ReadResult {
                read: Ok(ReadSummary {
                    state_epoch: 2,
                    start_offset: 10,
                }),
                write: Some(Err(fenced())),
                reloaded_leader_epoch: 4,
            },
        ),
        ReadRow::same(
            ReadRequest {
                partition: 0,
                leader_epoch: 2,
            },
            ReadResult {
                read: Err(fenced()),
                write: Some(Ok(())),
                reloaded_leader_epoch: 3,
            },
        ),
        ReadRow::same(
            ReadRequest {
                partition: 1,
                leader_epoch: 0,
            },
            ReadResult {
                read: Err(refused(
                    codes::INVALID_REQUEST,
                    message::READ_UNINITIALIZED_SHARE_PARTITION,
                )),
                write: None,
                reloaded_leader_epoch: 3,
            },
        ),
        ReadRow::same(
            ReadRequest {
                partition: -1,
                leader_epoch: 0,
            },
            ReadResult {
                read: Err(refused(
                    codes::INVALID_REQUEST,
                    message::NEGATIVE_PARTITION_ID,
                )),
                write: None,
                reloaded_leader_epoch: 3,
            },
        ),
        // `-1` is "not supplied" in 4.3.1: a plain read that skips the fence
        // and updates no leader epoch. Trunk refuses a negative epoch.
        ReadRow {
            request: ReadRequest {
                partition: 0,
                leader_epoch: -1,
            },
            kafka_4_3_1: ReadResult {
                read: Ok(ReadSummary {
                    state_epoch: 2,
                    start_offset: 10,
                }),
                write: Some(Ok(())),
                reloaded_leader_epoch: 3,
            },
            trunk: ReadResult {
                read: Err(refused(
                    codes::INVALID_REQUEST,
                    message::NEGATIVE_LEADER_EPOCH,
                )),
                write: None,
                reloaded_leader_epoch: 3,
            },
        },
        ReadRow::same(
            ReadRequest {
                partition: 2,
                leader_epoch: 0,
            },
            ReadResult {
                read: Err(refused(
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                    message::UNKNOWN_TOPIC_OR_PARTITION,
                )),
                write: None,
                reloaded_leader_epoch: 3,
            },
        ),
    ];

    for trunk in [false, true] {
        for (index, row) in rows.iter().enumerate() {
            let ReadRequest {
                partition,
                leader_epoch,
            } = row.request;
            let ReadResult {
                read: expected,
                write,
                reloaded_leader_epoch,
            } = if trunk { row.trunk } else { row.kafka_4_3_1 };
            let dir = tempdir().unwrap();
            let Seeded { coord, image } = seeded(dir.path(), trunk).await;
            let read = coord
                .read(&image, "g", TOPIC, partition, leader_epoch)
                .await
                .map(|st| ReadSummary {
                    state_epoch: st.state_epoch,
                    start_offset: st.start_offset.0,
                });
            check!(read == expected, "row {index}, trunk {trunk}");
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
                check!(written == expected_write, "row {index}, trunk {trunk}");
            }
            coord.reload_all_partitions_for_test().await;
            let summary = coord.read_summary("g", TOPIC, 0).await;
            check!(
                summary.map(|s| s.map(|(_, leader, ..)| leader)) == Ok(Some(reloaded_leader_epoch)),
                "row {index}, trunk {trunk}"
            );
        }
    }
}

/// A failed append leaves the in-memory state as it was.
#[tokio::test]
async fn failed_read_append_changes_no_state() {
    let dir = tempdir().unwrap();
    let Seeded { coord, image } = seeded(dir.path(), false).await;
    let state_partition = coord.state_partition_for("g", &TOPIC, 0).unwrap();
    coord.partitions.remove(SHARE_STATE_TOPIC, state_partition);

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
    let Seeded { coord, image } = seeded(dir.path(), false).await;
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
    let ConfiguredCoordinator {
        coord,
        registry: _reg,
        clock: _clock,
    } = configured_coordinator(dir.path(), config);
    let image = initialize_led_group().coordinator(&coord).call().await;
    let writes = [
        (0, 1, vec![
            batch(Offset(0)..=Offset(9)),
            batch(Offset(5)..=Offset(14)),
        ]),
        (10, 2, vec![batch(Offset(0)..=Offset(19))]),
        (12, 3, vec![batch(Offset(20)..=Offset(29))]),
        (12, 3, vec![batch(Offset(30)..=Offset(39))]),
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

    let state_partition = coord.state_partition_for("g", &TOPIC, 0).unwrap();
    let logged: Vec<Logged> = logged_records(&coord, state_partition)
        .into_iter()
        .map(|record| record.logged)
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
        update(0, 1, vec![batch(Offset(0)..=Offset(14))]),
        update(10, 2, vec![batch(Offset(10)..=Offset(19))]),
        Logged::Snapshot(ShareSnapshotValue {
            snapshot_epoch: 1,
            state_epoch: 1,
            leader_epoch: 0,
            start_offset: Offset(12),
            delivery_complete_count: 3,
            create_timestamp: NOW_MS,
            write_timestamp: NOW_MS,
            state_batches: vec![batch(Offset(12)..=Offset(29))],
        }),
        Logged::Update(ShareUpdateValue {
            snapshot_epoch: 1,
            leader_epoch: 0,
            start_offset: Offset(12),
            delivery_complete_count: 3,
            state_batches: vec![batch(Offset(30)..=Offset(39))],
        }),
    ];
    assert!(logged == expected);
    let state = coord.state_for_test("g", TOPIC, 0).await.unwrap();
    check!(state.state_batches == vec![batch(Offset(12)..=Offset(39))]);
    check!(state.updates_since_snapshot == 1);
}

/// One share-state operation on the key `("g", TOPIC, 0)`.
#[derive(Debug, Clone, Copy)]
enum Op {
    Initialize,
    Write,
    /// A read with a newer leader epoch, which writes that epoch.
    Read,
    Delete,
}

/// What happens to the state partition of the key around an operation.
#[derive(Debug, Clone, Copy)]
enum Fault {
    /// A follower in the ISR does not fetch, so no record commits.
    NoCommit,
    /// The partition moves to broker 2 while the operation waits.
    MovesAway,
    /// The partition leads on this broker at a newer epoch before the
    /// operation starts: the term of the coordinator has ended.
    NewerEpoch,
}

/// An operation answers only when its record commits under the term of the
/// coordinator, as Kafka's `CoordinatorRuntime` completes a write. A record
/// that does not commit in `share.coordinator.write.timeout.ms` answers
/// `COORDINATOR_NOT_AVAILABLE`. A term that ends first answers
/// `NOT_COORDINATOR`, and a coordinator whose term has ended appends nothing.
#[tokio::test]
async fn an_operation_answers_only_when_its_record_commits_in_the_term() {
    let timed_out = ShareStateError::Operation {
        code: codes::COORDINATOR_NOT_AVAILABLE,
        message: message::REQUEST_TIMED_OUT,
    };
    let not_coordinator = ShareStateError::Operation {
        code: codes::NOT_COORDINATOR,
        message: message::NOT_COORDINATOR,
    };
    // (fault, the answer of every operation, records in the log afterwards)
    let faults = [
        (Fault::NoCommit, timed_out, 2),
        (Fault::MovesAway, not_coordinator, 2),
        (Fault::NewerEpoch, not_coordinator, 1),
    ];
    for (fault, expected, logged) in faults {
        for op in [Op::Initialize, Op::Write, Op::Read, Op::Delete] {
            let dir = tempdir().unwrap();
            // The setup `initialize` below commits under the same timeout,
            // so the no-commit case keeps enough of it for a loaded machine.
            let timeout = match fault {
                Fault::NoCommit => Duration::from_secs(2),
                Fault::MovesAway | Fault::NewerEpoch => Duration::from_secs(30),
            };
            let config = ShareCoordinatorConfig {
                write_timeout: timeout,
                ..ShareCoordinatorConfig::default()
            };
            let ConfiguredCoordinator {
                coord,
                registry: reg,
                clock: _clock,
            } = configured_coordinator(dir.path(), config);
            let image = initialize_led_group().coordinator(&coord).call().await;
            let state_partition = coord.state_partition_for("g", &TOPIC, 0).unwrap();
            let part = reg
                .get(SHARE_STATE_TOPIC, state_partition)
                .expect("the state partition is open");
            let both = [krabka_raft::NodeId(1), krabka_raft::NodeId(2)];
            match fault {
                Fault::NoCommit | Fault::MovesAway => {
                    part.replica_state.lock().await.install_isr(
                        &both,
                        &both,
                        krabka_raft::NodeId(1),
                        Instant::now(),
                    );
                }
                Fault::NewerEpoch => part.install_leader_change(1, 1).await,
            }
            let mover = matches!(fault, Fault::MovesAway).then(|| {
                let part = Arc::clone(&part);
                tokio::spawn(async move {
                    // intentional: the operation has to wait before the move.
                    sleep(Duration::from_millis(20)).await;
                    part.install_leader_change(2, 1).await;
                })
            });

            let answer = match op {
                Op::Initialize => coord.initialize(&image, "g", TOPIC, 0, 2, Offset(5)).await,
                Op::Write => {
                    coord
                        .write(
                            &image,
                            "g",
                            TOPIC,
                            0,
                            share_write((1, 0), (0, 0), vec![batch(Offset(0)..=Offset(9))]),
                        )
                        .await
                }
                Op::Read => coord.read(&image, "g", TOPIC, 0, 1).await.map(|_| ()),
                Op::Delete => coord.delete(&image, "g", TOPIC, 0).await,
            };
            if let Some(mover) = mover {
                mover.await.expect("the partition moves");
            }

            check!(answer == Err(expected), "{op:?} under {fault:?}");
            check!(
                logged_records(&coord, state_partition).len() == logged,
                "{op:?} under {fault:?}"
            );
        }
    }
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(tag = "kind")]
enum ObservedShareOperation {
    Ok,
    Refused { code: i16, message: String },
    Operation { code: i16, message: String },
}

type ObservedShareSummary = (i32, i32, i64, i32);
type ShareSummaryObservation = Result<Option<ObservedShareSummary>, i16>;

fn observed_share_error(error: ShareStateError) -> ObservedShareOperation {
    match error {
        ShareStateError::Refused { code, message } => ObservedShareOperation::Refused {
            code,
            message: message.to_owned(),
        },
        ShareStateError::Operation { code, message } => ObservedShareOperation::Operation {
            code,
            message: message.to_owned(),
        },
    }
}

fn observed_share_operation(result: WriteOutcome) -> ObservedShareOperation {
    result.map_or_else(observed_share_error, |()| ObservedShareOperation::Ok)
}

fn observed_share_summary(
    result: Result<Option<ShareStateSummary>, i16>,
) -> ShareSummaryObservation {
    result
        .map(|summary| summary.map(|(state, leader, start, count)| (state, leader, start.0, count)))
}

#[derive(serde::Deserialize)]
struct InitializeStateRequest {
    state_epoch: i32,
    start_offset: i64,
}

#[derive(serde::Deserialize)]
struct ExpectedInitialization {
    outcome: ObservedShareOperation,
    snapshots: usize,
    summary: Option<ObservedShareSummary>,
}

#[derive(Default)]
struct ShareStateInitialization {
    setup: Option<ConfiguredCoordinator>,
    image: Option<MetadataImage>,
    request: Option<InitializeStateRequest>,
    reply: Option<ObservedShareOperation>,
    snapshots: Option<usize>,
    summary: Option<ShareSummaryObservation>,
    _directory_guard: Option<tempfile::TempDir>,
}

#[rstest::fixture]
fn share_state_initialization() -> ShareStateInitialization {
    // One initialized state key and its physical log belong to each example.
    ShareStateInitialization::default()
}

#[rstest_bdd_macros::given(
    "a share coordinator with trunk rules {trunk} initialized group g at state epoch 5 and start offset 100"
)]
async fn initialized_share_state_key(
    share_state_initialization: &mut ShareStateInitialization,
    trunk: bool,
) {
    let directory = tempdir().expect("fresh share state log directory");
    let setup = configured_coordinator(directory.path(), rules(trunk));
    lead_all(&setup.coord).await;
    let image = image_with_topic(TOPIC, 1);
    setup
        .coord
        .initialize(&image, "g", TOPIC, 0, 5, Offset(100))
        .await
        .expect("initial share state");
    share_state_initialization.setup = Some(setup);
    share_state_initialization.image = Some(image);
    share_state_initialization._directory_guard = Some(directory);
}

#[rstest_bdd_macros::given("the repeated InitializeShareGroupState request is JSON {request}")]
fn requested_share_state_initialization(
    share_state_initialization: &mut ShareStateInitialization,
    request: String,
) {
    share_state_initialization.request =
        Some(serde_json::from_str(&request).expect("explicit state epoch and start offset"));
}

#[rstest_bdd_macros::when("the coordinator initializes that stored share state key again")]
async fn initialize_existing_share_state(
    share_state_initialization: &mut ShareStateInitialization,
) {
    let coord = &share_state_initialization
        .setup
        .as_ref()
        .expect("initialized coordinator was given")
        .coord;
    let image = share_state_initialization
        .image
        .as_ref()
        .expect("topic metadata was given");
    let request = share_state_initialization
        .request
        .as_ref()
        .expect("second initialization request was given");
    share_state_initialization.reply = Some(observed_share_operation(
        coord
            .initialize(
                image,
                "g",
                TOPIC,
                0,
                request.state_epoch,
                Offset(request.start_offset),
            )
            .await,
    ));
    let partition = coord
        .state_partition_for("g", &TOPIC, 0)
        .expect("state partition route");
    share_state_initialization.snapshots = Some(
        logged_records(coord, partition)
            .into_iter()
            .filter(|record| matches!(record.logged, Logged::Snapshot(_)))
            .count(),
    );
    share_state_initialization.summary = Some(observed_share_summary(
        coord.read_summary("g", TOPIC, 0).await,
    ));
}

#[rstest_bdd_macros::then(
    "the initialization outcome, persisted snapshot count and stored summary are JSON {expected}"
)]
fn expected_share_state_initialization(
    share_state_initialization: &ShareStateInitialization,
    expected: String,
) {
    let expected: ExpectedInitialization =
        serde_json::from_str(&expected).expect("independent complete initialization observations");
    check!(share_state_initialization.reply.as_ref() == Some(&expected.outcome));
    check!(share_state_initialization.snapshots == Some(expected.snapshots));
    check!(share_state_initialization.summary.as_ref() == Some(&Ok(expected.summary)));
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/share_coordinator/coordinator/state_machine/tests/initialize.feature",
    name = "Initialize an existing key under Kafka release rules"
)]
#[tokio::test]
async fn initialize_follows_the_rules_of_each_kafka_release(
    share_state_initialization: ShareStateInitialization,
    case: String,
    trunk: bool,
    request: String,
    expected: String,
) {
    let _ = (share_state_initialization, case, trunk, request, expected);
}

#[derive(serde::Deserialize)]
struct ShareStateWriteRequest {
    partition: i32,
    state_epoch: i32,
    leader_epoch: i32,
    start_offset: i64,
    delivery_complete_count: i32,
    drop_log: bool,
}

#[derive(serde::Deserialize)]
struct ExpectedShareStateWrite {
    outcome: ObservedShareOperation,
    summary: Option<ObservedShareSummary>,
}

#[derive(Default)]
struct ShareStateWrite {
    seeded: Option<Seeded>,
    request: Option<ShareStateWriteRequest>,
    reply: Option<ObservedShareOperation>,
    summary: Option<ShareSummaryObservation>,
    _directory_guard: Option<tempfile::TempDir>,
}

#[rstest::fixture]
fn share_state_write() -> ShareStateWrite {
    // The seeded coordinator and each write's durable state are independent.
    ShareStateWrite::default()
}

#[rstest_bdd_macros::given(
    "a share coordinator with trunk rules {trunk} stores group g at state epoch 2, leader epoch 3, offset 10 and delivery count 4"
)]
async fn seeded_share_state_write(share_state_write: &mut ShareStateWrite, trunk: bool) {
    let directory = tempdir().expect("fresh share state write log");
    share_state_write.seeded = Some(seeded(directory.path(), trunk).await);
    share_state_write._directory_guard = Some(directory);
}

#[rstest_bdd_macros::given(
    "the WriteShareGroupState request and missing-log fault are JSON {request}"
)]
fn requested_share_state_write(share_state_write: &mut ShareStateWrite, request: String) {
    let request: ShareStateWriteRequest =
        serde_json::from_str(&request).expect("explicit share write and log availability inputs");
    if request.drop_log {
        let coord = &share_state_write
            .seeded
            .as_ref()
            .expect("seeded coordinator was given")
            .coord;
        let partition = coord
            .state_partition_for("g", &TOPIC, request.partition)
            .expect("state partition route");
        coord.partitions.remove(SHARE_STATE_TOPIC, partition);
    }
    share_state_write.request = Some(request);
}

#[rstest_bdd_macros::when("the coordinator writes the supplied share state and delivery range")]
async fn write_seeded_share_state(share_state_write: &mut ShareStateWrite) {
    let Seeded { coord, image } = share_state_write
        .seeded
        .as_ref()
        .expect("seeded coordinator was given");
    let request = share_state_write
        .request
        .as_ref()
        .expect("share write request was given");
    let result = coord
        .write(
            image,
            "g",
            TOPIC,
            request.partition,
            share_write(
                (request.state_epoch, request.leader_epoch),
                (request.start_offset, request.delivery_complete_count),
                vec![batch(
                    Offset(request.start_offset)..=Offset(request.start_offset + 9),
                )],
            ),
        )
        .await;
    share_state_write.reply = Some(observed_share_operation(result));
    share_state_write.summary = Some(observed_share_summary(
        coord.read_summary("g", TOPIC, request.partition).await,
    ));
}

#[rstest_bdd_macros::then(
    "the share write outcome and complete stored summary are JSON {expected}"
)]
fn expected_share_state_write(share_state_write: &ShareStateWrite, expected: String) {
    let expected: ExpectedShareStateWrite =
        serde_json::from_str(&expected).expect("independent share write result and full summary");
    check!(share_state_write.reply.as_ref() == Some(&expected.outcome));
    check!(share_state_write.summary.as_ref() == Some(&Ok(expected.summary)));
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/share_coordinator/coordinator/state_machine/tests/write.feature",
    name = "Write stored share state under Kafka release rules"
)]
#[tokio::test]
async fn write_matches_kafka_checks_and_record_rules(
    share_state_write: ShareStateWrite,
    case: String,
    trunk: bool,
    request: String,
    expected: String,
) {
    let _ = (share_state_write, case, trunk, request, expected);
}
