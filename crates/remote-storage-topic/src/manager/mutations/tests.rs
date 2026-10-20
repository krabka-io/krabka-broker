//! Local mutation outcomes and completion when consumption stops.

use {
    super::{MutationKey, MutationWaiters},
    crate::{
        error::MetadataLogError,
        log::{
            AssignmentHandle, InProcessMetadataEventLog, MetadataEventLog, MetadataEventStream,
            PartitionStart,
        },
        manager::{
            TopicBasedRemoteLogMetadataManager,
            test_support::{
                HeldPublishLog, Hold, finish, on_blocking, start_manager_all, started, tp,
            },
        },
        partitioning::metadata_partition_for,
        serde::MetadataEvent,
    },
    assert2::assert,
    futures_util::{StreamExt as _, stream},
    krabka_remote_storage::{
        RemoteLogMetadataManager, RemoteLogSegmentState, RemotePartitionDeleteMetadata,
        RemotePartitionDeleteState, RemoteStorageError,
    },
    std::{sync::Arc, time::Duration},
    tokio_util::sync::CancellationToken,
};

#[tokio::test(flavor = "multi_thread")]
async fn rejected_mutations_return_the_cache_error_and_valid_retries_succeed() {
    let manager = start_manager_all(InProcessMetadataEventLog::new(2)).await;
    let m = manager.clone();
    on_blocking(move || {
        m.add_remote_log_segment_metadata(started(10, 0, 99))
            .unwrap();
        assert!(matches!(
            m.add_remote_log_segment_metadata(started(10, 0, 99)),
            Err(RemoteStorageError::InvalidAdd { .. })
        ));
        assert!(matches!(
            m.update_remote_log_segment_metadata(finish(11)),
            Err(RemoteStorageError::SegmentNotFound(_))
        ));
        let mut skipped = finish(10);
        skipped.state = RemoteLogSegmentState::DeleteSegmentFinished;
        assert!(matches!(
            m.update_remote_log_segment_metadata(skipped),
            Err(RemoteStorageError::InvalidSegmentTransition { .. })
        ));
        assert!(matches!(
            m.put_remote_partition_delete_metadata(RemotePartitionDeleteMetadata {
                topic_id_partition: tp(),
                state: RemotePartitionDeleteState::DeletePartitionStarted,
                event_timestamp_ms: 500,
                broker_id: 1,
            }),
            Err(RemoteStorageError::InvalidPartitionDeleteTransition { .. })
        ));
        m.update_remote_log_segment_metadata(finish(10)).unwrap();
        // Kafka permits an exact retry of an already-applied state update.
        m.update_remote_log_segment_metadata(finish(10)).unwrap();
        let listed = m.list_remote_log_segments(&tp()).unwrap();
        assert!(listed == vec![started(10, 0, 99).with_update(&finish(10)).unwrap()]);
    })
    .await;
    assert!(manager.mutation_waiters.is_idle());
    manager.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_without_local_waiters_keeps_no_outcome_history() {
    let log = InProcessMetadataEventLog::new(2);
    let manager = start_manager_all(log.clone()).await;
    let partition = metadata_partition_for(&tp(), log.partition_count());
    let mut last = -1;
    for _ in 0..64 {
        last = log
            .publish(
                partition,
                MetadataEvent::AddSegment(started(10, 0, 99)).encode(),
            )
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        let index = usize::try_from(partition).unwrap();
        while manager.applied.lock().unwrap()[index] < last {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all replay outcomes consumed");
    assert!(manager.mutation_waiters.is_idle());
    manager.shutdown();
}

struct SilentLog {
    inner: Arc<InProcessMetadataEventLog>,
    close: CancellationToken,
}

#[krabka_macros::metadata_log_delegate(crate)]
#[async_trait::async_trait]
impl MetadataEventLog for SilentLog {
    fn subscribe(
        &self,
        assignment: Vec<PartitionStart>,
    ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
        let (_, handle) = self.inner.subscribe(assignment);
        let records = stream::pending().take_until(self.close.clone().cancelled_owned());
        (Box::pin(records), handle)
    }
}

#[yare::parameterized(
    shutdown = { "shutdown" },
    stream_close = { "stream close" },
    unassignment = { "unassignment" }
)]
#[test_macro(tokio::test(flavor = "multi_thread"))]
async fn shutdown_stream_close_and_unassignment_release_pending_mutations(cause: &str) {
    let log = Arc::new(SilentLog {
        inner: InProcessMetadataEventLog::new(2),
        close: CancellationToken::new(),
    });
    let manager = start_manager_all(log.clone()).await;
    let m = manager.clone();
    let mutation =
        tokio::task::spawn_blocking(move || m.add_remote_log_segment_metadata(started(10, 0, 99)));
    tokio::time::timeout(Duration::from_secs(2), async {
        while manager.mutation_waiters.registered() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("mutation registered");
    match cause {
        "shutdown" => manager.shutdown(),
        "stream close" => log.close.cancel(),
        "unassignment" => manager.reconcile_assignment(&[]).await,
        _ => unreachable!(),
    }
    let error = tokio::time::timeout(Duration::from_secs(2), mutation)
        .await
        .expect("pending mutation released")
        .unwrap()
        .unwrap_err();
    if cause == "unassignment" {
        assert2::assert!(matches!(error, RemoteStorageError::NotReady { .. }));
    } else {
        assert2::assert!(error.to_string() == MetadataLogError::Closed.into_storage().to_string());
        let m = manager.clone();
        let error = on_blocking(move || m.add_remote_log_segment_metadata(started(11, 100, 199)))
            .await
            .unwrap_err();
        assert2::assert!(error.to_string() == MetadataLogError::Closed.into_storage().to_string());
    }
    assert2::assert!(manager.mutation_waiters.is_idle());
    manager.shutdown();
}

/// Wait until the pump has applied every record on `partition`.
async fn wait_applied(
    manager: &TopicBasedRemoteLogMetadataManager,
    log: &HeldPublishLog,
    partition: i32,
) {
    let index = usize::try_from(partition).unwrap();
    let last = log.inner.high_water_marks().await.unwrap()[index] - 1;
    tokio::time::timeout(Duration::from_secs(2), async {
        while manager.applied.lock().unwrap()[index] < last {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pump applied every record");
}

/// A manager consuming every partition of a two-partition [`HeldPublishLog`],
/// and the metadata partition [`tp`] maps to.
struct HeldManager {
    log: Arc<HeldPublishLog>,
    manager: Arc<TopicBasedRemoteLogMetadataManager>,
    partition: i32,
}

async fn held_manager() -> HeldManager {
    let log = HeldPublishLog::new(2);
    let manager = start_manager_all(log.clone()).await;
    let partition = metadata_partition_for(&tp(), log.partition_count());
    HeldManager {
        log,
        manager,
        partition,
    }
}

type Mutation = tokio::task::JoinHandle<Result<(), RemoteStorageError>>;

/// Start adding segment 10 with its publish parked at `hold`.
async fn parked_add(
    manager: &Arc<TopicBasedRemoteLogMetadataManager>,
    log: &HeldPublishLog,
    hold: Hold,
) -> Mutation {
    log.set_hold(Some(hold));
    let m = manager.clone();
    let mutation =
        tokio::task::spawn_blocking(move || m.add_remote_log_segment_metadata(started(10, 0, 99)));
    tokio::time::timeout(Duration::from_secs(2), log.wait_parked())
        .await
        .expect("publish parked");
    mutation
}

async fn released(log: &HeldPublishLog, mutation: Mutation) -> Result<(), RemoteStorageError> {
    log.release();
    tokio::time::timeout(Duration::from_secs(2), mutation)
        .await
        .expect("publisher completed")
        .unwrap()
}

/// Release the parked publish and check what the publisher learned, then
/// that no mutation state is left behind.
async fn expect_released(
    manager: &TopicBasedRemoteLogMetadataManager,
    log: &HeldPublishLog,
    mutation: Mutation,
    expected: fn(&Result<(), RemoteStorageError>) -> bool,
) {
    let result = released(log, mutation).await;
    assert!(expected(&result), "{result:?}");
    assert!(manager.mutation_waiters.is_idle());
    manager.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pending_publish_does_not_stall_the_pump() {
    let HeldManager {
        log,
        manager,
        partition,
    } = held_manager().await;
    let mutation = parked_add(&manager, &log, Hold::BeforeAppend).await;
    // Another writer's record lands while this broker's publish is pending.
    log.inner
        .publish(
            partition,
            MetadataEvent::AddSegment(started(11, 100, 199)).encode(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while manager.list_remote_log_segments(&tp()).unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the pump applied a record while a publish was pending");
    assert!(!mutation.is_finished());
    assert!(released(&log, mutation).await.is_ok());
    let listed = manager.list_remote_log_segments(&tp()).unwrap();
    assert!(listed == vec![started(10, 0, 99), started(11, 100, 199)]);
    assert!(manager.mutation_waiters.is_idle());
    manager.shutdown();
}

#[yare::parameterized(
    applied = { false, Result::is_ok },
    rejected = { true, |r| matches!(r, Err(RemoteStorageError::InvalidAdd { .. })) },
)]
#[test_macro(tokio::test(flavor = "multi_thread"))]
async fn an_outcome_applied_before_the_acknowledgement_reaches_the_publisher(
    replay: bool,
    expected: fn(&Result<(), RemoteStorageError>) -> bool,
) {
    let HeldManager {
        log,
        manager,
        partition,
    } = held_manager().await;
    if replay {
        // Another writer already added the segment, so the cache rejects ours.
        log.inner
            .publish(
                partition,
                MetadataEvent::AddSegment(started(10, 0, 99)).encode(),
            )
            .await
            .unwrap();
    }
    let mutation = parked_add(&manager, &log, Hold::AfterAppend).await;
    wait_applied(&manager, &log, partition).await;
    // The pump applied the record before the publisher registered its offset.
    assert2::assert!(manager.mutation_waiters.registered() == 0);
    expect_released(&manager, &log, mutation, expected).await;
}

#[yare::parameterized(
    unconsumed = { Hold::BeforeAppend, |r| matches!(r, Err(RemoteStorageError::NotReady { .. })) },
    already_applied = { Hold::AfterAppend, Result::is_ok },
)]
#[test_macro(tokio::test(flavor = "multi_thread"))]
async fn unassigning_a_partition_completes_a_publish_in_flight_on_it(
    hold: Hold,
    expected: fn(&Result<(), RemoteStorageError>) -> bool,
) {
    let HeldManager {
        log,
        manager,
        partition,
    } = held_manager().await;
    let mutation = parked_add(&manager, &log, hold).await;
    if hold == Hold::AfterAppend {
        wait_applied(&manager, &log, partition).await;
    }
    manager.reconcile_assignment(&[1 - partition]).await;
    expect_released(&manager, &log, mutation, expected).await;
}

/// One step the pump or the reconciler takes while a publish on partition 0
/// is in flight.
#[derive(Clone, Copy)]
enum Step {
    /// The pump applies the record at this offset, accepted or rejected.
    Apply(i64, bool),
    /// The reconciler unassigns partition 0.
    Unassign,
}

const PUBLISHED: i64 = 5;

fn apply(waiters: &MutationWaiters, offset: i64, accepted: bool) {
    waiters.complete(
        MutationKey {
            partition: 0,
            offset,
        },
        || {
            if accepted {
                Ok(())
            } else {
                Err(RemoteStorageError::InvalidArgument("rejected".into()))
            }
        },
    );
}

fn run(waiters: &MutationWaiters, steps: &[Step]) {
    for step in steps {
        match *step {
            Step::Apply(offset, accepted) => apply(waiters, offset, accepted),
            Step::Unassign => waiters.remove_partition(0, || {}),
        }
    }
}

/// What the publisher of the record at [`PUBLISHED`] learns.
fn outcome(
    reply: &mut tokio::sync::oneshot::Receiver<Result<(), RemoteStorageError>>,
) -> &'static str {
    match reply.try_recv() {
        Ok(Ok(())) => "ok",
        Ok(Err(RemoteStorageError::InvalidArgument(_))) => "rejected",
        Ok(Err(RemoteStorageError::NotReady { .. })) => "not ready",
        Ok(Err(_)) => "other error",
        Err(_) => "pending",
    }
}

#[yare::parameterized(
    applied_before_the_acknowledgement = { &[Step::Apply(PUBLISHED, true)], "ok" },
    rejected_before_the_acknowledgement = { &[Step::Apply(PUBLISHED, false)], "rejected" },
    replay_keeps_the_first_outcome = { &[Step::Apply(PUBLISHED, true), Step::Apply(PUBLISHED, false)], "ok" },
    not_yet_applied = { &[Step::Apply(PUBLISHED - 1, true)], "pending" },
    applied_then_unassigned = { &[Step::Apply(PUBLISHED, true), Step::Unassign], "ok" },
    rejected_then_unassigned = { &[Step::Apply(PUBLISHED, false), Step::Unassign], "rejected" },
    unassigned_unapplied = { &[Step::Unassign], "not ready" },
    unassigned_then_applied = { &[Step::Unassign, Step::Apply(PUBLISHED, true)], "not ready" },
    unassigned_then_rejected = { &[Step::Unassign, Step::Apply(PUBLISHED, false)], "not ready" },
    applied_after_two_unassignments = { &[Step::Unassign, Step::Unassign, Step::Apply(PUBLISHED, true)], "not ready" },
)]
fn a_publish_takes_the_outcome_applied_before_it_registers(steps: &[Step], expected: &str) {
    let waiters = MutationWaiters::default();
    let publish = waiters.begin_publish(0, || true).unwrap();
    run(&waiters, steps);
    let mut reply = publish.register(PUBLISHED);
    assert2::assert!(outcome(&mut reply) == expected);
    if expected == "pending" {
        apply(&waiters, PUBLISHED, true);
        assert2::assert!(outcome(&mut reply) == "ok");
    }
    assert2::assert!(waiters.is_idle());
}

#[test]
fn a_pending_publish_keeps_only_first_rejections_on_its_partition() {
    let waiters = MutationWaiters::default();
    let publish = waiters.begin_publish(0, || true).unwrap();
    for offset in 0..1_000 {
        apply(&waiters, offset, offset % 100 != 0);
        // A record on a partition with no publish in flight keeps nothing.
        waiters.complete(
            MutationKey {
                partition: 1,
                offset,
            },
            || Err(RemoteStorageError::InvalidArgument("rejected".into())),
        );
    }
    // Replays after a reassignment keep nothing either.
    for offset in 0..1_000 {
        apply(&waiters, offset, false);
    }
    assert!(waiters.kept() == 10);
    let mut reply = publish.register(2_000);
    assert!(outcome(&mut reply) == "pending");
    assert!(waiters.kept() == 0);
}
