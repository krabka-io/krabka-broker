//! Tests for the assignment reconciler and the read gate, which cover the
//! add, remove, and re-add transitions and the fail-closed HWM sentinel.

use std::sync::Arc;

use assert2::{assert, check};
use krabka_ids::LeaderEpoch;
use krabka_remote_storage::{
    RemoteLogMetadataManager, RemoteLogSegmentId, RemoteLogSegmentMetadata,
    RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, RemoteStorageError, TopicIdPartition,
};
use uuid::Uuid;

use crate::{
    log::{InProcessMetadataEventLog, MetadataEventLog},
    manager::test_support::{
        HwmFlakyLog, finish, on_blocking, start_manager, start_manager_all, started, tp, wait_ready,
    },
};

#[tokio::test(flavor = "multi_thread")]
async fn add_then_remove_drives_assignment_and_readiness() {
    use crate::partitioning::metadata_partition_for;

    let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
    // Pre-seed a finished segment for `tp()` so a ready read returns Some.
    {
        let writer = start_manager_all(log.clone()).await;
        let w2 = writer.clone();
        on_blocking(move || {
            w2.add_remote_log_segment_metadata(started(10, 0, 99))
                .unwrap();
        })
        .await;
        let w2 = writer.clone();
        on_blocking(move || w2.update_remote_log_segment_metadata(finish(10)).unwrap()).await;
        writer.shutdown();
    }

    let mp = metadata_partition_for(&tp(), log.partition_count());
    let m = start_manager(log);

    // Before assignment: the partition is not consumed → genuine miss.
    assert!(matches!(
        m.remote_log_segment_metadata(&tp(), LeaderEpoch(0), 42),
        Ok(None)
    ));

    // Assign it. add() must enqueue a PartitionStart for `mp`, and the
    // pump catches up; once applied >= HWM-1 the read returns Some.
    m.reconcile_assignment(&[mp]).await;
    assert!(m.assigned_metadata_partitions() == vec![mp]);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        match m.remote_log_segment_metadata(&tp(), LeaderEpoch(0), 42) {
            Ok(Some(md)) => {
                assert!(md.remote_log_segment_id().id == Uuid::from_u128(10));
                break;
            }
            Err(RemoteStorageError::NotReady { partition }) => {
                assert!(partition == mp, "NotReady names the catching-up partition");
                assert!(
                    std::time::Instant::now() < deadline,
                    "metadata partition never became ready"
                );
                tokio::task::yield_now().await;
            }
            other => panic!("unexpected read outcome: {other:?}"),
        }
    }

    // Remove it: assignment drops, and subsequent reads are a genuine
    // miss (Ok(None)) — the partition is no longer consumed.
    m.reconcile_assignment(&[]).await;
    assert!(m.assigned_metadata_partitions().is_empty());
    assert!(matches!(
        m.remote_log_segment_metadata(&tp(), LeaderEpoch(0), 42),
        Ok(None)
    ));
    m.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_partition_query_is_none() {
    let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(2);
    let m = start_manager(log);
    let other = TopicIdPartition::new(Uuid::from_u128(999), "nope", 0);
    check!(
        m.remote_log_segment_metadata(&other, LeaderEpoch(0), 0)
            .unwrap()
            == None
    );
    check!(m.highest_offset_for_epoch(&other, LeaderEpoch(0)).unwrap() == None);
    check!(m.list_remote_log_segments(&other).unwrap().is_empty());
    m.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn two_brokers_split_metadata_partitions() {
    use crate::partitioning::metadata_partition_for;

    // Use a wide metadata topic so two user-partitions land in distinct
    // buckets.
    let n = 16;
    let topic_id = Uuid::from_u128(0xFEED);
    let tp_a = TopicIdPartition::new(topic_id, "orders", 0);
    let tp_b = TopicIdPartition::new(topic_id, "orders", 1);
    let mp_a = metadata_partition_for(&tp_a, n);
    let mp_b = metadata_partition_for(&tp_b, n);
    assert!(
        mp_a != mp_b,
        "test needs the two partitions in distinct buckets"
    );

    let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(n);

    // Seed one finished segment for each user-partition via a transient
    // writer (consumes all partitions, no assignment gating).
    for (tp, id) in [(tp_a.clone(), 100u128), (tp_b.clone(), 200)] {
        let w = start_manager_all(log.clone()).await;
        let started = RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(tp.clone(), Uuid::from_u128(id)),
            0,
            99,
            100,
            1,
            100,
            krabka_remote_storage::RemoteLogSegmentDetails::new(
                2048,
                RemoteLogSegmentState::CopySegmentStarted,
                maplit::btreemap! {LeaderEpoch(0) => 0},
            ),
        )
        .unwrap();
        let w2 = w.clone();
        on_blocking(move || w2.add_remote_log_segment_metadata(started).unwrap()).await;
        let upd = RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: RemoteLogSegmentId::new(tp, Uuid::from_u128(id)),
            event_timestamp_ms: 200,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        };
        let w2 = w.clone();
        on_blocking(move || w2.update_remote_log_segment_metadata(upd).unwrap()).await;
        w.shutdown();
    }

    // Broker A consumes mp_a only; Broker B consumes mp_b only.
    let a = start_manager(log.clone());
    let b = start_manager(log);
    a.reconcile_assignment(&[mp_a]).await;
    b.reconcile_assignment(&[mp_b]).await;

    check!(a.assigned_metadata_partitions() == vec![mp_a]);
    check!(b.assigned_metadata_partitions() == vec![mp_b]);
    // Disjoint shares.
    check!(
        a.assigned_metadata_partitions()
            .iter()
            .all(|p| !b.assigned_metadata_partitions().contains(p)),
        "shares must be disjoint"
    );

    // Poll until each is caught up and serves its own partition.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let a_own = a.remote_log_segment_metadata(&tp_a, LeaderEpoch(0), 42);
        let b_own = b.remote_log_segment_metadata(&tp_b, LeaderEpoch(0), 42);
        if matches!(a_own, Ok(Some(_))) && matches!(b_own, Ok(Some(_))) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "managers did not catch up: a={a_own:?} b={b_own:?}"
        );
        tokio::task::yield_now().await;
    }

    // Cross reads (partition the broker does NOT consume) are a genuine
    // miss, not NotReady.
    assert!(
        matches!(
            a.remote_log_segment_metadata(&tp_b, LeaderEpoch(0), 42),
            Ok(None)
        ),
        "A does not consume mp_b → genuine miss"
    );
    assert!(
        matches!(
            b.remote_log_segment_metadata(&tp_a, LeaderEpoch(0), 42),
            Ok(None)
        ),
        "B does not consume mp_a → genuine miss"
    );

    a.shutdown();
    b.shutdown();
}

/// A runtime `remove` and then `add` reassignment must not double-deliver
/// a metadata partition's events into the cache. The lifecycle state
/// machine harmlessly rejects a re-applied `AddSegment`, so the segment
/// list stays at exactly one entry. That proves there is no duplicate
/// corruption after a remove and re-add.
#[tokio::test(flavor = "multi_thread")]
async fn reassignment_remove_then_readd_applies_no_duplicates() {
    use crate::partitioning::metadata_partition_for;

    let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
    // Pre-seed a single finished segment for `tp()`.
    {
        let writer = start_manager_all(log.clone()).await;
        let w2 = writer.clone();
        on_blocking(move || {
            w2.add_remote_log_segment_metadata(started(10, 0, 99))
                .unwrap();
        })
        .await;
        let w2 = writer.clone();
        on_blocking(move || w2.update_remote_log_segment_metadata(finish(10)).unwrap()).await;
        writer.shutdown();
    }

    let mp = metadata_partition_for(&tp(), log.partition_count());
    let m = start_manager(log);

    // Add → catch up → exactly one segment.
    m.reconcile_assignment(&[mp]).await;
    wait_ready(&m, &tp()).await;
    assert!(
        m.list_remote_log_segments(&tp()).unwrap().len() == 1,
        "one segment after first assignment"
    );

    // Remove (drops the live fetch task mid-flight if one is running) …
    m.reconcile_assignment(&[]).await;
    assert!(m.assigned_metadata_partitions().is_empty());

    // … then re-add. The pump re-injects the backlog from the resume
    // offset; the re-applied AddSegment is rejected by the lifecycle
    // machine, so NO duplicate lands in the cache.
    m.reconcile_assignment(&[mp]).await;
    wait_ready(&m, &tp()).await;

    let listed = m.list_remote_log_segments(&tp()).unwrap();
    assert!(
        listed.len() == 1,
        "remove + re-add must not duplicate the segment, got {listed:?}"
    );
    check!(listed[0].remote_log_segment_id().id == Uuid::from_u128(10));
    // The finished state survived (no half-applied duplicate update).
    check!(m.highest_offset_for_epoch(&tp(), LeaderEpoch(0)).unwrap() == Some(99));

    m.shutdown();
}

/// C1: a HWM-fetch failure must fail CLOSED. When `high_water_marks`
/// errors at assignment time, the newly-added partition must gate on the
/// retryable `NotReady`. It must NEVER return `Ok(None)`, which would be
/// a false end-of-tier. The sentinel must also self-heal on a later
/// reconcile once the HWM fetch succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn hwm_fetch_failure_gates_not_ready_then_self_heals() {
    use crate::partitioning::metadata_partition_for;

    let flaky = HwmFlakyLog::new(4);
    let log: Arc<dyn MetadataEventLog> = flaky.clone();

    // Pre-seed a finished segment for `tp()` via a healthy writer (HWM
    // not failing yet), so a ready read would return Some.
    {
        let writer = start_manager_all(log.clone()).await;
        let w2 = writer.clone();
        on_blocking(move || {
            w2.add_remote_log_segment_metadata(started(10, 0, 99))
                .unwrap();
        })
        .await;
        let w2 = writer.clone();
        on_blocking(move || w2.update_remote_log_segment_metadata(finish(10)).unwrap()).await;
        writer.shutdown();
    }

    let mp = metadata_partition_for(&tp(), log.partition_count());
    let m = start_manager(log);

    // Assign the partition WHILE the HWM RPC is failing. The partition
    // must be added (the broker owns it) but recorded with the sentinel
    // target so the gate returns NotReady, not Ok(None).
    flaky.set_fail_hwm(true);
    m.reconcile_assignment(&[mp]).await;
    assert!(
        m.assigned_metadata_partitions() == vec![mp],
        "partition is assigned even though HWM is unknown (broker owns it)"
    );

    // Give the pump ample time to drain the backlog. Even fully caught
    // up, the read must stay NotReady because the real HWM is unknown —
    // it must NEVER collapse to Ok(None).
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(300);
    while std::time::Instant::now() < deadline {
        match m.remote_log_segment_metadata(&tp(), LeaderEpoch(0), 42) {
            Err(RemoteStorageError::NotReady { partition }) => assert!(partition == mp),
            other => panic!("HWM-unknown partition must read NotReady, got {other:?}"),
        }
        // The list path is gated the same way.
        match m.list_remote_log_segments(&tp()) {
            Err(RemoteStorageError::NotReady { partition }) => assert!(partition == mp),
            other => panic!("HWM-unknown partition list must be NotReady, got {other:?}"),
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // Recover: HWM fetch now succeeds. A subsequent reconcile (which the
    // broker drives on each image change / tick) must replace the
    // sentinel with the real target. Once the pump has caught up the read
    // returns Some.
    flaky.set_fail_hwm(false);
    m.reconcile_assignment(&[mp]).await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        match m.remote_log_segment_metadata(&tp(), LeaderEpoch(0), 42) {
            Ok(Some(md)) => {
                assert!(md.remote_log_segment_id().id == Uuid::from_u128(10));
                break;
            }
            Err(RemoteStorageError::NotReady { partition }) => {
                assert!(partition == mp);
                assert!(
                    std::time::Instant::now() < deadline,
                    "partition never became ready after HWM recovered"
                );
                tokio::task::yield_now().await;
            }
            other => panic!("unexpected read outcome after recovery: {other:?}"),
        }
    }
    // The list path is now Ready too.
    assert!(m.list_remote_log_segments(&tp()).unwrap().len() == 1);
    assert!(m.highest_offset_for_epoch(&tp(), LeaderEpoch(0)).unwrap() == Some(99));

    m.shutdown();
}
