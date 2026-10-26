//! Criterion benchmarks for the per-partition remote epoch index behind
//! [`InmemoryRemoteLogMetadataManager`].
//!
//! Every partition holds `retained` finished segments, one epoch entry each,
//! so `retained` is also the number of entries in the epoch index. The
//! benchmarks measure, as that history grows:
//!
//! - `transition`: one segment lifecycle (add, finish, hide, remove) at the
//!   tail of the log, which is three index updates.
//! - `replay`: rebuilding the partition from empty by replaying `retained`
//!   add and finish events, as a metadata-topic consumer does on start-up.
//! - `restore`: importing a snapshot dump of `retained` finished segments.
//! - `lookup_idle`: one `(epoch, offset)` lookup.
//!
//! `ci.yml`'s nightly `bench` job compares every benchmark here against the
//! previous commit, so each one must be stable from pass to pass on one host.
//! A lookup racing a free-running writer thread is not: on the same code its
//! mean swung by up to 1.7x between passes, with scheduling deciding how often
//! the lookup waits. How long a lookup can wait behind a writer is the time
//! one transition holds the manager's mutex, which `transition` measures.

use {
    criterion::{
        BatchSize, BenchmarkGroup, BenchmarkId, Criterion, black_box, measurement::WallTime,
    },
    krabka_ids::LeaderEpoch,
    krabka_remote_storage::{
        InmemoryRemoteLogMetadataManager, PartitionDump, RemoteLogMetadataManager,
        RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadata,
        RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, RlmmCacheDump, TopicIdPartition,
    },
    std::{cell::RefCell, collections::BTreeMap},
    uuid::Uuid,
};

krabka_macros::bench_failures!();

/// Offsets per segment.
const SEGMENT_SPAN: i64 = 100;
/// Consecutive segments that share one leader epoch.
const SEGMENTS_PER_EPOCH: u64 = 16;

fn partition() -> TopicIdPartition { TopicIdPartition::new(Uuid::from_u128(1), "bench", 0) }

/// The `index`th segment of the log, in `CopySegmentStarted`.
fn segment(index: u64) -> BenchResult<RemoteLogSegmentMetadata> {
    let start = i64::try_from(index)? * SEGMENT_SPAN;
    let epoch = LeaderEpoch(i32::try_from(index / SEGMENTS_PER_EPOCH)?);
    Ok(RemoteLogSegmentMetadata::new(
        RemoteLogSegmentId::new(partition(), Uuid::from_u128(u128::from(index) + 1_000)),
        start,
        start + SEGMENT_SPAN - 1,
        start,
        1,
        0,
        RemoteLogSegmentDetails::new(
            1024,
            RemoteLogSegmentState::CopySegmentStarted,
            BTreeMap::from([(epoch, start)]),
        ),
    )?)
}

fn transition(index: u64, state: RemoteLogSegmentState) -> RemoteLogSegmentMetadataUpdate {
    RemoteLogSegmentMetadataUpdate {
        remote_log_segment_id: RemoteLogSegmentId::new(
            partition(),
            Uuid::from_u128(u128::from(index) + 1_000),
        ),
        event_timestamp_ms: 1,
        custom_metadata: None,
        state,
        broker_id: 1,
    }
}

fn add_finished(manager: &InmemoryRemoteLogMetadataManager, index: u64) -> BenchResult {
    manager.add_remote_log_segment_metadata(segment(index)?)?;
    manager.update_remote_log_segment_metadata(transition(
        index,
        RemoteLogSegmentState::CopySegmentFinished,
    ))?;
    Ok(())
}

/// A snapshot dump of a partition that retains `retained` finished segments.
fn retained_dump(retained: u64) -> BenchResult<RlmmCacheDump> {
    let segments = (0..retained)
        .map(|index| {
            Ok(segment(index)?.with_update(&transition(
                index,
                RemoteLogSegmentState::CopySegmentFinished,
            ))?)
        })
        .collect::<BenchResult<_>>()?;
    Ok(RlmmCacheDump {
        partitions: vec![PartitionDump {
            topic_id_partition: partition(),
            segments,
            delete_state: None,
        }],
    })
}

/// A manager whose partition retains `retained` finished segments, restored
/// from a snapshot so that building it costs one index build.
fn retained_manager(retained: u64) -> BenchResult<InmemoryRemoteLogMetadataManager> {
    let manager = InmemoryRemoteLogMetadataManager::new();
    manager.import(retained_dump(retained)?);
    Ok(manager)
}

/// Adds segment `index` past the retained tail, finishes it, hides it and
/// removes it, leaving the partition as it found it.
fn lifecycle(manager: &InmemoryRemoteLogMetadataManager, index: u64) -> BenchResult {
    add_finished(manager, index)?;
    for state in [
        RemoteLogSegmentState::DeleteSegmentStarted,
        RemoteLogSegmentState::DeleteSegmentFinished,
    ] {
        manager.update_remote_log_segment_metadata(transition(index, state))?;
    }
    Ok(())
}

/// A lookup inside the middle retained segment.
fn lookup(manager: &InmemoryRemoteLogMetadataManager, retained: u64) -> BenchResult {
    let middle = retained / 2;
    let epoch = LeaderEpoch(i32::try_from(middle / SEGMENTS_PER_EPOCH)?);
    let offset = i64::try_from(middle)? * SEGMENT_SPAN + 7;
    let found = manager
        .remote_log_segment_metadata(&partition(), epoch, offset)?
        .ok_or("the middle segment is not readable")?;
    black_box(found);
    Ok(())
}

const RETAINED: [u64; 3] = [1_000, 10_000, 100_000];

fn transition_cost(c: &mut Criterion) { run(c, "remote_epoch_index/transition", transitions); }

fn transitions(group: &mut BenchmarkGroup<'_, WallTime>) -> BenchResult {
    let mut failed = FirstError::default();
    group.sample_size(10);
    for retained in RETAINED {
        let manager = retained_manager(retained)?;
        let mut next = retained;
        group.bench_function(BenchmarkId::from_parameter(retained), |b| {
            b.iter(|| {
                failed.record(lifecycle(&manager, next));
                next += 1;
            });
        });
    }
    failed.into_result()
}

fn replay_cost(c: &mut Criterion) { run(c, "remote_epoch_index/replay", replays); }

fn replays(group: &mut BenchmarkGroup<'_, WallTime>) -> BenchResult {
    let failed = RefCell::new(FirstError::default());
    group.sample_size(10);
    for retained in [1_000, 10_000] {
        group.bench_function(BenchmarkId::from_parameter(retained), |b| {
            b.iter_batched(
                InmemoryRemoteLogMetadataManager::new,
                |manager| {
                    for index in 0..retained {
                        failed.borrow_mut().record(add_finished(&manager, index));
                    }
                    manager
                },
                BatchSize::LargeInput,
            );
        });
    }
    failed.into_inner().into_result()
}

fn restore_cost(c: &mut Criterion) { run(c, "remote_epoch_index/restore", restores); }

fn restores(group: &mut BenchmarkGroup<'_, WallTime>) -> BenchResult {
    group.sample_size(10);
    for retained in RETAINED {
        let dump = retained_dump(retained)?;
        group.bench_function(BenchmarkId::from_parameter(retained), |b| {
            b.iter_batched(
                || (InmemoryRemoteLogMetadataManager::new(), dump.clone()),
                |(manager, dump)| {
                    manager.import(dump);
                    manager
                },
                BatchSize::LargeInput,
            );
        });
    }
    Ok(())
}

fn lookup_latency(c: &mut Criterion) { run(c, "remote_epoch_index/lookup_idle", lookups); }

fn lookups(group: &mut BenchmarkGroup<'_, WallTime>) -> BenchResult {
    let mut failed = FirstError::default();
    for retained in RETAINED {
        let manager = retained_manager(retained)?;
        group.bench_function(BenchmarkId::from_parameter(retained), |b| {
            b.iter(|| failed.record(lookup(&manager, retained)));
        });
    }
    failed.into_result()
}

krabka_macros::bench_main!(
    transition_cost,
    replay_cost,
    restore_cost,
    lookup_latency,
    report_failures
);
