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
    assert2::assert,
    criterion::{BatchSize, BenchmarkId, Criterion, black_box, criterion_group, criterion_main},
    krabka_ids::LeaderEpoch,
    krabka_remote_storage::{
        InmemoryRemoteLogMetadataManager, PartitionDump, RemoteLogMetadataManager,
        RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadata,
        RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, RlmmCacheDump, TopicIdPartition,
    },
    std::collections::BTreeMap,
    uuid::Uuid,
};

/// Offsets per segment.
const SEGMENT_SPAN: i64 = 100;
/// Consecutive segments that share one leader epoch.
const SEGMENTS_PER_EPOCH: u64 = 16;

fn partition() -> TopicIdPartition { TopicIdPartition::new(Uuid::from_u128(1), "bench", 0) }

/// The `index`th segment of the log, in `CopySegmentStarted`.
fn segment(index: u64) -> RemoteLogSegmentMetadata {
    let start = i64::try_from(index).expect("index fits i64") * SEGMENT_SPAN;
    let epoch = LeaderEpoch(i32::try_from(index / SEGMENTS_PER_EPOCH).expect("epoch fits i32"));
    RemoteLogSegmentMetadata::new(
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
    )
    .expect("valid segment")
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

fn add_finished(manager: &InmemoryRemoteLogMetadataManager, index: u64) {
    manager
        .add_remote_log_segment_metadata(segment(index))
        .expect("add");
    manager
        .update_remote_log_segment_metadata(transition(
            index,
            RemoteLogSegmentState::CopySegmentFinished,
        ))
        .expect("finish");
}

/// A snapshot dump of a partition that retains `retained` finished segments.
fn retained_dump(retained: u64) -> RlmmCacheDump {
    let segments = (0..retained)
        .map(|index| {
            segment(index)
                .with_update(&transition(
                    index,
                    RemoteLogSegmentState::CopySegmentFinished,
                ))
                .expect("finish")
        })
        .collect();
    RlmmCacheDump {
        partitions: vec![PartitionDump {
            topic_id_partition: partition(),
            segments,
            delete_state: None,
        }],
    }
}

/// A manager whose partition retains `retained` finished segments, restored
/// from a snapshot so that building it costs one index build.
fn retained_manager(retained: u64) -> InmemoryRemoteLogMetadataManager {
    let manager = InmemoryRemoteLogMetadataManager::new();
    manager.import(retained_dump(retained));
    manager
}

/// Adds segment `index` past the retained tail, finishes it, hides it and
/// removes it, leaving the partition as it found it.
fn lifecycle(manager: &InmemoryRemoteLogMetadataManager, index: u64) {
    add_finished(manager, index);
    for state in [
        RemoteLogSegmentState::DeleteSegmentStarted,
        RemoteLogSegmentState::DeleteSegmentFinished,
    ] {
        manager
            .update_remote_log_segment_metadata(transition(index, state))
            .expect("delete transition");
    }
}

/// A lookup inside the middle retained segment.
fn lookup(manager: &InmemoryRemoteLogMetadataManager, retained: u64) {
    let middle = retained / 2;
    let epoch = LeaderEpoch(i32::try_from(middle / SEGMENTS_PER_EPOCH).expect("epoch fits i32"));
    let offset = i64::try_from(middle).expect("index fits i64") * SEGMENT_SPAN + 7;
    let found = manager
        .remote_log_segment_metadata(&partition(), epoch, offset)
        .expect("lookup");
    assert!(found.is_some(), "the middle segment is readable");
    black_box(found);
}

const RETAINED: [u64; 3] = [1_000, 10_000, 100_000];

fn transition_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("remote_epoch_index/transition");
    group.sample_size(10);
    for retained in RETAINED {
        let manager = retained_manager(retained);
        let mut next = retained;
        group.bench_function(BenchmarkId::from_parameter(retained), |b| {
            b.iter(|| {
                lifecycle(&manager, next);
                next += 1;
            });
        });
    }
    group.finish();
}

fn replay_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("remote_epoch_index/replay");
    group.sample_size(10);
    for retained in [1_000, 10_000] {
        group.bench_function(BenchmarkId::from_parameter(retained), |b| {
            b.iter_batched(
                InmemoryRemoteLogMetadataManager::new,
                |manager| {
                    for index in 0..retained {
                        add_finished(&manager, index);
                    }
                    manager
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

fn restore_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("remote_epoch_index/restore");
    group.sample_size(10);
    for retained in RETAINED {
        let dump = retained_dump(retained);
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
    group.finish();
}

fn lookup_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("remote_epoch_index/lookup_idle");
    for retained in RETAINED {
        let manager = retained_manager(retained);
        group.bench_function(BenchmarkId::from_parameter(retained), |b| {
            b.iter(|| lookup(&manager, retained));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    transition_cost,
    replay_cost,
    restore_cost,
    lookup_latency
);
criterion_main!(benches);
