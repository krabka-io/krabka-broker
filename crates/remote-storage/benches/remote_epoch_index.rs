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
        BatchSize, BenchmarkGroup, BenchmarkId, Criterion, abs_file, black_box, criterion_main,
        measurement::WallTime,
    },
    krabka_ids::LeaderEpoch,
    krabka_remote_storage::{
        InmemoryRemoteLogMetadataManager, PartitionDump, RemoteLogMetadataManager,
        RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadata,
        RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, RlmmCacheDump, TopicIdPartition,
    },
    std::{
        cell::RefCell,
        collections::BTreeMap,
        env,
        error::Error,
        process,
        sync::{Mutex, PoisonError},
    },
    uuid::Uuid,
};

/// What a benchmark's setup or measured work returns.
type BenchResult<T = ()> = Result<T, Box<dyn Error>>;

/// The first error a measured closure hit. A closure that criterion times
/// cannot return an error, so it records one here and the benchmark function
/// returns it once the measurement is over.
#[derive(Default)]
struct FirstError(Option<Box<dyn Error>>);

impl FirstError {
    /// The value of `result`, or `None` after recording its error.
    fn record<T, E: Into<Box<dyn Error>>>(&mut self, result: Result<T, E>) -> Option<T> {
        match result {
            Ok(value) => Some(value),
            Err(error) => {
                self.0.get_or_insert_with(|| error.into());
                None
            }
        }
    }

    fn into_result(self) -> BenchResult { self.0.map_or(Ok(()), Err) }
}

/// The error the benchmarks stopped with, which [`report_failures`] reports.
static FAILURE: Mutex<Option<String>> = Mutex::new(None);

/// Run the benchmarks of one group, keeping the error they stopped with.
fn run(
    c: &mut Criterion,
    name: &str,
    bench: impl FnOnce(&mut BenchmarkGroup<'_, WallTime>) -> BenchResult,
) {
    let mut group = c.benchmark_group(name);
    if let Err(error) = bench(&mut group) {
        FAILURE
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert_with(|| format!("{name}: {error}"));
    }
    group.finish();
}

/// Exit with a failure when a benchmark function stopped on an error. It runs
/// last, after every benchmark that could still measure.
fn report_failures(_c: &mut Criterion) {
    let failure = FAILURE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take();
    if let Some(failure) = failure {
        eprintln!("benchmark failed: {failure}");
        process::exit(1);
    }
}

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

/// A benchmark target, under the name `criterion_group!` would give it.
type Target = (&'static str, fn(&mut Criterion));

/// Every benchmark target, in run order. [`report_failures`] runs last.
const TARGETS: [Target; 5] = [
    ("transition_cost", transition_cost),
    ("replay_cost", replay_cost),
    ("restore_cost", restore_cost),
    ("lookup_latency", lookup_latency),
    ("report_failures", report_failures),
];

/// Runs every target in [`TARGETS`] against `criterion`.
fn run_targets(criterion: &mut Criterion) {
    for (name, target) in TARGETS {
        if env::var("CODSPEED_ENV").is_ok() {
            criterion.set_current_file(abs_file!());
            criterion.set_macro_group(format!("benches::{name}"));
        }
        target(criterion);
    }
}

/// The benchmark group, written out by hand where `criterion_group!` would
/// generate it.
fn benches() { run_targets(&mut Criterion::default().configure_from_args()); }

criterion_main!(benches);
