//! `CodSpeed` microbenchmarks for the thread-pool hand-off in the fetch read
//! loop.
//!
//! A consumer subscribed to many partitions turns one Fetch request into one
//! blocking log read per partition. How those reads reach a blocking thread is
//! a free choice, and the three ways of making it differ by a fixed cost per
//! read that a wide subscription pays once per partition:
//!
//!   * `spawn_blocking_per_partition` — one `spawn_blocking` per partition,
//!     awaited in turn. Every partition pays a task allocation, a queue push, a
//!     pool wakeup and a `JoinHandle` await, and the awaits serialize;
//!   * `spawn_blocking_batched` — one `spawn_blocking` for the whole pending
//!     set, which pays that cost once per fetch however wide it is;
//!   * `block_in_place` — no hand-off at all: one call per partition, as the
//!     read loop makes it, and the reads run on the reactor worker after tokio
//!     moves its other tasks to a fresh worker.
//!
//! The arms mirror `bench_append_handoff` in `krabka-log`'s suite, which
//! settled the same question for the append side, so the two sets of numbers
//! are comparable.
//!
//! The measured work is deliberately a warm read: the fetch offset is the same
//! on every iteration, so the bytes come from page cache and what remains
//! around them is the hand-off. That is the case the hand-off cost matters in.
//! A consumer that fetches 200 partitions and finds a handful of records on
//! each pays the per-partition cost 200 times over a few hundred microseconds
//! of real reading.

use {
    criterion::Criterion,
    krabka_broker::log_write_back,
    krabka_log::{Log, LogConfig, LogError, Offset},
    krabka_units::prelude::{ByteSize, mebibytes},
    std::{
        io, iter,
        sync::{Arc, Mutex, PoisonError},
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::{runtime::Runtime, task},
};

#[path = "support/first_error.rs"]
mod first_error;

use first_error::{BenchError, FirstError};

/// Partition counts: a single-partition fetch, a modest subscription, and the
/// wide one from the issue that motivated the measurement.
const PARTITION_COUNTS: [usize; 3] = [1, 16, 200];

/// Record shapes per partition, as `(name, records, payload)`.
///
/// The small shape is the one a wide subscription actually sees: a consumer on
/// 200 partitions rarely finds a full `fetch.max.bytes` waiting on each. The
/// larger shape is the check that the hand-off stops mattering once there are
/// real bytes to move.
const SHAPES: [BatchShape; 2] = [
    BatchShape {
        label: "1rec_1KiB",
        records: 1,
        payload: 1024,
    },
    BatchShape {
        label: "100rec_1KiB",
        records: 100,
        payload: 1024,
    },
];

/// One batch shape the sweep covers: its report label, the record count of each
/// partition's batch and the payload size of each record.
struct BatchShape {
    label: &'static str,
    records: i32,
    payload: usize,
}

/// A read budget larger than any partition the suite builds, so the read is
/// never clipped and every arm moves the same bytes.
const UNBOUNDED: ByteSize = mebibytes(64);

/// One partition's log, its read limit, and the temp dir that backs it.
struct BenchPartition {
    _dir: TempDir,
    log: Mutex<Log>,
    limit: Offset,
}

krabka_macros::record_batch_fixture!(make_batch);

/// One partition log holding one batch of the given shape.
fn partition(records: i32, payload: usize) -> Result<BenchPartition, BenchError> {
    let dir = tempfile::tempdir()?;
    let mut log = Log::open(dir.path(), LogConfig::default())?;
    log.append(&mut make_batch(records, payload))?;
    let limit = log.log_end_offset();
    Ok(BenchPartition {
        _dir: dir,
        log: Mutex::new(log),
        limit,
    })
}

/// `count` partition logs, each holding one batch of the given shape.
fn partitions(
    count: usize,
    records: i32,
    payload: usize,
) -> Result<Arc<[BenchPartition]>, BenchError> {
    iter::repeat_with(|| partition(records, payload))
        .take(count)
        .collect::<Result<Vec<_>, _>>()
        .map(Arc::from)
}

/// The blocking work one partition of a fetch does: take the log mutex, seek,
/// and copy the records run out.
fn read_one(partition: &BenchPartition) -> Result<usize, LogError> {
    let log = partition.log.lock().unwrap_or_else(PoisonError::into_inner);
    Ok(log
        .read_raw(Offset(0), partition.limit, UNBOUNDED)?
        .bytes
        .len())
}

/// [`read_one`] on the partition at `index`.
fn read_at(partitions: &[BenchPartition], index: usize) -> Result<usize, BenchError> {
    let partition = partitions
        .get(index)
        .ok_or("partition index out of range")?;
    Ok(read_one(partition)?)
}

fn read_all(partitions: &[BenchPartition]) -> Result<usize, LogError> {
    partitions.iter().map(read_one).sum()
}

/// The three hand-off shapes, in report order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Handoff {
    /// One `spawn_blocking` per partition, awaited in turn.
    PerPartition,
    /// One `spawn_blocking` covering the whole pending set.
    Batched,
    /// No hand-off: one `block_in_place` per partition on the reactor worker,
    /// which is the shape the read loop actually has -- `do_read` is called
    /// once per partition, so the choice this arm stands for is made once per
    /// partition too. The first call hands the worker's core to a replacement
    /// thread and the rest find no core left to hand over, so the per-call
    /// cost after the first is a thread-local check.
    InPlace,
}

impl Handoff {
    const ALL: [Self; 3] = [Self::PerPartition, Self::Batched, Self::InPlace];

    fn name(self) -> &'static str {
        match self {
            Self::PerPartition => "spawn_blocking_per_partition",
            Self::Batched => "spawn_blocking_batched",
            Self::InPlace => "block_in_place",
        }
    }
}

/// Serve one fetch across every partition, under one hand-off shape.
async fn serve_fetch(
    handoff: Handoff,
    partitions: &Arc<[BenchPartition]>,
) -> Result<usize, BenchError> {
    match handoff {
        Handoff::PerPartition => {
            let mut bytes = 0;
            for index in 0..partitions.len() {
                let partitions = Arc::clone(partitions);
                bytes += task::spawn_blocking(move || read_at(&partitions, index)).await??;
            }
            Ok(bytes)
        }
        Handoff::Batched => {
            let partitions = Arc::clone(partitions);
            Ok(task::spawn_blocking(move || read_all(&partitions)).await??)
        }
        Handoff::InPlace => Ok(partitions
            .iter()
            .map(|partition| task::block_in_place(|| read_one(partition)))
            .sum::<Result<usize, LogError>>()?),
    }
}

/// A multi-threaded runtime, which is what the broker's `#[tokio::main]` gives
/// it and what `block_in_place` requires: it panics on a current-thread
/// runtime, so the `InPlace` arm cannot even be measured without one.
fn runtime() -> io::Result<Runtime> { Runtime::new() }

/// Serve `iters` fetches, returning only the time they took.
fn timed_fetches(
    runtime: &Runtime,
    handoff: Handoff,
    partitions: &Arc<[BenchPartition]>,
    iters: u64,
) -> Result<Duration, BenchError> {
    runtime.block_on(async {
        // One untimed fetch, so the pool's threads are already spun up and the
        // partition files are already in page cache when the clock starts.
        serve_fetch(handoff, partitions).await?;
        let start = Instant::now();
        for _ in 0..iters {
            serve_fetch(handoff, partitions).await?;
        }
        Ok(start.elapsed())
    })
}

/// Installs the log write-back that the broker installs at startup, so the
/// logs below pace their dirty data as the broker's logs do. It runs first.
fn install_log_write_back(_c: &mut Criterion) { log_write_back::install(); }

fn bench_fetch_handoff(c: &mut Criterion) -> Result<(), BenchError> {
    let runtime = runtime()?;
    let failure = FirstError::default();
    {
        let mut group = c.benchmark_group("broker/fetch_handoff");

        for BatchShape {
            label: shape,
            records,
            payload,
        } in SHAPES
        {
            for count in PARTITION_COUNTS {
                let partitions = partitions(count, records, payload)?;
                for handoff in Handoff::ALL {
                    group.bench_function(format!("{shape}/{count}p/{}", handoff.name()), |b| {
                        b.iter_custom(|iters| {
                            failure
                                .keep(timed_fetches(&runtime, handoff, &partitions, iters))
                                .unwrap_or(Duration::ZERO)
                        });
                    });
                }
            }
        }

        group.finish();
    }
    failure.into_result()
}

/// Print microseconds per fetch for each hand-off shape, and the two ratios
/// against the per-partition arm the read loop starts from.
///
/// Criterion compares a benchmark against its own previous run, not against a
/// sibling, so the number this suite exists for — what the per-partition
/// hand-off costs against batching it — has to be computed here.
fn bench_ratio(_c: &mut Criterion) -> Result<(), BenchError> {
    /// Fetches per measurement run.
    const SAMPLES: u32 = 50;
    /// Measurement runs per case, of which the fastest is the one reported.
    ///
    /// A single run is noise-dominated on a machine that is doing anything
    /// else. Interference only ever adds time, so the minimum over a few runs
    /// is the estimator it cannot pull the wrong way.
    const RUNS: u32 = 5;

    let runtime = runtime()?;
    println!(
        "\nbroker/fetch_handoff — microseconds per fetch (fastest of {RUNS} runs), and what the per-partition hand-off costs"
    );
    println!(
        "{:<14} {:>6} {:>14} {:>14} {:>14} {:>14} {:>14}",
        "shape",
        "parts",
        "per_partition",
        "batched",
        "block_in_place",
        "per/batched",
        "per/in_place"
    );
    for BatchShape {
        label: shape,
        records,
        payload,
    } in SHAPES
    {
        for count in PARTITION_COUNTS {
            let partitions = partitions(count, records, payload)?;
            let [per_partition, batched, in_place] = Handoff::ALL
                .map(|handoff| fastest_micros(&runtime, handoff, &partitions, SAMPLES, RUNS));
            let (per_partition, batched, in_place) = (per_partition?, batched?, in_place?);
            println!(
                "{shape:<14} {count:>6} {per_partition:>14.1} {batched:>14.1} {in_place:>14.1} {:>13.2}x {:>13.2}x",
                per_partition / batched,
                per_partition / in_place
            );
        }
    }
    println!();
    Ok(())
}

/// Microseconds per fetch for one hand-off shape: the fastest of `runs`
/// measurement runs of `samples` fetches each.
fn fastest_micros(
    runtime: &Runtime,
    handoff: Handoff,
    partitions: &Arc<[BenchPartition]>,
    samples: u32,
    runs: u32,
) -> Result<f64, BenchError> {
    let mut fastest = f64::INFINITY;
    for _ in 0..runs {
        let elapsed = timed_fetches(runtime, handoff, partitions, u64::from(samples))?;
        fastest = fastest.min(elapsed.as_secs_f64() * 1.0e6 / f64::from(samples));
    }
    Ok(fastest)
}

krabka_macros::bench_main!(install_log_write_back, bench_fetch_handoff, bench_ratio);
