//! Mixed share acknowledgement transition benchmarks.
//!
//! A per-offset acknowledgement used to invoke the complete state transition
//! once for every equal-typed run. Alternating dispositions therefore made a
//! request repeatedly search, advance, and coalesce the same window. This
//! suite measures the one-pass transition over both the friendly uniform case
//! and that adversarial alternating case. It also repeats both shapes against
//! an already fragmented window, where every input run has a distinct
//! delivery count from its neighbours.
//!
//! State construction and the clone that resets an iteration are deliberately
//! outside Criterion's timer. The measured operation is exactly one
//! `acknowledge_mixed` call. Run with:
//!
//! ```text
//! cargo bench -p krabka-broker --bench share_acknowledge -- --noplot
//! ```

use {
    assert2::assert,
    criterion::{BatchSize, Criterion, Throughput, abs_file, criterion_main},
    krabka_broker::{
        share_coordinator::persistence::StateBatch,
        share_partition::state::{AckAction, AckType, AcquisitionState, DS_AVAILABLE},
    },
    krabka_log::Offset,
    std::{
        env,
        error::Error,
        hint::black_box,
        process,
        time::{Duration, Instant},
    },
};

/// Any failure while building or measuring a case; it stops the run.
type BenchError = Box<dyn Error + Send + Sync>;

const MEMBER: &str = "benchmark-member";
const SIZES: [u16; 5] = [100, 500, 1_000, 2_000, 4_000];
const MAX_ATTEMPTS: i16 = 10_000;
const LOCK: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
enum WindowShape {
    Contiguous,
    Fragmented,
}

impl WindowShape {
    const fn name(self) -> &'static str {
        match self {
            Self::Contiguous => "contiguous",
            Self::Fragmented => "fragmented",
        }
    }
}

#[derive(Clone, Copy)]
enum AckShape {
    UniformAccept,
    AlternatingReleaseAccept,
}

impl AckShape {
    const fn name(self) -> &'static str {
        match self {
            Self::UniformAccept => "uniform_accept",
            Self::AlternatingReleaseAccept => "alternating_release_accept",
        }
    }

    fn actions(self, size: u16) -> Vec<AckAction> {
        match self {
            Self::UniformAccept => vec![AckAction::Disposition(AckType::Accept); usize::from(size)],
            Self::AlternatingReleaseAccept => (0..size)
                .map(|offset| {
                    AckAction::Disposition(if offset % 2 == 0 {
                        AckType::Release
                    } else {
                        AckType::Accept
                    })
                })
                .collect(),
        }
    }
}

fn acquired_state(size: u16, shape: WindowShape) -> AcquisitionState {
    let end = Offset(i64::from(size));
    let mut state = AcquisitionState::new(Offset(0));
    match shape {
        WindowShape::Contiguous => state.materialize(end, i32::MAX),
        WindowShape::Fragmented => {
            let batches = (0..size)
                .map(|offset| StateBatch {
                    first_offset: Offset(i64::from(offset)),
                    last_offset: Offset(i64::from(offset)),
                    delivery_state: DS_AVAILABLE,
                    // Acquisition increments this to alternating 1/2,
                    // keeping every neighbouring run distinct.
                    delivery_count: i16::from(offset % 2 == 1),
                })
                .collect::<Vec<_>>();
            state.load_from(Offset(0), 0, 0, &batches);
        }
    }

    let acquired = state.acquire(
        MEMBER,
        i32::from(size),
        end - 1,
        Instant::now(),
        LOCK,
        MAX_ATTEMPTS,
    );
    let acquired_records: i64 = acquired
        .iter()
        .map(|range| range.last.0 - range.first.0 + 1)
        .sum();
    assert!(acquired_records == i64::from(size));
    state
}

fn bench_share_acknowledge(c: &mut Criterion) -> Result<(), BenchError> {
    // The first error code a measured acknowledgement was refused with.
    let mut refused: Option<i16> = None;
    {
        let mut group = c.benchmark_group("broker/share_acknowledge");

        for size in SIZES {
            let last = Offset(i64::from(size) - 1);
            for window in [WindowShape::Contiguous, WindowShape::Fragmented] {
                let template = acquired_state(size, window);
                for ack in [AckShape::UniformAccept, AckShape::AlternatingReleaseAccept] {
                    let actions = ack.actions(size);
                    group.throughput(Throughput::Elements(u64::from(size)));
                    group.bench_function(
                        format!("{}/{}/{}", window.name(), ack.name(), size),
                        |b| {
                            b.iter_batched(
                                || template.clone(),
                                |mut state| {
                                    let acknowledged = state.acknowledge_mixed(
                                        MEMBER,
                                        Offset(0),
                                        last,
                                        black_box(&actions),
                                        MAX_ATTEMPTS,
                                    );
                                    match acknowledged {
                                        Ok(()) => {
                                            let expected_start = match ack {
                                                AckShape::UniformAccept => last + 1,
                                                AckShape::AlternatingReleaseAccept => Offset(0),
                                            };
                                            assert!(state.start_offset == expected_start);
                                        }
                                        Err(code) => {
                                            refused.get_or_insert(code);
                                        }
                                    }
                                    black_box(state);
                                },
                                BatchSize::SmallInput,
                            );
                        },
                    );
                }
            }
        }

        group.finish();
    }
    refused.map_or(Ok(()), |code| {
        Err(format!("the benchmark acknowledgement was refused with error code {code}").into())
    })
}

/// A benchmark target, under the name `criterion_group!` would give it.
type Target = (&'static str, fn(&mut Criterion) -> Result<(), BenchError>);

/// Every benchmark target, in run order.
const TARGETS: [Target; 1] = [("bench_share_acknowledge", bench_share_acknowledge)];

/// Runs every target in [`TARGETS`], stopping at the first one that fails.
fn run_targets(criterion: &mut Criterion) -> Result<(), (&'static str, BenchError)> {
    for (name, target) in TARGETS {
        if env::var("CODSPEED_ENV").is_ok() {
            criterion.set_current_file(abs_file!());
            criterion.set_macro_group(format!("benches::{name}"));
        }
        target(criterion).map_err(|error| (name, error))?;
    }
    Ok(())
}

/// The benchmark group, written out by hand where `criterion_group!` would
/// generate it, so that a failing target reports its error and stops the run.
fn benches() {
    let result = run_targets(&mut Criterion::default().configure_from_args());
    if let Err((name, error)) = result {
        eprintln!("{name}: {error}");
        process::exit(1);
    }
}

criterion_main!(benches);
