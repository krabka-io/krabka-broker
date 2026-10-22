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
    criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main},
    krabka_broker::{
        share_coordinator::persistence::StateBatch,
        share_partition::state::{AckAction, AckType, AcquisitionState, DS_AVAILABLE},
    },
    krabka_log::Offset,
    std::{
        hint::black_box,
        time::{Duration, Instant},
    },
};

const MEMBER: &str = "benchmark-member";
const SIZES: [usize; 5] = [100, 500, 1_000, 2_000, 4_000];
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

    fn actions(self, size: usize) -> Vec<AckAction> {
        match self {
            Self::UniformAccept => vec![AckAction::Disposition(AckType::Accept); size],
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

fn acquired_state(size: usize, shape: WindowShape) -> AcquisitionState {
    let end = Offset(i64::try_from(size).expect("benchmark sizes fit i64"));
    let mut state = AcquisitionState::new(Offset(0));
    match shape {
        WindowShape::Contiguous => state.materialize(end, i32::MAX),
        WindowShape::Fragmented => {
            let batches = (0..size)
                .map(|offset| {
                    let offset = i64::try_from(offset).expect("benchmark sizes fit i64");
                    StateBatch {
                        first_offset: Offset(offset),
                        last_offset: Offset(offset),
                        delivery_state: DS_AVAILABLE,
                        // Acquisition increments this to alternating 1/2,
                        // keeping every neighbouring run distinct.
                        delivery_count: i16::try_from(offset % 2)
                            .expect("the fragmented delivery count is zero or one"),
                    }
                })
                .collect::<Vec<_>>();
            state.load_from(Offset(0), 0, 0, &batches);
        }
    }

    let acquired = state.acquire(
        MEMBER,
        i32::try_from(size).unwrap(),
        end - 1,
        Instant::now(),
        LOCK,
        MAX_ATTEMPTS,
    );
    let acquired_records: i64 = acquired
        .iter()
        .map(|range| range.last.0 - range.first.0 + 1)
        .sum();
    assert!(acquired_records == i64::try_from(size).unwrap());
    state
}

fn bench_share_acknowledge(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker/share_acknowledge");

    for size in SIZES {
        let last = Offset(i64::try_from(size).unwrap() - 1);
        for window in [WindowShape::Contiguous, WindowShape::Fragmented] {
            let template = acquired_state(size, window);
            for ack in [AckShape::UniformAccept, AckShape::AlternatingReleaseAccept] {
                let actions = ack.actions(size);
                group.throughput(Throughput::Elements(u64::try_from(size).unwrap()));
                group.bench_function(format!("{}/{}/{}", window.name(), ack.name(), size), |b| {
                    b.iter_batched(
                        || template.clone(),
                        |mut state| {
                            state
                                .acknowledge_mixed(
                                    MEMBER,
                                    Offset(0),
                                    last,
                                    black_box(&actions),
                                    MAX_ATTEMPTS,
                                )
                                .expect("the benchmark acknowledgement is valid");
                            let expected_start = match ack {
                                AckShape::UniformAccept => last + 1,
                                AckShape::AlternatingReleaseAccept => Offset(0),
                            };
                            assert!(state.start_offset == expected_start);
                            black_box(state);
                        },
                        BatchSize::SmallInput,
                    );
                });
            }
        }
    }

    group.finish();
}

criterion_group!(benches, bench_share_acknowledge);
criterion_main!(benches);
