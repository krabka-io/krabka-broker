//! `OffsetFetch` actor-turn latency and allocation measurements.
//!
//! The request widths are fixed at 0, 1 and 10 while retained committed and
//! transactional state grows. Allocation counts are measured (not inferred)
//! once per case with `stats_alloc`; Criterion separately samples latency.

use {
    assert2::check,
    criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main},
    krabka_broker::coordinator::unified::offset_fetch_bench::{Fixture, Request},
    stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc},
    std::{alloc::System, time::Instant},
};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const STORED: [usize; 3] = [100, 1_000, 10_000];
const REQUESTED: [usize; 3] = [0, 1, 10];

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker/offset_fetch_actor_turn");
    for stored in STORED {
        let fixture = Fixture::new(stored);
        for requested in REQUESTED {
            let request = Request::new(requested);
            let all_region = Region::new(GLOBAL);
            let all_result = black_box(fixture.all_actor_turn());
            let all_stats = all_region.change();
            let region = Region::new(GLOBAL);
            let result = black_box(fixture.actor_turn(&request));
            let stats = region.change();
            check!(result == (requested, requested));
            eprintln!(
                "offset_fetch allocations: stored={stored} requested={requested} all=({}, {}) selected=({}, {})",
                all_stats.allocations,
                all_stats.bytes_allocated,
                stats.allocations,
                stats.bytes_allocated
            );
            check!(all_result == (stored, stored));
            let iterations = 100_u32;
            let started = Instant::now();
            for _ in 0..iterations {
                black_box(fixture.all_actor_turn());
            }
            let all_ns = started.elapsed().as_nanos() / u128::from(iterations);
            let started = Instant::now();
            for _ in 0..iterations {
                black_box(fixture.actor_turn(&request));
            }
            let selected_ns = started.elapsed().as_nanos() / u128::from(iterations);
            eprintln!(
                "offset_fetch latency: stored={stored} requested={requested} all_ns={all_ns} selected_ns={selected_ns} iterations={iterations} profile={}",
                if cfg!(debug_assertions) {
                    "dev"
                } else {
                    "bench"
                }
            );
            group.bench_function(
                BenchmarkId::new(format!("all/requested-{requested}"), stored),
                |b| b.iter(|| black_box(fixture.all_actor_turn())),
            );
            group.bench_with_input(
                BenchmarkId::new(format!("selected/requested-{requested}"), stored),
                &request,
                |b, request| {
                    b.iter(|| black_box(fixture.actor_turn(black_box(request))));
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
