//! The remote-log quota measures Kafka's `SimpleRate` over its sample
//! windows, answers `QuotaUtils.throttleTime`, keeps its samples across a new
//! bound.

use {super::*, assert2::check};

/// One step of a quota scenario: record bytes, change the bound, or read the
/// throttle time, at a clock reading in milliseconds.
#[derive(Debug, Clone, Copy)]
enum Step {
    Record { at: i64, bytes: u64 },
    Update { bytes_per_second: i64 },
    Throttle { at: i64 },
    Acquire { at: i64, bytes: u64 },
}

/// What a scenario's `Throttle` and `Acquire` steps answered, in order.
fn run_steps(bound: i64, steps: &[Step]) -> Vec<Result<(), i64>> {
    let quota = RlmQuotaManager::new(bound, QuotaWindow::default());
    steps
        .iter()
        .filter_map(|step| match *step {
            Step::Record { at, bytes } => {
                quota.record_at(bytes, at);
                None
            }
            Step::Update { bytes_per_second } => {
                quota.update_quota(bytes_per_second);
                None
            }
            Step::Throttle { at } => Some(match quota.throttle_time_ms_at(at) {
                0 => Ok(()),
                throttle => Err(throttle),
            }),
            Step::Acquire { at, bytes } => Some(quota.try_acquire_at(bytes, at)),
        })
        .collect()
}

#[yare::parameterized(
    // Nothing recorded measures zero, so the first read always goes ahead.
    an_idle_quota_allows = { 1000, &[Step::Throttle { at: 0 }], &[Ok(())] },
    // 2000 bytes over the one-second floor of the window is 2000 B/s: one
    // second over a 1000 B/s bound, `(2000 - 1000) / 1000 * 1000`.
    a_burst_over_the_bound_throttles_for_the_excess = {
        1000,
        &[Step::Record { at: 0, bytes: 2000 }, Step::Throttle { at: 0 }],
        &[Err(1000)],
    },
    // At 1.5 s the window is 1.5 s wide: 1333 B/s, and
    // `(1333.3 - 1000) / 1000 * 1500` rounds to 500 ms.
    the_window_widens_with_elapsed_time = {
        1000,
        &[Step::Record { at: 0, bytes: 2000 }, Step::Throttle { at: 1500 }],
        &[Err(500)],
    },
    // At 2 s the rate is exactly the bound, which is still acceptable.
    a_rate_at_the_bound_is_allowed = {
        1000,
        &[Step::Record { at: 0, bytes: 2000 }, Step::Throttle { at: 2000 }],
        &[Ok(())],
    },
    // Eleven one-second samples: a sample 11 s old is purged.
    a_sample_older_than_the_whole_window_is_purged = {
        1,
        &[Step::Record { at: 0, bytes: 1_000_000 }, Step::Throttle { at: 10_999 }, Step::Throttle { at: 11_000 }],
        &[Err(999_989_001), Ok(())],
    },
    // Samples in later windows add up, measured from the oldest one.
    later_samples_add_to_the_rate = {
        1000,
        &[
            Step::Record { at: 0, bytes: 1000 },
            Step::Record { at: 1000, bytes: 1000 },
            Step::Record { at: 2000, bytes: 2000 },
            Step::Throttle { at: 2000 },
        ],
        &[Err(2000)],
    },
    // A new bound applies to the samples already recorded.
    raising_the_bound_releases_the_recorded_bytes = {
        1000,
        &[
            Step::Record { at: 0, bytes: 2000 },
            Step::Throttle { at: 0 },
            Step::Update { bytes_per_second: 4000 },
            Step::Throttle { at: 0 },
        ],
        &[Err(1000), Ok(())],
    },
    lowering_the_bound_throttles_the_recorded_bytes = {
        i64::MAX,
        &[
            Step::Record { at: 0, bytes: 2000 },
            Step::Throttle { at: 0 },
            Step::Update { bytes_per_second: 500 },
            Step::Throttle { at: 0 },
        ],
        &[Ok(()), Err(3000)],
    },
    // The copy path's check-and-record: the first segment goes ahead and is
    // counted, so the second one waits.
    acquire_records_only_what_it_admits = {
        1000,
        &[Step::Acquire { at: 0, bytes: 3000 }, Step::Acquire { at: 0, bytes: 1 }, Step::Throttle { at: 0 }],
        &[Ok(()), Err(2000), Err(2000)],
    },
)]
fn the_quota_follows_kafkas_simple_rate(bound: i64, steps: &[Step], expected: &[Result<(), i64>]) {
    check!(run_steps(bound, steps) == expected);
}

#[test]
fn an_unlimited_quota_never_throttles() {
    let quota = RlmQuotaManager::unlimited();
    quota.record_at(u64::MAX, 0);
    check!(quota.throttle_time_ms_at(0) == 0);
    check!(quota.bytes_per_second() == i64::MAX);
}
