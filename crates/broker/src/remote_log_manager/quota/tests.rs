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

#[cfg(test)]
#[rstest_bdd_macros::then(
    "remote log manager quota tests: the quota follows kafkas simple rate for {example}"
)]
fn bdd_verify_the_quota_follows_kafkas_simple_rate(example: String) {
    type Example<'a> = (i64, &'a [Step], &'a [Result<(), i64>]);
    let (bound, steps, expected): Example<'_> = match example.as_str() {
        "an idle quota allows" => (1000, &[Step::Throttle { at: 0 }], &[Ok(())]),
        "a burst over the bound throttles for the excess" => (
            1000,
            &[Step::Record { at: 0, bytes: 2000 }, Step::Throttle {
                at: 0,
            }],
            &[Err(1000)],
        ),
        "the window widens with elapsed time" => (
            1000,
            &[Step::Record { at: 0, bytes: 2000 }, Step::Throttle {
                at: 1500,
            }],
            &[Err(500)],
        ),
        "a rate at the bound is allowed" => (
            1000,
            &[Step::Record { at: 0, bytes: 2000 }, Step::Throttle {
                at: 2000,
            }],
            &[Ok(())],
        ),
        "a sample older than the whole window is purged" => (
            1,
            &[
                Step::Record {
                    at: 0,
                    bytes: 1_000_000,
                },
                Step::Throttle { at: 10_999 },
                Step::Throttle { at: 11_000 },
            ],
            &[Err(999_989_001), Ok(())],
        ),
        "later samples add to the rate" => (
            1000,
            &[
                Step::Record { at: 0, bytes: 1000 },
                Step::Record {
                    at: 1000,
                    bytes: 1000,
                },
                Step::Record {
                    at: 2000,
                    bytes: 2000,
                },
                Step::Throttle { at: 2000 },
            ],
            &[Err(2000)],
        ),
        "raising the bound releases the recorded bytes" => (
            1000,
            &[
                Step::Record { at: 0, bytes: 2000 },
                Step::Throttle { at: 0 },
                Step::Update {
                    bytes_per_second: 4000,
                },
                Step::Throttle { at: 0 },
            ],
            &[Err(1000), Ok(())],
        ),
        "lowering the bound throttles the recorded bytes" => (
            i64::MAX,
            &[
                Step::Record { at: 0, bytes: 2000 },
                Step::Throttle { at: 0 },
                Step::Update {
                    bytes_per_second: 500,
                },
                Step::Throttle { at: 0 },
            ],
            &[Ok(()), Err(3000)],
        ),
        "acquire records only what it admits" => (
            1000,
            &[
                Step::Acquire { at: 0, bytes: 3000 },
                Step::Acquire { at: 0, bytes: 1 },
                Step::Throttle { at: 0 },
            ],
            &[Ok(()), Err(2000), Err(2000)],
        ),
        _ => panic!("unknown BDD example: {example}"),
    };

    check!(run_steps(bound, steps) == expected);
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/remote_log_manager/quota/tests.feature",
    name = "the quota follows kafkas simple rate"
)]
fn the_quota_follows_kafkas_simple_rate(example: String) { let _ = example; }

#[test]
fn an_unlimited_quota_never_throttles() {
    let quota = RlmQuotaManager::unlimited();
    quota.record_at(u64::MAX, 0);
    check!(quota.throttle_time_ms_at(0) == 0);
    check!(quota.bytes_per_second() == i64::MAX);
}
