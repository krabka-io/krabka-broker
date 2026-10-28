use {
    super::{FetchVisibility, FetchWatermarks, fetch_visibility},
    rstest::fixture,
    rstest_bdd_macros::{given, scenarios, then, when},
};

struct FetchScenario {
    watermarks: Option<FetchWatermarks>,
    visibility: Option<FetchVisibility>,
}

#[fixture]
fn fetch() -> FetchScenario {
    FetchScenario {
        watermarks: None,
        visibility: None,
    }
}

#[given(
    "fetch watermarks start {log_start} high {hw} stable {lso} end {log_end} deliverable {deliverable}"
)]
fn fetch_watermarks(
    fetch: &mut FetchScenario,
    log_start: i64,
    hw: i64,
    lso: i64,
    log_end: i64,
    deliverable: i64,
) {
    fetch.watermarks = Some(FetchWatermarks {
        log_start,
        hw,
        lso,
        log_end,
        deliverable,
    });
}

#[when("a follower {is_follower} committed {read_committed} fetches offset {fetch_offset}")]
fn fetch_offset(
    fetch: &mut FetchScenario,
    is_follower: bool,
    read_committed: bool,
    fetch_offset: i64,
) {
    fetch.visibility = Some(fetch_visibility(
        is_follower,
        read_committed,
        fetch.watermarks.expect("fetch watermarks were given"),
        fetch_offset,
    ));
}

#[then(
    "fetch bounds are out of range {out_of_range} empty {empty} limit {limit_offset} stable {effective_lso} aborts {read_committed_aborts}"
)]
fn fetch_bounds(
    fetch: &FetchScenario,
    out_of_range: bool,
    empty: bool,
    limit_offset: i64,
    effective_lso: i64,
    read_committed_aborts: bool,
) {
    let actual = fetch.visibility.expect("a fetch was evaluated");
    assert2::assert!(
        actual
            == FetchVisibility {
                out_of_range,
                empty,
                limit_offset,
                effective_lso,
                read_committed_aborts,
                ..actual
            }
    );
}

#[then("fetch reports high watermark {response_hw} and stable offset {response_lso}")]
fn fetch_reported_watermarks(fetch: &FetchScenario, response_hw: i64, response_lso: i64) {
    let actual = fetch.visibility.expect("a fetch was evaluated");
    assert2::assert!(
        actual
            == FetchVisibility {
                response_hw,
                response_lso,
                ..actual
            }
    );
}

scenarios!("tests/features/fetch_visibility", fixtures = [fetch: FetchScenario]);
