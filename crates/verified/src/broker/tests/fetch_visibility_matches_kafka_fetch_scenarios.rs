use {
    super::{FetchReader, FetchVisibility, FetchWatermarks, Isolation, fetch_visibility},
    rstest::fixture,
    rstest_bdd_macros::{given, then, when},
};

struct FetchScenario {
    watermarks: Option<FetchWatermarks>,
    visibility: Option<FetchVisibility>,
}

#[fixture]
fn fetch() -> FetchScenario {
    // Start this scenario with fresh inputs and observations.

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

#[when("a {reader} using {isolation} fetches offset {fetch_offset}")]
fn fetch_offset(fetch: &mut FetchScenario, reader: String, isolation: String, fetch_offset: i64) {
    let isolation = match isolation.as_str() {
        "read-committed" => Isolation::ReadCommitted,
        "read-uncommitted" => Isolation::ReadUncommitted,
        _ => panic!("unknown fetch isolation: {isolation}"),
    };
    let reader = match reader.as_str() {
        "consumer" => FetchReader::Consumer(isolation),
        "follower" => FetchReader::Follower,
        _ => panic!("unknown fetch reader: {reader}"),
    };
    fetch.visibility = Some(fetch_visibility(
        reader,
        fetch.watermarks.expect("fetch watermarks were given"),
        fetch_offset,
    ));
}

#[then("the fetch offset is {range} and the computed window is {availability}")]
fn fetch_range(fetch: &FetchScenario, range: String, availability: String) {
    let out_of_range = match range.as_str() {
        "in-range" => false,
        "out-of-range" => true,
        _ => panic!("unknown fetch range: {range}"),
    };
    let empty = match availability.as_str() {
        "empty" => true,
        "nonempty" => false,
        _ => panic!("unknown fetch availability: {availability}"),
    };
    let actual = fetch.visibility.expect("a fetch was evaluated");
    assert2::assert!(
        actual
            == FetchVisibility {
                out_of_range,
                empty,
                ..actual
            }
    );
}

#[then(
    "its visible prefix ends at {limit_offset} with stable offset {effective_lso} and abort metadata {aborts}"
)]
fn fetch_bounds(fetch: &FetchScenario, limit_offset: i64, effective_lso: i64, aborts: String) {
    let read_committed_aborts = match aborts.as_str() {
        "included" => true,
        "omitted" => false,
        _ => panic!("unknown abort metadata policy: {aborts}"),
    };
    let actual = fetch.visibility.expect("a fetch was evaluated");
    assert2::assert!(
        actual
            == FetchVisibility {
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

krabka_bdd::scenarios!("tests/features/fetch_visibility", fixtures = [fetch: FetchScenario]);
