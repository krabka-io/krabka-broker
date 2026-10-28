use super::*;
#[cfg(test)]
use std::vec;

#[test]
fn reclaim_needs_the_grace_period_and_no_reference() {
    check!(diskless_object_reclaimable(false, true));
    check!(!diskless_object_reclaimable(true, true));
    check!(!diskless_object_reclaimable(false, false));
}

#[yare::parameterized(
    below_hwm_within_lag = { 90, 100, 10, 50, (true, 90) },
    hwm_below_frontier = { 100, 90, 10, 50, (true, 80) },
    frontier_equals_current = { 80, 90, 10, 80, (false, 80) },
    frontier_below_current = { 70, 90, 10, 80, (false, 80) },
    maximum_frontier = { i64::MAX, i64::MAX, 0, i64::MAX - 1, (true, i64::MAX) },
    saturated_subtraction = { i64::MAX, 0, i64::MAX, 0, (false, 0) },
    negative_lag = { 10, 10, -1, 0, (true, 10) },
    negative_frontier = { -1, 10, 0, 0, (false, 0) },
    negative_hwm = { 10, -1, 0, 0, (false, 0) },
    negative_current = { 10, 10, 0, -1, (false, -1) },
    all_zero = { 0, 0, 0, 0, (false, 0) },
)]
fn trim_is_bounded_non_regressing_and_overflow_safe(
    frontier: i64,
    high_watermark: i64,
    lag: i64,
    current: i64,
    expected: (bool, i64),
) {
    let decision = diskless_trim_decision(frontier, high_watermark, lag, current);
    check!(
        decision
            == DisklessTrimDecision {
                should_trim: expected.0,
                target: expected.1,
            }
    );
}

use {
    rstest::fixture,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
};

struct DisklessRetention {
    policy: Option<DisklessRetentionPolicy>,
    timestamps: Vec<i64>,
    byte_lengths: Vec<u64>,
    last_offsets: Vec<i64>,
    prefix: Option<usize>,
}

#[fixture]
fn retention() -> DisklessRetention {
    // Start this scenario with fresh inputs and observations.

    DisklessRetention {
        policy: None,
        timestamps: Vec::new(),
        byte_lengths: Vec::new(),
        last_offsets: Vec::new(),
        prefix: None,
    }
}

fn signed_millis(value: &str) -> i64 {
    match value {
        "minimum" => i64::MIN,
        "maximum" => i64::MAX,
        _ => value.parse().expect("a signed millisecond value"),
    }
}

#[given(
    "diskless retention age {age} ms, size {bytes} bytes, delete floor {floor}, and current time {now} ms"
)]
fn retention_policy(
    retention: &mut DisklessRetention,
    age: String,
    bytes: String,
    floor: i64,
    now: String,
) {
    let age = if age == "none" {
        None
    } else {
        Some(RetentionMillis(signed_millis(&age)))
    };
    let bytes = match bytes.as_str() {
        "none" => None,
        "maximum" => Some(RetentionBytes(u64::MAX)),
        _ => Some(RetentionBytes(
            bytes
                .parse()
                .expect("retention bytes is an unsigned integer"),
        )),
    };
    retention.policy = Some(
        policy()
            .maybe_retention(age)
            .maybe_bytes(bytes)
            .start(floor)
            .now(signed_millis(&now))
            .call(),
    );
}

#[derive(DataTableRow)]
struct LogicalRangeRow {
    max_timestamp_ms: i64,
    bytes: u64,
    last_offset: i64,
}

#[given("the diskless index contains these ranges oldest first:")]
fn logical_ranges(retention: &mut DisklessRetention, #[datatable] rows: Rows<LogicalRangeRow>) {
    for row in rows {
        retention.timestamps.push(row.max_timestamp_ms);
        retention.byte_lengths.push(row.bytes);
        retention.last_offsets.push(row.last_offset);
    }
}

#[when("Kafka diskless retention evaluates the removable prefix")]
fn removable_ranges(retention: &mut DisklessRetention) {
    retention.prefix = Some(diskless_retention_prefix(
        &retention.timestamps,
        &retention.byte_lengths,
        &retention.last_offsets,
        retention
            .policy
            .expect("a diskless retention policy was given"),
    ));
}

#[then("the oldest {count} diskless ranges are removable")]
fn expired_prefix(retention: &DisklessRetention, count: usize) {
    check!(retention.prefix == Some(count));
}

scenarios!("tests/features/migrated/src/diskless/tests/reclaim_needs_the_grace_period_and_no_reference.feature", fixtures = [retention: DisklessRetention]);
