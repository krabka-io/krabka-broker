use super::*;
#[cfg(test)]
use std::vec;

const THREE_TIMESTAMPS: &[i64] = &[10, 20, 30];
const THREE_BYTE_LENS: &[u64] = &[100, 100, 100];
const THREE_LAST_OFFSETS: &[i64] = &[4, 9, 14];

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

#[cfg(test)]
#[rstest_bdd_macros::then(
    "diskless tests reclaim needs the grace period and no reference: retention prefix applies each kafka predicate and keeps the newest range for {example}"
)]
fn bdd_verify_retention_prefix_applies_each_kafka_predicate_and_keeps_the_newest_range(
    example: String,
) {
    type Example = ([i64; 3], Option<i64>, Option<u64>, i64, usize);
    let (max_timestamps, retention_ms, retention_bytes, floor, expired): Example =
        match example.as_str() {
            "no policy" => ([10, 20, 30], None, None, 0, 0),
            "time keeps newest range" => ([100, 200, 900], Some(500), None, 0, 2),
            "time past every range" => ([100, 200, 300], Some(500), None, 0, 2),
            "time stops before first kept" => ([100, 900, 100], Some(500), None, 0, 1),
            "bytes oldest only" => ([10, 20, 30], None, Some(150), 0, 1),
            "bytes exact debt" => ([10, 20, 30], None, Some(200), 0, 1),
            "bytes budget too small" => ([10, 20, 30], None, Some(250), 0, 0),
            "index already fits budget" => ([10, 20, 30], None, Some(300), 0, 0),
            "delete floor" => ([10, 20, 30], None, None, 5, 1),
            "floor past every range" => ([10, 20, 30], None, None, 99, 2),
            "predicates union" => ([100, 100, 900], Some(500), None, 5, 2),
            "expired range still pays size debt" => ([100, 900, 900], Some(500), Some(150), 0, 1),
            _ => panic!("unknown BDD example: {example}"),
        };

    const BYTE_LENS: [u64; 3] = [100, 100, 100];
    const LAST_OFFSETS: [i64; 3] = [4, 9, 14];
    const NOW_MS: i64 = 1_000;

    let prefix = diskless_retention_prefix(
        &max_timestamps,
        &BYTE_LENS,
        &LAST_OFFSETS,
        policy()
            .maybe_retention(retention_ms.map(RetentionMillis))
            .maybe_bytes(retention_bytes.map(RetentionBytes))
            .start(floor)
            .now(NOW_MS)
            .call(),
    );
    check!(prefix == expired);
}

/// Every case runs over the same three ranges: 100 bytes each, covering
/// offsets 0-4, 5-9 and 10-14, read at `now_ms = 1_000`. Only the batch
/// timestamps and the topic's retention change, which is what each Kafka
/// predicate keys on.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/diskless/tests/reclaim_needs_the_grace_period_and_no_reference.feature",
    name = "retention prefix applies each kafka predicate and keeps the newest range"
)]
fn retention_prefix_applies_each_kafka_predicate_and_keeps_the_newest_range(example: String) {
    let _ = example;
}

#[cfg(test)]
#[rstest_bdd_macros::then(
    "diskless tests reclaim needs the grace period and no reference: retention prefix boundaries follow kafkas strict and inclusive comparisons for {example}"
)]
fn bdd_verify_retention_prefix_boundaries_follow_kafkas_strict_and_inclusive_comparisons(
    example: String,
) {
    type Example<'a> = (
        &'a [i64],
        &'a [u64],
        &'a [i64],
        DisklessRetentionPolicy,
        usize,
    );
    let (max_timestamps, byte_lens, last_offsets, policy, expired): Example<'_> =
        match example.as_str() {
            "floor equal to last offset" => (
                &[100, 200][..],
                &[10, 10][..],
                &[10, 20][..],
                policy().start(10).call(),
                0,
            ),
            "timestamp equal to horizon" => (
                &[500, 900][..],
                &[10, 10][..],
                &[10, 20][..],
                policy().maybe_retention(Some(500)).call(),
                0,
            ),
            "zero diff zero byte range" => (
                &[100, 200][..],
                &[0, 10][..],
                &[10, 20][..],
                policy().maybe_bytes(Some(10)).call(),
                1,
            ),
            "horizon below min" => (
                THREE_TIMESTAMPS,
                THREE_BYTE_LENS,
                THREE_LAST_OFFSETS,
                policy().maybe_retention(Some(1)).now(i64::MIN).call(),
                0,
            ),
            "horizon above max" => (
                THREE_TIMESTAMPS,
                THREE_BYTE_LENS,
                THREE_LAST_OFFSETS,
                policy().maybe_retention(Some(-1)).now(i64::MAX).call(),
                0,
            ),
            "size sum does not overflow" => (
                THREE_TIMESTAMPS,
                &[u64::MAX, u64::MAX, u64::MAX][..],
                THREE_LAST_OFFSETS,
                policy().maybe_bytes(Some(u64::MAX)).call(),
                2,
            ),
            "newest range is preserved" => (
                &[10][..],
                &[100][..],
                &[4][..],
                policy()
                    .maybe_retention(Some(1))
                    .maybe_bytes(Some(0))
                    .start(99)
                    .call(),
                0,
            ),
            "empty index" => (
                &[][..],
                &[][..],
                &[][..],
                policy()
                    .maybe_retention(Some(1))
                    .maybe_bytes(Some(0))
                    .start(99)
                    .call(),
                0,
            ),
            _ => panic!("unknown BDD example: {example}"),
        };

    check!(diskless_retention_prefix(max_timestamps, byte_lens, last_offsets, policy) == expired);
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/diskless/tests/reclaim_needs_the_grace_period_and_no_reference.feature",
    name = "retention prefix boundaries follow kafkas strict and inclusive comparisons"
)]
fn retention_prefix_boundaries_follow_kafkas_strict_and_inclusive_comparisons(example: String) {
    let _ = example;
}
