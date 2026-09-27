//! Pure local-trim decision for the diskless WAL flusher.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Whether and where to advance one diskless partition's local log start.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct DisklessTrimDecision {
    pub should_trim: bool,
    pub target: i64,
}

/// One decoded-batch step in a diskless cold-read run.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum DisklessBatchStep {
    Invalid,
    Skip(usize),
    Start(usize),
    Continue(usize),
    Stop,
}

/// One diskless partition's retention configuration and floor, read at `now_ms`.
#[cfg_attr(creusot, derive(Clone, Copy))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct DisklessRetentionPolicy {
    /// `retention.ms`, or `None` for Kafka's unlimited sentinel.
    pub retention_ms: Option<i64>,
    /// `retention.bytes`, or `None` for Kafka's unlimited sentinel.
    pub retention_bytes: Option<u64>,
    /// The `DeleteRecords` floor.
    pub log_start_offset: i64,
    pub now_ms: i64,
}

/// The bytes of the oldest `count` ranges.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[variant(count)]
pub fn indexed_bytes(byte_lens: Seq<u64>, count: Int) -> Int {
    pearlite! {
        if count <= 0 {
            0
        } else {
            indexed_bytes(byte_lens, count - 1) + byte_lens[count - 1]@
        }
    }
}

/// Kafka's `diff` when the size walk reaches range `i`: the index's bytes
/// over `retention.bytes`, less the ranges already expired before `i`. It is
/// negative when the index fits the budget.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn size_debt_before(byte_lens: Seq<u64>, budget: u64, i: Int) -> Int {
    pearlite! {
        indexed_bytes(byte_lens, byte_lens.len()) - budget@ - indexed_bytes(byte_lens, i)
    }
}

/// Whether Kafka's `UnifiedLog.deleteOldSegments` lets range `i` expire,
/// given that every older range expires first.
///
/// - `deleteLogStartOffsetBreachedSegments`: the range ends below the
///   `DeleteRecords` floor.
/// - `deleteRetentionSizeBreachedSegments`: `diff - segmentSize >= 0`, so
///   `retention.bytes` never deletes past its own budget.
/// - `deleteRetentionMsBreachedSegments`: `now - largestTimestamp >
///   retention.ms`, with a horizon `now - retention.ms` that `i64` cannot
///   represent expiring nothing.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn expirable(
    max_timestamps: Seq<i64>,
    byte_lens: Seq<u64>,
    last_offsets: Seq<i64>,
    policy: DisklessRetentionPolicy,
    i: Int,
) -> bool {
    pearlite! {
        last_offsets[i]@ < policy.log_start_offset@
            || match policy.retention_bytes {
                Some(budget) => size_debt_before(byte_lens, budget, i) - byte_lens[i]@ >= 0,
                None => false,
            }
            || match policy.retention_ms {
                Some(retention) => i64::MIN@ <= policy.now_ms@ - retention@
                    && policy.now_ms@ - retention@ <= i64::MAX@
                    && max_timestamps[i]@ < policy.now_ms@ - retention@,
                None => false,
            }
    }
}

/// Select the oldest contiguous prefix of one diskless partition's committed
/// WAL index ranges that retention allows to expire.
///
/// Kafka runs three walks oldest first, each stopping at the first segment it
/// must keep: the log-start-offset walk, then the size walk over what is left,
/// then the time walk over what is left after that. The first two predicates
/// only ever hold on a prefix (the size one because sizes are nonnegative, the
/// floor one because the host supplies ranges in offset order), so the three
/// walks together expire exactly the longest prefix on which one of the three
/// holds at every range. That prefix is what this returns, capped one short of
/// the newest range; see `expirable`.
///
/// The ranges arrive oldest first, one entry per index range, and the three
/// slices are parallel.
///
/// The newest range never expires. Kafka keeps the active segment for the same
/// reason, and here it is also what keeps the flusher's `flushed_frontier`
/// pointing past the last flushed offset: an empty index would send the next
/// tick back to the local log start and re-upload a prefix the bucket holds.
#[requires(max_timestamps@.len() == byte_lens@.len())]
#[requires(max_timestamps@.len() == last_offsets@.len())]
#[ensures(result@ <= max_timestamps@.len())]
#[ensures(max_timestamps@.len() > 0 ==> result@ < max_timestamps@.len())]
#[ensures(forall<i: Int> 0 <= i && i < result@ ==>
    expirable(max_timestamps@, byte_lens@, last_offsets@, policy, i))]
#[ensures(result@ + 1 < max_timestamps@.len() ==>
    !expirable(max_timestamps@, byte_lens@, last_offsets@, policy, result@))]
#[must_use]
pub fn diskless_retention_prefix(
    max_timestamps: &[i64],
    byte_lens: &[u64],
    last_offsets: &[i64],
    policy: DisklessRetentionPolicy,
) -> usize {
    if matches!(max_timestamps.len(), 0) {
        return 0;
    }
    let max_expire = max_timestamps.len() - 1;
    let horizon = match policy.retention_ms {
        Some(retention) => policy.now_ms.checked_sub(retention),
        None => None,
    };
    // Kafka's `diff`, `None` once it is negative. A `u128` holds the sum of
    // any slice of `u64` lengths without overflow.
    let mut debt: Option<u128> = match policy.retention_bytes {
        Some(budget) => {
            let mut indexed = 0u128;
            let mut scanned = 0usize;
            #[invariant(scanned@ <= byte_lens@.len())]
            #[invariant(indexed@ == indexed_bytes(byte_lens@, scanned@))]
            #[invariant(indexed@ <= scanned@ * u64::MAX@)]
            #[variant(byte_lens@.len() - scanned@)]
            while scanned < byte_lens.len() {
                proof_assert!(indexed_bytes(byte_lens@, scanned@ + 1)
                    == indexed_bytes(byte_lens@, scanned@) + byte_lens@[scanned@]@);
                indexed += u128::from(byte_lens[scanned]);
                scanned += 1;
            }
            indexed.checked_sub(u128::from(budget))
        }
        None => None,
    };

    let mut len = 0usize;
    #[invariant(len@ <= max_expire@)]
    #[invariant(match policy.retention_bytes {
        Some(budget) => match debt {
            Some(debt) => debt@ == size_debt_before(byte_lens@, budget, len@),
            None => size_debt_before(byte_lens@, budget, len@) < 0,
        },
        None => debt == None,
    })]
    #[invariant(forall<i: Int> 0 <= i && i < len@ ==>
        expirable(max_timestamps@, byte_lens@, last_offsets@, policy, i))]
    #[variant(max_expire@ - len@)]
    while len < max_expire {
        proof_assert!(indexed_bytes(byte_lens@, len@ + 1)
            == indexed_bytes(byte_lens@, len@) + byte_lens@[len@]@);
        let below_floor = last_offsets[len] < policy.log_start_offset;
        let aged_out = match horizon {
            Some(horizon) => max_timestamps[len] < horizon,
            None => false,
        };
        let size = u128::from(byte_lens[len]);
        let over_budget = match debt {
            Some(debt) => debt >= size,
            None => false,
        };
        if !below_floor && !aged_out && !over_budget {
            break;
        }
        debt = match debt {
            Some(debt) => debt.checked_sub(size),
            None => None,
        };
        len += 1;
    }
    len
}

/// Permit object deletion only after the grace period and with no index
/// reference in the projection protected by the caller's cache lock.
#[ensures(result == (!referenced && grace_elapsed))]
#[must_use]
pub const fn diskless_object_reclaimable(referenced: bool, grace_elapsed: bool) -> bool {
    !referenced && grace_elapsed
}

/// Select the covering logical range, or the first successor after a gap.
#[requires(forall<i: Int> 0 <= i && i < entries@.len()
    ==> entries@[i].0@ <= entries@[i].1@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
    ==> entries@[i].1@ < entries@[j].0@)]
#[ensures(match result {
    Some(index) => index@ < entries@.len()
        && ((entries@[index@].0@ <= requested@ && requested@ <= entries@[index@].1@)
            || (requested@ < entries@[index@].0@
                && index@ > 0
                && entries@[index@ - 1].1@ < requested@)),
    None => entries@.len() == 0
        || requested@ < entries@[0].0@
        || entries@[entries@.len() - 1].1@ < requested@,
})]
#[must_use]
pub fn diskless_logical_range(entries: &[(i64, i64)], requested: i64) -> Option<usize> {
    let mut lo = 0usize;
    let mut hi = entries.len();
    #[invariant(lo@ <= hi@ && hi@ <= entries@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < lo@ ==> entries@[i].0@ <= requested@)]
    #[invariant(forall<i: Int> hi@ <= i && i < entries@.len()
        ==> requested@ < entries@[i].0@)]
    #[variant(hi@ - lo@)]
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if entries[mid].0 <= requested {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == 0 {
        return None;
    }
    let floor = lo - 1;
    if requested <= entries[floor].1 {
        Some(floor)
    } else if lo < entries.len() {
        Some(lo)
    } else {
        None
    }
}

/// Extend an object byte span only across a contiguous whole indexed range.
///
/// The span extends exactly when the next range sits in the same object,
/// starts where the span ends, and the grown span stays within `max_bytes`.
#[ensures(match result {
    Some(total) => same_object
        && current_start@ + current_len@ == next_start@
        && total@ == current_len@ + next_len@
        && total@ <= max_bytes@,
    None => !same_object
        || current_start@ + current_len@ != next_start@
        || current_len@ + next_len@ > max_bytes@,
})]
#[must_use]
pub fn diskless_span_extension(
    current_start: u64,
    current_len: u64,
    next_start: u64,
    next_len: u64,
    same_object: bool,
    max_bytes: u64,
) -> Option<u64> {
    if !same_object || current_start.checked_add(current_len) != Some(next_start) {
        return None;
    }
    let total = current_len.checked_add(next_len)?;
    (total <= max_bytes).then_some(total)
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn batch_step_valid(
    selected_start: Option<usize>,
    batch_start: usize,
    encoded_len: usize,
    base_offset: i64,
    last_offset_delta: i32,
) -> bool {
    pearlite! {
        encoded_len@ > 0
            && last_offset_delta@ >= 0
            && batch_start@ + encoded_len@ <= usize::MAX@
            && base_offset@ + last_offset_delta@ <= i64::MAX@
            && match selected_start {
                Some(start) => start@ <= batch_start@,
                None => true,
            }
    }
}

/// Classify one decoded batch without splitting it or overflowing coordinates.
///
/// A batch is `Invalid` exactly when it is empty, has a negative last offset
/// delta, ends past `usize` or `i64`, or starts before the selected run. Every
/// valid batch lands in exactly one of the other four steps, so each valid
/// input advances the read by the batch's encoded length or stops it.
#[ensures((result == DisklessBatchStep::Invalid) == !batch_step_valid(
    selected_start,
    batch_start,
    encoded_len,
    base_offset,
    last_offset_delta,
))]
#[ensures(match result {
    DisklessBatchStep::Skip(next) => selected_start == None
        && next@ == batch_start@ + encoded_len@
        && base_offset@ + last_offset_delta@ < floor@,
    DisklessBatchStep::Start(next) => selected_start == None
        && next@ == batch_start@ + encoded_len@
        && floor@ <= base_offset@ + last_offset_delta@,
    DisklessBatchStep::Continue(next) => match selected_start {
        Some(start) => start@ <= batch_start@
            && next@ == batch_start@ + encoded_len@
            && next@ - start@ <= max_bytes@,
        None => false,
    },
    DisklessBatchStep::Stop => match selected_start {
        Some(start) => start@ <= batch_start@
            && batch_start@ + encoded_len@ - start@ > max_bytes@,
        None => false,
    },
    // Pinned by the `Invalid` iff above.
    DisklessBatchStep::Invalid => true,
})]
#[must_use]
pub fn diskless_batch_step(
    selected_start: Option<usize>,
    batch_start: usize,
    encoded_len: usize,
    base_offset: i64,
    last_offset_delta: i32,
    floor: i64,
    max_bytes: usize,
) -> DisklessBatchStep {
    if encoded_len == 0 || last_offset_delta < 0 {
        return DisklessBatchStep::Invalid;
    }
    let Some(next) = batch_start.checked_add(encoded_len) else {
        return DisklessBatchStep::Invalid;
    };
    let Some(last_offset) = base_offset.checked_add(i64::from(last_offset_delta)) else {
        return DisklessBatchStep::Invalid;
    };
    if let Some(start) = selected_start {
        if start > batch_start {
            return DisklessBatchStep::Invalid;
        }
        if next - start > max_bytes {
            DisklessBatchStep::Stop
        } else {
            DisklessBatchStep::Continue(next)
        }
    } else if last_offset < floor {
        DisklessBatchStep::Skip(next)
    } else {
        DisklessBatchStep::Start(next)
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn effective_trim_lag(safety_lag: i64) -> Int {
    pearlite! { if safety_lag@ < 0 { 0 } else { safety_lag@ } }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn trim_target(frontier: i64, high_watermark: i64, safety_lag: i64) -> Int {
    pearlite! {
        let high_watermark_floor = high_watermark@ - effective_trim_lag(safety_lag);
        if frontier@ < high_watermark_floor { frontier@ } else { high_watermark_floor }
    }
}

/// Plan a local trim behind both the committed object-store frontier and the
/// high watermark's configured safety lag.
///
/// Negative offsets and a lag larger than the high watermark fail closed. A
/// negative lag retains the caller's previous behavior and is treated as zero.
#[ensures(result.should_trim == (
    frontier@ >= 0
        && high_watermark@ >= 0
        && current_start@ >= 0
        && effective_trim_lag(safety_lag) <= high_watermark@
        && current_start@ < trim_target(frontier, high_watermark, safety_lag)
))]
#[ensures(result.target@ == if result.should_trim {
    trim_target(frontier, high_watermark, safety_lag)
} else {
    current_start@
})]
#[ensures(result.target@ >= current_start@)]
#[ensures(result.should_trim ==> result.target@ <= frontier@)]
#[ensures(result.should_trim ==>
    result.target@ + effective_trim_lag(safety_lag) <= high_watermark@)]
#[must_use]
pub fn diskless_trim_decision(
    frontier: i64,
    high_watermark: i64,
    safety_lag: i64,
    current_start: i64,
) -> DisklessTrimDecision {
    if frontier < 0 || high_watermark < 0 || current_start < 0 {
        return DisklessTrimDecision {
            should_trim: false,
            target: current_start,
        };
    }

    let safety_lag = safety_lag.max(0);
    if safety_lag > high_watermark {
        return DisklessTrimDecision {
            should_trim: false,
            target: current_start,
        };
    }

    let target = frontier.min(high_watermark - safety_lag);
    if target <= current_start {
        DisklessTrimDecision {
            should_trim: false,
            target: current_start,
        }
    } else {
        DisklessTrimDecision {
            should_trim: true,
            target,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn reclaim_needs_the_grace_period_and_no_reference() {
        check!(diskless_object_reclaimable(false, true));
        check!(!diskless_object_reclaimable(true, true));
        check!(!diskless_object_reclaimable(false, false));
    }

    #[test]
    fn trim_is_bounded_non_regressing_and_overflow_safe() {
        for (frontier, high_watermark, lag, current, expected) in [
            (90, 100, 10, 50, (true, 90)),
            (100, 90, 10, 50, (true, 80)),
            (80, 90, 10, 80, (false, 80)),
            (70, 90, 10, 80, (false, 80)),
            (i64::MAX, i64::MAX, 0, i64::MAX - 1, (true, i64::MAX)),
            (i64::MAX, 0, i64::MAX, 0, (false, 0)),
            (10, 10, -1, 0, (true, 10)),
            (-1, 10, 0, 0, (false, 0)),
            (10, -1, 0, 0, (false, 0)),
            (10, 10, 0, -1, (false, -1)),
            (0, 0, 0, 0, (false, 0)),
        ] {
            let decision = diskless_trim_decision(frontier, high_watermark, lag, current);
            check!(
                decision
                    == DisklessTrimDecision {
                        should_trim: expected.0,
                        target: expected.1,
                    }
            );
        }
    }

    fn policy(
        retention_ms: Option<i64>,
        retention_bytes: Option<u64>,
        log_start_offset: i64,
        now_ms: i64,
    ) -> DisklessRetentionPolicy {
        DisklessRetentionPolicy {
            retention_ms,
            retention_bytes,
            log_start_offset,
            now_ms,
        }
    }

    /// Every case runs over the same three ranges: 100 bytes each, covering
    /// offsets 0-4, 5-9 and 10-14, read at `now_ms = 1_000`. Only the batch
    /// timestamps and the topic's retention change, which is what each Kafka
    /// predicate keys on.
    #[test]
    fn retention_prefix_applies_each_kafka_predicate_and_keeps_the_newest_range() {
        const BYTE_LENS: [u64; 3] = [100, 100, 100];
        const LAST_OFFSETS: [i64; 3] = [4, 9, 14];
        const NOW_MS: i64 = 1_000;

        // `(what, batch max timestamps, retention.ms, retention.bytes,
        // DeleteRecords floor, expired prefix)`.
        for (what, max_timestamps, retention_ms, retention_bytes, floor, expired) in [
            (
                "nothing configured expires nothing",
                [10, 20, 30],
                None,
                None,
                0,
                0,
            ),
            (
                "time leaves what is newer than now - 500",
                [100, 200, 900],
                Some(500),
                None,
                0,
                2,
            ),
            (
                "time past every range still keeps the newest",
                [100, 200, 300],
                Some(500),
                None,
                0,
                2,
            ),
            (
                "time stops at the first range it must keep",
                [100, 900, 100],
                Some(500),
                None,
                0,
                1,
            ),
            // Kafka's `diff` is 150: the first range leaves 50, which the
            // second cannot cover.
            (
                "bytes expires the oldest range only",
                [10, 20, 30],
                None,
                Some(150),
                0,
                1,
            ),
            // `diff` is 100, and `100 - 100 >= 0` is Kafka's delete rule.
            (
                "bytes expires a range that pays the debt off exactly",
                [10, 20, 30],
                None,
                Some(200),
                0,
                1,
            ),
            // `diff` is 50, and no range fits inside it.
            (
                "bytes never deletes past its own budget",
                [10, 20, 30],
                None,
                Some(250),
                0,
                0,
            ),
            (
                "a budget the index already fits expires nothing",
                [10, 20, 30],
                None,
                Some(300),
                0,
                0,
            ),
            (
                "the floor expires every range that ends below it",
                [10, 20, 30],
                None,
                None,
                5,
                1,
            ),
            (
                "a floor past every range still keeps the newest",
                [10, 20, 30],
                None,
                None,
                99,
                2,
            ),
            // The floor clears the first range, time the second, and neither
            // reaches the third.
            (
                "the predicates union",
                [100, 100, 900],
                Some(500),
                None,
                5,
                2,
            ),
            // Time clears the first range, which pays `diff` 150 down to 50;
            // the second range needs 100.
            (
                "a range time expires still pays the size debt",
                [100, 900, 900],
                Some(500),
                Some(150),
                0,
                1,
            ),
        ] {
            let prefix = diskless_retention_prefix(
                &max_timestamps,
                &BYTE_LENS,
                &LAST_OFFSETS,
                policy(retention_ms, retention_bytes, floor, NOW_MS),
            );
            check!(prefix == expired, "{what}");
        }
    }

    #[test]
    fn retention_prefix_boundaries_follow_kafkas_strict_and_inclusive_comparisons() {
        // `(what, max timestamps, byte lens, last offsets, policy, expired)`.
        for (what, max_timestamps, byte_lens, last_offsets, policy, expired) in [
            (
                "a floor equal to the last offset keeps the range",
                &[100, 200][..],
                &[10, 10][..],
                &[10, 20][..],
                policy(None, None, 10, 1_000),
                0,
            ),
            (
                "a max timestamp equal to the horizon keeps the range",
                &[500, 900][..],
                &[10, 10][..],
                &[10, 20][..],
                policy(Some(500), None, 0, 1_000),
                0,
            ),
            // `diff` is 0 and `0 - 0 >= 0`.
            (
                "a zero diff still expires a zero-byte range",
                &[100, 200][..],
                &[0, 10][..],
                &[10, 20][..],
                policy(None, Some(10), 0, 1_000),
                1,
            ),
            (
                "a horizon below i64::MIN expires nothing",
                &[10, 20, 30][..],
                &[100, 100, 100][..],
                &[4, 9, 14][..],
                policy(Some(1), None, 0, i64::MIN),
                0,
            ),
            (
                "a horizon above i64::MAX expires nothing",
                &[10, 20, 30][..],
                &[100, 100, 100][..],
                &[4, 9, 14][..],
                policy(Some(-1), None, 0, i64::MAX),
                0,
            ),
            (
                "u64::MAX ranges do not overflow the size sum",
                &[10, 20, 30][..],
                &[u64::MAX, u64::MAX, u64::MAX][..],
                &[4, 9, 14][..],
                policy(None, Some(u64::MAX), 0, 1_000),
                2,
            ),
            (
                "one range is the newest range, whatever retention says",
                &[10][..],
                &[100][..],
                &[4][..],
                policy(Some(1), Some(0), 99, 1_000),
                0,
            ),
            (
                "an empty index expires nothing",
                &[][..],
                &[][..],
                &[][..],
                policy(Some(1), Some(0), 99, 1_000),
                0,
            ),
        ] {
            check!(
                diskless_retention_prefix(max_timestamps, byte_lens, last_offsets, policy)
                    == expired,
                "{what}"
            );
        }
    }

    #[test]
    fn logical_range_selects_the_cover_or_the_successor_after_a_gap() {
        let entries = [(0, 4), (7, 9), (12, 15)];
        for (requested, expected) in [
            (-1, None),
            (0, Some(0)),
            (5, Some(1)),
            (15, Some(2)),
            (16, None),
        ] {
            check!(diskless_logical_range(&entries, requested) == expected);
        }
    }

    #[test]
    fn span_extends_only_across_a_contiguous_range_of_the_same_object() {
        // `(what, current start, current len, next start, next len, same
        // object, max bytes, extended span)`.
        for (what, start, len, next_start, next_len, same_object, max_bytes, expected) in [
            (
                "contiguous and within the cap",
                10,
                5,
                15,
                7,
                true,
                12,
                Some(12),
            ),
            ("contiguous and over the cap", 10, 5, 15, 7, true, 11, None),
            ("a gap", 10, 5, 16, 7, true, 12, None),
            ("another object", 10, 5, 15, 7, false, 12, None),
            ("an end past u64::MAX", u64::MAX, 1, 0, 1, true, 2, None),
            (
                "a total past u64::MAX",
                0,
                u64::MAX,
                u64::MAX,
                1,
                true,
                u64::MAX,
                None,
            ),
        ] {
            check!(
                diskless_span_extension(start, len, next_start, next_len, same_object, max_bytes)
                    == expected,
                "{what}"
            );
        }
    }

    #[test]
    fn batch_steps_advance_by_the_encoded_length_or_stop() {
        use DisklessBatchStep::{Continue, Invalid, Skip, Start, Stop};

        // `(what, selected start, batch start, encoded len, base offset,
        // last offset delta, floor, max bytes, step)`.
        for (what, selected, batch_start, encoded_len, base, delta, floor, max_bytes, expected) in [
            ("below the floor", None, 0, 10, 0, 0, 1, 5, Skip(10)),
            ("at the floor", None, 10, 10, 1, 0, 1, 5, Start(20)),
            (
                "within the cap",
                Some(10),
                20,
                10,
                2,
                0,
                1,
                20,
                Continue(30),
            ),
            (
                "the first batch",
                Some(20),
                20,
                10,
                2,
                0,
                1,
                20,
                Continue(30),
            ),
            ("over the cap", Some(10), 20, 10, 2, 0, 1, 19, Stop),
            ("empty", None, 0, 0, 0, 0, 1, 5, Invalid),
            ("a negative delta", None, 0, 10, 0, -1, 1, 5, Invalid),
            (
                "an end past usize::MAX",
                None,
                usize::MAX,
                1,
                0,
                0,
                0,
                usize::MAX,
                Invalid,
            ),
            (
                "a last offset past i64::MAX",
                None,
                0,
                1,
                i64::MAX,
                1,
                0,
                usize::MAX,
                Invalid,
            ),
            (
                "a run that starts later",
                Some(21),
                20,
                10,
                2,
                0,
                1,
                20,
                Invalid,
            ),
        ] {
            check!(
                diskless_batch_step(
                    selected,
                    batch_start,
                    encoded_len,
                    base,
                    delta,
                    floor,
                    max_bytes
                ) == expected,
                "{what}"
            );
        }
    }
}
