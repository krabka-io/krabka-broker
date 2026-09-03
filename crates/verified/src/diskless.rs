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

/// Mutation selected by one WAL-index replay event.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum DisklessWalReplayAction {
    Ignore,
    Store,
    Remove,
}

/// One WAL-index replay step and its dominance markers.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct DisklessWalReplayDecision {
    pub action: DisklessWalReplayAction,
    pub keyed_range: bool,
    pub replay_tombstone: bool,
}

/// Classify one WAL-index replay event.
///
/// Event tags are `0 = legacy value`, `1 = keyed value`, and
/// `2 = keyed tombstone`. Keyed values and tombstones dominate legacy values
/// while the cross-partition legacy replay is active. A keyed value may follow
/// a tombstone because equal Kafka keys share a partition and retain order.
#[ensures(result.action == if event@ == 1
    || (event@ == 0 && !current_keyed && !current_tombstone) {
    DisklessWalReplayAction::Store
} else if event@ == 2 {
    DisklessWalReplayAction::Remove
} else {
    DisklessWalReplayAction::Ignore
})]
#[ensures(result.keyed_range == if event@ == 1 {
    true
} else if event@ == 2 {
    false
} else {
    current_keyed
})]
#[ensures(result.replay_tombstone == if event@ == 1 {
    false
} else if event@ == 2 {
    !legacy_replay_finished
} else {
    current_tombstone
})]
#[must_use]
pub const fn diskless_wal_replay_decision(
    event: u8,
    current_keyed: bool,
    current_tombstone: bool,
    legacy_replay_finished: bool,
) -> DisklessWalReplayDecision {
    match event {
        0 => DisklessWalReplayDecision {
            action: if current_keyed || current_tombstone {
                DisklessWalReplayAction::Ignore
            } else {
                DisklessWalReplayAction::Store
            },
            keyed_range: current_keyed,
            replay_tombstone: current_tombstone,
        },
        1 => DisklessWalReplayDecision {
            action: DisklessWalReplayAction::Store,
            keyed_range: true,
            replay_tombstone: false,
        },
        2 => DisklessWalReplayDecision {
            action: DisklessWalReplayAction::Remove,
            keyed_range: false,
            replay_tombstone: !legacy_replay_finished,
        },
        _ => DisklessWalReplayDecision {
            action: DisklessWalReplayAction::Ignore,
            keyed_range: current_keyed,
            replay_tombstone: current_tombstone,
        },
    }
}

/// Select the oldest contiguous prefix of one diskless partition's committed
/// WAL index ranges that retention allows to expire.
///
/// The three predicates are Kafka's, from `UnifiedLog.deleteOldSegments`: a
/// range whose newest record is older than `retention.ms`, an oldest range the
/// `retention.bytes` budget cannot keep, and a range that ends below the
/// `DeleteRecords` floor. Kafka walks oldest first and stops at the first
/// segment it must keep, so this returns a prefix length rather than a set.
///
/// The ranges arrive oldest first, one entry per index range, and the three
/// slices are parallel. `retention_ms` and `retention_bytes` are `None` for
/// Kafka's unlimited sentinel, and a `now_ms - retention_ms` that cannot be
/// represented expires nothing by time.
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
    last_offsets@[i]@ < log_start_offset@
    || retention_bytes != None
    || match retention_ms {
        Some(retention) => now_ms@ - retention@ >= i64::MIN@
            && max_timestamps@[i]@ < now_ms@ - retention@,
        None => false,
    })]
#[must_use]
pub fn diskless_retention_prefix(
    max_timestamps: &[i64],
    byte_lens: &[u64],
    last_offsets: &[i64],
    retention_ms: Option<i64>,
    retention_bytes: Option<u64>,
    log_start_offset: i64,
    now_ms: i64,
) -> usize {
    if matches!(max_timestamps.len(), 0) {
        return 0;
    }
    let max_expire = max_timestamps.len() - 1;
    let horizon = match retention_ms {
        Some(retention) => now_ms.checked_sub(retention),
        None => None,
    };
    let mut indexed_bytes = 0u64;
    let mut scanned = 0usize;
    #[invariant(scanned@ <= byte_lens@.len())]
    #[variant(byte_lens@.len() - scanned@)]
    while scanned < byte_lens.len() {
        indexed_bytes = indexed_bytes.saturating_add(byte_lens[scanned]);
        scanned += 1;
    }
    let mut size_debt = match retention_bytes {
        Some(budget) => indexed_bytes.saturating_sub(budget),
        None => 0,
    };

    let mut len = 0usize;
    #[invariant(len@ <= max_expire@)]
    #[invariant(size_debt@ > 0 ==> retention_bytes != None)]
    #[invariant(forall<i: Int> 0 <= i && i < len@ ==>
        last_offsets@[i]@ < log_start_offset@
        || retention_bytes != None
        || match retention_ms {
            Some(retention) => now_ms@ - retention@ >= i64::MIN@
                && max_timestamps@[i]@ < now_ms@ - retention@,
            None => false,
        })]
    #[variant(max_expire@ - len@)]
    while len < max_expire {
        let below_floor = last_offsets[len] < log_start_offset;
        let aged_out = match horizon {
            Some(horizon) => max_timestamps[len] < horizon,
            None => false,
        };
        // Kafka subtracts the segment size only while the remainder stays
        // non-negative, so `retention.bytes` never deletes past its own
        // budget.
        let over_budget = size_debt > 0 && size_debt >= byte_lens[len];
        if !below_floor && !aged_out && !over_budget {
            break;
        }
        size_debt = size_debt.saturating_sub(byte_lens[len]);
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
#[ensures(match result {
    Some(total) => same_object
        && current_start@ + current_len@ == next_start@
        && total@ == current_len@ + next_len@
        && total@ <= max_bytes@,
    None => true,
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

/// Classify one decoded batch without splitting it or overflowing coordinates.
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
    fn wal_replay_dominance_and_reclaim_are_total() {
        use DisklessWalReplayAction::{Ignore, Remove, Store};

        for keyed in [false, true] {
            for tombstone in [false, true] {
                let legacy = diskless_wal_replay_decision(0, keyed, tombstone, false);
                check!(legacy.action == if keyed || tombstone { Ignore } else { Store });
                check!(legacy.keyed_range == keyed);
                check!(legacy.replay_tombstone == tombstone);

                let keyed_value = diskless_wal_replay_decision(1, keyed, tombstone, false);
                check!(keyed_value.action == Store);
                check!(keyed_value.keyed_range);
                check!(!keyed_value.replay_tombstone);

                let tombstone_event = diskless_wal_replay_decision(2, keyed, tombstone, false);
                check!(tombstone_event.action == Remove);
                check!(!tombstone_event.keyed_range);
                check!(tombstone_event.replay_tombstone);

                let invalid = diskless_wal_replay_decision(u8::MAX, keyed, tombstone, false);
                check!(invalid.action == Ignore);
                check!(invalid.keyed_range == keyed);
                check!(invalid.replay_tombstone == tombstone);
            }
        }

        check!(!diskless_wal_replay_decision(2, true, false, true).replay_tombstone);
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
        ] {
            let decision = diskless_trim_decision(frontier, high_watermark, lag, current);
            check!(decision.should_trim == expected.0);
            check!(decision.target == expected.1);
            check!(decision.target >= current);
            if decision.should_trim {
                check!(decision.target <= frontier);
                check!(decision.target <= high_watermark - lag.max(0));
            }
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
            // The 150-byte debt over a 150-byte budget is paid down to 50 by
            // the first range, which the second cannot cover.
            (
                "bytes expires the oldest range only",
                [10, 20, 30],
                None,
                Some(150),
                0,
                1,
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
        ] {
            let prefix = diskless_retention_prefix(
                &max_timestamps,
                &BYTE_LENS,
                &LAST_OFFSETS,
                retention_ms,
                retention_bytes,
                floor,
                NOW_MS,
            );
            check!(prefix == expired, "{what}");
            check!(prefix < max_timestamps.len(), "{what}");
        }
    }

    #[test]
    fn retention_prefix_is_total_on_short_inputs_and_unrepresentable_windows() {
        // A window `now_ms - retention_ms` cannot represent expires nothing.
        check!(
            diskless_retention_prefix(
                &[10, 20, 30],
                &[100, 100, 100],
                &[4, 9, 14],
                Some(-1),
                None,
                0,
                i64::MIN,
            ) == 0
        );
        // One range is the newest range, whatever retention says.
        check!(diskless_retention_prefix(&[10], &[100], &[4], Some(1), Some(0), 99, 1_000) == 0);
        check!(diskless_retention_prefix(&[], &[], &[], Some(1), Some(0), 99, 1_000) == 0);
    }

    #[test]
    fn cold_read_decisions_cover_boundaries_gaps_caps_and_limits() {
        let entries = [(0, 4), (7, 9), (12, 15)];
        assert2::assert!(diskless_logical_range(&entries, -1).is_none());
        assert2::assert!(diskless_logical_range(&entries, 0) == Some(0));
        assert2::assert!(diskless_logical_range(&entries, 5) == Some(1));
        assert2::assert!(diskless_logical_range(&entries, 15) == Some(2));
        assert2::assert!(diskless_logical_range(&entries, 16).is_none());

        assert2::assert!(diskless_span_extension(10, 5, 15, 7, true, 12) == Some(12));
        assert2::assert!(diskless_span_extension(10, 5, 16, 7, true, 12).is_none());
        assert2::assert!(diskless_span_extension(10, 5, 15, 7, false, 12).is_none());
        assert2::assert!(diskless_span_extension(u64::MAX, 1, 0, 1, true, 2).is_none());

        assert2::assert!(
            diskless_batch_step(None, 0, 10, 0, 0, 1, 5) == DisklessBatchStep::Skip(10)
        );
        assert2::assert!(
            diskless_batch_step(None, 10, 10, 1, 0, 1, 5) == DisklessBatchStep::Start(20)
        );
        assert2::assert!(
            diskless_batch_step(Some(10), 20, 10, 2, 0, 1, 20) == DisklessBatchStep::Continue(30)
        );
        assert2::assert!(
            diskless_batch_step(Some(10), 20, 10, 2, 0, 1, 19) == DisklessBatchStep::Stop
        );
        assert2::assert!(
            diskless_batch_step(None, usize::MAX, 1, 0, 0, 0, usize::MAX)
                == DisklessBatchStep::Invalid
        );
        assert2::assert!(
            diskless_batch_step(None, 0, 1, i64::MAX, 1, 0, usize::MAX)
                == DisklessBatchStep::Invalid
        );
    }
}
