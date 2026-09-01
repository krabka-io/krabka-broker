//! Remote-segment admission and relative-offset arithmetic.

use creusot_std::prelude::*;

/// Select the earliest valid finished remote segment.
///
/// The three slices are parallel arrays supplied from one metadata listing.
/// Negative or inverted ranges are not candidates, even when their lifecycle
/// state says the copy finished.
#[requires(starts@.len() == ends@.len())]
#[requires(starts@.len() == finished@.len())]
#[ensures(match result {
    Some(best) => best@ < starts@.len()
        && finished@[best@]
        && 0 <= starts@[best@]@
        && starts@[best@]@ <= ends@[best@]@
        && forall<i: Int> 0 <= i && i < starts@.len()
            && finished@[i]
            && 0 <= starts@[i]@
            && starts@[i]@ <= ends@[i]@
            ==> starts@[best@]@ <= starts@[i]@,
    None => forall<i: Int> 0 <= i && i < starts@.len()
        ==> !finished@[i] || starts@[i]@ < 0 || ends@[i]@ < starts@[i]@,
})]
#[must_use]
pub fn tiered_earliest_finished_index(
    starts: &[i64],
    ends: &[i64],
    finished: &[bool],
) -> Option<usize> {
    let mut best: Option<usize> = None;
    let mut index = 0usize;
    #[invariant(index@ <= starts@.len())]
    #[invariant(match best {
        Some(best) => best@ < index@
            && finished@[best@]
            && 0 <= starts@[best@]@
            && starts@[best@]@ <= ends@[best@]@
            && forall<i: Int> 0 <= i && i < index@
                && finished@[i]
                && 0 <= starts@[i]@
                && starts@[i]@ <= ends@[i]@
                ==> starts@[best@]@ <= starts@[i]@,
        None => forall<i: Int> 0 <= i && i < index@
            ==> !finished@[i] || starts@[i]@ < 0 || ends@[i]@ < starts@[i]@,
    })]
    #[variant(starts@.len() - index@)]
    while index < starts.len() {
        if finished[index] && starts[index] >= 0 && starts[index] <= ends[index] {
            match best {
                Some(current) if starts[current] <= starts[index] => {}
                _ => best = Some(index),
            }
        }
        index += 1;
    }
    best
}

/// Select the finished remote segment with the greatest valid inclusive end.
#[requires(starts@.len() == ends@.len())]
#[requires(starts@.len() == finished@.len())]
#[ensures(match result {
    Some(best) => best@ < starts@.len()
        && finished@[best@]
        && 0 <= starts@[best@]@
        && starts@[best@]@ <= ends@[best@]@
        && forall<i: Int> 0 <= i && i < starts@.len()
            && finished@[i]
            && 0 <= starts@[i]@
            && starts@[i]@ <= ends@[i]@
            ==> ends@[i]@ <= ends@[best@]@,
    None => forall<i: Int> 0 <= i && i < starts@.len()
        ==> !finished@[i] || starts@[i]@ < 0 || ends@[i]@ < starts@[i]@,
})]
#[must_use]
pub fn tiered_latest_finished_index(
    starts: &[i64],
    ends: &[i64],
    finished: &[bool],
) -> Option<usize> {
    let mut best: Option<usize> = None;
    let mut index = 0usize;
    #[invariant(index@ <= starts@.len())]
    #[invariant(match best {
        Some(best) => best@ < index@
            && finished@[best@]
            && 0 <= starts@[best@]@
            && starts@[best@]@ <= ends@[best@]@
            && forall<i: Int> 0 <= i && i < index@
                && finished@[i]
                && 0 <= starts@[i]@
                && starts@[i]@ <= ends@[i]@
                ==> ends@[i]@ <= ends@[best@]@,
        None => forall<i: Int> 0 <= i && i < index@
            ==> !finished@[i] || starts@[i]@ < 0 || ends@[i]@ < starts@[i]@,
    })]
    #[variant(starts@.len() - index@)]
    while index < starts.len() {
        if finished[index] && starts[index] >= 0 && starts[index] <= ends[index] {
            match best {
                Some(current) if ends[current] >= ends[index] => {}
                _ => best = Some(index),
            }
        }
        index += 1;
    }
    best
}

/// Select the valid leader epoch whose start is greatest at or below a
/// segment's inclusive end.
#[requires(epochs@.len() == starts@.len())]
#[requires(0 <= segment_start@)]
#[requires(segment_start@ <= segment_end@)]
#[ensures(match result {
    Some(best) => best@ < starts@.len()
        && 0 <= epochs@[best@]@
        && segment_start@ <= starts@[best@]@
        && starts@[best@]@ <= segment_end@
        && forall<i: Int> 0 <= i && i < starts@.len()
            && 0 <= epochs@[i]@
            && segment_start@ <= starts@[i]@
            && starts@[i]@ <= segment_end@
            ==> starts@[i]@ <= starts@[best@]@,
    None => forall<i: Int> 0 <= i && i < starts@.len()
        ==> epochs@[i]@ < 0 || starts@[i]@ < segment_start@ || segment_end@ < starts@[i]@,
})]
#[must_use]
pub fn tiered_owning_epoch_index(
    epochs: &[i32],
    starts: &[i64],
    segment_start: i64,
    segment_end: i64,
) -> Option<usize> {
    let mut best: Option<usize> = None;
    let mut index = 0usize;
    #[invariant(index@ <= starts@.len())]
    #[invariant(match best {
        Some(best) => best@ < index@
            && 0 <= epochs@[best@]@
            && segment_start@ <= starts@[best@]@
            && starts@[best@]@ <= segment_end@
            && forall<i: Int> 0 <= i && i < index@
                && 0 <= epochs@[i]@
                && segment_start@ <= starts@[i]@
                && starts@[i]@ <= segment_end@
                ==> starts@[i]@ <= starts@[best@]@,
        None => forall<i: Int> 0 <= i && i < index@
            ==> epochs@[i]@ < 0 || starts@[i]@ < segment_start@ || segment_end@ < starts@[i]@,
    })]
    #[variant(starts@.len() - index@)]
    while index < starts.len() {
        if epochs[index] >= 0 && starts[index] >= segment_start && starts[index] <= segment_end {
            match best {
                Some(current) if starts[current] >= starts[index] => {}
                _ => best = Some(index),
            }
        }
        index += 1;
    }
    best
}

/// Decide whether one decoded time-index offset belongs to the usable prefix.
///
/// The first entry is usable. Every later entry must strictly advance the
/// relative offset; otherwise it is the padding terminator.
#[ensures(result == match previous_relative_offset {
    Some(previous) => previous@ < relative_offset@,
    None => true,
})]
#[must_use]
pub const fn remote_time_index_offset_usable(
    previous_relative_offset: Option<u32>,
    relative_offset: u32,
) -> bool {
    match previous_relative_offset {
        Some(previous) => previous < relative_offset,
        None => true,
    }
}

/// Return the length of the usable strict-predecessor prefix of a remote time
/// index.
///
/// Relative offsets must increase throughout the usable prefix. The first
/// non-increasing offset is padding and terminates the search. Timestamps are
/// nondecreasing within that prefix. The returned count therefore selects the
/// last usable entry whose timestamp is strictly below `target_timestamp`.
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
    && (forall<k: Int> 1 <= k && k <= j ==> entries@[k - 1].1@ < entries@[k].1@)
    ==> entries@[i].0@ <= entries@[j].0@)]
#[ensures(result@ <= entries@.len())]
#[ensures(forall<i: Int> 0 <= i && i < result@
    ==> entries@[i].0@ < target_timestamp@)]
#[ensures(forall<i: Int> 1 <= i && i < result@
    ==> entries@[i - 1].1@ < entries@[i].1@)]
#[ensures(forall<j: Int> result@ <= j && j < entries@.len()
    && (forall<k: Int> 1 <= k && k <= j ==> entries@[k - 1].1@ < entries@[k].1@)
    ==> entries@[j].0@ >= target_timestamp@)]
#[ensures(result@ < entries@.len() ==> result@ == 0
    || entries@[result@].1@ <= entries@[result@ - 1].1@
    || entries@[result@].0@ >= target_timestamp@)]
#[must_use]
pub fn remote_time_index_candidate_count(entries: &[(i64, u32)], target_timestamp: i64) -> usize {
    let mut count = 0usize;
    #[invariant(count@ <= entries@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < count@
        ==> entries@[i].0@ < target_timestamp@)]
    #[invariant(forall<i: Int> 1 <= i && i < count@
        ==> entries@[i - 1].1@ < entries@[i].1@)]
    #[variant(entries@.len() - count@)]
    while count < entries.len() {
        if count > 0 && entries[count].1 <= entries[count - 1].1 {
            break;
        }
        if entries[count].0 >= target_timestamp {
            break;
        }
        count += 1;
    }
    count
}

/// Compute the inclusive end of a capped remote-segment fetch.
///
/// A zero cap means read to the segment end. A finite end exists only when the
/// exclusive mathematical end stays strictly inside the segment; wider
/// arithmetic prevents the admission check itself from wrapping.
#[ensures(match result {
    Some(end) => max_bytes@ > 0
        && start_position@ + max_bytes@ < segment_size@
        && end@ == start_position@ + max_bytes@ - 1
        && end@ < segment_size@,
    None => max_bytes@ == 0 || start_position@ + max_bytes@ >= segment_size@,
})]
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    reason = "the mathematical end is checked below segment_size before conversion"
)]
pub fn remote_fetch_end_position(
    start_position: u32,
    segment_size: u32,
    max_bytes: u32,
) -> Option<u32> {
    if max_bytes == 0 {
        return None;
    }
    let exclusive_end = u64::from(start_position) + u64::from(max_bytes);
    if exclusive_end >= u64::from(segment_size) {
        None
    } else {
        Some((exclusive_end - 1) as u32)
    }
}

/// Admit a remote segment for one requested offset and derive its relative
/// offset for Kafka's `u32` sparse index.
///
/// A segment is usable only after its copy finished, when it contains the
/// requested offset, and when the offset falls inside the requested leader
/// epoch's `[epoch_start, next_epoch_start)` subrange. The last epoch has no
/// next boundary and runs through the segment end. A segment wider than the
/// relative-index representation fails closed rather than truncating or
/// defaulting the relative offset.
#[ensures(match result {
    Some(delta) => match epoch_start {
        Some(epoch_start) => copy_finished
            && start_offset@ <= epoch_start@
            && epoch_start@ <= requested_offset@
            && requested_offset@ <= end_offset@
            && match next_epoch_start {
                Some(next) => epoch_start@ < next@
                    && next@ <= end_offset@
                    && requested_offset@ < next@,
                None => true,
            }
            && delta@ == requested_offset@ - start_offset@,
        None => false,
    },
    None => match epoch_start {
        Some(epoch_start) => !copy_finished
            || epoch_start@ < start_offset@
            || requested_offset@ < epoch_start@
            || requested_offset@ > end_offset@
            || match next_epoch_start {
                Some(next) => next@ <= epoch_start@
                    || next@ > end_offset@
                    || requested_offset@ >= next@,
                None => false,
            }
            || requested_offset@ - start_offset@ > u32::MAX@,
        None => true,
    },
})]
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the mathematical delta is checked to fit u32 before conversion"
)]
#[allow(
    clippy::manual_let_else,
    clippy::question_mark,
    reason = "explicit matching avoids unsupported Creusot v0.13 residual-control obligations"
)]
pub fn remote_read_relative_offset(
    start_offset: i64,
    end_offset: i64,
    requested_offset: i64,
    copy_finished: bool,
    epoch_start: Option<i64>,
    next_epoch_start: Option<i64>,
) -> Option<u32> {
    let epoch_start = match epoch_start {
        Some(epoch_start) => epoch_start,
        None => return None,
    };
    if !copy_finished
        || epoch_start < start_offset
        || requested_offset < epoch_start
        || requested_offset > end_offset
    {
        return None;
    }
    match next_epoch_start {
        Some(next) if next <= epoch_start || next > end_offset || requested_offset >= next => {
            return None;
        }
        _ => {}
    }

    let delta = i128::from(requested_offset) - i128::from(start_offset);
    if delta > i128::from(u32::MAX) {
        None
    } else {
        Some(delta as u32)
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn tiered_frontiers_are_finished_valid_and_exact() {
        let starts = [20, 0, -1, 40];
        let ends = [39, 19, 100, 59];
        let finished = [true, true, true, false];
        check!(tiered_earliest_finished_index(&starts, &ends, &finished) == Some(1));
        check!(tiered_latest_finished_index(&starts, &ends, &finished) == Some(0));
        check!(tiered_earliest_finished_index(&[], &[], &[]) == None);
        check!(tiered_latest_finished_index(&[], &[], &[]) == None);
    }

    #[test]
    fn tiered_epoch_owner_has_the_greatest_valid_start() {
        let epochs = [0, 2, -1, 4];
        let starts = [20, 30, 38, 35];
        check!(tiered_owning_epoch_index(&epochs, &starts, 20, 39) == Some(3));
        check!(tiered_owning_epoch_index(&[7], &[40], 20, 39) == None);
    }

    #[test]
    fn remote_time_index_selects_strict_predecessor_prefix() {
        let entries = [(1_000, 0), (2_000, 10), (2_000, 20), (3_000, 30)];
        for (target, expected) in [
            (500, 0),
            (1_000, 0),
            (1_500, 1),
            (2_000, 1),
            (2_500, 3),
            (4_000, 4),
        ] {
            check!(remote_time_index_candidate_count(&entries, target) == expected);
        }
    }

    #[test]
    fn remote_time_index_stops_at_padding() {
        let entries = [(1_000, 0), (2_000, 10), (0, 0), (9_000, 20)];
        check!(remote_time_index_candidate_count(&entries, i64::MAX) == 2);
        check!(remote_time_index_candidate_count(&[], i64::MAX) == 0);
        check!(remote_time_index_offset_usable(None, 0));
        check!(remote_time_index_offset_usable(Some(0), 10));
        check!(!remote_time_index_offset_usable(Some(10), 10));
        check!(!remote_time_index_offset_usable(Some(10), 0));
    }

    #[test]
    fn fetch_end_position_handles_exact_and_overflow_boundaries() {
        for (start, segment, max_bytes, expected) in [
            (0, 2, 1, Some(0)),
            (0, 1, 1, None),
            (0, 1, 0, None),
            (u32::MAX - 2, u32::MAX, 1, Some(u32::MAX - 2)),
            (u32::MAX - 1, u32::MAX, 1, None),
            (u32::MAX, u32::MAX, u32::MAX, None),
        ] {
            check!(remote_fetch_end_position(start, segment, max_bytes) == expected);
        }
    }

    #[test]
    fn admits_exact_finished_lineage_range_and_rejects_every_other_case() {
        for (start, end, requested, finished, epoch_start, next_epoch, expected) in [
            (100, 199, 100, true, Some(100), None, Some(0)),
            (100, 199, 199, true, Some(100), None, Some(99)),
            (100, 199, 99, true, Some(100), None, None),
            (100, 199, 200, true, Some(100), None, None),
            (200, 100, 150, true, Some(200), None, None),
            (100, 199, 150, false, Some(100), None, None),
            (100, 199, 150, true, None, None, None),
            (0, 99, 49, true, Some(0), Some(50), Some(49)),
            (0, 99, 50, true, Some(0), Some(50), None),
            (0, 99, 49, true, Some(50), None, None),
            (0, 99, 50, true, Some(50), None, Some(50)),
            (0, 99, 49, true, Some(0), Some(100), None),
            (
                i64::MIN,
                i64::MAX,
                i64::MAX,
                true,
                Some(i64::MIN),
                None,
                None,
            ),
            (
                i64::MIN,
                i64::MAX,
                i64::MIN,
                true,
                Some(i64::MIN),
                None,
                Some(0),
            ),
            (
                i64::MIN,
                i64::MAX,
                i64::MIN + i64::from(u32::MAX),
                true,
                Some(i64::MIN),
                None,
                Some(u32::MAX),
            ),
        ] {
            check!(
                remote_read_relative_offset(
                    start,
                    end,
                    requested,
                    finished,
                    epoch_start,
                    next_epoch,
                ) == expected
            );
        }
    }
}
