use creusot_std::prelude::*;

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
fn finished_selection(
    starts: Seq<i64>,
    ends: Seq<i64>,
    finished: Seq<bool>,
    best: Option<usize>,
    count: Int,
    latest: bool,
) -> bool {
    pearlite! {
        match best {
            Some(best) => best@ < count && finished[best@]
                && 0 <= starts[best@]@ && starts[best@]@ <= ends[best@]@
                && forall<i: Int> 0 <= i && i < count && finished[i]
                    && 0 <= starts[i]@ && starts[i]@ <= ends[i]@
                    ==> if latest { ends[i]@ <= ends[best@]@ } else { starts[best@]@ <= starts[i]@ },
            None => forall<i: Int> 0 <= i && i < count
                ==> !finished[i] || starts[i]@ < 0 || ends[i]@ < starts[i]@,
        }
    }
}

#[requires(starts@.len() == ends@.len() && starts@.len() == finished@.len())]
#[ensures(finished_selection(starts@, ends@, finished@, result, starts@.len(), latest))]
fn select_finished_index(
    starts: &[i64],
    ends: &[i64],
    finished: &[bool],
    latest: bool,
) -> Option<usize> {
    let mut best: Option<usize> = None;
    let mut index = 0usize;
    #[invariant(index@ <= starts@.len())]
    #[invariant(finished_selection(starts@, ends@, finished@, best, index@, latest))]
    #[variant(starts@.len() - index@)]
    while index < starts.len() {
        if finished[index] && starts[index] >= 0 && starts[index] <= ends[index] {
            let keep = match best {
                Some(current) => {
                    if latest {
                        ends[current] >= ends[index]
                    } else {
                        starts[current] <= starts[index]
                    }
                }
                None => false,
            };
            if !keep {
                best = Some(index);
            }
        }
        index += 1;
    }
    best
}

/// Select the earliest valid finished remote segment.
///
/// The three slices are parallel arrays supplied from one metadata listing.
/// Negative or inverted ranges are not candidates, even when their lifecycle
/// state says the copy finished.
#[requires(starts@.len() == ends@.len() && starts@.len() == finished@.len())]
#[ensures(finished_selection(starts@, ends@, finished@, result, starts@.len(), false))]
#[must_use]
pub fn tiered_earliest_finished_index(
    starts: &[i64],
    ends: &[i64],
    finished: &[bool],
) -> Option<usize> {
    select_finished_index(starts, ends, finished, false)
}

/// Select the finished remote segment with the greatest valid inclusive end.
#[requires(starts@.len() == ends@.len() && starts@.len() == finished@.len())]
#[ensures(finished_selection(starts@, ends@, finished@, result, starts@.len(), true))]
#[must_use]
pub fn tiered_latest_finished_index(
    starts: &[i64],
    ends: &[i64],
    finished: &[bool],
) -> Option<usize> {
    select_finished_index(starts, ends, finished, true)
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

/// Return the length of the usable strict-predecessor prefix of a remote time
/// index.
///
/// Kafka preallocates a time index, so its tail is padding. The usable prefix
/// is where relative offsets strictly increase; the first non-increasing
/// offset is padding and ends it. The count stops there or at the first entry
/// whose timestamp is at or above `target_timestamp`, whichever comes first.
///
/// Every counted entry is strictly below the target. A time-index entry holds
/// the largest timestamp at or before its offset, so no record up to that
/// offset can match the target, and each counted entry, the last one included,
/// is a safe place to start a scan. The bytes come from the remote tier, so
/// nothing here assumes their timestamps are sorted: for a Kafka-written index
/// they are, and the last counted entry is then the latest safe one.
#[ensures(result@ <= entries@.len())]
#[ensures(forall<i: Int> 0 <= i && i < result@
    ==> entries@[i].0@ < target_timestamp@)]
#[ensures(forall<i: Int> 1 <= i && i < result@
    ==> entries@[i - 1].1@ < entries@[i].1@)]
#[ensures(result@ < entries@.len() ==>
    (result@ > 0 && entries@[result@].1@ <= entries@[result@ - 1].1@)
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
