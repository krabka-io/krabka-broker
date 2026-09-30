use creusot_std::prelude::*;

use super::{restore_index_frontier, restore_time_index_entry_valid, time_index_lookup};

/// Restored time-index cursors stay inside the segment and never move backwards
/// when the target increases, even when timestamps repeat.
#[requires(lower_target@ <= upper_target@)]
#[ensures(result)]
pub(super) fn validated_time_cursors_are_monotone(
    entries: &[(i64, u32)],
    segment_base: i64,
    segment_end: i64,
    lower_target: i64,
    upper_target: i64,
) -> bool {
    let Some(max_relative) = restore_index_frontier(segment_base, segment_end) else {
        return true;
    };
    let mut i = 0usize;
    let mut previous = None;
    #[invariant(i@ <= entries@.len())]
    #[invariant(previous == if i@ == 0 { None } else { Some(entries@[i@ - 1]) })]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> entries@[j].1@ <= max_relative@)]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < k && k < i@
        ==> entries@[j].0@ <= entries@[k].0@ && entries@[j].1@ < entries@[k].1@)]
    #[variant(entries@.len() - i@)]
    while i < entries.len() {
        let (timestamp, relative) = entries[i];
        if !restore_time_index_entry_valid(previous, timestamp, relative, max_relative) {
            return true;
        }
        previous = Some((timestamp, relative));
        i += 1;
    }
    let lower = time_index_lookup(entries, lower_target);
    let upper = time_index_lookup(entries, upper_target);
    match (
        segment_base.checked_add(i64::from(lower)),
        segment_base.checked_add(i64::from(upper)),
    ) {
        (Some(lower_cursor), Some(upper_cursor)) => {
            segment_base <= lower_cursor
                && lower_cursor <= upper_cursor
                && upper_cursor <= segment_end
        }
        _ => false,
    }
}
