use creusot_std::prelude::*;

use super::{
    EpochEntry, FetchWatermarks, LeaderEpoch, Offset, epoch_and_offset_for_entries,
    fetch_visibility, restore_leader_epoch_entry_valid, truncation_frontier,
};

/// Restored epoch rows establish the reconciliation lookup's global ordering.
/// A resolved cut cannot grow the log, and clamped consumer frontiers cannot
/// expose its discarded tail. The host must apply the cut durably.
#[ensures(result)]
pub(super) fn validated_epochs_bound_truncated_fetch(
    entries: &[EpochEntry],
    requested: i32,
    segment_base: i64,
    w: FetchWatermarks,
) -> bool {
    let mut i = 0usize;
    let mut previous = None;
    #[invariant(i@ <= entries@.len())]
    #[invariant(i@ > 0 ==> segment_base@ >= 0)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> entries@[j].epoch.0@ >= 0)]
    #[invariant(previous == if i@ == 0 { None } else {
        Some((entries@[i@ - 1].epoch.0, entries@[i@ - 1].start_offset.0))
    })]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        segment_base@ <= entries@[j].start_offset.0@
            && entries@[j].start_offset.0@ <= w.log_end@)]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < k && k < i@ ==>
        entries@[j].epoch.0@ < entries@[k].epoch.0@
            && entries@[j].start_offset.0@ < entries@[k].start_offset.0@)]
    #[variant(entries@.len() - i@)]
    while i < entries.len() {
        let epoch = entries[i].epoch.0;
        let start = entries[i].start_offset.0;
        if !restore_leader_epoch_entry_valid(previous, epoch, start, segment_base, w.log_end) {
            return true;
        }
        previous = Some((epoch, start));
        i += 1;
    }
    let (found, end) =
        epoch_and_offset_for_entries(entries, LeaderEpoch(requested), Offset(w.log_end));
    if end.0 == -1 {
        return found.0 == -1;
    }
    proof_assert!(segment_base@ <= end.0@ && end.0@ <= w.log_end@);
    proof_assert!(found.0@ <= requested@);
    let bounded = FetchWatermarks {
        log_end: end.0,
        hw: truncation_frontier(w.hw, end.0),
        lso: truncation_frontier(w.lso, end.0),
        deliverable: truncation_frontier(w.deliverable, end.0),
        ..w
    };
    let visibility = fetch_visibility(false, true, bounded, w.log_start);
    segment_base <= end.0
        && end.0 <= w.log_end
        && found.0 <= requested
        && visibility.limit_offset <= end.0
        && visibility.response_hw <= end.0
        && visibility.response_lso <= end.0
}
