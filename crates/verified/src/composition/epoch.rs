use creusot_std::prelude::*;

use super::{
    EpochEntry, FetchWatermarks, LeaderEpoch, Offset, epoch_and_offset_for_entries,
    fetch_visibility, restore_leader_epoch_entry_valid, truncation_frontier,
};
use crate::broker::FetchVisibility;
#[cfg(creusot)]
use crate::leader_epoch::kafka_end_offset_for;

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub(super) fn epoch_window_valid(base: Int, start: Int, end: Int) -> bool {
    pearlite! { 0 <= base && base <= end && 0 <= start && start <= end }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub(super) fn epoch_archive_valid(entries: Seq<EpochEntry>, base: Int, end: Int) -> bool {
    pearlite! {
        (forall<i: Int> 0 <= i && i < entries.len() ==> entries[i].epoch.0@ >= 0
            && base <= entries[i].start_offset.0@ && entries[i].start_offset.0@ <= end)
        && (forall<i: Int, j: Int> 0 <= i && i < j && j < entries.len()
            ==> entries[i].epoch.0@ < entries[j].epoch.0@
                && entries[i].start_offset.0@ < entries[j].start_offset.0@)
    }
}

type EpochTruncation = Result<Option<(i32, FetchWatermarks, FetchVisibility)>, ()>;

/// Validate complete epoch history and export its actual resolved cut and
/// clamped Fetch view. Err is invalid history/window; Ok(None) is an unplaceable
/// epoch. Cuts below retained floors require the host's reset path. Applying
/// the cut durably and faithfully decoded, batch-aligned history are external.
#[ensures(match result {
    Err(()) => !epoch_window_valid(segment_base@, w.log_start@, w.log_end@)
        || !epoch_archive_valid(entries@, segment_base@, w.log_end@),
    Ok(None) => epoch_window_valid(segment_base@, w.log_start@, w.log_end@)
        && epoch_archive_valid(entries@, segment_base@, w.log_end@)
        && kafka_end_offset_for(entries@, requested@, w.log_end@, -1, -1),
    Ok(Some((found, bounded, visibility))) => epoch_window_valid(segment_base@, w.log_start@, w.log_end@)
        && epoch_archive_valid(entries@, segment_base@, w.log_end@)
        && kafka_end_offset_for(entries@, requested@, w.log_end@, found@, bounded.log_end@)
        && segment_base@ <= bounded.log_end@ && bounded.log_end@ <= w.log_end@ && found@ <= requested@
        && bounded.log_start == w.log_start && bounded.hw@ == w.hw@.min(bounded.log_end@)
        && bounded.lso@ == w.lso@.min(bounded.log_end@)
        && bounded.deliverable@ == w.deliverable@.min(bounded.log_end@)
        && visibility.limit_offset@ == bounded.hw@.min(bounded.lso@).min(bounded.deliverable@)
        && visibility.response_hw == bounded.hw && visibility.response_lso@ == bounded.hw@.min(bounded.lso@)
        && visibility.effective_lso@ == bounded.hw@.min(bounded.lso@) && visibility.read_committed_aborts
        && !visibility.out_of_range && visibility.empty == (w.log_start@ >= bounded.hw@.min(bounded.deliverable@)),
})]
pub(super) fn validated_epochs_bound_truncated_fetch(
    entries: &[EpochEntry],
    requested: i32,
    segment_base: i64,
    w: FetchWatermarks,
) -> EpochTruncation {
    if segment_base < 0 || segment_base > w.log_end || w.log_start < 0 || w.log_start > w.log_end {
        return Err(());
    }
    let mut i = 0usize;
    let mut previous = None;
    #[invariant(i@ <= entries@.len())]
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
            return Err(());
        }
        previous = Some((epoch, start));
        i += 1;
    }
    let (found, end) =
        epoch_and_offset_for_entries(entries, LeaderEpoch(requested), Offset(w.log_end));
    if end.0 == -1 {
        return Ok(None);
    }
    let bounded = FetchWatermarks {
        log_end: end.0,
        hw: truncation_frontier(w.hw, end.0),
        lso: truncation_frontier(w.lso, end.0),
        deliverable: truncation_frontier(w.deliverable, end.0),
        ..w
    };
    let visibility = fetch_visibility(false, true, bounded, w.log_start);
    Ok(Some((found.0, bounded, visibility)))
}
