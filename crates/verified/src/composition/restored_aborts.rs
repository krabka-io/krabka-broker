use creusot_std::prelude::*;

use super::{
    FetchWatermarks, RestoreAbortedTxn, RestoreSegmentExtent, aborted_transaction_interval,
    aborted_transaction_overlaps, fetch_visibility, restore_txn_index_entry_valid,
    truncation_frontier,
};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub(super) fn restored_abort_index_valid(
    entries: Seq<RestoreAbortedTxn>,
    owner: RestoreSegmentExtent,
) -> bool {
    pearlite! { forall<i: Int> 0 <= i && i < entries.len()
    ==> entries[i].producer_id@ >= 0 && owner.base_offset@ >= 0
        && 0 <= entries[i].start_offset@ && entries[i].start_offset@ <= entries[i].last_offset@
        && owner.base_offset@ <= entries[i].last_offset@ && entries[i].last_offset@ <= owner.last_offset@
        && (i > 0 ==> entries[i - 1].last_offset@ < entries[i].last_offset@) }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub(super) fn abort_intersects_fetch(entry: RestoreAbortedTxn, from: Int, end: Int) -> bool {
    pearlite! { from < end && entry.start_offset@ < end && entry.last_offset@ >= from }
}

/// Admit the complete index and return every selected original row index.
/// Narrowing consumer visibility loses no still-overlapping abort and never
/// introduces one. Transactions may start before their marker's segment.
#[ensures(match result {
    None => !restored_abort_index_valid(entries@, segment),
    Some(selected) => restored_abort_index_valid(entries@, segment)
        && selected@.len() <= entries@.len()
        && (forall<i: Int> 0 <= i && i < selected@.len() ==> selected@[i]@ < entries@.len())
        && (forall<i: Int, j: Int> 0 <= i && i < j && j < selected@.len()
            ==> selected@[i]@ < selected@[j]@)
        && (forall<i: Int> 0 <= i && i < entries@.len()
            ==> (exists<j: Int> 0 <= j && j < selected@.len() && selected@[j]@ == i)
                == abort_intersects_fetch(entries@[i], fetch_start@,
                    w.hw@.min(w.lso@).min(w.deliverable@).min(cut@)))
        && (forall<i: Int> 0 <= i && i < selected@.len() ==>
            abort_intersects_fetch(entries@[selected@[i]@], fetch_start@, w.hw@.min(w.lso@).min(w.deliverable@).min(cut@))
            && abort_intersects_fetch(entries@[selected@[i]@], fetch_start@, w.hw@.min(w.lso@).min(w.deliverable@))
            && entries@[selected@[i]@].start_offset@ < w.hw@
            && entries@[selected@[i]@].start_offset@ < w.lso@
            && entries@[selected@[i]@].start_offset@ < w.deliverable@),
})]
pub(super) fn restored_aborts_remain_bounded_when_fetch_shrinks(
    entries: &[RestoreAbortedTxn],
    segment: RestoreSegmentExtent,
    w: FetchWatermarks,
    fetch_start: i64,
    cut: i64,
) -> Option<Vec<usize>> {
    let visibility = fetch_visibility(false, true, w, fetch_start);
    let narrowed = truncation_frontier(visibility.limit_offset, cut);
    let mut selected: Vec<usize> = Vec::new();
    let mut i = 0usize;
    let mut previous_last = None;
    #[invariant(i@ <= entries@.len() && selected@.len() <= i@)]
    #[invariant(previous_last == if i@ == 0 { None } else { Some(entries@[i@ - 1].last_offset) })]
    #[invariant(restored_abort_index_valid(entries@.subsequence(0, i@), segment))]
    #[invariant(forall<j: Int> 0 <= j && j < selected@.len() ==> selected@[j]@ < i@)]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < k && k < selected@.len()
        ==> selected@[j]@ < selected@[k]@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@
        ==> (exists<k: Int> 0 <= k && k < selected@.len() && selected@[k]@ == j)
            == abort_intersects_fetch(entries@[j], fetch_start@, narrowed@))]
    #[variant(entries@.len() - i@)]
    while i < entries.len() {
        let entry = entries[i];
        if !restore_txn_index_entry_valid(previous_last, entry, segment) {
            return None;
        }
        let (start, last) = aborted_transaction_interval(
            Some(entry.start_offset),
            entry.last_offset,
            entry.producer_id,
        )
        .expect("admitted interval");
        if aborted_transaction_overlaps(start, last, fetch_start, narrowed) {
            selected.push(i);
        }
        previous_last = Some(entry.last_offset);
        i += 1;
    }
    Some(selected)
}
