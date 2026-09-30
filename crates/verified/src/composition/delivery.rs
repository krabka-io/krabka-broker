use creusot_std::prelude::*;

use super::{
    FetchWatermarks, delivery_watermark_advance, earliest_max_timestamp_index, fetch_visibility,
    restore_batch_step, scheduled_delivery_visible,
};

/// The maximum over actual batch activation times is due iff every batch is
/// due. This justifies the whole-segment activation shortcut, including empty
/// segments, signed timestamp extremes, and deadline overflow.
#[ensures(result == (forall<i: Int> 0 <= i && i < activations@.len() ==>
    uncertainty@ >= 0 && activations@[i]@ + uncertainty@ <= i64::MAX@
        && activations@[i]@ + uncertainty@ <= now@))]
pub(super) fn segment_maximum_proves_delivery(
    activations: &[i64],
    uncertainty: i64,
    now: i64,
) -> bool {
    match earliest_max_timestamp_index(activations) {
        Some(index) => scheduled_delivery_visible(true, uncertainty, activations[index], now),
        None => true,
    }
}

/// A complete batch walk derives a delivery frontier that hides every waiting
/// batch from consumers while leaving follower replication ungated. Compacted
/// offset gaps are allowed; the window begins on a batch boundary. This
/// recomputes from the start, not a cached cursor.
#[requires(batches@.len() == activations@.len())]
#[requires(0 <= w.log_start@ && w.log_start@ <= w.log_end@)]
#[ensures(result)]
pub(super) fn scheduled_prefix_bounds_fetch(
    batches: &[(i64, i32)],
    activations: &[i64],
    uncertainty: i64,
    now: i64,
    w: FetchWatermarks,
) -> bool {
    let all_due = segment_maximum_proves_delivery(activations, uncertainty, now);
    let mut cursor = w.log_start;
    let mut candidate = w.log_end;
    let mut i = 0usize;
    #[invariant(i@ <= batches@.len())]
    #[invariant(w.log_start@ <= cursor@ && cursor@ <= w.log_end@)]
    #[invariant(w.log_start@ <= candidate@ && candidate@ <= w.log_end@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ && !(uncertainty@ >= 0
        && activations@[j]@ + uncertainty@ <= i64::MAX@
        && activations@[j]@ + uncertainty@ <= now@) ==> candidate@ <= batches@[j].0@)]
    #[variant(batches@.len() - i@)]
    while i < batches.len() {
        let (base, delta) = batches[i];
        let Some(next) = restore_batch_step(cursor, base, delta) else {
            return true;
        };
        if next > w.log_end {
            return true;
        }
        if !all_due
            && !scheduled_delivery_visible(true, uncertainty, activations[i], now)
            && base < candidate
        {
            candidate = base;
        }
        cursor = next;
        i += 1;
    }
    if cursor != w.log_end {
        return true;
    }
    let deliverable = delivery_watermark_advance(w.log_start, w.log_start, candidate, w.log_end);
    let bounded = FetchWatermarks { deliverable, ..w };
    let consumer = fetch_visibility(false, true, bounded, w.log_start);
    let follower = fetch_visibility(true, true, bounded, w.log_start);
    if deliverable != candidate
        || consumer.limit_offset > candidate
        || follower.limit_offset != w.log_end
    {
        return false;
    }
    i = 0;
    #[invariant(i@ <= batches@.len())]
    #[variant(batches@.len() - i@)]
    while i < batches.len() {
        if !scheduled_delivery_visible(true, uncertainty, activations[i], now)
            && consumer.limit_offset > batches[i].0
        {
            return false;
        }
        i += 1;
    }
    true
}
