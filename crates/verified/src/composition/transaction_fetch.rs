use creusot_std::prelude::*;

use super::{
    FetchWatermarks, LogBatchKind, aborted_transaction_interval, fetch_visibility,
    first_unstable_offset, local_append_coordinates, log_batch_kind, transaction_marker_closes,
};

/// Return the actual unstable frontier and read-committed visibility. The
/// limit is the largest prefix bounded by every live/unreplicated transaction,
/// HW and delivery; a transaction beyond the log end rejects the whole input.
/// The starts must be a complete, coherent snapshot of transaction state.
#[ensures(match result {
    None => exists<i: Int> 0 <= i && i < starts@.len() && starts@[i]@ > w.log_end@,
    Some((lso, visibility)) => lso@ <= w.log_end@
        && (forall<i: Int> 0 <= i && i < starts@.len() ==> starts@[i]@ <= w.log_end@)
        && ((starts@.len() == 0 && lso@ == w.log_end@)
            || (starts@.len() > 0
                && (exists<i: Int> 0 <= i && i < starts@.len() && lso@ == starts@[i]@)
                && (forall<i: Int> 0 <= i && i < starts@.len() ==> lso@ <= starts@[i]@)))
        && visibility.limit_offset@ == lso@.min(w.hw@).min(w.deliverable@)
        && visibility.effective_lso@ == lso@.min(w.hw@)
        && visibility.response_lso@ == lso@.min(w.hw@)
        && visibility.response_hw@ == w.hw@
        && visibility.read_committed_aborts && !visibility.out_of_range
        && visibility.empty == (w.log_start@ >= w.hw@.min(w.deliverable@))
        && (forall<i: Int> 0 <= i && i < starts@.len()
            ==> visibility.limit_offset@ <= starts@[i]@),
})]
#[ensures(match result {
    None => true,
    Some((_, visibility)) => forall<v: Int> v <= w.log_end@ && v <= w.hw@
        && v <= w.deliverable@
        && (forall<i: Int> 0 <= i && i < starts@.len() ==> v <= starts@[i]@)
        ==> v <= visibility.limit_offset@,
})]
pub(super) fn committed_fetch_excludes_unstable(
    starts: &[i64],
    w: FetchWatermarks,
) -> Option<(i64, crate::broker::FetchVisibility)> {
    let lso = first_unstable_offset(starts, w.log_end)?;
    let visibility = fetch_visibility(false, true, FetchWatermarks { lso, ..w }, w.log_start);
    Some((lso, visibility))
}

/// Decode the actual control key, close only its producer, keep a completed
/// transaction unstable until HW passes the entire marker, and expose the
/// largest read-committed prefix allowed by all remaining transactions.
/// `other_starts` must enumerate every other live/unreplicated transaction.
/// The marker span is already admitted by the append-coordinate guard.
#[requires(0 <= span.0@ && 0 <= span.1@ && span.0@ + span.1@ < i64::MAX@)]
#[requires(producer.0@ >= 0 && 0 <= producer.2@ && producer.2@ <= span.0@)]
#[requires(forall<i: Int> 0 <= i && i < other_starts@.len()
    ==> 0 <= other_starts@[i]@ && other_starts@[i]@ <= span.0@)]
#[ensures(result.0@ <= span.0@ + span.1@ + 1
    && result.0@ <= bounds.0@ && result.0@ <= bounds.1@)]
#[ensures(forall<i: Int> 0 <= i && i < other_starts@.len()
    ==> result.0@ <= other_starts@[i]@)]
#[ensures(!(is_control && key@.len() >= 4 && key@[2]@ == 0
    && (key@[3]@ == 0 || key@[3]@ == 1) && producer.0 == producer.1
    && bounds.0@ > span.0@ + span.1@) ==> result.0@ <= producer.2@)]
#[ensures(match result.1 {
    Some((start, last)) => is_control && key@.len() >= 4 && key@[2]@ == 0
        && key@[3]@ == 0 && producer.0 == producer.1
        && start@ == producer.2@ && last@ == span.0@ + span.1@,
    None => !(is_control && key@.len() >= 4 && key@[2]@ == 0
        && key@[3]@ == 0 && producer.0 == producer.1),
})]
#[ensures(forall<v: Int> v <= span.0@ + span.1@ + 1 && v <= bounds.0@ && v <= bounds.1@
    && (forall<i: Int> 0 <= i && i < other_starts@.len() ==> v <= other_starts@[i]@)
    && ((is_control && key@.len() >= 4 && key@[2]@ == 0
        && (key@[3]@ == 0 || key@[3]@ == 1) && producer.0 == producer.1
        && bounds.0@ > span.0@ + span.1@) || v <= producer.2@)
    ==> v <= result.0@)]
pub(super) fn control_marker_bounds_committed_fetch(
    key: &[u8],
    is_control: bool,
    producer: (i64, i64, i64), // pending PID, marker PID, pending start
    span: (i64, i32),          // marker base and last-offset delta
    other_starts: &[i64],
    bounds: (i64, i64), // HW and deliverable frontier
) -> (i64, Option<(i64, i64)>) {
    let Some((last, end)) = local_append_coordinates(span.0, span.0, span.1) else {
        return (i64::MIN, None);
    };
    let kind = log_batch_kind(is_control, key);
    let is_abort = matches!(kind, LogBatchKind::Abort);
    let is_commit = matches!(kind, LogBatchKind::Commit);
    let closes = transaction_marker_closes(is_abort, is_commit, producer.0 == producer.1);
    let aborted = if closes && is_abort {
        aborted_transaction_interval(Some(producer.2), last, producer.1)
    } else {
        None
    };
    let Some(mut lso) = first_unstable_offset(other_starts, end) else {
        return (i64::MIN, None);
    };
    if !closes || last >= bounds.0 {
        lso = lso.min(producer.2);
    }
    let visibility = fetch_visibility(
        false,
        true,
        FetchWatermarks {
            log_start: 0,
            log_end: end,
            hw: bounds.0,
            lso,
            deliverable: bounds.1,
        },
        0,
    );
    (visibility.limit_offset, aborted)
}
