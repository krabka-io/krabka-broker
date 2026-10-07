use creusot_std::prelude::*;

#[cfg(creusot)]
use super::time_index::sparse_timestamp_window_valid;
#[cfg(creusot)]
use super::time_index::time_segment_valid;
use super::{
    FetchWatermarks, SparseTimestampWindow, committed_fetch_excludes_unstable,
    constructed_time_range_preserves_first,
};

/// Consume the derived transaction/HW/delivery prefix and a constructed interval
/// scan. Offset order makes its exclusive limit safe even when timestamps regress;
/// inherited LSO and timestamp cursors cannot replace the actual derived limit.
/// Complete decoded records and a valid minimum-equivalent transaction snapshot,
/// coherent publication, physical reads and client-side abort filtering remain
/// external. This proves prefix gating only.
#[requires(sparse_timestamp_window_valid(window.0@, window.1@, window.2@))]
#[requires(span.0@ >= 0 && w.log_start@ >= 0 && targets.0@ <= targets.1@)]
#[requires(forall<i: Int> 0 <= i && i < window.0@.len() ==> span.0@ + window.0@[i]@ <= i64::MAX@)]
#[ensures(match result {
    Err(()) => (exists<i: Int> 0 <= i && i < starts@.len() && starts@[i]@ > w.log_end@)
        || !time_segment_valid(span.0@, span.1@)
        || exists<i: Int> 0 <= i && i < window.0@.len() && span.0@ + window.0@[i]@ > span.1@,
    Ok((lso, limit, selected)) => time_segment_valid(span.0@, span.1@)
        && (forall<i: Int> 0 <= i && i < window.0@.len() ==> span.0@ + window.0@[i]@ <= span.1@)
        && (forall<i: Int> 0 <= i && i < starts@.len() ==> starts@[i]@ <= w.log_end@)
        && lso@ <= w.log_end@
        && ((starts@.len() == 0 && lso == w.log_end)
            || (starts@.len() > 0 && (exists<i: Int> 0 <= i && i < starts@.len() && lso == starts@[i])
                && (forall<i: Int> 0 <= i && i < starts@.len() ==> lso@ <= starts@[i]@)))
        && limit@ == lso@.min(w.hw@).min(w.deliverable@)
        && (forall<v: Int> v <= w.log_end@ && v <= w.hw@ && v <= w.deliverable@
            && (forall<i: Int> 0 <= i && i < starts@.len() ==> v <= starts@[i]@) ==> v <= limit@)
        && match selected {
            None => forall<i: Int> 0 <= i && i < window.0@.len()
                ==> span.0@ + window.0@[i]@ < w.log_start@ || span.0@ + window.0@[i]@ >= limit@
                    || window.1@[i]@ < targets.0@ || window.1@[i]@ > targets.1@,
            Some(index) => index@ < window.0@.len() && w.log_start@ <= span.0@ + window.0@[index@]@
                && span.0@ + window.0@[index@]@ < limit@ && targets.0@ <= window.1@[index@]@ && window.1@[index@]@ <= targets.1@
                && (forall<i: Int> 0 <= i && i < starts@.len() ==> span.0@ + window.0@[index@]@ < starts@[i]@)
                && forall<i: Int> 0 <= i && i < index@
                    ==> span.0@ + window.0@[i]@ < w.log_start@ || span.0@ + window.0@[i]@ >= limit@
                        || window.1@[i]@ < targets.0@ || window.1@[i]@ > targets.1@,
        },
})]
pub(super) fn stable_time_range_preserves_first(
    window: SparseTimestampWindow<'_>,
    starts: &[i64],
    span: (i64, i64), // segment base and inclusive end
    w: FetchWatermarks,
    targets: (i64, i64),
) -> Result<(i64, i64, Option<usize>), ()> {
    let (lso, visibility) = committed_fetch_excludes_unstable(starts, w).ok_or(())?;
    let (_lower, _upper, _scan, candidate) =
        constructed_time_range_preserves_first(window, (span.0, span.1, w.log_start), targets)?;
    let selected = match candidate {
        Some(index) if span.0 + i64::from(window.0[index]) < visibility.limit_offset => Some(index),
        _ => None,
    };
    Ok((lso, visibility.limit_offset, selected))
}
