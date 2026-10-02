use creusot_std::prelude::*;

use super::{
    SparseTimestampWindow, constructed_index_retained_candidate,
    diskless_trim_reconciliation_preserves_coverage,
};

/// Keep logical visibility unchanged while physical eviction reroutes the first
/// retained match to committed remote coverage or the surviving local cache.
/// Inherited physical frontiers must already obey coverage/lag bounds. Complete
/// decoding, actual object coverage and durable step completion are external.
#[requires(base@ >= 0 && logical_floor@ >= 0 && window.0@.len() == window.1@.len())]
#[requires(0 <= frontiers.3@ && frontiers.3@ <= frontiers.0@
    && frontiers.3@ + frontiers.2@.max(0) <= frontiers.1@)]
#[requires(0 <= frontiers.4@ && frontiers.4@ <= frontiers.0@
    && frontiers.4@ + frontiers.2@.max(0) <= frontiers.1@)]
#[requires(forall<i: Int> 0 <= i && i < window.0@.len() ==> base@ + window.0@[i]@ <= i64::MAX@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < window.0@.len() ==> window.0@[i]@ < window.0@[j]@)]
#[requires(forall<i: Int> 0 <= i && i < window.2@.len() ==> window.2@[i].0@ <= window.2@[i].1@ && window.2@[i].1@ < window.1@.len())]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < window.2@.len() ==> window.2@[i].0@ < window.2@[j].0@ && window.2@[i].1@ <= window.2@[j].1@)]
#[ensures(frontiers.3@ <= result.0@ && frontiers.4@ <= result.1@)]
#[ensures(result.0@ <= frontiers.0@ && result.1@ <= frontiers.0@)]
#[ensures(result.0@ + frontiers.2@.max(0) <= frontiers.1@ && result.1@ + frontiers.2@.max(0) <= frontiers.1@)]
#[ensures(match result.2 {
    None => forall<i: Int> 0 <= i && i < window.0@.len() ==> base@ + window.0@[i]@ < logical_floor@ || window.1@[i]@ < target@,
    Some((index, remote)) => index@ < window.0@.len() && window.1@[index@]@ >= target@
        && logical_floor@ <= base@ + window.0@[index@]@
        && (forall<i: Int> 0 <= i && i < index@ ==> base@ + window.0@[i]@ < logical_floor@ || window.1@[i]@ < target@)
        && remote == (base@ + window.0@[index@]@ < result.1@)
        && (remote ==> base@ + window.0@[index@]@ < frontiers.0@
            && base@ + window.0@[index@]@ + frontiers.2@.max(0) < frontiers.1@),
})]
pub(super) fn physical_eviction_routes_retained_timestamp(
    frontiers: (i64, i64, i64, i64, i64), // indexed coverage, HWM, lag, WAL, cache
    applied: &[bool],
    window: SparseTimestampWindow<'_>,
    base: i64,
    logical_floor: i64,
    target: i64,
) -> (i64, i64, Option<(usize, bool)>) {
    let (wal, local) = diskless_trim_reconciliation_preserves_coverage(
        frontiers.0,
        frontiers.1,
        frontiers.2,
        frontiers.3,
        frontiers.4,
        applied,
    );
    let selected = constructed_index_retained_candidate(window, base, logical_floor, target)
        .map(|index| (index, base + i64::from(window.0[index]) < local));
    (wal, local, selected)
}
