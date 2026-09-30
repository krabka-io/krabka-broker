use creusot_std::prelude::*;

#[cfg(creusot)]
use super::restored_aborts::{abort_intersects_fetch, restored_abort_index_valid};
use super::{
    FetchWatermarks, RestoreAbortedTxn, RestoreSegmentExtent, committed_fetch_excludes_unstable,
    restored_abort_sources_cover_committed_fetch, truncation_frontier,
};

type StableAbortFetch = (i64, i64, Vec<(i64, i64)>);

/// Complete unstable starts determine the read-committed prefix, whose remote
/// and local abort sources supply exactly the necessary unique wire rows.
/// Starts, indexes and watermarks must describe one coherent log lineage;
/// byte reads, requested-floor authorization and client filtering are external.
#[requires(from@ >= 0)]
#[ensures(match result {
    None => (exists<i: Int> 0 <= i && i < starts@.len() && starts@[i]@ > w.log_end@)
        || !restored_abort_index_valid(remote@, owners.0)
        || !restored_abort_index_valid(local@, owners.1),
    Some((lso, limit, rows)) => restored_abort_index_valid(remote@, owners.0)
        && restored_abort_index_valid(local@, owners.1)
        && lso@ <= w.log_end@
        && (forall<i: Int> 0 <= i && i < starts@.len()
            ==> starts@[i]@ <= w.log_end@ && lso@ <= starts@[i]@ && limit@ <= starts@[i]@)
        && ((starts@.len() == 0 && lso@ == w.log_end@)
            || (exists<i: Int> 0 <= i && i < starts@.len() && lso@ == starts@[i]@))
        && limit@ == lso@.min(w.hw@).min(w.deliverable@).min(cut@)
        && (forall<i: Int, j: Int> 0 <= i && i < j && j < rows@.len() ==> rows@[i] != rows@[j])
        && (forall<i: Int> 0 <= i && i < rows@.len() ==>
            (exists<j: Int> 0 <= j && j < remote@.len()
                && rows@[i] == (remote@[j].producer_id, remote@[j].start_offset)
                && abort_intersects_fetch(remote@[j], from@, limit@))
            || (exists<j: Int> 0 <= j && j < local@.len()
                && rows@[i] == (local@[j].producer_id, local@[j].start_offset)
                && abort_intersects_fetch(local@[j], from@, limit@)))
        && (forall<i: Int> 0 <= i && i < remote@.len()
            && abort_intersects_fetch(remote@[i], from@, limit@)
            ==> exists<j: Int> 0 <= j && j < rows@.len()
                && rows@[j] == (remote@[i].producer_id, remote@[i].start_offset))
        && (forall<i: Int> 0 <= i && i < local@.len()
            && abort_intersects_fetch(local@[i], from@, limit@)
            ==> exists<j: Int> 0 <= j && j < rows@.len()
                && rows@[j] == (local@[i].producer_id, local@[i].start_offset)),
})]
#[ensures(match result {
    None => true,
    Some((_, limit, _)) => forall<v: Int> v <= w.log_end@ && v <= w.hw@
        && v <= w.deliverable@ && v <= cut@
        && (forall<i: Int> 0 <= i && i < starts@.len() ==> v <= starts@[i]@)
        ==> v <= limit@,
})]
pub(super) fn stable_abort_sources_cover_fetch(
    starts: &[i64],
    remote: &[RestoreAbortedTxn],
    local: &[RestoreAbortedTxn],
    owners: (RestoreSegmentExtent, RestoreSegmentExtent),
    w: FetchWatermarks,
    from: i64,
    cut: i64,
) -> Option<StableAbortFetch> {
    let (lso, visibility) = committed_fetch_excludes_unstable(starts, w)?;
    let limit = truncation_frontier(visibility.limit_offset, cut);
    let rows = restored_abort_sources_cover_committed_fetch(
        remote,
        local,
        owners,
        FetchWatermarks { lso, ..w },
        from,
        cut,
    )?;
    Some((lso, limit, rows))
}
