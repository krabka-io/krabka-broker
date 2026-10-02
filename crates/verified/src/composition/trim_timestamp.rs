use creusot_std::prelude::*;

#[cfg(creusot)]
use super::trim::{trim_frontier, trim_well_formed};
use super::{
    DeleteRecordsTrimDecision, DeleteRecordsTrimFacts, SparseTimestampWindow,
    admitted_trim_bounds_reload_and_retry, constructed_index_retained_candidate,
};

type TrimTimestampWitness = (i64, Option<usize>, i64, Option<usize>);

/// Read from the actual completed logical floor, independently of the newer
/// producer replay cursor. A retained match below that cursor remains readable.
/// Decoding, snapshot contents and durable completion remain host obligations.
#[requires(0 <= stores.0@ && stores.0@ <= facts.high_watermark@ && stores.0@ <= facts.log_end@)]
#[requires(0 <= stores.1@ && stores.1@ <= facts.high_watermark@ && stores.1@ <= facts.log_end@)]
#[requires(facts.has_delivery_watermark ==> stores.0@ <= facts.delivery_watermark@ && stores.1@ <= facts.delivery_watermark@)]
#[requires(base@ >= 0 && window.0@.len() == window.1@.len())]
#[requires(forall<i: Int> 0 <= i && i < window.0@.len() ==> base@ + window.0@[i]@ < facts.log_end@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < window.0@.len() ==> window.0@[i]@ < window.0@[j]@)]
#[requires(forall<i: Int> 0 <= i && i < window.2@.len() ==> window.2@[i].0@ <= window.2@[i].1@ && window.2@[i].1@ < window.1@.len())]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < window.2@.len() ==> window.2@[i].0@ < window.2@[j].0@ && window.2@[i].1@ <= window.2@[j].1@)]
#[ensures(match result {
    Err(error) => match error {
        DeleteRecordsTrimDecision::RejectMalformed => !trim_well_formed(facts),
        DeleteRecordsTrimDecision::RejectOutOfRange => trim_well_formed(facts) && facts.requested@ != -1 && facts.requested@ > facts.high_watermark@,
        _ => false,
    },
    Ok((floor, snapshot, cursor, selected)) => trim_well_formed(facts)
        && (facts.requested@ == -1 || facts.requested@ <= facts.high_watermark@)
        && floor@ == trim_frontier(facts).max(stores.0@).max(stores.1@)
        && 0 <= floor@ && floor@ <= cursor@ && cursor@ <= facts.log_end@
        && floor@ <= facts.high_watermark@ && (!facts.has_delivery_watermark || floor@ <= facts.delivery_watermark@)
        && match snapshot {
            None => cursor == floor && forall<i: Int> 0 <= i && i < snapshots@.len() ==> !(floor@ < snapshots@[i]@ && snapshots@[i]@ <= facts.log_end@),
            Some(index) => index@ < snapshots@.len() && floor@ < snapshots@[index@]@ && cursor == snapshots@[index@]
                && forall<i: Int> 0 <= i && i < snapshots@.len() && floor@ < snapshots@[i]@ && snapshots@[i]@ <= facts.log_end@
                    ==> snapshots@[i]@ <= cursor@,
        }
        && match selected {
            None => forall<i: Int> 0 <= i && i < window.0@.len() ==> base@ + window.0@[i]@ < floor@ || window.1@[i]@ < target@,
            Some(index) => index@ < window.0@.len() && floor@ <= base@ + window.0@[index@]@ && base@ + window.0@[index@]@ < facts.log_end@
                && window.1@[index@]@ >= target@
                && forall<i: Int> 0 <= i && i < index@ ==> base@ + window.0@[i]@ < floor@ || window.1@[i]@ < target@,
        },
})]
pub(super) fn completed_trim_preserves_retained_timestamp(
    facts: DeleteRecordsTrimFacts,
    stores: (i64, i64),
    snapshots: &[i64],
    window: SparseTimestampWindow<'_>,
    base: i64,
    target: i64,
) -> Result<TrimTimestampWitness, DeleteRecordsTrimDecision> {
    let (wal, local) = stores;
    let (floor, snapshot, cursor) =
        admitted_trim_bounds_reload_and_retry(facts, wal, local, snapshots)?;
    let selected = constructed_index_retained_candidate(window, base, floor, target);
    Ok((floor, snapshot, cursor, selected))
}
