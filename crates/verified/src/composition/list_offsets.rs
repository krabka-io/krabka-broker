use creusot_std::prelude::*;

use super::{first_unstable_offset, indexed_timestamp_scan_finds_first};
use crate::list_offsets::{
    ListOffsetsBoundDecision, ListOffsetsBoundFacts, ListOffsetsSelectionDecision,
    ListOffsetsSelectionFacts, list_offsets_bound_decision, list_offsets_kind,
    list_offsets_selection_decision,
};

/// Sparse timestamp lookup, transaction-derived isolation, and response
/// selection return the first visible match exactly when one exists.
/// Arrays are the complete decoded data window; sparse maxima bound earlier
/// timestamps. Frontiers and pending starts describe one coherent snapshot.
#[requires(offsets@.len() == timestamps@.len())]
#[requires(0 <= frontiers.0@ && frontiers.0@ <= frontiers.1@
    && 0 <= frontiers.2@ && frontiers.2@ <= frontiers.1@)]
#[requires(request.2@ >= 0 && candidate_epoch@ >= -1)]
#[requires(forall<i: Int> 0 <= i && i < offsets@.len()
    ==> frontiers.0@ + offsets@[i]@ < frontiers.1@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < offsets@.len()
    ==> offsets@[i]@ < offsets@[j]@)]
#[requires(forall<i: Int> 0 <= i && i < starts@.len()
    ==> 0 <= starts@[i]@ && starts@[i]@ <= frontiers.1@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
    ==> entries@[i].0@ <= entries@[j].0@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < entries@.len()
    && 0 <= j && j < offsets@.len() && offsets@[j]@ < entries@[i].1@
    ==> timestamps@[j]@ <= entries@[i].0@)]
#[ensures(match result {
    ListOffsetsSelectionDecision::RejectMalformed => false,
    ListOffsetsSelectionDecision::Resolved { offset, timestamp, leader_epoch } =>
        leader_epoch == candidate_epoch
        && exists<i: Int> 0 <= i && i < offsets@.len()
            && offset@ == frontiers.0@ + offsets@[i]@
            && timestamp == timestamps@[i] && timestamp@ >= request.2@
            && (forall<j: Int> 0 <= j && j < i ==> timestamps@[j]@ < request.2@)
            && offset@ < (if request.0@ == -1 { frontiers.2@ } else { frontiers.1@ })
            && (request.0@ == -1 && request.1@ == 1 ==>
                forall<j: Int> 0 <= j && j < starts@.len() ==> offset@ < starts@[j]@),
    ListOffsetsSelectionDecision::Unknown =>
        !(exists<i: Int> 0 <= i && i < offsets@.len() && timestamps@[i]@ >= request.2@
            && frontiers.0@ + offsets@[i]@
                < (if request.0@ == -1 { frontiers.2@ } else { frontiers.1@ })
            && (request.0@ == -1 && request.1@ == 1 ==>
                forall<j: Int> 0 <= j && j < starts@.len()
                    ==> frontiers.0@ + offsets@[i]@ < starts@[j]@)),
})]
pub(super) fn timestamp_list_offsets_finds_first_visible(
    entries: &[(i64, u32)],
    offsets: &[u32],
    timestamps: &[i64],
    starts: &[i64],
    frontiers: (i64, i64, i64), // segment base, log end, HWM
    request: (i32, i8, i64),    // replica id, isolation level, timestamp
    candidate_epoch: i32,       // host lookup for the matched record
) -> ListOffsetsSelectionDecision {
    let lso = first_unstable_offset(starts, frontiers.1).expect("coherent pending starts");
    let bound = match list_offsets_bound_decision(ListOffsetsBoundFacts {
        replica_id: request.0,
        isolation_level: request.1,
        log_end: frontiers.1,
        high_watermark: frontiers.2,
        last_stable: lso,
    }) {
        ListOffsetsBoundDecision::Bound { offset } => offset,
        ListOffsetsBoundDecision::RejectMalformed => {
            return ListOffsetsSelectionDecision::RejectMalformed;
        }
    };
    let (offset, timestamp) =
        match indexed_timestamp_scan_finds_first(entries, offsets, timestamps, request.2) {
            Some(index) => (frontiers.0 + i64::from(offsets[index]), timestamps[index]),
            None => (-1, -1),
        };
    list_offsets_selection_decision(ListOffsetsSelectionFacts {
        kind: list_offsets_kind(request.2, 11),
        candidate_offset: offset,
        candidate_timestamp: timestamp,
        candidate_epoch,
        last_fetchable: bound,
    })
}
