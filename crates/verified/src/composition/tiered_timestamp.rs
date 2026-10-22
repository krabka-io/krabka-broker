use {
    crate::{
        list_offsets::{
            ListOffsetsSelectionDecision, ListOffsetsSelectionFacts, earliest_timestamp_candidate,
            list_offsets_kind, list_offsets_selection_decision,
        },
        timestamp::{first_timestamp_index, first_timestamp_record_index},
    },
    creusot_std::prelude::*,
};

/// One tier as a by-timestamp seek visits it: each segment's decoded records
/// in offset order, and each segment's largest timestamp (Kafka's
/// `LogSegment.largestTimestamp` or `RemoteLogSegmentMetadata.maxTimestampMs`).
type TimestampTier<'a> = (&'a [&'a [(i64, i64)]], &'a [i64]);

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// `candidate` is the first segment whose largest timestamp reaches `target`:
/// the one segment Kafka's seek reads.
fn first_candidate(largest: Seq<i64>, target: Int, candidate: Int) -> bool {
    pearlite! {
        0 <= candidate && candidate < largest.len() && largest[candidate]@ >= target
        && forall<i: Int> 0 <= i && i < candidate ==> largest[i]@ < target
    }
}

/// Kafka 4.3.1's answer for one tier: the first record at or above `minimum`
/// at or after `target` in the first candidate segment, as `(segment,
/// record)` indexes. No later segment is read, so `None` also covers a first
/// candidate whose matches all sit below `minimum`.
#[requires(tier.0@.len() == tier.1@.len())]
#[ensures(match result {
    Some((segment, record)) => first_candidate(tier.1@, target@, segment@)
        && record@ < tier.0@[segment@]@.len()
        && tier.0@[segment@]@[record@].0@ >= minimum@ && tier.0@[segment@]@[record@].1@ >= target@
        && forall<i: Int> 0 <= i && i < record@
            ==> tier.0@[segment@]@[i].0@ < minimum@ || tier.0@[segment@]@[i].1@ < target@,
    None => forall<c: Int, i: Int> first_candidate(tier.1@, target@, c) && 0 <= i && i < tier.0@[c]@.len()
        ==> tier.0@[c]@[i].0@ < minimum@ || tier.0@[c]@[i].1@ < target@,
})]
fn first_candidate_record(
    tier: TimestampTier<'_>,
    minimum: i64,
    target: i64,
) -> Option<(usize, usize)> {
    match first_timestamp_index(tier.1, target) {
        None => None,
        Some(segment) => {
            proof_assert!(forall<c: Int> first_candidate(tier.1@, target@, c) ==> c == segment@);
            first_timestamp_record_index(tier.0[segment], minimum, target)
                .map(|record| (segment, record))
        }
    }
}

/// Each tier answers from its first candidate segment only, as Kafka 4.3.1's
/// `UnifiedLog.searchOffsetInLocalLog` and `RemoteLogManager.findOffsetByTimestamp`
/// do, even when that segment holds no retained match and a later one does.
/// The earlier of the two answers then passes exclusive `ListOffsets`
/// visibility with its own timestamp and the supplied epoch. Complete decoding,
/// truthful segment maxima, candidate order, consistent overlapping records and
/// coherent captured frontiers remain host obligations; failed tier reads are
/// outside this success trace.
#[requires(target@ >= 0 && minimum@ >= 0 && bound@ >= 0 && epoch@ >= -1)]
#[requires(remote.0@.len() == remote.1@.len() && local.0@.len() == local.1@.len())]
#[requires(forall<s: Int, i: Int, j: Int> 0 <= s && s < remote.0@.len() && 0 <= i && i < j && j < remote.0@[s]@.len()
    ==> remote.0@[s]@[i].0@ < remote.0@[s]@[j].0@)]
#[requires(forall<s: Int, i: Int, j: Int> 0 <= s && s < local.0@.len() && 0 <= i && i < j && j < local.0@[s]@.len()
    ==> local.0@[s]@[i].0@ < local.0@[s]@[j].0@)]
#[ensures(match result {
    ListOffsetsSelectionDecision::RejectMalformed => false,
    ListOffsetsSelectionDecision::Resolved { offset, timestamp, leader_epoch } =>
        minimum@ <= offset@ && offset@ < bound@ && timestamp@ >= target@ && leader_epoch == epoch
        && ((exists<s: Int, i: Int> first_candidate(remote.1@, target@, s) && 0 <= i && i < remote.0@[s]@.len()
                && (offset, timestamp) == remote.0@[s]@[i])
            || (exists<s: Int, i: Int> first_candidate(local.1@, target@, s) && 0 <= i && i < local.0@[s]@.len()
                && (offset, timestamp) == local.0@[s]@[i]))
        && (forall<s: Int, i: Int> first_candidate(remote.1@, target@, s) && 0 <= i && i < remote.0@[s]@.len()
            && remote.0@[s]@[i].0@ >= minimum@ && remote.0@[s]@[i].1@ >= target@
            ==> offset@ <= remote.0@[s]@[i].0@)
        && (forall<s: Int, i: Int> first_candidate(local.1@, target@, s) && 0 <= i && i < local.0@[s]@.len()
            && local.0@[s]@[i].0@ >= minimum@ && local.0@[s]@[i].1@ >= target@
            ==> offset@ <= local.0@[s]@[i].0@),
    ListOffsetsSelectionDecision::Unknown =>
        (forall<s: Int, i: Int> first_candidate(remote.1@, target@, s) && 0 <= i && i < remote.0@[s]@.len()
            ==> remote.0@[s]@[i].0@ < minimum@ || remote.0@[s]@[i].1@ < target@
                || remote.0@[s]@[i].0@ >= bound@)
        && (forall<s: Int, i: Int> first_candidate(local.1@, target@, s) && 0 <= i && i < local.0@[s]@.len()
            ==> local.0@[s]@[i].0@ < minimum@ || local.0@[s]@[i].1@ < target@
                || local.0@[s]@[i].0@ >= bound@),
})]
pub(super) fn tiered_timestamp_lookup_answers_first_candidate(
    remote: TimestampTier<'_>,
    local: TimestampTier<'_>,
    target: i64,
    minimum: i64,
    bound: i64,
    epoch: i32,
) -> ListOffsetsSelectionDecision {
    let remote_hit = first_candidate_record(remote, minimum, target)
        .map(|(segment, record)| remote.0[segment][record]);
    let local_hit = first_candidate_record(local, minimum, target)
        .map(|(segment, record)| local.0[segment][record]);
    let (offset, timestamp) =
        earliest_timestamp_candidate(remote_hit, local_hit).unwrap_or((-1, -1));
    list_offsets_selection_decision(ListOffsetsSelectionFacts {
        kind: list_offsets_kind(target, 11),
        candidate_offset: offset,
        candidate_timestamp: timestamp,
        candidate_epoch: epoch,
        last_fetchable: bound,
    })
}
