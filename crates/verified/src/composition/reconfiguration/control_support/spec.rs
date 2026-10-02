use creusot_std::prelude::*;

use super::super::spec::{expected_member, grant_count, has_node};
use crate::reconfiguration::VoterChangeKind;

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn control_record_count(version: u16, kind: VoterChangeKind) -> Int {
    pearlite! { match kind {
        VoterChangeKind::FinalizeKraftVersion => 2,
        VoterChangeKind::Update => if version@ == 0 { 0 } else { 1 },
        VoterChangeKind::Add | VoterChangeKind::Remove => 1,
    } }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn next_size(count: Int, kind: VoterChangeKind) -> Int {
    pearlite! { match kind { VoterChangeKind::Add => count + 1,
    VoterChangeKind::Remove => count - 1, _ => count } }
}

/// Count reports reaching the actual exclusive batch end, only for real IDs.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
#[variant(count)]
pub fn prefix_count(
    old: Seq<u64>,
    reports: Seq<(i64, i64)>,
    kind: VoterChangeKind,
    node: u64,
    end: Int,
    new: bool,
    count: Int,
) -> Int {
    pearlite! { if count <= 0 { 0 } else {
        prefix_count(old, reports, kind, node, end, new, count - 1)
        + if count - 1 < old.len() {
            if new { if reports[count - 1].1@ >= end
                && expected_member(old, old.len(), kind, node, old[count - 1]) { 1 } else { 0 } }
            else { if reports[count - 1].0@ >= end { 1 } else { 0 } }
        } else { if new && reports[count - 1].1@ >= end
            && !has_node(old, old.len(), node)
            && expected_member(old, old.len(), kind, node, node) { 1 } else { 0 } }
    } }
}

/// Bridge actual prefix reports to the previous composition's grant ledger.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= count && count <= votes.len() && votes.len() == reports.len())]
#[requires(forall<i: Int> 0 <= i && i < votes.len() ==>
    votes[i].0 == (reports[i].0@ >= end) && votes[i].1 == (reports[i].1@ >= end))]
#[requires(forall<id: u64> has_node(next, next.len(), id)
    == expected_member(old, old.len(), kind, node, id))]
#[ensures(grant_count(old, next, votes, node, new, count)
    == prefix_count(old, reports, kind, node, end, new, count))]
#[variant(count)]
pub fn prefix_grants_agree(
    old: Seq<u64>,
    next: Seq<u64>,
    votes: Seq<(bool, bool)>,
    reports: Seq<(i64, i64)>,
    kind: VoterChangeKind,
    node: u64,
    end: Int,
    new: bool,
    count: Int,
) {
    if count > 0 {
        prefix_grants_agree(old, next, votes, reports, kind, node, end, new, count - 1);
    }
}
