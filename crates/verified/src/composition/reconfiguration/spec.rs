#[cfg(creusot)]
use creusot_std::prelude::*;

#[cfg(creusot)]
use crate::reconfiguration::{TargetMembership, VoterChangeKind};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn has_node(nodes: Seq<u64>, count: Int, node: u64) -> bool {
    pearlite! { exists<i: Int> 0 <= i && i < count && nodes[i] == node }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn valid_old(nodes: Seq<u64>) -> bool {
    pearlite! { nodes.len() > 0 && forall<i: Int, j: Int>
    0 <= i && i < j && j < nodes.len() ==> nodes[i] != nodes[j] }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn membership_coherent(
    nodes: Seq<u64>,
    node: u64,
    membership: TargetMembership,
    kind: VoterChangeKind,
) -> bool {
    pearlite! { kind == VoterChangeKind::FinalizeKraftVersion
    || has_node(nodes, nodes.len(), node) == (membership != TargetMembership::Absent) }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn expected_member(
    nodes: Seq<u64>,
    count: Int,
    kind: VoterChangeKind,
    target: u64,
    node: u64,
) -> bool {
    pearlite! { match kind {
        VoterChangeKind::Add => has_node(nodes, count, node) || node == target,
        VoterChangeKind::Remove => has_node(nodes, count, node) && node != target,
        VoterChangeKind::Update | VoterChangeKind::FinalizeKraftVersion => has_node(nodes, count, node),
    } }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 < old && 0 < next && old - 1 <= next && next <= old + 1)]
#[ensures(old / 2 + 1 + next / 2 + 1 > old.max(next))]
pub fn adjacent_majorities_exceed_union(old: Int, next: Int) {}

/// Mathematical grant ledger over actual membership, rather than supplied
/// counts. The extra candidate slot is ignored if its ID already occurs.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
#[variant(count)]
pub fn grant_count(
    old: Seq<u64>,
    next: Seq<u64>,
    votes: Seq<(bool, bool)>,
    node: u64,
    new: bool,
    count: Int,
) -> Int {
    pearlite! { if count <= 0 { 0 } else {
        grant_count(old, next, votes, node, new, count - 1)
        + if new {
            if votes[count - 1].1 && (if count - 1 < old.len() {
                has_node(next, next.len(), old[count - 1])
            } else { !has_node(old, old.len(), node) && has_node(next, next.len(), node) }) { 1 } else { 0 }
        } else { if count - 1 < old.len() && votes[count - 1].0 { 1 } else { 0 } }
    } }
}

/// A prefix gains exactly the next node, including preservation of old IDs.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= count && count < nodes.len())]
#[ensures(forall<id: u64> has_node(nodes, count + 1, id)
    == (has_node(nodes, count, id) || nodes[count] == id))]
pub fn prefix_node_extend(nodes: Seq<u64>, count: Int) {}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub(super) fn single_change_shape(
    old: Seq<u64>,
    next: Seq<u64>,
    kind: VoterChangeKind,
    node: u64,
) -> bool {
    pearlite! {
        old.len() - 1 <= next.len() && next.len() <= old.len() + 1
        && (forall<id: u64> has_node(next, next.len(), id) == expected_member(old, old.len(), kind, node, id))
        && (forall<i: Int, j: Int> 0 <= i && i < j && j < next.len() ==> next[i] != next[j])
    }
}
