use creusot_std::prelude::*;

use super::{election_has_quorum, select_wal_voters, wal_voter_set_valid};

/// Full production placement establishes the installer's identity invariant;
/// incomplete/zero placements fail closed. No supplied uniqueness boolean is used.
#[ensures(result)]
pub(super) fn constructed_wal_placement_is_installable(
    candidates: &[(u64, u64)],
    local_node: u64,
    requested: usize,
) -> bool {
    let selected = select_wal_voters(candidates, local_node, requested);
    let mut nodes: Vec<u64> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= selected@.len() && nodes@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> nodes@[j] == selected@[j].0)]
    #[variant(selected@.len() - i@)]
    while i < selected.len() {
        nodes.push(selected[i].0);
        i += 1;
    }
    wal_voter_set_valid(&nodes, local_node, requested)
        == (requested > 0 && selected.len() == requested)
}

/// A complete placement of at least three voters leaves the original majority
/// satisfiable after any one configured rack disappears. This connects the
/// actual placement, not a round-robin model, to quorum arithmetic. Remaining
/// brokers must still communicate and fsync; rack labels must name real domains.
#[requires(requested@ >= 3)]
#[ensures(result)]
pub(super) fn wal_placement_survives_one_rack_loss(
    candidates: &[(u64, u64)],
    local_node: u64,
    requested: usize,
    failed_rack: u64,
) -> bool {
    let selected = select_wal_voters(candidates, local_node, requested);
    if selected.len() != requested {
        // An incomplete placement cannot be installed by the other theorem.
        return true;
    }
    let mut surviving = 0usize;
    let mut removed: Option<usize> = None;
    let mut i = 0usize;
    #[invariant(i@ <= selected@.len())]
    #[invariant(surviving@ + (if removed == None { 0 } else { 1 }) == i@)]
    #[invariant(match removed {
        Some(index) => index@ < i@ && selected@[index@].1 == failed_rack,
        None => true,
    })]
    #[variant(selected@.len() - i@)]
    while i < selected.len() {
        if selected[i].1 == failed_rack {
            if removed.is_some() {
                return false;
            }
            removed = Some(i);
        } else {
            surviving += 1;
        }
        i += 1;
    }
    election_has_quorum(selected.len(), surviving)
}
