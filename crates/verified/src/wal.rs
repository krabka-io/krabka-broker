//! Diskless WAL admission decisions.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Result of authorizing and epoch-fencing one diskless WAL Fetch.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum WalFetchAdmission {
    Denied,
    FencedLeaderEpoch,
    UnknownLeaderEpoch,
    Serve,
}

#[cfg(creusot)]
#[logic]
fn wal_fetch_authorized(
    authenticated_node: Option<u64>,
    claimed_node: u64,
    local_node: u64,
    voters: Seq<u64>,
) -> bool {
    pearlite! {
        authenticated_node == Some(claimed_node)
            && voters.len() > 0
            && voters[0] == local_node
            && exists<i: Int> 0 <= i && i < voters.len() && voters[i] == claimed_node
    }
}

#[ensures(result == (exists<i: Int>
    0 <= i && i < voters@.len() && voters@[i] == node))]
fn contains_voter(voters: &[u64], node: u64) -> bool {
    let mut i = 0;
    #[cfg_attr(creusot, invariant(i@ <= voters@.len()))]
    #[cfg_attr(creusot, invariant(forall<k: Int> 0 <= k && k < i@ ==> voters@[k] != node))]
    #[cfg_attr(creusot, variant(voters@.len() - i@))]
    while i < voters.len() {
        if voters[i] == node {
            return true;
        }
        i += 1;
    }
    false
}

/// Select the first registered/racked broker whose node and rack are unused.
/// When `require_local` is set, only the local broker is eligible.
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < candidates@.len()
    ==> candidates@[i].0@ < candidates@[j].0@)]
#[ensures(match result {
    None => forall<i: Int> 0 <= i && i < candidates@.len() ==>
        (exists<j: Int> 0 <= j && j < used_nodes@.len()
            && used_nodes@[j] == candidates@[i].0)
        || (exists<j: Int> 0 <= j && j < used_racks@.len()
            && used_racks@[j] == candidates@[i].1)
        || (require_local && candidates@[i].0 != local_node),
    Some(index) => index@ < candidates@.len()
        && (forall<j: Int> 0 <= j && j < used_nodes@.len()
            ==> used_nodes@[j] != candidates@[index@].0)
        && (forall<j: Int> 0 <= j && j < used_racks@.len()
            ==> used_racks@[j] != candidates@[index@].1)
        && (!require_local || candidates@[index@].0 == local_node)
        && (forall<i: Int> 0 <= i && i < index@ ==>
            (exists<j: Int> 0 <= j && j < used_nodes@.len()
                && used_nodes@[j] == candidates@[i].0)
            || (exists<j: Int> 0 <= j && j < used_racks@.len()
                && used_racks@[j] == candidates@[i].1)
            || (require_local && candidates@[i].0 != local_node)),
})]
#[must_use]
pub fn select_wal_voter_index(
    candidates: &[(u64, u64)],
    used_nodes: &[u64],
    used_racks: &[u64],
    local_node: u64,
    require_local: bool,
) -> Option<usize> {
    let mut i = 0usize;
    #[invariant(i@ <= candidates@.len())]
    #[invariant(forall<k: Int> 0 <= k && k < i@ ==>
        (exists<j: Int> 0 <= j && j < used_nodes@.len()
            && used_nodes@[j] == candidates@[k].0)
        || (exists<j: Int> 0 <= j && j < used_racks@.len()
            && used_racks@[j] == candidates@[k].1)
        || (require_local && candidates@[k].0 != local_node))]
    #[variant(candidates@.len() - i@)]
    while i < candidates.len() {
        let candidate = candidates[i];
        if !contains_voter(used_nodes, candidate.0)
            && !contains_voter(used_racks, candidate.1)
            && (!require_local || candidate.0 == local_node)
        {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Authorize a diskless WAL Fetch and classify its leader epoch.
///
/// Authorization deliberately precedes epoch classification, so an
/// unauthenticated caller learns no placement epoch.
#[ensures((result == WalFetchAdmission::Denied)
    == !wal_fetch_authorized(authenticated_node, claimed_node, local_node, voters@))]
#[ensures((result == WalFetchAdmission::FencedLeaderEpoch)
    == (wal_fetch_authorized(authenticated_node, claimed_node, local_node, voters@)
        && request_epoch@ >= 0 && request_epoch@ < leader_epoch@))]
#[ensures((result == WalFetchAdmission::UnknownLeaderEpoch)
    == (wal_fetch_authorized(authenticated_node, claimed_node, local_node, voters@)
        && request_epoch@ >= 0 && request_epoch@ > leader_epoch@))]
#[ensures((result == WalFetchAdmission::Serve)
    == (wal_fetch_authorized(authenticated_node, claimed_node, local_node, voters@)
        && (request_epoch@ < 0 || request_epoch@ == leader_epoch@)))]
#[must_use]
pub fn wal_fetch_admission(
    authenticated_node: Option<u64>,
    claimed_node: u64,
    local_node: u64,
    voters: &[u64],
    request_epoch: i32,
    leader_epoch: i32,
) -> WalFetchAdmission {
    if authenticated_node != Some(claimed_node)
        || voters.first() != Some(&local_node)
        || !contains_voter(voters, claimed_node)
    {
        return WalFetchAdmission::Denied;
    }
    if request_epoch < 0 || request_epoch == leader_epoch {
        return WalFetchAdmission::Serve;
    }
    if request_epoch < leader_epoch {
        WalFetchAdmission::FencedLeaderEpoch
    } else {
        WalFetchAdmission::UnknownLeaderEpoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_fetch_admission_fails_closed_and_classifies_epochs() {
        let voters = [1, 2, 3];
        for (authenticated, claimed, local, epoch, expected) in [
            (None, 2, 1, 8, WalFetchAdmission::Denied),
            (Some(3), 2, 1, 8, WalFetchAdmission::Denied),
            (Some(2), 2, 9, 8, WalFetchAdmission::Denied),
            (Some(4), 4, 1, 8, WalFetchAdmission::Denied),
            (Some(2), 2, 1, -1, WalFetchAdmission::Serve),
            (Some(2), 2, 1, 8, WalFetchAdmission::Serve),
            (Some(2), 2, 1, 7, WalFetchAdmission::FencedLeaderEpoch),
            (Some(2), 2, 1, 9, WalFetchAdmission::UnknownLeaderEpoch),
        ] {
            assert2::assert!(
                wal_fetch_admission(authenticated, claimed, local, &voters, epoch, 8) == expected
            );
        }
    }

    #[test]
    fn wal_voter_selection_is_local_first_and_rack_distinct() {
        let candidates = [(1, 10), (2, 20), (3, 10), (4, 30)];
        assert2::assert!(select_wal_voter_index(&candidates, &[], &[], 2, true) == Some(1));
        assert2::assert!(select_wal_voter_index(&candidates, &[2], &[20], 2, false) == Some(0));
        assert2::assert!(
            select_wal_voter_index(&candidates, &[1, 2], &[10, 20], 2, false) == Some(3)
        );
        assert2::assert!(
            select_wal_voter_index(&candidates, &[1, 2, 4], &[10, 20, 30], 2, false) == None
        );
    }
}
