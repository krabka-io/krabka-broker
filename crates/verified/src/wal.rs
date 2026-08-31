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
}
