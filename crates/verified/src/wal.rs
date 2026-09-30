//! Diskless WAL admission decisions.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(bases.len() == lasts.len())]
#[requires(0 <= index && index <= bases.len())]
#[variant(bases.len() - index)]
fn exact_wal_batch_suffix(
    bases: Seq<i64>,
    lasts: Seq<i64>,
    index: Int,
    expected: Int,
    target: Int,
) -> bool {
    pearlite! {
        if index == bases.len() {
            expected == target
        } else {
            bases[index]@ == expected
                && bases[index] <= lasts[index]
                && lasts[index] < i64::MAX
                && exact_wal_batch_suffix(
                    bases,
                    lasts,
                    index + 1,
                    lasts[index]@ + 1,
                    target,
                )
        }
    }
}

// cargo-mutants: #[cfg(creusot)] mathematical layout; not compiled at runtime.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn wal_batch_layout(bases: Seq<i64>, lasts: Seq<i64>, start: Int, target: Int) -> bool {
    pearlite! {
        bases.len() == lasts.len() && if start == target {
            bases.len() == 0
        } else {
            (forall<i: Int> 0 <= i && i < bases.len() ==>
                bases[i] <= lasts[i] && lasts[i] < i64::MAX
                && bases[i]@ == if i == 0 { start } else { lasts[i - 1]@ + 1 })
            && target == if bases.len() == 0 { start } else { lasts[bases.len() - 1]@ + 1 }
        }
    }
}

/// Check that decoded WAL batches cover exactly one contiguous half-open range.
#[cfg_attr(creusot, ensures(result == (bases@.len() == lasts@.len()
    && if start@ == target@ {
        bases@.len() == 0
    } else {
        exact_wal_batch_suffix(bases@, lasts@, 0, start@, target@)
    })))]
#[cfg_attr(creusot, ensures(result == wal_batch_layout(bases@, lasts@, start@, target@)))]
#[cfg_attr(creusot, ensures(result ==> forall<i: Int> 0 <= i && i < bases@.len()
    ==> start@ <= bases@[i]@ && bases@[i]@ <= lasts@[i]@ && lasts@[i]@ < target@))]
#[must_use]
pub fn exact_wal_batch_range(bases: &[i64], lasts: &[i64], start: i64, target: i64) -> bool {
    if bases.len() != lasts.len() {
        return false;
    }
    if start == target {
        return matches!(bases.len(), 0);
    }

    let mut expected = start;
    let mut i = 0usize;
    #[cfg_attr(creusot, invariant(i@ <= bases@.len()))]
    #[cfg_attr(creusot, invariant(bases@.len() == lasts@.len()))]
    #[cfg_attr(creusot, invariant(exact_wal_batch_suffix(bases@, lasts@, 0, start@, target@)
        == exact_wal_batch_suffix(bases@, lasts@, i@, expected@, target@)))]
    #[cfg_attr(creusot, invariant(expected@ == if i@ == 0 { start@ } else { lasts@[i@ - 1]@ + 1 }))]
    #[cfg_attr(creusot, invariant(start@ <= expected@))]
    #[cfg_attr(creusot, invariant(forall<j: Int> 0 <= j && j < i@ ==>
        start@ <= bases@[j]@ && bases@[j]@ <= lasts@[j]@ && lasts@[j]@ < expected@
        && lasts@[j] < i64::MAX
        && bases@[j]@ == if j == 0 { start@ } else { lasts@[j - 1]@ + 1 }))]
    #[cfg_attr(creusot, variant(bases@.len() - i@))]
    while i < bases.len() {
        if bases[i] != expected || bases[i] > lasts[i] {
            return false;
        }
        let Some(next) = lasts[i].checked_add(1) else {
            return false;
        };
        expected = next;
        i += 1;
    }
    expected == target
}

/// Whole batches may cover an interior logical floor. They must still form
/// an exact physical prefix ending at `target`; the first batch contains `start`.
#[cfg_attr(creusot, ensures((match result { None => false, Some(_) => true }) == (
    0 <= start@ && start@ <= target@ && bases@.len() == lasts@.len()
    && if start == target { bases@.len() == 0 } else {
        bases@.len() > 0 && 0 <= bases@[0]@ && bases@[0]@ <= start@ && start@ <= lasts@[0]@
        && wal_batch_layout(bases@, lasts@, bases@[0]@, target@)
    }
)))]
#[cfg_attr(creusot, ensures(match result {
    None => true,
    Some(physical) => physical@ == (if bases@.len() == 0 { start@ } else { bases@[0]@ })
        && 0 <= physical@ && physical@ <= start@ && start@ <= target@
        && wal_batch_layout(bases@, lasts@, physical@, target@)
        && (forall<i: Int> 0 <= i && i < bases@.len() ==>
            physical@ <= bases@[i]@ && bases@[i]@ <= lasts@[i]@ && lasts@[i]@ < target@),
}))]
#[must_use]
pub fn wal_covering_batch_range(
    bases: &[i64],
    lasts: &[i64],
    start: i64,
    target: i64,
) -> Option<i64> {
    if start < 0 || start > target || bases.len() != lasts.len() {
        return None;
    }
    if start == target {
        return if matches!(bases.len(), 0) {
            Some(start)
        } else {
            None
        };
    }
    if matches!(bases.len(), 0) || bases[0] < 0 || bases[0] > start || lasts[0] < start {
        return None;
    }
    if !exact_wal_batch_range(bases, lasts, bases[0], target) {
        return None;
    }
    Some(bases[0])
}

/// A nonempty checkpoint must end at the observed whole batch's successor.
/// Its logical floor may lie inside a batch; an empty range is reset at that floor.
#[ensures(result == (0 <= recovered_start@ && recovered_start@ <= start@
    && start@ <= end@ && end@ <= recovered_end@
    && (start == end || (match observed_last { Some(last) => last@ == end@ - 1, None => false }))))]
#[must_use]
pub fn wal_checkpoint_range_valid(
    recovered_start: i64,
    recovered_end: i64,
    start: i64,
    end: i64,
    observed_last: Option<i64>,
) -> bool {
    0 <= recovered_start
        && recovered_start <= start
        && start <= end
        && end <= recovered_end
        && (start == end || observed_last == end.checked_sub(1))
}

/// Compare actual encoded batch bytes and their inclusive offset coordinates.
/// Matching lengths or frontiers alone do not establish the same records.
#[ensures(result == (left.0 == right.0 && left.1 == right.1 && left.2@ == right.2@))]
#[must_use]
pub fn wal_batch_equal(left: (i64, i64, &[u8]), right: (i64, i64, &[u8])) -> bool {
    if left.0 != right.0 || left.1 != right.1 || left.2.len() != right.2.len() {
        return false;
    }
    let mut i = 0usize;
    #[invariant(i@ <= left.2@.len())]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> left.2@[j] == right.2@[j])]
    #[variant(left.2@.len() - i@)]
    while i < left.2.len() {
        if left.2[i] != right.2[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// Result of authorizing and epoch-fencing one diskless WAL Fetch.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum WalFetchAdmission {
    Denied,
    FencedLeaderEpoch,
    UnknownLeaderEpoch,
    Serve,
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
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

/// Whether `value` occurs in `values`; used for voter ids and rack ids alike.
#[ensures(result == (exists<i: Int>
    0 <= i && i < values@.len() && values@[i] == value))]
fn contains(values: &[u64], value: u64) -> bool {
    let mut i = 0;
    #[cfg_attr(creusot, invariant(i@ <= values@.len()))]
    #[cfg_attr(creusot, invariant(forall<k: Int> 0 <= k && k < i@ ==> values@[k] != value))]
    #[cfg_attr(creusot, variant(values@.len() - i@))]
    while i < values.len() {
        if values[i] == value {
            return true;
        }
        i += 1;
    }
    false
}

/// A `(node, rack)` candidate may not become a WAL voter when its node or its
/// rack is already used, or when only the local broker is eligible and the
/// candidate is another broker.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
fn voter_blocked(
    candidate: (u64, u64),
    used_nodes: Seq<u64>,
    used_racks: Seq<u64>,
    local_node: u64,
    require_local: bool,
) -> bool {
    pearlite! {
        (exists<j: Int> 0 <= j && j < used_nodes.len() && used_nodes[j] == candidate.0)
            || (exists<j: Int> 0 <= j && j < used_racks.len() && used_racks[j] == candidate.1)
            || (require_local && candidate.0 != local_node)
    }
}

/// Select the first candidate, in the caller's order, whose node and rack are
/// both unused. When `require_local` is set, only the local broker is
/// eligible.
#[ensures(match result {
    None => forall<i: Int> 0 <= i && i < candidates@.len() ==>
        voter_blocked(candidates@[i], used_nodes@, used_racks@, local_node, require_local),
    Some(index) => index@ < candidates@.len()
        && !voter_blocked(candidates@[index@], used_nodes@, used_racks@, local_node, require_local)
        && forall<i: Int> 0 <= i && i < index@ ==>
            voter_blocked(candidates@[i], used_nodes@, used_racks@, local_node, require_local),
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
        voter_blocked(candidates@[k], used_nodes@, used_racks@, local_node, require_local))]
    #[variant(candidates@.len() - i@)]
    while i < candidates.len() {
        let candidate = candidates[i];
        if !contains(used_nodes, candidate.0)
            && !contains(used_racks, candidate.1)
            && (!require_local || candidate.0 == local_node)
        {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Assemble the existing greedy, local-first placement across distinct nodes
/// and racks. An incomplete result must be rejected by the quorum installer.
#[ensures(result@.len() <= requested@)]
#[ensures((result@.len() == 0) == (requested@ == 0
    || forall<i: Int> 0 <= i && i < candidates@.len() ==> candidates@[i].0 != local_node))]
#[ensures(result@.len() > 0 ==> result@[0].0 == local_node)]
#[ensures(forall<i: Int> 0 <= i && i < result@.len()
    ==> exists<j: Int> 0 <= j && j < candidates@.len() && result@[i] == candidates@[j])]
#[ensures(forall<i: Int, j: Int> 0 <= i && i < j && j < result@.len()
    ==> result@[i].0 != result@[j].0 && result@[i].1 != result@[j].1)]
#[ensures(0 < result@.len() && result@.len() < requested@ ==>
    forall<i: Int> 0 <= i && i < candidates@.len() ==>
        exists<j: Int> 0 <= j && j < result@.len()
            && (candidates@[i].0 == result@[j].0 || candidates@[i].1 == result@[j].1))]
#[must_use]
pub fn select_wal_voters(
    candidates: &[(u64, u64)],
    local_node: u64,
    requested: usize,
) -> Vec<(u64, u64)> {
    let mut selected: Vec<(u64, u64)> = Vec::new();
    let mut nodes: Vec<u64> = Vec::new();
    let mut racks: Vec<u64> = Vec::new();
    #[invariant(selected@.len() <= requested@)]
    #[invariant(nodes@.len() == selected@.len() && racks@.len() == selected@.len())]
    #[invariant(selected@.len() > 0 ==> selected@[0].0 == local_node)]
    #[invariant(forall<i: Int> 0 <= i && i < selected@.len()
        ==> selected@[i].0 == nodes@[i] && selected@[i].1 == racks@[i])]
    #[invariant(forall<i: Int> 0 <= i && i < selected@.len()
        ==> exists<j: Int> 0 <= j && j < candidates@.len() && selected@[i] == candidates@[j])]
    #[invariant(forall<i: Int, j: Int> 0 <= i && i < j && j < selected@.len()
        ==> selected@[i].0 != selected@[j].0 && selected@[i].1 != selected@[j].1)]
    #[variant(requested@ - selected@.len())]
    while selected.len() < requested {
        let Some(index) = select_wal_voter_index(
            candidates,
            &nodes,
            &racks,
            local_node,
            matches!(selected.len(), 0),
        ) else {
            return selected;
        };
        let candidate = candidates[index];
        selected.push(candidate);
        nodes.push(candidate.0);
        racks.push(candidate.1);
    }
    selected
}

/// Admit exactly a complete, nonempty, local-first set of distinct voter IDs.
/// A repeated ID cannot vote twice through the same durable-offset map entry.
#[ensures(result == (expected@ > 0 && voters@.len() == expected@
    && voters@[0] == local_node
    && forall<i: Int, j: Int> 0 <= i && i < j && j < voters@.len()
        ==> voters@[i] != voters@[j]))]
#[must_use]
pub fn wal_voter_set_valid(voters: &[u64], local_node: u64, expected: usize) -> bool {
    if expected == 0 || voters.len() != expected || voters[0] != local_node {
        return false;
    }
    let mut i = 0usize;
    #[invariant(i@ <= voters@.len())]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < k && k < i@ ==> voters@[j] != voters@[k])]
    #[variant(voters@.len() - i@)]
    while i < voters.len() {
        if contains(&voters[..i], voters[i]) {
            return false;
        }
        i += 1;
    }
    true
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
        || !contains(voters, claimed_node)
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

    proptest::proptest! {
        #[test]
        fn batch_equality_matches_independent_tuple_oracle(
            left_base in proptest::prelude::any::<i64>(),
            left_last in proptest::prelude::any::<i64>(),
            left_bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64),
            right_base in proptest::prelude::any::<i64>(),
            right_last in proptest::prelude::any::<i64>(),
            right_bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64),
        ) {
            let left = (left_base, left_last, left_bytes.as_slice());
            let right = (right_base, right_last, right_bytes.as_slice());
            assert2::assert!(wal_batch_equal(left, right) == (left == right));
            assert2::assert!(wal_batch_equal(left, left));
            if !left_bytes.is_empty() {
                let mut changed = left_bytes.clone();
                let last = changed.len() - 1;
                changed[last] ^= 1;
                assert2::assert!(!wal_batch_equal(left, (left_base, left_last, &changed)));
            }
        }

        #[test]
        fn full_wal_placement_is_distinct_and_exhausts_eligible_candidates(
            candidates in proptest::collection::vec((0u64..8, 0u64..8), 0..32),
            local in 0u64..8,
            requested in 0usize..12,
        ) {
            let selected = select_wal_voters(&candidates, local, requested);
            let nodes: std::collections::HashSet<_> = selected.iter().map(|(node, _)| *node).collect();
            let racks: std::collections::HashSet<_> = selected.iter().map(|(_, rack)| *rack).collect();
            assert2::assert!(nodes.len() == selected.len() && racks.len() == selected.len());
            assert2::assert!(selected.len() <= requested);
            assert2::assert!(selected.iter().all(|candidate| candidates.contains(candidate)));
            assert2::assert!(selected.is_empty() == (requested == 0 || !candidates.iter().any(|(node, _)| *node == local)));
            if let Some((first, _)) = selected.first() { assert2::assert!(*first == local); }
            if !selected.is_empty() && selected.len() < requested {
                assert2::assert!(candidates.iter().all(|(node, rack)| nodes.contains(node) || racks.contains(rack)));
            }
        }

        #[test]
        fn voter_installation_matches_set_oracle(
            voters in proptest::collection::vec(0u64..8, 0..16),
            local in 0u64..8,
            expected in 0usize..16,
        ) {
            let distinct: std::collections::HashSet<_> = voters.iter().copied().collect();
            let valid = expected > 0 && voters.len() == expected
                && voters.first() == Some(&local) && distinct.len() == voters.len();
            assert2::assert!(wal_voter_set_valid(&voters, local, expected) == valid);
        }
    }

    proptest::proptest! {
        #[test]
        fn covering_batch_ranges_match_an_independent_interval_oracle(
            rows in proptest::collection::vec((-2i64..60, -2i64..60), 0..12),
            start in -2i64..65, target in -2i64..65,
        ) {
            let bases: Vec<_> = rows.iter().map(|row| row.0).collect();
            let lasts: Vec<_> = rows.iter().map(|row| row.1).collect();
            let expected = if start < 0 || target < start { None }
                else if start == target { rows.is_empty().then_some(start) }
                else if let Some(&(first, last)) = rows.first() {
                    let mut cursor = i128::from(first);
                    let mut valid = first >= 0 && first <= start && start <= last;
                    for &(base, last) in &rows {
                        valid &= i128::from(base) == cursor && base <= last && last < i64::MAX;
                        cursor = i128::from(last) + 1;
                    }
                    (valid && cursor == i128::from(target)).then_some(first)
                } else { None };
            assert2::assert!(wal_covering_batch_range(&bases, &lasts, start, target) == expected);
        }
    }

    #[test]
    fn covering_batch_range_preserves_strict_ends_and_extreme_floors() {
        assert2::assert!(wal_covering_batch_range(&[0, 3], &[2, 5], 1, 6) == Some(0));
        assert2::assert!(!exact_wal_batch_range(&[0, 3], &[2, 5], 1, 6));
        assert2::assert!(wal_covering_batch_range(&[0], &[2], 1, 2).is_none());
        assert2::assert!(wal_covering_batch_range(&[0, 4], &[2, 5], 1, 6).is_none());
        assert2::assert!(wal_covering_batch_range(&[0], &[], 1, 3).is_none());
        assert2::assert!(wal_covering_batch_range(&[], &[], i64::MAX, i64::MAX) == Some(i64::MAX));
        assert2::assert!(
            wal_covering_batch_range(&[i64::MAX - 2], &[i64::MAX - 1], i64::MAX - 1, i64::MAX)
                == Some(i64::MAX - 2)
        );
    }

    #[test]
    fn exact_wal_range_rejects_every_discontinuity_and_overflow() {
        assert2::assert!(exact_wal_batch_range(&[], &[], 4, 4));
        assert2::assert!(exact_wal_batch_range(&[4, 6], &[5, 8], 4, 9));
        assert2::assert!(!exact_wal_batch_range(&[4, 7], &[5, 8], 4, 9));
        assert2::assert!(!exact_wal_batch_range(&[4, 5], &[5, 8], 4, 9));
        assert2::assert!(!exact_wal_batch_range(&[6, 4], &[8, 5], 4, 9));
        assert2::assert!(!exact_wal_batch_range(&[4], &[3], 4, 4));
        assert2::assert!(!exact_wal_batch_range(
            &[i64::MAX],
            &[i64::MAX],
            i64::MAX,
            i64::MAX
        ));
        assert2::assert!(exact_wal_batch_range(
            &[i64::MAX - 1],
            &[i64::MAX - 1],
            i64::MAX - 1,
            i64::MAX
        ));
        assert2::assert!(!exact_wal_batch_range(&[4], &[], 4, 5));
    }

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
            (Some(2), 2, 1, 0, WalFetchAdmission::FencedLeaderEpoch),
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
