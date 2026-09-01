//! Partition-reassignment transition decisions.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// The next safe phase of one in-flight partition reassignment.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum ReassignmentAction {
    WaitForReplication,
    WaitForLeader,
    Handoff(usize),
    Complete,
}

/// Wait for new replicas, hand leadership to the first eligible target, or
/// complete while the existing leader remains in the target assignment.
#[ensures(!additions_caught_up ==> result == ReassignmentAction::WaitForReplication)]
#[ensures(additions_caught_up && !leader_removed ==> result == ReassignmentAction::Complete)]
#[ensures(match result {
    ReassignmentAction::WaitForLeader => additions_caught_up
        && leader_removed
        && (forall<i: Int> 0 <= i && i < eligible_handoffs@.len()
            ==> !eligible_handoffs@[i]),
    ReassignmentAction::Handoff(index) => additions_caught_up
        && leader_removed
        && index@ < eligible_handoffs@.len()
        && eligible_handoffs@[index@]
        && (forall<i: Int> 0 <= i && i < index@ ==> !eligible_handoffs@[i]),
    _ => true,
})]
#[must_use]
pub fn reassignment_action(
    additions_caught_up: bool,
    leader_removed: bool,
    eligible_handoffs: &[bool],
) -> ReassignmentAction {
    if !additions_caught_up {
        return ReassignmentAction::WaitForReplication;
    }
    if !leader_removed {
        return ReassignmentAction::Complete;
    }
    let mut i = 0usize;
    #[invariant(i@ <= eligible_handoffs@.len())]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> !eligible_handoffs@[j])]
    #[variant(eligible_handoffs@.len() - i@)]
    while i < eligible_handoffs.len() {
        if eligible_handoffs[i] {
            return ReassignmentAction::Handoff(i);
        }
        i += 1;
    }
    ReassignmentAction::WaitForLeader
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reassignment_phase_table_is_fail_closed() {
        use ReassignmentAction::{Complete, Handoff, WaitForLeader, WaitForReplication};

        assert2::assert!(reassignment_action(false, false, &[true]) == WaitForReplication);
        assert2::assert!(reassignment_action(false, true, &[true]) == WaitForReplication);
        assert2::assert!(reassignment_action(true, false, &[]) == Complete);
        assert2::assert!(reassignment_action(true, true, &[false, true, true]) == Handoff(1));
        assert2::assert!(reassignment_action(true, true, &[false]) == WaitForLeader);
    }
}
