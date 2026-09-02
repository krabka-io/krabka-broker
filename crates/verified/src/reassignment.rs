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

/// Pointwise membership in a reassignment's union, additions, and removals.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct ReassignmentSetMembership {
    pub in_union: bool,
    pub adding: bool,
    pub removing: bool,
}

/// Classify one unique replica against the current and requested assignments.
#[ensures(result.in_union == (in_current || in_target))]
#[ensures(result.adding == (in_target && !in_current))]
#[ensures(result.removing == (in_current && !in_target))]
#[ensures(!(result.adding && result.removing))]
#[must_use]
pub fn reassignment_set_membership(in_current: bool, in_target: bool) -> ReassignmentSetMembership {
    ReassignmentSetMembership {
        in_union: in_current || in_target,
        adding: in_target && !in_current,
        removing: in_current && !in_target,
    }
}

/// Final mutation admission for either a reassignment start or cancel.
#[ensures(result == (
    readiness.0
        && readiness.1
        && if mode.0 {
            mode.1 && mode.2
        } else {
            start.0 && start.1
        }
))]
#[must_use]
pub fn reassignment_plan_admission(
    mode: (bool, bool, bool),
    start: (bool, bool),
    readiness: (bool, bool),
) -> bool {
    let (is_cancel, cancel_approved, cancel_in_progress) = mode;
    let (target_nonempty_unique_registered, rf_policy_satisfied) = start;
    let (leader_eligible, epoch_available) = readiness;
    leader_eligible
        && epoch_available
        && if is_cancel {
            cancel_approved && cancel_in_progress
        } else {
            target_nonempty_unique_registered && rf_policy_satisfied
        }
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

    #[test]
    fn planning_kernels_cover_set_algebra_and_fences() {
        assert2::assert!(
            reassignment_set_membership(false, false)
                == ReassignmentSetMembership {
                    in_union: false,
                    adding: false,
                    removing: false,
                }
        );
        assert2::assert!(reassignment_set_membership(true, false).removing);
        assert2::assert!(reassignment_set_membership(false, true).adding);
        assert2::assert!(reassignment_set_membership(true, true).in_union);

        assert2::assert!(reassignment_plan_admission(
            (false, false, false),
            (true, true),
            (true, true),
        ));
        assert2::assert!(reassignment_plan_admission(
            (true, true, true),
            (false, false),
            (true, true),
        ));
        for denied in [
            reassignment_plan_admission((false, false, false), (false, true), (true, true)),
            reassignment_plan_admission((false, false, false), (true, false), (true, true)),
            reassignment_plan_admission((true, false, true), (false, false), (true, true)),
            reassignment_plan_admission((true, true, false), (false, false), (true, true)),
            reassignment_plan_admission((false, false, false), (true, true), (false, true)),
            reassignment_plan_admission((false, false, false), (true, true), (true, false)),
        ] {
            assert2::assert!(!denied);
        }
    }
}
