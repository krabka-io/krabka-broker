//! Break-glass proposal admission and deterministic selection.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// First fail-closed reason that prevents a proposal from being spent.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum BreakGlassAdmission {
    Withdrawn,
    Consumed,
    Expired,
    NotEnoughApprovals,
    Unsigned,
    Usable,
}

/// Apply the break-glass lifecycle and approval checks in reporting order.
#[ensures((result == BreakGlassAdmission::Withdrawn) == withdrawn)]
#[ensures((result == BreakGlassAdmission::Consumed) == (!withdrawn && consumed))]
#[ensures((result == BreakGlassAdmission::Expired) == (!withdrawn && !consumed && expired))]
#[ensures((result == BreakGlassAdmission::NotEnoughApprovals) ==
    (!withdrawn && !consumed && !expired && held@ < required@))]
#[ensures((result == BreakGlassAdmission::Unsigned) ==
    (!withdrawn && !consumed && !expired && held@ >= required@
        && signature_required && !all_signed))]
#[ensures((result == BreakGlassAdmission::Usable) ==
    (!withdrawn && !consumed && !expired && held@ >= required@
        && (!signature_required || all_signed)))]
#[allow(
    clippy::fn_params_excessive_bools,
    reason = "the proof classifies independent proposal lifecycle facts"
)]
#[must_use]
pub fn break_glass_admission(
    withdrawn: bool,
    consumed: bool,
    expired: bool,
    held: usize,
    required: usize,
    signature_required: bool,
    all_signed: bool,
) -> BreakGlassAdmission {
    if withdrawn {
        BreakGlassAdmission::Withdrawn
    } else if consumed {
        BreakGlassAdmission::Consumed
    } else if expired {
        BreakGlassAdmission::Expired
    } else if held < required {
        BreakGlassAdmission::NotEnoughApprovals
    } else if signature_required && !all_signed {
        BreakGlassAdmission::Unsigned
    } else {
        BreakGlassAdmission::Usable
    }
}

/// Select the earliest `(expiry, UUID high bits, UUID low bits)` key.
#[ensures((result == None) == (candidates@.len() == 0))]
#[ensures(match result {
    None => true,
    Some(index) => index@ < candidates@.len()
        && (forall<j: Int> 0 <= j && j < candidates@.len() ==>
            candidates@[index@].0@ <= candidates@[j].0@)
        && (forall<j: Int> 0 <= j && j < candidates@.len()
            && candidates@[index@].0@ == candidates@[j].0@ ==>
            candidates@[index@].1@ <= candidates@[j].1@)
        && (forall<j: Int> 0 <= j && j < candidates@.len()
            && candidates@[index@].0@ == candidates@[j].0@
            && candidates@[index@].1@ == candidates@[j].1@ ==>
            candidates@[index@].2@ <= candidates@[j].2@),
})]
#[allow(
    clippy::len_zero,
    reason = "Creusot 0.13 has no contract for slice::is_empty"
)]
#[must_use]
pub fn select_break_glass_candidate(candidates: &[(i64, u64, u64)]) -> Option<usize> {
    if candidates.len() == 0 {
        return None;
    }
    let mut best = 0usize;
    let mut i = 1usize;
    #[invariant(1 <= i@ && i@ <= candidates@.len())]
    #[invariant(best@ < i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        candidates@[best@].0@ <= candidates@[j].0@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@
        && candidates@[best@].0@ == candidates@[j].0@ ==>
        candidates@[best@].1@ <= candidates@[j].1@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@
        && candidates@[best@].0@ == candidates@[j].0@
        && candidates@[best@].1@ == candidates@[j].1@ ==>
        candidates@[best@].2@ <= candidates@[j].2@)]
    #[variant(candidates@.len() - i@)]
    while i < candidates.len() {
        let candidate = candidates[i];
        let current = candidates[best];
        if candidate.0 < current.0
            || (candidate.0 == current.0 && candidate.1 < current.1)
            || (candidate.0 == current.0 && candidate.1 == current.1 && candidate.2 < current.2)
        {
            best = i;
        }
        i += 1;
    }
    Some(best)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn break_glass_checks_are_ordered_and_fail_closed() {
        use BreakGlassAdmission::{
            Consumed, Expired, NotEnoughApprovals, Unsigned, Usable, Withdrawn,
        };

        assert2::assert!(break_glass_admission(true, true, true, 0, 2, true, false) == Withdrawn);
        assert2::assert!(break_glass_admission(false, true, true, 0, 2, true, false) == Consumed);
        assert2::assert!(break_glass_admission(false, false, true, 0, 2, true, false) == Expired);
        assert2::assert!(
            break_glass_admission(false, false, false, 1, 2, true, true) == NotEnoughApprovals
        );
        assert2::assert!(break_glass_admission(false, false, false, 2, 2, true, false) == Unsigned);
        assert2::assert!(break_glass_admission(false, false, false, 2, 2, true, true) == Usable);
    }

    #[test]
    fn proposal_selection_uses_expiry_then_uuid() {
        assert2::assert!(select_break_glass_candidate(&[]) == None);
        assert2::assert!(select_break_glass_candidate(&[(20, 0, 0), (10, 9, 9)]) == Some(1));
        assert2::assert!(select_break_glass_candidate(&[(10, 4, 9), (10, 3, 99)]) == Some(1));
        assert2::assert!(select_break_glass_candidate(&[(10, 3, 9), (10, 3, 8)]) == Some(1));
    }
}
