//! Contiguous retention-prefix selection.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct RetentionPrefix {
    pub len: usize,
    pub remaining_size_debt: u64,
}

#[requires(finished@.len() == time_expired@.len())]
#[requires(finished@.len() == sizes@.len())]
#[ensures(result.len@ <= finished@.len())]
#[ensures(result.remaining_size_debt@ <= initial_size_debt@)]
#[ensures(!mutable ==> result.len@ == 0
    && result.remaining_size_debt@ == initial_size_debt@)]
#[ensures(forall<i: Int> 0 <= i && i < result.len@ ==> finished@[i])]
#[ensures(initial_size_debt@ == 0 ==>
    forall<i: Int> 0 <= i && i < result.len@ ==> time_expired@[i])]
#[ensures(mutable && result.len@ < finished@.len() ==>
    !finished@[result.len@]
        || (!time_expired@[result.len@] && result.remaining_size_debt@ == 0))]
#[must_use]
pub fn retention_prefix(
    mutable: bool,
    finished: &[bool],
    time_expired: &[bool],
    sizes: &[u64],
    initial_size_debt: u64,
) -> RetentionPrefix {
    if !mutable {
        return RetentionPrefix {
            len: 0,
            remaining_size_debt: initial_size_debt,
        };
    }
    let mut len = 0usize;
    let mut remaining_size_debt = initial_size_debt;
    #[invariant(len@ <= finished@.len())]
    #[invariant(remaining_size_debt@ <= initial_size_debt@)]
    #[invariant(forall<i: Int> 0 <= i && i < len@ ==> finished@[i])]
    #[invariant(initial_size_debt@ == 0 ==>
        forall<i: Int> 0 <= i && i < len@ ==> time_expired@[i])]
    #[variant(finished@.len() - len@)]
    while len < finished.len() {
        if !finished[len] || (!time_expired[len] && remaining_size_debt == 0) {
            break;
        }
        if remaining_size_debt > 0 {
            remaining_size_debt = remaining_size_debt.saturating_sub(sizes[len]);
        }
        len += 1;
    }
    RetentionPrefix {
        len,
        remaining_size_debt,
    }
}

#[ensures(last_offset == None ==> result == None)]
#[ensures(match last_offset {
    None => true,
    Some(last) => last@ == i64::MAX@ ==> result == None,
})]
#[ensures(match last_offset {
    None => true,
    Some(last) => last@ < i64::MAX@ ==> result != None,
})]
#[ensures(match (last_offset, result) {
    (Some(last), Some(target)) => target@ == last@ + 1,
    _ => true,
})]
#[must_use]
pub fn retention_delete_target(last_offset: Option<i64>) -> Option<i64> {
    match last_offset {
        Some(last) if last < i64::MAX => Some(last + 1),
        Some(_) | None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_selection_is_a_safe_contiguous_prefix() {
        assert2::assert!(
            retention_prefix(
                true,
                &[true, true, true],
                &[true, false, true],
                &[10, 10, 10],
                0
            ) == RetentionPrefix {
                len: 1,
                remaining_size_debt: 0,
            }
        );
        assert2::assert!(
            retention_prefix(true, &[true, true, true], &[false; 3], &[10, 10, 10], 15)
                == RetentionPrefix {
                    len: 2,
                    remaining_size_debt: 0,
                }
        );
        assert2::assert!(retention_prefix(false, &[true], &[true], &[1], 1).len == 0);
        assert2::assert!(
            retention_prefix(true, &[true, false, true], &[true; 3], &[1; 3], 0).len == 1
        );
    }

    #[test]
    fn delete_target_rejects_offset_exhaustion() {
        assert2::assert!(retention_delete_target(None) == None);
        assert2::assert!(retention_delete_target(Some(9)) == Some(10));
        assert2::assert!(retention_delete_target(Some(i64::MAX)) == None);
    }
}
