//! Local-log segment-chain and torn-tail recovery decisions.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct LocalRecoveryStep {
    pub valid_end: u64,
    pub last_offset: i64,
    pub next_offset: i64,
}

/// Crash-recovery action for one discovered `.log.swap` file.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum LocalRecoverySwapAction {
    DiscardSwap,
    PromoteAll,
    PromoteSidecars,
    Reject,
}

/// Classify a discovered swap set by whether its final log and swap log exist.
#[ensures(match result {
    LocalRecoverySwapAction::DiscardSwap => original_log_exists && log_swap_exists,
    LocalRecoverySwapAction::PromoteAll => !original_log_exists && log_swap_exists,
    LocalRecoverySwapAction::PromoteSidecars => original_log_exists && !log_swap_exists,
    LocalRecoverySwapAction::Reject => !original_log_exists && !log_swap_exists,
})]
#[must_use]
pub const fn local_recovery_swap_action(
    original_log_exists: bool,
    log_swap_exists: bool,
) -> LocalRecoverySwapAction {
    if original_log_exists {
        if log_swap_exists {
            LocalRecoverySwapAction::DiscardSwap
        } else {
            LocalRecoverySwapAction::PromoteSidecars
        }
    } else if log_swap_exists {
        LocalRecoverySwapAction::PromoteAll
    } else {
        LocalRecoverySwapAction::Reject
    }
}

/// Validate the discovered segment bases as one strictly ordered,
/// nonnegative chain.
#[ensures(result == (forall<i: Int> 0 <= i && i < bases@.len() ==>
    bases@[i]@ >= 0
        && (i == 0 || bases@[i - 1]@ < bases@[i]@)))]
#[must_use]
pub fn local_recovery_segment_chain(bases: &[i64]) -> bool {
    let mut i = 0usize;
    #[invariant(i@ <= bases@.len())]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        bases@[j]@ >= 0
            && (j == 0 || bases@[j - 1]@ < bases@[j]@))]
    #[variant(bases@.len() - i@)]
    while i < bases.len() {
        if bases[i] < 0 || (i > 0 && bases[i - 1] >= bases[i]) {
            return false;
        }
        i += 1;
    }
    true
}

/// Close a sealed segment exactly one offset before the next segment base.
#[ensures(match result {
    Some(last) => base@ >= 0 && next_base@ > base@ && last@ == next_base@ - 1,
    None => base@ < 0 || next_base@ <= base@,
})]
#[must_use]
pub fn local_recovery_sealed_last(base: i64, next_base: i64) -> Option<i64> {
    if base < 0 || next_base <= base {
        None
    } else {
        Some(next_base - 1)
    }
}

/// Admit one completely decoded batch into the maximal valid tail prefix.
/// Compaction gaps are allowed, but overlap, empty byte progress, file-bound
/// overflow, malformed offsets, and signed offset overflow stop the prefix.
#[ensures(match result {
    Some(step) => position@ <= file_end@
        && encoded_len@ > 0
        && step.valid_end@ == position@ + encoded_len@
        && step.valid_end@ <= file_end@
        && batch_base@ >= expected_offset@
        && last_offset_delta@ >= 0
        && step.last_offset@ == batch_base@ + last_offset_delta@
        && step.next_offset@ == step.last_offset@ + 1
        && step.next_offset@ > expected_offset@,
    None => position@ > file_end@
        || encoded_len@ == 0
        || position@ + encoded_len@ > u64::MAX@
        || position@ + encoded_len@ > file_end@
        || batch_base@ < expected_offset@
        || last_offset_delta@ < 0
        || batch_base@ + last_offset_delta@ > i64::MAX@
        || batch_base@ + last_offset_delta@ + 1 > i64::MAX@,
})]
#[must_use]
pub fn local_recovery_batch_step(
    position: u64,
    file_end: u64,
    expected_offset: i64,
    batch_base: i64,
    last_offset_delta: i32,
    encoded_len: u64,
) -> Option<LocalRecoveryStep> {
    if position > file_end || encoded_len == 0 {
        return None;
    }
    let valid_end = position.checked_add(encoded_len)?;
    if valid_end > file_end {
        return None;
    }
    let next_offset =
        crate::restore::restore_batch_step(expected_offset, batch_base, last_offset_delta)?;
    Some(LocalRecoveryStep {
        valid_end,
        last_offset: next_offset - 1,
        next_offset,
    })
}

/// Convert the recovered inclusive last offset into the exclusive relative
/// frontier used to trim time-index entries.
#[ensures(match result {
    Some(frontier) => segment_base@ >= 0
        && last_offset@ >= segment_base@ - 1
        && last_offset@ < i64::MAX@
        && frontier@ == last_offset@ + 1 - segment_base@
        && frontier@ <= u32::MAX@,
    None => segment_base@ < 0
        || last_offset@ < segment_base@ - 1
        || last_offset@ == i64::MAX@
        || last_offset@ + 1 - segment_base@ > u32::MAX@,
})]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
#[must_use]
pub fn local_recovery_index_frontier(segment_base: i64, last_offset: i64) -> Option<u32> {
    if segment_base < 0 || last_offset < segment_base - 1 {
        return None;
    }
    let next = last_offset.checked_add(1)?;
    let relative = next.checked_sub(segment_base)?;
    if relative > i64::from(u32::MAX) {
        None
    } else {
        Some(relative as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_chain_and_sealed_boundary_are_exact() {
        assert2::check!(local_recovery_segment_chain(&[]));
        assert2::check!(local_recovery_segment_chain(&[0, 10, i64::MAX]));
        assert2::check!(!local_recovery_segment_chain(&[-1, 10]));
        assert2::check!(!local_recovery_segment_chain(&[0, 10, 10]));
        assert2::check!(local_recovery_sealed_last(0, 10) == Some(9));
        assert2::check!(local_recovery_sealed_last(10, 10) == None);
        assert2::check!(
            local_recovery_swap_action(true, true) == LocalRecoverySwapAction::DiscardSwap
        );
        assert2::check!(
            local_recovery_swap_action(false, true) == LocalRecoverySwapAction::PromoteAll
        );
        assert2::check!(
            local_recovery_swap_action(true, false) == LocalRecoverySwapAction::PromoteSidecars
        );
        assert2::check!(
            local_recovery_swap_action(false, false) == LocalRecoverySwapAction::Reject
        );
    }

    #[test]
    fn batch_step_keeps_only_bounded_progress() {
        let step = local_recovery_batch_step(100, 200, 10, 12, 2, 50).unwrap();
        assert2::check!(step.valid_end == 150);
        assert2::check!(step.last_offset == 14);
        assert2::check!(step.next_offset == 15);
        assert2::check!(local_recovery_batch_step(100, 149, 10, 12, 2, 50) == None);
        assert2::check!(local_recovery_batch_step(100, 200, 13, 12, 2, 50) == None);
        assert2::check!(local_recovery_batch_step(100, 200, 10, 10, -1, 50) == None);
        assert2::check!(local_recovery_batch_step(u64::MAX, u64::MAX, 0, 0, 0, 1) == None);
        assert2::check!(local_recovery_batch_step(0, u64::MAX, i64::MAX, i64::MAX, 0, 1) == None);
    }

    #[test]
    fn index_frontier_covers_empty_boundary_and_overflow() {
        assert2::check!(local_recovery_index_frontier(10, 9) == Some(0));
        assert2::check!(local_recovery_index_frontier(10, 20) == Some(11));
        assert2::check!(local_recovery_index_frontier(10, 8) == None);
        assert2::check!(local_recovery_index_frontier(0, i64::MAX) == None);
        assert2::check!(local_recovery_index_frontier(0, i64::from(u32::MAX)) == None);
    }
}
