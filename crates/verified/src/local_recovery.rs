//! Local-log segment-chain and torn-tail recovery decisions.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::ensures;
#[cfg(creusot)]
use creusot_std::prelude::{DeepModel, Int, invariant, logic};

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct LocalRecoveryStep {
    pub valid_end: u64,
    pub last_offset: i64,
    pub next_offset: i64,
}

/// What `Log::open` observes at one base offset that carries a compaction
/// `.swap` file, from a single directory listing.
///
/// Compaction writes its survivor segment under `.cleaned` names, fsyncs it,
/// renames the sidecars and then the log from `.cleaned` to `.swap`, and only
/// after that rename is durable deletes the segments it replaces. So a
/// `.log.swap` with no `.log.cleaned` beside it is a complete, durable segment,
/// and it may already be the only copy of the records it holds.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct LocalRecoverySwapFacts {
    /// `<base>.log.cleaned` exists: the rewrite, or its rename to `.swap`,
    /// never finished.
    pub log_cleaned_exists: bool,
    /// `<base>.log.swap` exists.
    pub log_swap_exists: bool,
    /// `<base>.log` exists.
    pub final_log_exists: bool,
}

/// Crash-recovery action for one base offset that carries `.swap` files.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum LocalRecoverySwapAction {
    /// The swap never committed: delete its `.swap` files. No segment it
    /// would replace has been touched.
    AbortSwap,
    /// The swap committed: delete every segment it replaces, then rename the
    /// swap into place. This is Kafka's `LogLoader.load` second and third
    /// passes.
    CompleteSwap,
    /// The log rename already finished; promote the remaining sidecars.
    PromoteSidecars,
    /// Sidecar swaps with no log in any form.
    Reject,
}

/// A `.log.swap` is authoritative once it exists without its `.log.cleaned`.
///
/// Kafka's `LocalLog.replaceSegments` renames `.cleaned` to `.swap` before it
/// deletes a single old segment, and `LogLoader.load` completes every such
/// swap rather than discarding it, because the old segments may be gone.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn swap_committed(facts: LocalRecoverySwapFacts) -> bool {
    pearlite! { facts.log_swap_exists && !facts.log_cleaned_exists }
}

/// Classify one swap set: abort it only while it is uncommitted, and complete
/// it whenever it is committed, whether or not the original log still exists.
///
/// Aborting is safe only because the host deletes no replaced segment before
/// the `.cleaned`-to-`.swap` rename of the log is durable (host
/// responsibility: `atomic_swap` fsyncs the directory between the two steps).
#[ensures((result == LocalRecoverySwapAction::AbortSwap) == facts.log_cleaned_exists)]
#[ensures((result == LocalRecoverySwapAction::CompleteSwap) == swap_committed(facts))]
#[ensures((result == LocalRecoverySwapAction::PromoteSidecars)
    == (!facts.log_cleaned_exists && !facts.log_swap_exists && facts.final_log_exists))]
#[ensures((result == LocalRecoverySwapAction::Reject)
    == (!facts.log_cleaned_exists && !facts.log_swap_exists && !facts.final_log_exists))]
#[must_use]
pub const fn local_recovery_swap_action(facts: LocalRecoverySwapFacts) -> LocalRecoverySwapAction {
    if facts.log_cleaned_exists {
        LocalRecoverySwapAction::AbortSwap
    } else if facts.log_swap_exists {
        LocalRecoverySwapAction::CompleteSwap
    } else if facts.final_log_exists {
        LocalRecoverySwapAction::PromoteSidecars
    } else {
        LocalRecoverySwapAction::Reject
    }
}

/// Whether completing a swap based at `swap_base`, whose records end before
/// `swap_next`, replaces the segment based at `segment_base`.
///
/// This is `LogLoader.load`'s second pass: a segment whose base lies in
/// `[swap_base, swap_next)` was compacted into the swap and is deleted. The
/// swap's own base is always replaced, even by an empty swap, because the
/// swap is renamed onto it.
#[ensures(result == (segment_base@ == swap_base@
    || (swap_base@ < segment_base@ && segment_base@ < swap_next@)))]
#[must_use]
pub const fn local_recovery_swap_replaces(
    swap_base: i64,
    swap_next: i64,
    segment_base: i64,
) -> bool {
    segment_base == swap_base || (swap_base < segment_base && segment_base < swap_next)
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

/// The exclusive frontier of a recovered segment, relative to its base, when
/// it fits Kafka's 32-bit relative-offset index.
///
/// The host uses the result as an admission check on tail recovery: a segment
/// whose recovered records reach past the `u32` index range is corrupt. The
/// frontier is returned as `i64` so that one executable body is the one
/// Creusot proves; its range is what the contract pins.
#[ensures(match result {
    Some(frontier) => segment_base@ >= 0
        && last_offset@ >= segment_base@ - 1
        && frontier@ == last_offset@ + 1 - segment_base@
        && 0 <= frontier@
        && frontier@ <= u32::MAX@,
    None => segment_base@ < 0
        || last_offset@ < segment_base@ - 1
        || last_offset@ + 1 - segment_base@ > u32::MAX@,
})]
#[must_use]
pub fn local_recovery_index_frontier(segment_base: i64, last_offset: i64) -> Option<i64> {
    if segment_base < 0 || last_offset < segment_base - 1 {
        return None;
    }
    // `segment_base >= 0`, so the subtraction cannot overflow, and the
    // increment happens only once the result is inside the `u32` range.
    let relative_last = last_offset - segment_base;
    if relative_last >= i64::from(u32::MAX) {
        None
    } else {
        Some(relative_last + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LocalRecoverySwapAction, LocalRecoverySwapFacts, local_recovery_batch_step,
        local_recovery_index_frontier, local_recovery_sealed_last, local_recovery_segment_chain,
        local_recovery_swap_action, local_recovery_swap_replaces,
    };

    #[test]
    fn segment_chain_and_sealed_boundary_are_exact() {
        assert2::check!(local_recovery_segment_chain(&[]));
        assert2::check!(local_recovery_segment_chain(&[0, 10, i64::MAX]));
        assert2::check!(!local_recovery_segment_chain(&[-1, 10]));
        assert2::check!(!local_recovery_segment_chain(&[0, 10, 10]));
        assert2::check!(local_recovery_sealed_last(0, 10) == Some(9));
        assert2::check!(local_recovery_sealed_last(5, 10) == Some(9));
        assert2::check!(local_recovery_sealed_last(-1, 10) == None);
        assert2::check!(local_recovery_sealed_last(10, 10) == None);
    }

    /// Every directory state a crash can leave at a swap's base, named by the
    /// compaction step it interrupted.
    #[test]
    fn swap_action_completes_every_committed_swap() {
        use LocalRecoverySwapAction::{AbortSwap, CompleteSwap, PromoteSidecars, Reject};

        for (stage, (log_cleaned_exists, log_swap_exists, final_log_exists), expected) in [
            (
                "rewrite or sidecar rename in flight",
                (true, false, true),
                AbortSwap,
            ),
            (
                "stale cleaned log beside a swap",
                (true, true, true),
                AbortSwap,
            ),
            (
                "cleaned log with no original",
                (true, false, false),
                AbortSwap,
            ),
            (
                "cleaned log and swap, no original",
                (true, true, false),
                AbortSwap,
            ),
            (
                "committed, originals untouched",
                (false, true, true),
                CompleteSwap,
            ),
            (
                "committed, base original deleted",
                (false, true, false),
                CompleteSwap,
            ),
            (
                "log promoted, sidecars pending",
                (false, false, true),
                PromoteSidecars,
            ),
            ("sidecars with no log at all", (false, false, false), Reject),
        ] {
            let facts = LocalRecoverySwapFacts {
                log_cleaned_exists,
                log_swap_exists,
                final_log_exists,
            };
            assert2::check!(local_recovery_swap_action(facts) == expected, "{stage}");
        }
    }

    /// A swap at 0 whose records end before 20 replaces the segments at 0 and
    /// 10, and never the next segment at 20 or one below its base.
    #[test]
    fn swap_replaces_exactly_the_segments_it_covers() {
        for (swap_base, swap_next, segment_base, expected) in [
            (0, 20, 0, true),
            (0, 20, 10, true),
            (0, 20, 19, true),
            (0, 20, 20, false),
            (10, 20, 0, false),
            (10, 10, 10, true),
            (10, 10, 11, false),
        ] {
            assert2::check!(
                local_recovery_swap_replaces(swap_base, swap_next, segment_base) == expected,
                "swap [{swap_base}, {swap_next}) vs segment {segment_base}"
            );
        }
    }

    #[test]
    fn batch_step_keeps_only_bounded_progress() {
        let step = local_recovery_batch_step(100, 200, 10, 12, 2, 50).unwrap();
        assert2::check!(step.valid_end == 150);
        assert2::check!(step.last_offset == 14);
        assert2::check!(step.next_offset == 15);
        assert2::check!(local_recovery_batch_step(100, 150, 10, 12, 2, 50).is_some());
        assert2::check!(local_recovery_batch_step(100, 200, 10, 10, 0, 0) == None);
        assert2::check!(local_recovery_batch_step(100, 149, 10, 12, 2, 50) == None);
        assert2::check!(local_recovery_batch_step(100, 200, 13, 12, 2, 50) == None);
        assert2::check!(local_recovery_batch_step(100, 200, 10, 10, -1, 50) == None);
        assert2::check!(local_recovery_batch_step(u64::MAX, u64::MAX, 0, 0, 0, 1) == None);
        assert2::check!(local_recovery_batch_step(0, u64::MAX, i64::MAX, i64::MAX, 0, 1) == None);
    }

    #[test]
    fn index_frontier_covers_empty_boundary_and_overflow() {
        let u32_max = i64::from(u32::MAX);
        for (segment_base, last_offset, expected) in [
            (-1, 0, None),
            (0, 0, Some(1)),
            (10, 9, Some(0)),
            (10, 20, Some(11)),
            (10, 8, None),
            (0, i64::MAX, None),
            (0, u32_max - 1, Some(u32_max)),
            (0, u32_max, None),
            (5, u32_max + 4, Some(u32_max)),
            (5, u32_max + 5, None),
        ] {
            assert2::check!(
                local_recovery_index_frontier(segment_base, last_offset) == expected,
                "base {segment_base}, last {last_offset}"
            );
        }
    }
}
