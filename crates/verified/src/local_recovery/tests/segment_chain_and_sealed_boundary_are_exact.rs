use super::*;

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
