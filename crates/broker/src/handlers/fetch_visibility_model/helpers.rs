use assert2::assert;

use super::*;

/// Kafka's reported high watermark: the partition's own, whoever fetches.
/// Neither the fetch shape, the log end nor the delivery watermark is an input:
/// the broker reports the true high watermark on a scheduled topic too, and a
/// follower learns the committed bound rather than the leader's log end.
fn response_hw(s: &VisState) -> i64 {
    s.hw
}

/// Kafka's reported last stable offset, `UnifiedLog.lastStableOffset`: the
/// first unstable offset capped at the high watermark, whoever fetches.
fn response_lso(s: &VisState) -> i64 {
    s.lso.min(s.hw)
}

/// KIP-227: a watermark advance must never lower the reported HW/LSO. The
/// delivery watermark is not an input to either formula, so an advance of it
/// must leave both exactly where they were.
pub(super) fn assert_monotonic(old: &VisState, new: &VisState) {
    assert!(
        response_hw(new) >= response_hw(old),
        "response_hw regressed on advance"
    );
    assert!(
        response_lso(new) >= response_lso(old),
        "response_lso regressed on advance"
    );
}

pub(super) fn assert_fetch_contract(
    s: &VisState,
    is_follower: bool,
    read_committed: bool,
    fetch_offset: i64,
    w: &super::super::VisibilityWindow,
) {
    // Unwrap the `Offset` window fields into this model's `i64` world.
    let limit_offset = w.limit_offset.0;
    let win_response_hw = w.response_hw.0;
    let win_response_lso = w.response_lso.0;
    let effective_lso = w.effective_lso.0;
    // Valid targets.
    assert!(limit_offset >= 0 && win_response_hw >= 0 && win_response_lso >= 0);
    // out_of_range / empty correctness.
    assert!(w.out_of_range == (fetch_offset < s.log_start));
    let upper = if is_follower {
        s.log_end
    } else {
        s.deliverable
    };
    if !w.out_of_range {
        assert!(w.empty == (fetch_offset >= upper));
    }
    // Response single-source-of-truth contract (OOR and success paths share
    // it). Neither field moves with the fetch shape or the delivery watermark,
    // and neither ever names an offset beyond the high watermark.
    assert!(win_response_hw == response_hw(s));
    assert!(win_response_lso == response_lso(s));
    assert!(win_response_lso <= win_response_hw && win_response_hw <= s.hw);
    if is_follower {
        // Follower bound: serve up to the log-end (>= hw), ungated by the
        // delivery watermark, so a scheduled record replicates and counts
        // toward the ISR before any consumer can see it. The bytes run past
        // the HW; the reported HW does not.
        assert!(limit_offset == s.log_end && limit_offset >= s.hw);
    } else {
        // No dirty read: never expose beyond the high-watermark.
        assert!(limit_offset <= s.hw, "consumer fetch exposed beyond HW");
        // KFC-1: never expose a record before it is due.
        assert!(
            limit_offset <= s.deliverable,
            "consumer fetch exposed beyond the delivery watermark"
        );
        if read_committed {
            assert!(effective_lso == s.lso.min(s.hw));
            assert!(limit_offset <= s.lso.min(s.hw));
        }
    }
}
