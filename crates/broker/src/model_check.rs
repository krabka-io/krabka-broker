//! Shared runner for the crate's exhaustive Stateright models.
//!
//! Every model test checks the same way: a breadth-first search under a depth
//! cap and a state-count target, then a check that neither cap truncated it.
//! [`run_bfs`] holds that sequence. Each caller supplies its own unique-state
//! pin to [`assert_pinned_count`] and calls `assert_properties`.

krabka_macros::bounded_bfs!(run_bfs);

/// Reject changes to the reachable state set using each model's independent pin.
pub(crate) fn assert_pinned_count(actual: usize, expected: usize, label: &str) {
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        actual == expected,
        "[{label}] unique-state count moved: the reachable set of this model changed"
    );
}
