//! Shared runner for the crate's exhaustive Stateright models.
//!
//! Every model test checks the same way: a breadth-first search under a depth
//! cap and a state-count target, then a check that neither cap truncated it.
//! [`run_bfs`] holds that sequence. Each caller supplies its own unique-state
//! pin to [`assert_pinned_count`] and calls `assert_properties`.

use std::{fmt::Debug, hash::Hash};

krabka_macros::bounded_bfs!(run_bfs);

/// Reject changes to the reachable state set using each model's independent pin.
pub fn assert_pinned_count(actual: usize, expected: usize, label: &str) {
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        actual == expected,
        "[{label}] unique-state count moved: the reachable set of this model changed"
    );
}

/// Run an exhaustive model with its independent unique-state bound and pin.
pub fn check_model<M>(model: M, label: &str, limits: (usize, usize, usize), pinned: usize)
where
    M: stateright::Model + Send + Sync + 'static,
    M::State: Debug + Hash + Send + Sync + Clone + PartialEq + 'static,
    M::Action: Debug + Clone + PartialEq,
{
    use stateright::Checker as _;
    let (depth, states, unique) = limits;
    let checker = run_bfs(model, label, depth, states);
    assert2::assert!(
        checker.unique_state_count() < unique,
        "[{label}] unique-state bound exceeded"
    );
    assert_pinned_count(checker.unique_state_count(), pinned, label);
    checker.assert_properties();
}
