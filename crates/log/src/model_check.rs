//! Shared runner for the crate's exhaustive Stateright models.
//!
//! Every model test checks the same way: a breadth-first search under a depth
//! cap and a state-count target, then a check that neither cap truncated it.
//! [`run_bfs`] holds that sequence. Each caller still pins its own unique-state
//! count and calls `assert_properties`.

use std::hash::Hash;

use stateright::{Checker, Model};

/// Runs `model` breadth-first to completion and returns the joined checker.
///
/// It prints one stats line to stderr, tagged with `label`. It fails when the
/// search reached `max_depth` or generated `max_states` states, because a
/// truncated search verified the `always` properties only in part.
pub(crate) fn run_bfs<M>(
    model: M,
    label: &str,
    max_depth: usize,
    max_states: usize,
) -> impl Checker<M>
where
    M: Model + Send + Sync + 'static,
    M::State: Hash + Send + Sync + Clone + PartialEq + 'static,
    M::Action: Clone + PartialEq,
{
    let checker = model
        .checker()
        .target_max_depth(max_depth)
        .target_state_count(max_states)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert2::assert!(
        checker.max_depth() < max_depth,
        "[{label}] hit depth cap {max_depth}: depth-truncated, not exhaustive"
    );
    assert2::assert!(
        checker.state_count() < max_states,
        "[{label}] hit state cap {max_states}: truncated, not exhaustive"
    );
    checker
}
