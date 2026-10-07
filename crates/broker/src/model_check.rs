//! Shared runner for the crate's exhaustive Stateright models.
//!
//! Every model test checks the same way: a breadth-first search under a depth
//! cap and a state-count target, then a check that neither cap truncated it.
//! [`run_bfs`] holds that sequence. Each caller still pins its own unique-state
//! count and calls `assert_properties`.

krabka_macros::bounded_bfs!(run_bfs);
