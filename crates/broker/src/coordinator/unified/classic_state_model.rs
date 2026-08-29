//! Exhaustive stateright enumeration of the classic consumer-group membership
//! state machine (KIP-345/62). It wraps the real [`super::ClassicGroup`] and
//! drives the real `add_member`, `remove_member`, `complete_rebalance`,
//! `install_assignments`, and `expire_dead_members`, plus the handler's KIP-345
//! fence pre-check (`current_member_id_for_instance`). It covers every
//! interleaving of join, which can be dynamic, static, or fenced, heartbeat,
//! leave, rebalance completion, sync, and session-timeout expiry. It asserts
//! the static-index coherence, single-owner, and static-never-expired
//! invariants. See the design spec at
//! `docs/superpowers/specs/2026-06-14-krabka-classic-group-fencing-model-design.md`.
//!
//! # Module layout
//!
//! This file is the module root. It holds the two checked configurations, one
//! per test. Each child holds one concern: `config` the bounded shape of a run,
//! `state` the enumerated state and its canonical fingerprint, `fixtures` the
//! logical clock and the member builder, `invariants` the two membership
//! predicates every transition asserts, `properties` the stateright
//! [`Model`](stateright::Model) implementation, `runner` the checker bounds,
//! and `fuzz` the randomized proptest companion.

// Each child is declared with an explicit `#[path]`, because this root is
// itself reached through a `#[path]` and so owns its declaring directory.
#[path = "classic_state_model/config.rs"]
mod config;
#[path = "classic_state_model/fixtures.rs"]
mod fixtures;
#[path = "classic_state_model/invariants.rs"]
mod invariants;
#[path = "classic_state_model/properties.rs"]
mod properties;
#[path = "classic_state_model/runner.rs"]
mod runner;
#[path = "classic_state_model/state.rs"]
mod state;

#[cfg(test)]
#[path = "classic_state_model/fuzz.rs"]
mod fuzz;

use self::{config::ClassicModel, runner::run};

#[test]
fn classic_basic() {
    run(
        ClassicModel {
            members: vec!["a", "b"],
            instances: vec!["x"],
            max_clock: 4,
        },
        "classic_basic",
    );
}

#[test]
fn classic_wide() {
    run(
        ClassicModel {
            members: vec!["a", "b", "c"],
            instances: vec!["x", "y"],
            max_clock: 5,
        },
        "classic_wide",
    );
}
