//! `scenarios!` keeps `@formulated` and `@inprogress` scenarios out of the
//! gating run.
//!
//! The `@inprogress` scenario's Then always fails. The suite passes only
//! because rstest-bdd marks that scenario `#[ignore]`, and
//! `cargo test -- --ignored` still runs it and goes red.

#![cfg(test)]

use {
    krabka_bdd::Given,
    rstest_bdd_macros::{then, when},
};

struct World {
    reached: Given<bool>,
}

#[rstest::fixture]
fn world() -> World {
    let step = "When a scenario reaches its outcome";
    World {
        reached: Given::new("reached", step),
    }
}

#[when("a scenario reaches its outcome")]
fn reach_outcome(world: &mut World) { world.reached.set(true); }

#[then("the outcome holds")]
fn outcome_holds(world: &World) {
    assert2::assert!(*world.reached.get());
}

#[then("the behavior it documents is not built yet")]
fn not_built_yet() {
    panic!("an @inprogress scenario ran in the gating run");
}

krabka_bdd::scenarios!("tests/features/lifecycle", fixtures = [world: World]);
