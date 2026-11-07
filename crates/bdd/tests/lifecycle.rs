//! `scenarios!` keeps an unfinished scenario from failing the gating run.
//!
//! The `@inprogress` scenario's Then always fails. The suite passes only
//! because the runner treats an `@inprogress` scenario as an expected
//! failure, which would itself fail once the scenario passed.

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
