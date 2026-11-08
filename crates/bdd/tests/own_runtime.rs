#![cfg(test)]
//! A sync step runs outside the scenario runtime's `block_on`, so a fixture
//! can own a Tokio runtime and a sync step can block on it, as with plain
//! rstest-bdd. Inside `block_on`, Tokio panics with "Cannot start a runtime
//! from within a runtime", and dropping the fixture's runtime panics too.

use {
    assert2::assert,
    krabka_bdd::Given,
    tokio::runtime::{Builder, Runtime},
};

const OWNED: &str = "Given a fixture owns a Tokio runtime of its own";

struct World {
    runtime: Given<Runtime>,
    result: Given<u32>,
}

#[rstest::fixture]
fn world() -> World {
    World {
        runtime: Given::new("runtime", OWNED),
        result: Given::new(
            "result",
            "When a sync step blocks on a task of that runtime",
        ),
    }
}

#[krabka_bdd::given("a fixture owns a Tokio runtime of its own")]
fn owns_runtime(world: &mut World) {
    world
        .runtime
        .set(Builder::new_current_thread().enable_all().build().unwrap());
}

#[krabka_bdd::when("a sync step blocks on a task of that runtime")]
fn blocks_on_task(world: &mut World) {
    let result = world.runtime.get().block_on(async { 6 * 7 });
    world.result.set(result);
}

#[krabka_bdd::then("the task has handed the step its result")]
fn handed_result(world: &World) {
    assert!(*world.result.get() == 42);
}

krabka_bdd::scenarios!("tests/features/own_runtime", fixtures = [world: World]);
