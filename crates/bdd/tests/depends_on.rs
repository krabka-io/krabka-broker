#![cfg(test)]
//! A `Depends on:` line naming a feature file that exists, relative to the
//! feature, compiles: the lint and `scenarios!` agree on where the path
//! points.

use {assert2::assert, krabka_bdd::Given};

/// Kafka's `TopicConfig.MAX_NAME_LENGTH`.
const MAX_NAME_LENGTH: usize = 249;

struct World {
    accepted: Given<bool>,
}

#[rstest::fixture]
fn world() -> World {
    World {
        accepted: Given::new(
            "accepted",
            "When a topic name of <length> characters is checked",
        ),
    }
}

#[krabka_bdd::when("a topic name of {length} characters is checked")]
fn check_name(world: &mut World, length: usize) {
    world
        .accepted
        .set("t".repeat(length).len() <= MAX_NAME_LENGTH);
}

#[krabka_bdd::then("the name is accepted")]
fn name_accepted(world: &World) {
    assert!(*world.accepted.get());
}

#[krabka_bdd::then("the name is refused")]
fn name_refused(world: &World) {
    assert!(!*world.accepted.get());
}

krabka_bdd::scenarios!("tests/features/depends_on", fixtures = [world: World]);
