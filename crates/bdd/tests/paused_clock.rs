#![cfg(test)]
//! `runtime = "tokio-current-thread-paused"` starts the scenario's clock
//! paused, as `#[tokio::test(start_paused = true)]` does, so a long sleep
//! finishes as soon as every task waits. The suite also binds one `.feature`
//! file rather than a directory.

use {
    assert2::assert,
    krabka_bdd::Given,
    std::time::{Duration, Instant as WallClock},
    tokio::time::{Instant, sleep},
};

const HOUR: Duration = Duration::from_secs(3600);

const READ: &str = "Given the paused clock has been read";

struct World {
    started: Given<(Instant, WallClock)>,
}

#[rstest::fixture]
fn world() -> World {
    World {
        started: Given::new("started", READ),
    }
}

#[krabka_bdd::given("the paused clock has been read")]
fn clock_read(world: &mut World) { world.started.set((Instant::now(), WallClock::now())); }

#[krabka_bdd::when("a step sleeps for an hour")]
async fn sleeps_an_hour() { sleep(HOUR).await; }

#[krabka_bdd::then("an hour has passed on the paused clock")]
fn an_hour_has_passed(world: &World) {
    let (tokio_start, wall_start) = *world.started.get();
    assert!(tokio_start.elapsed() >= HOUR);
    assert!(wall_start.elapsed() < Duration::from_secs(60));
}

krabka_bdd::scenarios!(
    "tests/features/paused_clock/paused_clock.feature",
    fixtures = [world: World],
    runtime = "tokio-current-thread-paused",
);
