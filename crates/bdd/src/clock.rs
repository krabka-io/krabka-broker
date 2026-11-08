//! Stub Dependency for time: a manual clock that a scenario advances.
//!
//! Production code takes its clocks as `Arc<dyn MonotonicClock>` and
//! `Arc<dyn WallClock>` from `qubit-clock`. A scenario about a deadline hands
//! it a [`StubClock`] instead, whose time moves only when a step says so:
//! "When 5 seconds pass" calls [`StubClock::advance`], and the deadline check
//! that follows is exact, with no sleep and no flake.
//!
//! # Pair a stub with a periodic real run
//!
//! A stub proves the logic against the time the scenario states; it cannot
//! prove that the real clock, timer and dependencies behave as the stub
//! does. Pair each scenario that relies on a stub with a periodic real run:
//! the same feature, tagged `@docker`, whose `#[krabka_bdd::before(tags =
//! "@docker")]` hook wires the real clock and the real dependencies in place
//! of the stubs, run on a schedule rather than on every push. The scenario
//! text stays the same; only the infrastructure layer changes.

use {
    qubit_clock::{ManualMonotonicClock, ManualWallClock, MonotonicClock, WallClock},
    std::{
        sync::Arc,
        time::{Duration, SystemTime},
    },
};

/// A manual monotonic clock and a wall clock projected from it, for a
/// scenario that moves time by hand.
///
/// [`advance`](Self::advance) moves both clocks forward;
/// [`set_wall`](Self::set_wall) re-anchors the wall clock alone, as an
/// operator changing the system time does. [`monotonic`](Self::monotonic) and
/// [`wall`](Self::wall) hand out the trait objects production code takes.
///
/// # Examples
///
/// ```
/// use {
///     assert2::assert,
///     krabka_bdd::StubClock,
///     std::time::{Duration, SystemTime},
/// };
///
/// let clock = StubClock::default();
/// let monotonic = clock.monotonic();
/// let deadline = monotonic.deadline_after(Duration::from_secs(5)).unwrap();
///
/// clock.advance(Duration::from_secs(5));
/// assert!(monotonic.now() >= deadline);
/// assert!(clock.wall().now() == SystemTime::UNIX_EPOCH + Duration::from_secs(5));
/// ```
#[derive(Debug)]
pub struct StubClock {
    monotonic: Arc<ManualMonotonicClock>,
    wall: Arc<ManualWallClock>,
}

impl Default for StubClock {
    /// A clock at its origin, with the wall clock at the Unix epoch.
    fn default() -> Self { Self::starting_at(SystemTime::UNIX_EPOCH) }
}

impl StubClock {
    /// Creates a clock at its origin, with the wall clock reading `wall`.
    #[must_use]
    pub fn starting_at(wall: SystemTime) -> Self {
        let monotonic = ManualMonotonicClock::new_shared();
        let wall = monotonic.new_wall_clock(wall);
        Self { monotonic, wall }
    }

    /// Moves both clocks forward by `by`, waking any timer whose deadline it
    /// reaches.
    ///
    /// # Panics
    ///
    /// Panics when the monotonic clock cannot represent the new time.
    #[track_caller]
    pub fn advance(&self, by: Duration) {
        if let Err(error) = self.monotonic.advance(by) {
            panic!("the stub clock cannot advance by {by:?}: {error}");
        }
    }

    /// Sets the wall clock to `wall` without moving the monotonic clock.
    pub fn set_wall(&self, wall: SystemTime) { self.wall.reanchor(wall); }

    /// How far the clock has advanced since it was created.
    #[must_use]
    pub fn elapsed(&self) -> Duration { self.monotonic.now().elapsed_since_origin() }

    /// The monotonic clock, as production code takes it.
    #[must_use]
    pub fn monotonic(&self) -> Arc<dyn MonotonicClock> {
        Arc::<ManualMonotonicClock>::clone(&self.monotonic)
    }

    /// The wall clock, as production code takes it.
    #[must_use]
    pub fn wall(&self) -> Arc<dyn WallClock> { Arc::<ManualWallClock>::clone(&self.wall) }

    /// The manual monotonic clock itself, for its waiter and deadline
    /// helpers such as `wait_for_waiters` and `advance_to_next_deadline`.
    #[must_use]
    pub const fn manual_monotonic(&self) -> &Arc<ManualMonotonicClock> { &self.monotonic }

    /// The manual wall clock itself.
    #[must_use]
    pub const fn manual_wall(&self) -> &Arc<ManualWallClock> { &self.wall }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, rstest::rstest, rstest_bdd::panic_message, std::panic};

    const START: Duration = Duration::from_secs(1_700_000_000);

    /// One step a test takes on the clock.
    #[derive(Debug, Clone, Copy)]
    enum Move {
        Advance(Duration),
        SetWall(Duration),
    }

    /// What both clocks read: the elapsed monotonic time and the wall time
    /// since the epoch.
    #[derive(Debug, PartialEq, Eq)]
    struct Reading {
        elapsed: Duration,
        wall: Duration,
    }

    fn read(clock: &StubClock) -> Reading {
        Reading {
            elapsed: clock.monotonic().now().elapsed_since_origin(),
            wall: clock
                .wall()
                .now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap(),
        }
    }

    #[rstest]
    #[case::untouched(&[], Reading { elapsed: Duration::ZERO, wall: START })]
    #[case::advance(
        &[Move::Advance(Duration::from_secs(5))],
        Reading { elapsed: Duration::from_secs(5), wall: START + Duration::from_secs(5) },
    )]
    #[case::advances_add_up(
        &[Move::Advance(Duration::from_secs(2)), Move::Advance(Duration::from_millis(500))],
        Reading { elapsed: Duration::from_millis(2500), wall: START + Duration::from_millis(2500) },
    )]
    #[case::set_wall_moves_only_the_wall(
        &[Move::SetWall(Duration::from_secs(10))],
        Reading { elapsed: Duration::ZERO, wall: Duration::from_secs(10) },
    )]
    #[case::advance_after_set_wall(
        &[Move::Advance(Duration::from_secs(1)), Move::SetWall(Duration::from_secs(10)),
          Move::Advance(Duration::from_secs(3))],
        Reading { elapsed: Duration::from_secs(4), wall: Duration::from_secs(13) },
    )]
    fn moves_change_both_readings(#[case] moves: &[Move], #[case] expected: Reading) {
        let clock = StubClock::starting_at(SystemTime::UNIX_EPOCH + START);
        for step in moves {
            match *step {
                Move::Advance(by) => clock.advance(by),
                Move::SetWall(since_epoch) => clock.set_wall(SystemTime::UNIX_EPOCH + since_epoch),
            }
        }
        assert!(read(&clock) == expected);
        assert!(clock.elapsed() == expected.elapsed);
    }

    #[test]
    fn default_starts_at_the_epoch() {
        assert!(
            read(&StubClock::default())
                == Reading {
                    elapsed: Duration::ZERO,
                    wall: Duration::ZERO,
                }
        );
    }

    #[test]
    fn the_handed_out_clocks_share_the_stub_timeline() {
        let clock = StubClock::default();
        let monotonic = clock.monotonic();
        let deadline = monotonic.deadline_after(Duration::from_secs(5)).unwrap();
        clock.advance(Duration::from_secs(4));
        assert!(monotonic.now() < deadline);
        clock
            .manual_monotonic()
            .advance(Duration::from_secs(1))
            .unwrap();
        assert!(monotonic.now() == deadline);
        assert!(clock.manual_wall().now() == clock.wall().now());
    }

    #[test]
    fn advancing_past_the_representable_time_panics_naming_the_step() {
        let clock = StubClock::default();
        clock.advance(Duration::MAX);
        let payload = panic::catch_unwind(|| clock.advance(Duration::from_secs(1))).unwrap_err();
        assert!(
            panic_message(payload.as_ref())
                == "the stub clock cannot advance by 1s: monotonic instant overflow"
        );
    }
}
