#![cfg(test)]
//! [`StubClock`] as a Stub Dependency: a test-local session takes its clock
//! as the `Arc<dyn MonotonicClock>` production code takes, and a step moves
//! the stub across the session's deadline.

use {
    assert2::assert,
    krabka_bdd::{Given, ScenarioContext, StepParam, StubClock},
    qubit_clock::{MonotonicClock, MonotonicInstant},
    rstest::rstest,
    std::{sync::Arc, time::Duration},
};

/// A session that expires once `timeout` has passed since it opened.
struct ExpiringSession {
    clock: Arc<dyn MonotonicClock>,
    opened: MonotonicInstant,
    timeout: Duration,
}

impl ExpiringSession {
    fn open(clock: Arc<dyn MonotonicClock>, timeout: Duration) -> Self {
        let opened = clock.now();
        Self {
            clock,
            opened,
            timeout,
        }
    }

    fn state(&self) -> SessionState {
        let elapsed = self.clock.now().duration_since(self.opened).unwrap();
        if elapsed >= self.timeout {
            SessionState::Expired
        } else {
            SessionState::Live
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, StepParam)]
enum SessionState {
    Live,
    Expired,
}

#[derive(ScenarioContext)]
struct SessionScenario {
    clock: StubClock,
    #[introduced_by("Given a session with a timeout of {seconds} seconds has been opened")]
    session: Given<ExpiringSession>,
}

#[rstest::fixture]
fn session_scenario() -> SessionScenario {
    SessionScenario {
        ..SessionScenario::default()
    }
}

#[krabka_bdd::given("a session with a timeout of {seconds} seconds has been opened")]
fn a_session_has_been_opened(session_scenario: &mut SessionScenario, seconds: u64) {
    let session = ExpiringSession::open(
        session_scenario.clock.monotonic(),
        Duration::from_secs(seconds),
    );
    session_scenario.set_session(session);
}

#[krabka_bdd::when("{seconds} seconds pass")]
fn seconds_pass(session_scenario: &SessionScenario, seconds: u64) {
    session_scenario.clock.advance(Duration::from_secs(seconds));
}

#[krabka_bdd::then("the session is {state}")]
fn the_session_is(session_scenario: &SessionScenario, state: SessionState) {
    assert!(session_scenario.the_session().state() == state);
}

krabka_bdd::scenarios!(
    "tests/features/clock_scenarios",
    fixtures = [session_scenario: SessionScenario],
);

#[rstest]
#[case::opened(&[], SessionState::Live)]
#[case::one_step_short(&[Duration::from_secs(4), Duration::from_millis(999)], SessionState::Live)]
#[case::in_two_steps(&[Duration::from_secs(4), Duration::from_secs(1)], SessionState::Expired)]
fn the_session_reads_the_stub_time(#[case] passes: &[Duration], #[case] expected: SessionState) {
    let clock = StubClock::default();
    let session = ExpiringSession::open(clock.monotonic(), Duration::from_secs(5));
    for &by in passes {
        clock.advance(by);
    }
    assert!(session.state() == expected);
}
