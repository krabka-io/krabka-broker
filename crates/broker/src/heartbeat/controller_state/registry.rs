//! The liveness registry itself: the per-broker entry, the states it moves
//! between, and the constructors that build the registry.
//!
//! The behaviour over these types is split by concern into the sibling
//! modules, which reach the fields from here.

use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use krabka_units::{Time, convert::TimeExt as _};
use tokio::sync::Mutex;

use super::clock::Clock;
#[cfg(test)]
use super::clock::TestClock;

/// Per-broker liveness state as seen by the controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrokerLivenessState {
    /// Broker has sent a heartbeat within the timeout window.
    Alive,
    /// No heartbeat received within the timeout window.
    Dead,
}

/// An edge transition emitted by [`ControllerLivenessState::tick`] or
/// [`ControllerLivenessState::record_fenced_heartbeat`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LivenessTransition {
    /// Broker was `Dead`; this heartbeat revived it.
    DeadToAlive(u64),
    /// Broker crossed the deadline; marked `Dead`.
    AliveToDead(u64),
}

pub(super) struct BrokerEntry {
    pub(super) last_heartbeat: Instant,
    pub(super) state: BrokerLivenessState,
    pub(super) fenced: bool,
}

/// Controller-side heartbeat registry.
///
/// One instance lives on the `Broker` struct. Handlers call
/// [`record_fenced_heartbeat`](Self::record_fenced_heartbeat) on every incoming
/// `BrokerHeartbeat` RPC. The liveness ticker calls [`tick`](Self::tick)
/// every second to expire stale entries.
pub(crate) struct ControllerLivenessState {
    pub(super) timeout: Duration,
    pub(super) clock: Clock,
    pub(super) brokers: Mutex<HashMap<u64, BrokerEntry>>,
    /// Brokers that signaled `want_shut_down=true` on a recent
    /// heartbeat. The controller tries to move leadership away from
    /// these brokers and returns `should_shut_down=true` once every
    /// partition has been re-led.
    pub(super) wants_shutdown: Mutex<HashSet<u64>>,
}

impl ControllerLivenessState {
    /// Create a new registry with the given heartbeat timeout.
    pub(crate) fn new(timeout: Time) -> Self {
        Self {
            timeout: timeout.to_std(),
            clock: Clock::Real,
            brokers: Mutex::new(HashMap::new()),
            wants_shutdown: Mutex::new(HashSet::new()),
        }
    }

    /// Construct with a test-controlled [`Clock`] so liveness windows are driven
    /// by explicit `advance` calls instead of wall-clock sleeps.
    #[cfg(test)]
    pub(super) fn with_clock(timeout: Duration, clock: Clock) -> Self {
        Self {
            timeout,
            clock,
            brokers: Mutex::new(HashMap::new()),
            wants_shutdown: Mutex::new(HashSet::new()),
        }
    }

    /// Construct with a [`TestClock`]. Tests outside this module use it to
    /// drive a broker to `Dead` through [`tick`](Self::tick) without a
    /// wall-clock sleep.
    #[cfg(test)]
    pub(crate) fn with_test_clock(timeout: Duration, clock: &TestClock) -> Self {
        Self::with_clock(timeout, clock.clock())
    }
}
