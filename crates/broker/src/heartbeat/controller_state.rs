//! Controller-side liveness tracking for KIP-500 broker heartbeats.
//!
//! `ControllerLivenessState` tracks the last-seen timestamp for every
//! registered broker and drives a periodic liveness ticker that emits
//! `LivenessTransition` events when a broker goes dead or comes alive.
//!
//! One concern per module: `registry` holds the state itself, `clock` holds the
//! time source the windows are measured against, `session` opens, refreshes and
//! expires a broker's heartbeat session, `snapshot` answers the questions the
//! controller's maintenance loops ask, and `shutdown` holds the
//! controlled-shutdown intent.

mod clock;
mod registry;
mod session;
mod shutdown;
mod snapshot;

#[cfg(test)]
pub(crate) use self::clock::TestClock;
pub(crate) use self::registry::{BrokerLivenessState, ControllerLivenessState, LivenessTransition};
