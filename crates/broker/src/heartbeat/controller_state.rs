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

/// Every broker the image registers, with the fence the image replicates for
/// it: what [`ControllerLivenessState::seed_brokers`] starts a controller term
/// from.
pub(crate) fn replicated_fences(image: &krabka_metadata::MetadataImage) -> Vec<(u64, bool)> {
    image
        .brokers()
        .map(|broker| {
            (
                broker.node_id.0,
                crate::config_keys::resolve_broker_fenced(image, broker.node_id),
            )
        })
        .collect()
}
