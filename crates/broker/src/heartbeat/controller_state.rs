//! Controller-side liveness tracking for KIP-500 broker heartbeats.
//!
//! `ControllerLivenessState` tracks the last-seen timestamp for every
//! registered broker and drives a periodic liveness ticker that emits
//! `LivenessTransition` events when a broker goes dead or comes alive.
//!
//! One concern per module: `registry` holds the state itself, `clock` holds the
//! time source the windows are measured against, `session` opens, refreshes and
//! expires a broker's heartbeat session, `snapshot` answers the questions the
//! controller's maintenance loops ask, and `shutdown` holds the heartbeat state
//! machine: fenced, unfenced, and controlled shutdown.

mod clock;
mod registry;
mod session;
mod shutdown;
mod snapshot;

#[cfg(test)]
pub(crate) use self::clock::TestClock;
pub(crate) use self::{
    registry::{BrokerLivenessState, ControllerLivenessState, LivenessTransition},
    shutdown::{BrokerControlState, HeartbeatFacts, HeartbeatWants, next_broker_state},
};

/// The fence and controlled shutdown the metadata log holds for one broker's
/// registration, which `RegisterBrokerRecord` and
/// `BrokerRegistrationChangeRecord` replay into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReplicatedRegistration {
    pub(crate) node_id: u64,
    pub(crate) fenced: bool,
    pub(crate) in_controlled_shutdown: bool,
}

impl ReplicatedRegistration {
    /// An unfenced registration that is not in controlled shutdown.
    #[cfg(test)]
    pub(crate) const fn unfenced(node_id: u64) -> Self {
        Self {
            node_id,
            fenced: false,
            in_controlled_shutdown: false,
        }
    }

    /// A fenced registration that is not in controlled shutdown.
    #[cfg(test)]
    pub(crate) const fn fenced(node_id: u64) -> Self {
        Self {
            node_id,
            fenced: true,
            in_controlled_shutdown: false,
        }
    }
}

/// Every broker the image registers, with the fence and controlled shutdown
/// its registration holds: what [`ControllerLivenessState::seed_brokers`]
/// starts a controller term from.
pub(crate) fn replicated_registrations(
    image: &krabka_metadata::MetadataImage,
) -> Vec<ReplicatedRegistration> {
    image
        .brokers()
        .map(|broker| ReplicatedRegistration {
            node_id: broker.node_id.0,
            fenced: broker.fenced,
            in_controlled_shutdown: broker.in_controlled_shutdown,
        })
        .collect()
}
