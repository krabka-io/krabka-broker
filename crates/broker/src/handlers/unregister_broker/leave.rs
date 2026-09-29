//! The partition changes that take a broker out of every ISR before its
//! registration is dropped.
//!
//! Kafka's `ReplicationControlManager.handleBrokerUnregistered` runs
//! `generateLeaderAndIsrUpdates` with the broker to remove, then appends the
//! `UnregisterBrokerRecord`, all in one record list. Every ISR that names the
//! broker loses it, and every partition it leads elects another acceptable
//! replica. That is what the dead-broker failover scan computes, so this
//! module asks it about the broker.

use std::sync::Arc;

use krabka_metadata::{MetadataImage, MetadataRecord, NodeId};

use crate::{
    broker::Broker,
    heartbeat::controller_state::{ControllerLivenessState, replicated_registrations},
};

/// The partition changes that make `node_id` leave every ISR and every
/// leadership another replica can take, as `image` holds them.
///
/// A partition that no other replica can lead is left alone, as it is when the
/// broker is fenced. The liveness sweep hands it to the offset-aware recovery
/// once the registry declares the broker dead.
pub(super) async fn leave_isrs(
    broker: &Broker,
    image: &MetadataImage,
    node_id: NodeId,
) -> Vec<MetadataRecord> {
    let is_controller_leader =
        *broker.controller.watch_leader().borrow() == Some(broker.config.node_id);
    leave_isrs_as(broker, image, node_id, is_controller_leader).await
}

/// [`leave_isrs`] for a node that is, or is not, the active controller.
///
/// Who may take over is Kafka's `ClusterControlManager.isActive`: a registered
/// broker that is neither fenced nor in controlled shutdown. The active
/// controller answers that from its liveness registry, which also knows the
/// brokers that stopped heartbeating. Any other node, a broker-only one
/// included, keeps no such registry, so it reads the fence and the controlled
/// shutdown from the image's registrations.
pub(super) async fn leave_isrs_as(
    broker: &Broker,
    image: &MetadataImage,
    node_id: NodeId,
    is_controller_leader: bool,
) -> Vec<MetadataRecord> {
    let liveness = if is_controller_leader {
        // The registry of this controller term, as `AlterPartition` reads it,
        // so a request served right after a failover does not read the
        // registry that an earlier term left.
        broker
            .liveness
            .seed_term(
                broker.controller.current_controller_epoch(),
                replicated_registrations(image),
            )
            .await;
        Arc::clone(&broker.liveness)
    } else {
        let registrations = ControllerLivenessState::new(broker.config.heartbeat_timeout);
        registrations
            .seed_brokers(replicated_registrations(image))
            .await;
        Arc::new(registrations)
    };
    crate::leader_election::compute_failover_changes(image, node_id, &liveness, &broker.metrics)
        .await
        .changes
}
