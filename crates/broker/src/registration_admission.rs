//! The controller's half of a broker registering anew: retiring the previous
//! incarnation of its broker id in the batch that registers the new one.
//!
//! A krabka broker registers itself by submitting its `V1BrokerRegistration`,
//! not through `BrokerRegistration`, so the batch reaches the leader without
//! passing through the `BrokerRegistration` handler. Kafka's
//! `ClusterControlManager.registerBroker` does two things for a new incarnation
//! of a registered broker id that the bare record does not:
//!
//! - it calls `ReplicationControlManager.handleBrokerShutdown` and puts the
//!   records it returns ahead of the `RegisterBrokerRecord`, in the same batch.
//!   On either branch they take the broker out of every ISR and elect another
//!   leader for every partition it led, so the new process comes back as a
//!   fenced follower and never resumes a leadership whose log it may no longer
//!   hold;
//! - it removes the heartbeat session of the previous incarnation and registers
//!   the new one fenced.
//!
//! Without them a broker killed and restarted inside its heartbeat session
//! kept every leadership at the same leader epoch, and its heartbeats kept the
//! old session alive, so the session never expired and nothing failed over
//! (krabka-io/krabka-broker#1450).
//!
//! The raft leader hands every batch that carries a new registration to
//! [`BrokerRegistrationAdmission`], which adds those records with
//! [`compute_reregistration_changes`]. A `BrokerRegistration` the handler
//! already answered passes through it too, and finds nothing left to do: the
//! handler's records are applied to the image the admission decides on.
//!
//! The session check of `registerBroker`, which refuses a new incarnation with
//! `DUPLICATE_BROKER_REGISTRATION` while the previous one still heartbeats,
//! stays with the handler. A self-registration is never refused for it: the
//! batch that registers the new process retires the old one instead.

use {
    crate::{
        broker::Broker, config_keys::RecoveryStrategy,
        controller_admin::BrokerControllerAdminRouter,
        heartbeat::controller_state::ControllerLivenessState,
        leader_election::compute_reregistration_changes, metrics::BrokerMetrics,
        unclean_recovery::RecoveryJob,
    },
    krabka_metadata::{BrokerRegistrationRecord, MetadataImage, MetadataRecord, NodeId},
    krabka_raft::{RaftError, RegistrationAdmission, RegistrationAdmissionFuture},
    std::{sync::Arc, time::Duration},
    uuid::Uuid,
};

/// How long the hand-off of a re-registration's offset-aware recoveries waits
/// for the batch to commit before it gives them up.
const RECOVERY_HANDOFF_TIMEOUT: Duration = Duration::from_secs(30);

/// What admitting one batch decided.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Admitted {
    /// The batch to append: the submitted records, with the changes that retire
    /// each previous incarnation ahead of the registration that replaces it.
    pub(crate) records: Vec<MetadataRecord>,
    /// Each broker the batch registers as a new incarnation, with that
    /// incarnation's id.
    pub(crate) new_incarnations: Vec<(NodeId, Uuid)>,
    /// The partitions to hand to the Unclean Recovery Manager once the batch
    /// commits, as the dead-broker failover hands them over.
    pub(crate) recoveries: Vec<(String, i32, RecoveryStrategy)>,
}

/// The registration `record` makes of a new incarnation of a registered
/// broker: a registration with no epoch yet, of a broker id `image` holds under
/// another incarnation id.
///
/// Kafka's `registerBroker` also runs `handleBrokerShutdown` for a broker id
/// with no registration at all. That id can sit in no ISR and lead nothing,
/// since `UnregisterBroker` takes it out of every ISR in the batch that
/// unregisters it, so a first registration is left as it was submitted.
fn new_incarnation<'a>(
    image: &MetadataImage,
    record: &'a MetadataRecord,
) -> Option<&'a BrokerRegistrationRecord> {
    match record {
        MetadataRecord::V1BrokerRegistration(registration)
            if registration.broker_epoch < 0
                && image.broker(registration.node_id).is_some_and(|previous| {
                    previous.incarnation_id != registration.incarnation_id
                }) =>
        {
            Some(registration)
        }
        _ => None,
    }
}

/// Admit `records` against `image`: put Kafka's `handleBrokerShutdown` changes
/// ahead of every registration of a new incarnation.
///
/// Each decision is made on `image` with the batch's earlier records applied,
/// so the changes the `BrokerRegistration` handler already wrote, or an earlier
/// registration in the same batch, are not written twice. A registration with
/// the incarnation the image holds is the same process registering again and
/// adds nothing, as Kafka's amend does not.
pub(crate) async fn admit(
    image: &MetadataImage,
    records: Vec<MetadataRecord>,
    liveness: &ControllerLivenessState,
    metrics: &BrokerMetrics,
) -> Admitted {
    let mut scratch = image.clone();
    let mut admitted = Admitted::default();
    for record in records {
        if let Some(registration) = new_incarnation(&scratch, &record) {
            let node = registration.node_id;
            let plan = compute_reregistration_changes(&scratch, node, liveness, metrics).await;
            for (topic, partition) in &plan.unavailable {
                tracing::warn!(
                    %topic, partition, node_id = node.0,
                    "re-registering broker led this partition and no live ISR replica can take it; partition unavailable"
                );
            }
            for change in &plan.changes {
                scratch.apply(change);
            }
            admitted.records.extend(plan.changes);
            admitted.recoveries.extend(plan.recoveries);
            admitted
                .new_incarnations
                .push((node, registration.incarnation_id));
        }
        scratch.apply(&record);
        admitted.records.push(record);
    }
    admitted
}

/// Late-bound bridge from the raft leader, which starts before the broker, to
/// the heartbeat registry and the election ladder the admission decides with.
/// It reaches the broker through the controller listener's
/// [`BrokerControllerAdminRouter`], which the broker binds once it is built.
pub(crate) struct BrokerRegistrationAdmission {
    /// This controller's own node id.
    node_id: NodeId,
    /// The controller's published image, which the answer before the broker
    /// binds reads.
    images: tokio::sync::watch::Receiver<Arc<MetadataImage>>,
    router: Arc<BrokerControllerAdminRouter>,
}

impl BrokerRegistrationAdmission {
    pub(crate) const fn new(
        node_id: NodeId,
        images: tokio::sync::watch::Receiver<Arc<MetadataImage>>,
        router: Arc<BrokerControllerAdminRouter>,
    ) -> Self {
        Self {
            node_id,
            images,
            router,
        }
    }

    /// The answer before this node's broker layer exists.
    ///
    /// A batch that retires another broker's incarnation is refused, and the
    /// registering broker retries it: admitting it unchanged would leave that
    /// broker's leaderships with the new process. This node's own registration
    /// is the one this node's startup is waiting on, and its previous
    /// incarnation is the process this one replaced, so it goes through as it
    /// was submitted.
    fn before_bind(&self, records: Vec<MetadataRecord>) -> Result<Vec<MetadataRecord>, RaftError> {
        if retires_another_broker(&self.images.borrow(), self.node_id, &records) {
            return Err(RaftError::ControllerAdmin(
                "broker startup is incomplete".into(),
            ));
        }
        Ok(records)
    }
}

/// Whether `records` register a new incarnation of a broker other than
/// `this_node`. A broker starting for the first time, as every broker of a new
/// cluster does, retires nothing and is let through.
fn retires_another_broker(
    image: &MetadataImage,
    this_node: NodeId,
    records: &[MetadataRecord],
) -> bool {
    records.iter().any(|record| {
        new_incarnation(image, record).is_some_and(|registration| registration.node_id != this_node)
    })
}

/// Hand `recoveries` to the Unclean Recovery Manager once `image` shows
/// `incarnation` registered as `node`, which is when the batch that decided
/// them has committed.
fn hand_off_recoveries(
    broker: &Arc<Broker>,
    (node, incarnation): (NodeId, Uuid),
    recoveries: Vec<(String, i32, RecoveryStrategy)>,
) {
    if recoveries.is_empty() {
        return;
    }
    let mut images = broker.controller.watch_image();
    let recovery = broker.unclean_recovery.clone();
    tokio::spawn(async move {
        let committed = tokio::time::timeout(RECOVERY_HANDOFF_TIMEOUT, async {
            loop {
                if images
                    .borrow_and_update()
                    .broker(node)
                    .is_some_and(|registration| registration.incarnation_id == incarnation)
                {
                    return true;
                }
                if images.changed().await.is_err() {
                    return false;
                }
            }
        })
        .await;
        if committed != Ok(true) {
            return;
        }
        for (topic, partition, strategy) in recoveries {
            recovery
                .enqueue(RecoveryJob {
                    topic,
                    partition,
                    strategy,
                    reply: None,
                    // Nobody asked for this recovery, as for the one an
                    // unclean `BrokerRegistration` starts.
                    proposal: None,
                })
                .await;
        }
    });
}

impl RegistrationAdmission for BrokerRegistrationAdmission {
    fn admit(&self, records: Vec<MetadataRecord>) -> RegistrationAdmissionFuture<'_> {
        Box::pin(async move {
            let Some(broker) = self.router.broker() else {
                return self.before_bind(records);
            };
            let image = broker.controller.current_image();
            let Admitted {
                records,
                new_incarnations,
                recoveries,
            } = admit(&image, records, &broker.liveness, &broker.metrics).await;
            // `ClusterControlManager.registerBroker` removes the session of the
            // previous incarnation and registers the new one fenced.
            for (node, _) in &new_incarnations {
                broker.liveness.replace_incarnation(node.0).await;
            }
            if let Some(first) = new_incarnations.first() {
                hand_off_recoveries(&broker, *first, recoveries);
            }
            Ok(records)
        })
    }
}

#[cfg(test)]
mod tests;
