//! The broker's creation of its coordinator topics on first use.
//!
//! This follows Kafka's `DefaultAutoTopicCreationManager`. No broker creates
//! `__consumer_offsets`, `__transaction_state`, `__share_group_state` or
//! `__barrier_state` when it starts. The first request that needs one of them
//! asks for it, and the broker creates it in the background with its
//! configured partition count, replication factor and topic configs. The
//! requester is answered `COORDINATOR_NOT_AVAILABLE` and retries, as Kafka's
//! `KafkaApis.getCoordinator` answers it. A cluster that has fewer live
//! brokers than the configured replication factor gets no topic: the placement
//! refuses the creation with `INVALID_REPLICATION_FACTOR`, and a later request
//! tries again once enough brokers have registered. The replication factor is
//! never lowered to fit the cluster.
//!
//! The creation goes through the same checks as a client's `CreateTopics` row
//! ([`TopicCreation`]): the replica placement, the topic policy and the config
//! validation. It runs without a principal, as Kafka's
//! `TopicCreator.createTopicWithoutPrincipal` does, so no ACL is checked and
//! no controller-mutation quota is charged.
//!
//! One set of in-flight names keeps two concurrent requests from creating the
//! same topic twice: a name that a creation holds is skipped until that
//! creation ends, as Kafka's `filterCreatableTopics` skips it.

use std::sync::{
    Arc, OnceLock, Weak,
    atomic::{AtomicUsize, Ordering},
};

use dashmap::DashMap;
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    create_topics_response::CreatableTopicResult,
};

use crate::{
    broker::Broker, config::BrokerConfig, coordinator::bootstrap::OFFSETS_TOPIC,
    handlers::create_topics::TopicCreation,
};

/// The broker's auto-creation of its coordinator topics.
///
/// The coordinators that need a topic start before the [`Broker`] exists, so
/// the broker binds itself here once it is built. A request made before then
/// creates nothing, and its caller retries as it does for any other
/// `COORDINATOR_NOT_AVAILABLE`.
#[derive(Debug, Default)]
pub struct AutoTopicCreation {
    /// A weak pointer, so the broker that owns this component is not kept
    /// alive by it.
    broker: OnceLock<Weak<Broker>>,
    /// The names whose creation is in flight (Kafka's `inflightTopics`).
    in_flight: DashMap<String, ()>,
    /// The number of creations this component has started.
    started: AtomicUsize,
}

impl AutoTopicCreation {
    /// Binds the broker that the creations run on.
    ///
    /// # Errors
    ///
    /// Returns an error when a broker is bound already.
    pub fn bind(&self, broker: &Arc<Broker>) -> Result<(), &'static str> {
        self.broker
            .set(Arc::downgrade(broker))
            .map_err(|_| "auto topic creation already bound")
    }

    /// Marks `name` in flight. Returns `false` when another creation of the
    /// name is in flight already.
    pub fn begin(&self, name: &str) -> bool {
        self.in_flight.insert(name.to_owned(), ()).is_none()
    }

    /// Clears the in-flight mark of `name`.
    pub fn end(&self, name: &str) {
        self.in_flight.remove(name);
    }

    /// Test-only: whether a creation of `name` is in flight.
    #[cfg(test)]
    #[must_use]
    pub fn is_in_flight(&self, name: &str) -> bool {
        self.in_flight.contains_key(name)
    }

    /// Test-only: the number of creations [`Self::request`] has started.
    #[cfg(test)]
    #[must_use]
    pub fn started(&self) -> usize {
        self.started.load(Ordering::Relaxed)
    }

    /// Asks for the coordinator topic `name` to be created in the background,
    /// as Kafka's `createTopics` does with no request context.
    ///
    /// The call returns at once. It does nothing when `name` is not a
    /// coordinator topic, when no broker is bound yet, or when another
    /// creation of the name is in flight. A failed creation is logged, and
    /// the next request tries again.
    pub fn request(self: &Arc<Self>, name: &str) {
        let Some(broker) = self.broker.get().and_then(Weak::upgrade) else {
            tracing::debug!(topic = name, "auto topic creation is not bound yet");
            return;
        };
        let Some(topic) = coordinator_topic(&broker.config, name) else {
            return;
        };
        if !self.begin(name) {
            return;
        }
        self.started.fetch_add(1, Ordering::Relaxed);
        let this = Arc::clone(self);
        let name = name.to_owned();
        tokio::spawn(async move {
            let result = create(&broker, topic).await;
            this.end(&name);
            match result {
                Ok(()) => tracing::info!(topic = %name, "auto-created a coordinator topic"),
                Err(row) => tracing::warn!(
                    topic = %name,
                    error_code = row.error_code,
                    error_message = ?row.error_message,
                    "auto topic creation failed"
                ),
            }
        });
    }
}

/// The `CreatableTopic` Kafka's `DefaultAutoTopicCreationManager.creatableTopic`
/// builds for a coordinator topic: its configured partition count,
/// replication factor and topic configs. `None` for any other name.
#[must_use]
pub fn coordinator_topic(config: &BrokerConfig, name: &str) -> Option<CreatableTopic> {
    let (num_partitions, replication_factor, configs) = match name {
        OFFSETS_TOPIC => (
            config.offsets_topic_num_partitions,
            config.offsets_topic_replication_factor,
            crate::coordinator::bootstrap::offsets_topic_configs(config),
        ),
        crate::txn::bootstrap::TOPIC => (
            config.transaction_state_num_partitions,
            config.transaction_state_replication_factor,
            crate::txn::bootstrap::topic_configs(
                config.transaction_state_segment_bytes,
                config.transaction_state_min_isr,
            ),
        ),
        crate::share_coordinator::bootstrap::TOPIC => (
            config.share_coordinator.state_topic_num_partitions,
            config.share_coordinator.state_topic_replication_factor,
            crate::share_coordinator::bootstrap::topic_configs(&config.share_coordinator),
        ),
        crate::barrier::STATE_TOPIC => (
            config.barrier_state_num_partitions,
            config.barrier_state_replication_factor,
            crate::barrier::bootstrap::topic_configs(),
        ),
        _ => return None,
    };
    Some(CreatableTopic {
        name: name.to_owned(),
        num_partitions,
        replication_factor,
        configs: configs
            .into_iter()
            .map(|(name, value)| CreatableTopicConfig {
                name,
                value: Some(value),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    })
}

/// Creates `topic` on `broker` without a principal, and waits for the
/// commit.
///
/// # Errors
///
/// Returns the refused row, in the shape `CreateTopics` answers it.
pub async fn create(
    broker: &Broker,
    topic: CreatableTopic,
) -> Result<(), Box<CreatableTopicResult>> {
    let image = broker.controller.current_image();
    TopicCreation::new(broker, &image, false)
        .create(topic, None)
        .await
        .map(drop)
}

#[cfg(test)]
mod tests;
