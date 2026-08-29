//! Transaction, share, and barrier coordinator bring-up. Each coordinator is
//! constructed, given its inter-broker marker transport, and recovered from the
//! current metadata image here, so the startup sequence sees one call instead
//! of three near-identical construct-configure-recover blocks.

use std::sync::Arc;

use crate::{config::BrokerConfig, partition_registry::PartitionRegistry};

pub(super) struct CoordinatorStartup {
    pub(super) txn_coordinator: Arc<crate::txn::coordinator::TxnCoordinator>,
    pub(super) barrier_coordinator: Arc<crate::barrier::coordinator::BarrierCoordinator>,
    pub(super) share_coordinator: Arc<crate::share_coordinator::coordinator::ShareCoordinator>,
    pub(super) share_partition_leaders:
        Arc<crate::share_partition::manager::SharePartitionLeaderManager>,
    pub(super) share_persister: Arc<crate::share_coordinator::persister_client::SharePersister>,
}

pub(super) async fn start_coordinators(
    config: &BrokerConfig,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    partitions: &Arc<PartitionRegistry>,
    group_coordinator: &Arc<crate::coordinator::GroupCoordinator>,
    producer_ids: &Arc<crate::producer_id_manager::ProducerIdManager>,
    inter_broker_client: &Arc<crate::network::client::InterBrokerClient>,
    metrics: &crate::metrics::BrokerMetrics,
) -> CoordinatorStartup {
    let listener_protocol = config
        .effective_listeners()
        .iter()
        .find(|listener| listener.name == config.inter_broker_listener_name)
        .map_or(krabka_security::ListenerProtocol::Plaintext, |listener| {
            listener.protocol
        });
    let mut txn_coordinator = crate::txn::coordinator::TxnCoordinator::new(
        config.node_id,
        Arc::clone(partitions),
        Arc::clone(producer_ids),
        config.transaction_state_num_partitions,
        config.transaction_recovery_read_max,
    );
    txn_coordinator.configure_marker_transport(
        Arc::clone(controller),
        Arc::clone(inter_broker_client),
        listener_protocol,
        config.inter_broker_listener_name.clone(),
        config.inter_broker_server_name.clone(),
        Arc::clone(group_coordinator),
    );
    let txn_coordinator = Arc::new(txn_coordinator);
    if let Err(error) = txn_coordinator.recover(&controller.current_image()).await {
        tracing::warn!(%error, "transaction coordinator recovery error");
    }
    let mut share_coordinator_config = (*config.share_coordinator).clone();
    share_coordinator_config.recovery_read_max = config.share_recovery_read_max;
    let share_coordinator = Arc::new(
        crate::share_coordinator::coordinator::ShareCoordinator::new(
            config.node_id,
            Arc::clone(partitions),
            share_coordinator_config,
        ),
    );
    if let Err(error) = share_coordinator.recover(&controller.current_image()).await {
        tracing::warn!(%error, "share coordinator recovery error");
    }
    let share_persister = Arc::new(
        crate::share_coordinator::persister_client::SharePersister::new(
            config.node_id,
            Arc::clone(&share_coordinator),
            Arc::clone(controller),
            Arc::clone(inter_broker_client),
            listener_protocol,
            config.inter_broker_listener_name.clone(),
        ),
    );
    group_coordinator.set_share_persister(Arc::clone(&share_persister));
    group_coordinator.set_metadata_source(Arc::clone(controller));
    let share_partition_leaders = Arc::new(
        crate::share_partition::manager::SharePartitionLeaderManager::new(
            config.node_id,
            Arc::clone(partitions),
            Arc::clone(controller),
            Arc::clone(&share_persister),
            Arc::new((*config.share_group).clone()),
            config.share_session_cache_max_when_unlimited,
        ),
    );
    share_partition_leaders.spawn_lock_sweeper();
    let mut barrier_coordinator = crate::barrier::coordinator::BarrierCoordinator::new(
        config.node_id,
        Arc::clone(partitions),
        Arc::clone(controller),
        crate::barrier::config::BarrierConfig {
            state_topic_num_partitions: config.barrier_state_num_partitions,
            state_topic_replication_factor: config.barrier_state_replication_factor,
            recovery_read_max: config.barrier_recovery_read_max,
            injection_timeout: config.barrier_injection_timeout,
            default_retained_cuts: config.barrier_retained_cuts,
            ..crate::barrier::config::BarrierConfig::default()
        },
        Arc::new(crate::barrier::metrics::BrokerBarrierMetrics::new(
            metrics.clone(),
        )),
    );
    // The transport has to be bound before the coordinator goes into its `Arc`.
    // Without it the coordinator marks only the partitions it leads, and every
    // remote partition lands in the `missing` list of the cut.
    barrier_coordinator.configure_marker_transport(Arc::new(
        crate::barrier::handlers::transport::InterBrokerMarkerWriter::new(
            config.node_id,
            Arc::clone(controller),
            Arc::clone(inter_broker_client),
            listener_protocol,
            config.inter_broker_listener_name.clone(),
            config.inter_broker_server_name.clone(),
        ),
    ));
    let barrier_coordinator = Arc::new(barrier_coordinator);
    // __barrier_state is created when the first group is defined, not here.
    // Creating it at every startup put 50 partitions into the metadata log on
    // every broker, whether or not anything used barriers, and a cluster that
    // cannot satisfy the replication factor then leaves all of them
    // leaderless for the election sweep to walk on every pass.
    //
    // Recovery below needs no bootstrap: it replays the partitions this broker
    // leads, and a topic that does not exist has none.
    if let Err(error) = barrier_coordinator
        .recover(&controller.current_image())
        .await
    {
        tracing::warn!(%error, "barrier coordinator recovery error");
    }
    CoordinatorStartup {
        txn_coordinator,
        barrier_coordinator,
        share_coordinator,
        share_partition_leaders,
        share_persister,
    }
}
