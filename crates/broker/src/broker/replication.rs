//! The replicator supervisor hand-off. It resolves the inter-broker listener
//! protocol and assembles the supervisor's configuration from the pieces the
//! earlier startup phases produced, which is enough bookkeeping to deserve its
//! own module.

use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{broker::DisklessRuntime, config::BrokerConfig, partition_registry::PartitionRegistry};

#[derive(Clone, Copy)]
pub(super) struct ReplicatorStorage<'a> {
    pub(super) log_dir_status: &'a crate::log_dir_status::LogDirRegistry,
    pub(super) producer_state: &'a Arc<crate::producer_state::ProducerState>,
    pub(super) log_dir_ids: &'a crate::log_dir_id::LogDirIds,
    pub(super) diskless: &'a DisklessRuntime,
}

pub(super) fn spawn_replicator_supervisor(
    config: &BrokerConfig,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    partitions: &Arc<PartitionRegistry>,
    coordinators: (
        &Arc<crate::txn::coordinator::TxnCoordinator>,
        &Arc<crate::share_coordinator::coordinator::ShareCoordinator>,
    ),
    inter_broker_client: &Arc<crate::network::client::InterBrokerClient>,
    runtime: (
        &CancellationToken,
        &Arc<crate::throttle::ThrottleState>,
        &crate::metrics::BrokerMetrics,
    ),
    storage: ReplicatorStorage<'_>,
) -> JoinHandle<()> {
    let protocol = config
        .effective_listeners()
        .iter()
        .find(|listener| listener.name == config.inter_broker_listener_name)
        .map_or(krabka_security::ListenerProtocol::Plaintext, |listener| {
            listener.protocol
        });
    crate::replicator_supervisor::ReplicatorSupervisor::new(
        crate::replicator_supervisor::ReplicatorSupervisorConfig {
            node_id: config.node_id,
            broker_id: config.broker_id,
            controller: Arc::clone(controller),
            partitions: Arc::clone(partitions),
            log_dirs: storage.log_dir_status.online_subset(&config.all_log_dirs()),
            log_config: config.log_config.clone(),
            client_id: format!("krabka-broker-{}-replicator", config.broker_id),
            shutdown: runtime.0.clone(),
            txn_coordinator: Some(Arc::clone(coordinators.0)),
            share_coordinator: Some(Arc::clone(coordinators.1)),
            inter_broker_client: Arc::clone(inter_broker_client),
            inter_broker_listener_protocol: protocol,
            inter_broker_server_name: config.inter_broker_server_name.clone(),
            inter_broker_listener_name: config.inter_broker_listener_name.clone(),
            controller_listener_protocol: config.controller_listener_protocol,
            controller_server_name: config
                .controller_server_name
                .clone()
                .unwrap_or_else(|| "localhost".to_owned()),
            controller_quorum_voters: config.controller_quorum_voters.clone(),
            replication: config.replication.clone(),
            throttle_state: Arc::clone(runtime.1),
            log_dir_status: storage.log_dir_status.clone(),
            producer_state: Arc::clone(storage.producer_state),
            producer_id_expiration: config.producer_id_expiration,
            max_produce_group: config.max_produce_group,
            partition_writer_queue_depth: config.partition_writer_queue_depth,
            diskless_wal_local_replica_count: config.diskless_wal_local_replica_count,
            metrics: runtime.2.clone(),
            log_dir_ids: storage.log_dir_ids.clone(),
            hot_tail: Arc::clone(&storage.diskless.hot_tail),
            wal_shards: Arc::clone(&storage.diskless.wal_shards),
        },
    )
    .spawn()
}
