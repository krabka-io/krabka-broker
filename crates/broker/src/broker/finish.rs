//! The final assembly step. It binds the data-plane listeners, builds the
//! [`Broker`] from every startup phase's output, starts the accept loops and
//! the deferred bootstrap tasks, and returns the [`BrokerHandle`]. It is its
//! own module because it is the one place that names every phase at once.

use std::sync::{Arc, atomic::AtomicBool};

use krabka_ids::PartitionIndex;
use krabka_units::convert::TimeExt as _;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    broker::{
        Broker, BrokerHandle, ConnectionLimiter, DisklessRuntime,
        diskless_index::{DisklessFlusherStartup, bootstrap_diskless_index_log},
        listeners::{ListenerStartup, bind_listeners_and_recover_moves, spawn_listener_tasks},
        rlmm::{KafkaSwapKickoff, bootstrap_topic_rlmm},
        runtime::BrokerRuntimeStartup,
    },
    config::BrokerConfig,
    error::BrokerError,
    partition_registry::PartitionRegistry,
};

pub(super) type BrokerCoordinatorSet = (
    Arc<crate::coordinator::GroupCoordinator>,
    Arc<crate::producer_id_manager::ProducerIdManager>,
    Arc<crate::producer_state::ProducerState>,
    Arc<crate::txn::coordinator::TxnCoordinator>,
    Arc<crate::share_coordinator::coordinator::ShareCoordinator>,
    Arc<crate::share_partition::manager::SharePartitionLeaderManager>,
    Arc<crate::barrier::coordinator::BarrierCoordinator>,
);

pub(super) struct BrokerStorageStartup {
    pub(super) log_dir_status: crate::log_dir_status::LogDirRegistry,
    pub(super) diskless: DisklessRuntime,
}

pub(super) async fn finish_broker_startup(
    mut config: BrokerConfig,
    data_listeners: Vec<TcpListener>,
    metadata: (
        Arc<dyn crate::metadata_source::MetadataSource>,
        Arc<PartitionRegistry>,
        Option<Arc<crate::controller_admin::BrokerControllerAdminRouter>>,
    ),
    coordinators: BrokerCoordinatorSet,
    transport: (
        Option<Arc<krabka_security::DynamicServerConfig>>,
        bool,
        Arc<crate::network::client::InterBrokerClient>,
    ),
    runtime: BrokerRuntimeStartup,
    storage: BrokerStorageStartup,
) -> Result<BrokerHandle, BrokerError> {
    let (controller, partitions, admin_router) = metadata;
    let ListenerStartup {
        bound,
        listen_addr,
        future_logs,
    } = bind_listeners_and_recover_moves(
        &mut config,
        data_listeners,
        &partitions,
        &runtime.throttle_state,
    )
    .await?;
    let connections = ConnectionLimiter::new(config.max_connections, config.max_connections_per_ip);
    let broker = Arc::new(Broker {
        config,
        controller,
        partitions,
        future_logs,
        group_coordinator: coordinators.0,
        producer_ids: coordinators.1,
        producer_state: coordinators.2,
        txn_coordinator: coordinators.3,
        share_coordinator: coordinators.4,
        share_partition_leaders: coordinators.5,
        barrier_coordinator: coordinators.6,
        supervisor_shutdown: runtime.supervisor_shutdown,
        supervisor_handle: tokio::sync::Mutex::new(Some(runtime.supervisor_handle)),
        disk_scanner_handle: tokio::sync::Mutex::new(runtime.disk_scanner_handle),
        liveness: runtime.liveness,
        controller_id_rotation:
            crate::handlers::advertised_controller::ControllerIdRotation::default(),
        tls_dynamic: transport.0,
        ktls_enabled: transport.1,
        inter_broker_client: transport.2,
        unclean_recovery: runtime.unclean_recovery,
        metrics: runtime.metrics,
        metrics_bound_addr: runtime.metrics_bound_addr,
        throttle_state: runtime.throttle_state,
        quota_buckets: runtime.quota_buckets,
        connections,
        fetch_session_cache: runtime.fetch_session_cache,
        want_shutdown: runtime.want_shutdown,
        should_shutdown: runtime.should_shutdown,
        remote_reader: runtime.remote_reader,
        diskless_read: runtime.diskless_read,
        hot_tail: storage.diskless.hot_tail,
        wal_shards: storage.diskless.wal_shards,
        log_dir_status: storage.log_dir_status,
        client_metrics: runtime.client_metrics,
        #[cfg(any(test, feature = "test-helpers"))]
        offset_for_leader_epoch_requests: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        audit_log: runtime.audit_log,
        audit_writer_handle: tokio::sync::Mutex::new(runtime.audit_writer_handle),
        handlers: crate::handlers::registry::build_registry(),
    });
    if let Some(router) = admin_router {
        router.bind(&broker).map_err(|error| {
            broker.supervisor_shutdown.cancel();
            BrokerError::Startup(error.into())
        })?;
    }
    let diskless_bootstrap = match (
        broker.diskless_read.as_ref(),
        runtime.kafka_swap_kickoff.as_ref(),
    ) {
        (Some(handle), Some(kickoff)) => Some((handle, kickoff)),
        (None, None) => None,
        _ => {
            broker.supervisor_shutdown.cancel();
            return Err(BrokerError::Startup(
                "diskless read and bootstrap configuration must be enabled together".into(),
            ));
        }
    };
    let (shutdown, listener_tasks) = spawn_listener_tasks(&broker, bound);
    emit_broker_started(&broker, runtime.audit_led_partition).await;
    let topic_rlmm_task = spawn_rlmm_bootstrap(
        &broker,
        runtime.kafka_swap_target.as_ref(),
        runtime.kafka_swap_kickoff.as_ref(),
        &shutdown,
    );
    let (diskless_task, diskless_flusher_ready) =
        diskless_bootstrap.map_or((None, None), |(handle, kickoff)| {
            let (task, ready) = spawn_diskless_bootstrap(&broker, handle, kickoff, &shutdown);
            (Some(task), Some(ready))
        });
    Ok(BrokerHandle {
        listen_addr,
        shutdown,
        listener_tasks,
        topic_rlmm_task,
        diskless_task,
        diskless_flusher_ready,
        broker,
    })
}

async fn emit_broker_started(broker: &Broker, audit_partition: Option<PartitionIndex>) {
    let Some(partition_index) = audit_partition else {
        return;
    };
    let topic = broker.config.audit_topic.clone();
    let partitions = Arc::clone(&broker.partitions);
    let _ = tokio::time::timeout(
        broker.config.audit_partition_wait_timeout.to_std(),
        async move {
            while !partitions.contains(&topic, partition_index) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        },
    )
    .await;
    broker.audit_log.emit(krabka_audit::AuditEvent::Lifecycle {
        kind: krabka_audit::LifecycleKind::BrokerStarted,
        node_id: i64::from(broker.config.broker_id),
        time_ms: crate::time_util::now_ms(),
    });
}

fn spawn_rlmm_bootstrap(
    broker: &Arc<Broker>,
    swap_target: Option<&Arc<krabka_remote_storage_topic::SwappableRlmm>>,
    kickoff: Option<&KafkaSwapKickoff>,
    shutdown: &CancellationToken,
) -> Option<JoinHandle<()>> {
    let (Some(swap), Some(kickoff)) = (swap_target, kickoff) else {
        return None;
    };
    let future = bootstrap_topic_rlmm(
        Arc::clone(swap),
        kickoff.clone(),
        tokio::runtime::Handle::current(),
        broker.metrics.clone(),
        broker.config.node_id,
        broker.controller.watch_image(),
        shutdown.clone(),
    );
    let shutdown = shutdown.clone();
    Some(tokio::spawn(async move {
        tokio::select! {
            () = shutdown.cancelled() => tracing::debug!("RLMM bootstrap cancelled"),
            () = future => {}
        }
    }))
}

fn spawn_diskless_bootstrap(
    broker: &Arc<Broker>,
    handle: &Arc<crate::diskless::read::DisklessReadHandle>,
    kickoff: &KafkaSwapKickoff,
    shutdown: &CancellationToken,
) -> (JoinHandle<()>, Arc<AtomicBool>) {
    let cache = Arc::clone(&handle.index);
    let ready = Arc::new(AtomicBool::new(false));
    let flusher = DisklessFlusherStartup {
        partitions: Arc::clone(&broker.partitions),
        image_rx: broker.controller.watch_image(),
        object_store: handle.object_store(),
        node_id: broker.config.node_id,
        broker_id: broker.config.broker_id,
        metrics: broker.metrics.clone(),
        flush_config: crate::diskless::flusher::FlushConfig::from_broker(&broker.config),
        ready: Arc::clone(&ready),
    };
    let kickoff = kickoff.clone();
    let shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        bootstrap_diskless_index_log(cache, kickoff, flusher, shutdown).await;
    });
    (task, ready)
}
