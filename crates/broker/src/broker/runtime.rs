//! The runtime startup phase. It starts every background service that a
//! running broker needs -- audit, replication, liveness, observability,
//! gauges, maintenance, remote storage, and the config-driven watchers -- and
//! collects their handles into one value the final assembly step consumes.

use std::{net::SocketAddr, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_units::{Time, convert::TimeExt};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    broker::{
        DisklessRuntime,
        adapters::{BreakGlassSweepControllerAdapter, DelegationTokenCleanupControllerAdapter},
        audit::start_audit_pipeline,
        gauges::spawn_broker_gauge_updater,
        liveness::{LivenessStartup, start_liveness_services},
        maintenance::{spawn_cluster_data_maintenance, spawn_storage_security_maintenance},
        observability::{ObservabilityStartup, start_observability},
        remote_storage::start_remote_storage,
        replication::{ReplicatorStorage, spawn_replicator_supervisor},
        rlmm::{KafkaSwapKickoff, kafka_swap_kickoff},
    },
    config::BrokerConfig,
    error::BrokerError,
    partition_registry::PartitionRegistry,
};

struct RuntimeCaches {
    fetch_sessions: Arc<crate::fetch_session::FetchSessionCache>,
    quota_buckets: Arc<crate::quota::QuotaBuckets>,
}

/// Publish the KFC-9 gauges that only the metadata image knows.
///
/// One image watch feeds both families, because both read the same image and
/// both must fall as well as rise: the freeze gauge drops when a thaw removes
/// an entry, and a proposal that moves from `Pending` to `Consumed` lowers one
/// series and raises another. A tick loop would hold a stale value between
/// ticks, and two loops would read two different images.
fn spawn_break_glass_gauges(
    config: &BrokerConfig,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    metrics: &crate::metrics::BrokerMetrics,
    shutdown: CancellationToken,
) {
    let images = controller.watch_image();
    let metrics = metrics.clone();
    let break_glass = config.break_glass.clone();
    tokio::spawn(crate::metadata_source::watch_image_loop(
        images,
        "break-glass and freeze gauges",
        shutdown,
        move |image| {
            metrics.record_topic_freezes_active(
                i64::try_from(image.topic_freezes().count()).unwrap_or(i64::MAX),
            );
            crate::break_glass::metrics::record_proposal_states(
                &metrics,
                image,
                &break_glass,
                crate::time_util::now_ms(),
            );
        },
    ));
}

fn start_runtime_watchers(
    config: &BrokerConfig,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    tls_dynamic: Option<&Arc<krabka_security::DynamicServerConfig>>,
    throttle_state: &Arc<crate::throttle::ThrottleState>,
    txn_coordinator: &Arc<crate::txn::coordinator::TxnCoordinator>,
    metrics: &crate::metrics::BrokerMetrics,
    shutdown: &CancellationToken,
) -> RuntimeCaches {
    if let (Some(dynamic), Some(tls_config)) = (tls_dynamic.cloned(), config.tls_config.clone()) {
        tokio::spawn(crate::tls_reload::run(
            dynamic,
            tls_config,
            config.tls_reload_interval,
            shutdown.child_token(),
        ));
    }
    crate::throttle::apply_image(&controller.current_image(), config.node_id, throttle_state);
    tokio::spawn(crate::throttle::run(
        controller.watch_image(),
        config.node_id,
        Arc::clone(throttle_state),
        shutdown.child_token(),
    ));
    let fetch_sessions = Arc::new(crate::fetch_session::FetchSessionCache::new(
        config.max_incremental_fetch_session_cache_slots,
    ));
    // KIP-13's `quota.window.num` * `quota.window.size.seconds`: the window a
    // client's byte rate is averaged over, which is also the burst a bucket
    // allows before it throttles (#418).
    let quota_buckets = Arc::new(crate::quota::QuotaBuckets::with_window(config.quota_window));
    tokio::spawn(crate::quota::run(
        controller.watch_image(),
        Arc::clone(&quota_buckets),
        shutdown.child_token(),
    ));
    // KIP-13 / KIP-599: a bucket, and the per-entity throttle series it
    // publishes, live only as long as the client behind them keeps arriving
    // (#396). Without this a cluster's `/metrics` body grows by one label set
    // per client id it has ever seen.
    tokio::spawn(crate::quota::run_expiry(
        Arc::clone(&quota_buckets),
        metrics.clone(),
        shutdown.child_token(),
    ));
    if config.delegation_token_secret_key.is_some() {
        let interval = config.delegation_token_expiry_check_interval;
        let token_controller: Arc<dyn crate::delegation_token_cleanup::DelegationTokenController> =
            Arc::new(DelegationTokenCleanupControllerAdapter {
                handle: Arc::clone(controller),
            });
        tokio::spawn(crate::delegation_token_cleanup::run(
            token_controller,
            interval,
            shutdown.child_token(),
        ));
    }
    // KFC-9. Every broker sweeps, the tombstone is idempotent, and a broker
    // that never sweeps is still safe, so the sweep needs no config gate.
    let break_glass_controller: Arc<dyn crate::break_glass::sweep::BreakGlassController> =
        Arc::new(BreakGlassSweepControllerAdapter {
            handle: Arc::clone(controller),
        });
    tokio::spawn(crate::break_glass::sweep::run(
        break_glass_controller,
        crate::break_glass::sweep::SWEEP_INTERVAL,
        crate::break_glass::sweep::PROPOSAL_RETENTION,
        shutdown.child_token(),
    ));
    spawn_break_glass_gauges(config, controller, metrics, shutdown.child_token());
    // Per-partition and per-topic series are created lazily by the data path
    // and are not released by anything else, so the image watch that adds a
    // partition is also what has to take its series away again.
    crate::metrics::spawn_metric_series_evictor(
        controller.watch_image(),
        config.node_id,
        metrics.clone(),
        shutdown.child_token(),
    );
    if config.txn_abort_cleanup_interval > <Time as TimeExt>::ZERO {
        tokio::spawn(crate::txn::expiration::run(
            Arc::clone(txn_coordinator),
            Arc::clone(controller),
            config.txn_abort_cleanup_interval,
            shutdown.child_token(),
        ));
    }
    if config.txn_id_expiration_cleanup_interval > <Time as TimeExt>::ZERO {
        tokio::spawn(crate::txn::id_expiration::run(
            Arc::clone(txn_coordinator),
            Arc::clone(controller),
            config.txn_id_expiration_cleanup_interval,
            config.txn_id_expiration,
            shutdown.child_token(),
        ));
    }
    RuntimeCaches {
        fetch_sessions,
        quota_buckets,
    }
}

pub(super) struct BrokerRuntimeStartup {
    pub(super) supervisor_shutdown: CancellationToken,
    pub(super) supervisor_handle: JoinHandle<()>,
    pub(super) disk_scanner_handle: Option<JoinHandle<()>>,
    pub(super) liveness: Arc<crate::heartbeat::controller_state::ControllerLivenessState>,
    pub(super) want_shutdown: Arc<tokio::sync::watch::Sender<bool>>,
    pub(super) should_shutdown: Arc<tokio::sync::watch::Sender<bool>>,
    pub(super) unclean_recovery: crate::unclean_recovery::UncleanRecoveryHandle,
    pub(super) metrics: crate::metrics::BrokerMetrics,
    pub(super) metrics_bound_addr: Option<SocketAddr>,
    pub(super) throttle_state: Arc<crate::throttle::ThrottleState>,
    pub(super) quota_buckets: Arc<crate::quota::QuotaBuckets>,
    pub(super) fetch_session_cache: Arc<crate::fetch_session::FetchSessionCache>,
    pub(super) remote_reader: Option<Arc<crate::remote_reader::RemoteReader>>,
    pub(super) diskless_read: Option<Arc<crate::diskless::read::DisklessReadHandle>>,
    pub(super) client_metrics: Arc<crate::client_metrics::ClientMetrics>,
    pub(super) audit_log: Arc<krabka_audit::AuditLog>,
    pub(super) audit_writer_handle: Option<JoinHandle<()>>,
    pub(super) audit_led_partition: Option<PartitionIndex>,
    pub(super) kafka_swap_kickoff: Option<KafkaSwapKickoff>,
    pub(super) kafka_swap_target: Option<Arc<krabka_remote_storage_topic::SwappableRlmm>>,
    pub(super) inter_listener_protocol: krabka_security::ListenerProtocol,
}

pub(super) async fn start_broker_runtime(
    config: &mut BrokerConfig,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    inter_broker_client: &Arc<crate::network::client::InterBrokerClient>,
    tls_dynamic: Option<&Arc<krabka_security::DynamicServerConfig>>,
    storage: (
        &Arc<PartitionRegistry>,
        &Arc<crate::producer_state::ProducerState>,
        &crate::log_dir_status::LogDirRegistry,
        &crate::log_dir_id::LogDirIds,
    ),
    coordinators: (
        &Arc<crate::txn::coordinator::TxnCoordinator>,
        &Arc<crate::share_coordinator::coordinator::ShareCoordinator>,
    ),
    // The three things the runtime needs pre-built: the diskless WAL runtime,
    // the metric registry the coordinators already report into, and the
    // health state the readiness probe and the gauge sampler both read.
    runtime_deps: (
        &DisklessRuntime,
        crate::metrics::BrokerMetrics,
        Option<crate::health::HealthState>,
    ),
) -> Result<BrokerRuntimeStartup, BrokerError> {
    let (diskless_runtime, metrics, health) = runtime_deps;
    let supervisor_shutdown = CancellationToken::new();
    let throttle_state = Arc::new(crate::throttle::ThrottleState::new());
    let inter_listener_protocol = config
        .effective_listeners()
        .iter()
        .find(|listener| listener.name == config.inter_broker_listener_name)
        .map_or(krabka_security::ListenerProtocol::Plaintext, |listener| {
            listener.protocol
        });
    let (audit_led_partition, audit_log, audit_writer_handle) = start_audit_pipeline(
        config,
        &**controller,
        storage.0,
        &metrics,
        &supervisor_shutdown,
    );
    let supervisor_handle = spawn_replicator_supervisor(
        config,
        controller,
        storage.0,
        coordinators,
        inter_broker_client,
        (&supervisor_shutdown, &throttle_state, &metrics),
        ReplicatorStorage {
            log_dir_status: storage.2,
            producer_state: storage.1,
            log_dir_ids: storage.3,
            diskless: diskless_runtime,
        },
    );
    let LivenessStartup {
        liveness,
        want_shutdown,
        should_shutdown,
        unclean_recovery,
    } = start_liveness_services(
        config,
        controller,
        inter_broker_client,
        inter_listener_protocol,
        (&metrics, &audit_log),
        &supervisor_shutdown,
        (storage.2, storage.3),
    );
    let ObservabilityStartup {
        metrics_bound_addr,
        client_metrics,
    } = start_observability(config, &metrics, &supervisor_shutdown).await?;
    spawn_broker_gauge_updater(
        Arc::clone(storage.0),
        Arc::clone(controller),
        Arc::clone(&liveness),
        storage.2.clone(),
        config.node_id,
        (metrics.clone(), health),
        config,
        supervisor_shutdown.child_token(),
    );
    let disk_scanner_handle = spawn_storage_security_maintenance(
        config,
        storage.0,
        controller,
        &metrics,
        &supervisor_shutdown,
    );
    spawn_cluster_data_maintenance(
        config,
        controller,
        &liveness,
        storage.0,
        storage.1,
        &metrics,
        &supervisor_shutdown,
    );
    let kafka_swap_kickoff = kafka_swap_kickoff(config);
    let remote = start_remote_storage(
        config,
        storage.0,
        controller,
        &metrics,
        &supervisor_shutdown,
    )?;
    let caches = start_runtime_watchers(
        config,
        controller,
        tls_dynamic,
        &throttle_state,
        coordinators.0,
        &metrics,
        &supervisor_shutdown,
    );
    Ok(BrokerRuntimeStartup {
        supervisor_shutdown,
        supervisor_handle,
        disk_scanner_handle,
        liveness,
        want_shutdown,
        should_shutdown,
        unclean_recovery,
        metrics,
        metrics_bound_addr,
        throttle_state,
        quota_buckets: caches.quota_buckets,
        fetch_session_cache: caches.fetch_sessions,
        remote_reader: remote.reader,
        diskless_read: remote.diskless_read,
        client_metrics,
        audit_log,
        audit_writer_handle,
        audit_led_partition,
        kafka_swap_kickoff,
        kafka_swap_target: remote.swap_target,
        inter_listener_protocol,
    })
}
