//! Background maintenance loops: ISR upkeep, the disk-usage scanner, JWKS
//! refresh, auto leader rebalance, reassignment completion, producer-id
//! expiry, the log cleaner and the local-retention sweep. They share no state
//! with each other, so they are grouped here purely as the periodic work the
//! broker spawns at startup.

use std::sync::Arc;

use krabka_units::{Time, convert::TimeExt};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    broker::adapters::{ControllerAdapter, ReassignmentControllerAdapter},
    config::BrokerConfig,
    partition_registry::PartitionRegistry,
};

pub(super) fn spawn_storage_security_maintenance(
    config: &BrokerConfig,
    partitions: &Arc<PartitionRegistry>,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    metrics: &crate::metrics::BrokerMetrics,
    shutdown: &CancellationToken,
) -> Option<JoinHandle<()>> {
    tokio::spawn(crate::isr_maintenance::run(
        crate::isr_maintenance::Config {
            client_dispatch_queue_capacity: config.client_dispatch_queue_capacity,
            client_frame_max: config.client_frame_max,
            node_id: config.node_id,
            partitions: Arc::clone(partitions),
            controller: Arc::clone(controller),
            replica_lag_time_max: config.replica_lag_time_max,
            scan_interval: config.isr_scan_interval,
            broker_id: config.broker_id,
            shutdown: shutdown.child_token(),
            metrics: metrics.clone(),
        },
    ));
    let disk_scanner = (config.partition_disk_scan_interval > <Time as TimeExt>::ZERO).then(|| {
        let scanner = crate::disk_scanner::DiskScanner {
            log_dirs: config.all_log_dirs(),
            interval: config.partition_disk_scan_interval,
            metrics: metrics.clone(),
            shutdown: shutdown.child_token(),
        };
        tokio::spawn(scanner.run())
    });
    if let Some(endpoint) = config.oauthbearer_jwks_endpoint.clone()
        && let Some(handle) = config.oauthbearer_validator.jwks_handle()
    {
        let signal_rx = config
            .oauthbearer_jwks_signal_rx
            .lock()
            .unwrap()
            .take()
            .expect("signed validator must park its JWKS signal receiver");
        let refresher = crate::oauth_jwks::JwksRefresher {
            endpoint,
            handle,
            interval: config.oauthbearer_jwks_refresh_interval,
            shutdown: shutdown.child_token(),
            tls_trust: config.oauthbearer_idp_tls_trust.clone(),
            signal_rx,
            min_on_demand_pause: config.oauthbearer_jwks_min_on_demand_pause,
            http_timeout: config.oauth_jwks_http_timeout,
            last_successful_fetch_ms: Arc::clone(&config.oauthbearer_jwks_last_successful_fetch_ms),
            cache_generation: Arc::clone(&config.oauthbearer_jwks_cache_generation),
            last_on_demand_refresh_ms: Arc::clone(
                &config.oauthbearer_jwks_last_on_demand_refresh_ms,
            ),
            ignore_key_use: config.features.oauthbearer_jwks_ignore_key_use,
            timer: Arc::new(qubit_clock::StdTimer::new()),
        };
        tokio::spawn(refresher.run());
    }
    disk_scanner
}

fn spawn_producer_expiry(
    producer_state: Arc<crate::producer_state::ProducerState>,
    scan_interval: Time,
    expiration: Time,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(scan_interval.to_std());
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    producer_state
                        .expire_older_than(crate::time_util::now_ms(), expiration)
                        .await;
                }
                () = shutdown.cancelled() => return,
            }
        }
    });
}

fn cleaner_config(config: &BrokerConfig) -> crate::cleaner::CleanerConfig {
    let interval = config.cleaner_interval;
    #[cfg(any(test, feature = "test-helpers"))]
    {
        crate::cleaner::CleanerConfig::system(config.cleaner_interval_override.unwrap_or(interval))
    }
    #[cfg(not(any(test, feature = "test-helpers")))]
    {
        crate::cleaner::CleanerConfig::system(interval)
    }
}

pub(super) fn spawn_cluster_data_maintenance(
    config: &BrokerConfig,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    liveness: &Arc<crate::heartbeat::controller_state::ControllerLivenessState>,
    partitions: &Arc<PartitionRegistry>,
    producer_state: &Arc<crate::producer_state::ProducerState>,
    metrics: &crate::metrics::BrokerMetrics,
    shutdown: &CancellationToken,
) {
    if config.features.auto_leader_rebalance_enable {
        let adapter: Arc<dyn crate::leader_rebalance::ControllerLike> =
            Arc::new(ControllerAdapter {
                handle: Arc::clone(controller),
                node_id: config.node_id,
            });
        tokio::spawn(crate::leader_rebalance::run(
            adapter,
            Arc::clone(liveness),
            crate::leader_rebalance::AutoRebalanceConfig {
                check_interval: config.leader_imbalance_check_interval,
            },
            shutdown.child_token(),
        ));
    }
    let reassignment: Arc<dyn crate::reassignment::ReassignmentController> =
        Arc::new(ReassignmentControllerAdapter {
            handle: Arc::clone(controller),
            node_id: config.node_id,
        });
    tokio::spawn(crate::reassignment::run(
        reassignment,
        Arc::clone(liveness),
        shutdown.child_token(),
    ));
    spawn_producer_expiry(
        Arc::clone(producer_state),
        config.producer_id_expiration_scan_interval,
        config.producer_id_expiration,
        shutdown.child_token(),
    );
    // The sweep reads the KFC-9 write-freeze registry from this authority, so
    // compaction stops on a frozen topic. Without it the cleaner resolves no
    // freeze and compacts every eligible partition, which would remove records
    // from a log that a disaster-recovery promotion needs byte-identical
    // between sites.
    let mut cleaner = cleaner_config(config);
    cleaner.metadata = Some(Arc::clone(controller));
    tokio::spawn(crate::cleaner::run(
        Arc::clone(partitions),
        config.node_id,
        cleaner,
        shutdown.child_token(),
        metrics.clone(),
    ));
    // Local retention, beside the cleaner and on its own Kafka setting
    // (`log.retention.check.interval.ms`). It reads the same freeze registry,
    // because retention removes data from the log and the KFC-9 rule refuses
    // every operation that does. It takes no `node_id`: Kafka's
    // `LogManager.cleanupLogs` runs over every log the broker hosts, so a
    // follower trims its own replica rather than accumulating segments until
    // it is elected.
    let mut retention =
        crate::log_retention::LogRetentionConfig::system(config.log_retention_check_interval);
    retention.metadata = Some(Arc::clone(controller));
    tokio::spawn(crate::log_retention::run(
        Arc::clone(partitions),
        retention,
        shutdown.child_token(),
        metrics.clone(),
    ));
}
