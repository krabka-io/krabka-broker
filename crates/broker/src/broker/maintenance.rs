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
    broker::adapters::ControllerAdapter, config::BrokerConfig,
    partition_registry::PartitionRegistry,
};

pub(super) fn spawn_storage_security_maintenance(
    config: &BrokerConfig,
    partitions: &Arc<PartitionRegistry>,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    inter_broker_client: &Arc<crate::network::client::InterBrokerClient>,
    metrics: &crate::metrics::BrokerMetrics,
    shutdown: &CancellationToken,
) -> Option<JoinHandle<()>> {
    tokio::spawn(crate::isr_maintenance::run(
        crate::isr_maintenance::Config {
            // Kafka sends `AlterPartition` to the active controller on its
            // CONTROLLER listener, as it sends `BrokerHeartbeat`. An isolated
            // controller has no broker listener at all.
            dialer: crate::controller_endpoint::ControllerDialer {
                outbound_client: Arc::clone(inter_broker_client),
                listener_protocol: config.controller_listener_protocol,
                server_name: config
                    .controller_server_name
                    .clone()
                    .unwrap_or_else(|| "localhost".to_owned()),
                quorum_voters: config.controller_quorum_voters.clone(),
            },
            node_id: config.node_id,
            partitions: Arc::clone(partitions),
            controller: Arc::clone(controller),
            replica_lag_time_max: config.replica_lag_time_max,
            default_min_insync_replicas: config.default_min_insync_replicas,
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
    // `BrokerConfig::validate` refuses a JWKS endpoint on wasm32-wasip1, which
    // has no HTTP client stack.
    #[cfg(not(target_family = "wasm"))]
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
            timer: crate::time_util::system_timer(),
        };
        tokio::spawn(refresher.run());
    }
    disk_scanner
}

fn spawn_producer_expiry(
    producer_state: Arc<crate::producer_state::ProducerState>,
    partitions: Arc<PartitionRegistry>,
    scan_interval: Time,
    expiration: Time,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        // Kafka's `UnifiedLog` schedules `PeriodicProducerExpirationCheck`
        // with `producer.id.expiration.check.interval.ms` as both the initial
        // delay and the period, so the first sweep runs one interval after
        // start, not at start.
        let period = scan_interval.to_std();
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let now_ms = crate::time_util::now_ms();
                    producer_state.expire_older_than(now_ms, expiration).await;
                    expire_log_producers(&partitions, now_ms, expiration).await;
                }
                () = shutdown.cancelled() => return,
            }
        }
    });
}

/// Remove the expired producers from the log of every partition that this
/// broker hosts, leader or follower.
///
/// Kafka's `PeriodicProducerExpirationCheck` runs
/// `ProducerStateManager.removeExpiredProducers` on each `UnifiedLog`, and
/// `DescribeProducers` answers from that state. The logs are locked on the
/// blocking pool: a log lock can wait behind a segment write, and that wait
/// must not hold a runtime worker thread.
async fn expire_log_producers(partitions: &PartitionRegistry, now_ms: i64, expiration: Time) {
    let hosted = partitions.arcs();
    let expiration_ms = expiration.millis_i64();
    let sweep = crate::blocking::spawn_blocking(move || {
        for partition in hosted {
            partition
                .log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove_expired_producers(now_ms, expiration_ms);
        }
    });
    if let Err(error) = sweep.await {
        tracing::warn!(%error, "producer expiry over the partition logs failed");
    }
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
        Arc::new(ControllerAdapter {
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
        Arc::clone(partitions),
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
    // The sweep takes no `node_id`: Kafka's `LogCleanerManager` cleans every
    // log the broker hosts, so a follower replica of a compacted topic
    // compacts its own copy instead of accumulating segments until it is
    // elected.
    tokio::spawn(crate::cleaner::run(
        Arc::clone(partitions),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use bytes::Bytes;
    use krabka_ids::PartitionIndex;
    use krabka_log::{ActiveProducer, ProducerId};
    use krabka_protocol::records::{Record, RecordBatch};
    use tokio::sync::Notify;

    use super::expire_log_producers;
    use crate::{partition::test_support::test_partition, partition_registry::PartitionRegistry};

    /// A one-record idempotent batch of `producer_id` at sequence 0.
    fn idempotent_batch(producer_id: i64, max_timestamp: i64) -> RecordBatch {
        RecordBatch {
            base_timestamp: max_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch: 0,
            base_sequence: 0,
            records: vec![Record {
                value: Some(Bytes::from_static(b"v")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        }
    }

    /// Kafka runs `removeExpiredProducers` on every `UnifiedLog` that the
    /// broker hosts. The sweep removes the producer whose last write is
    /// `producer.id.expiration.ms` old from the log of each hosted partition,
    /// and keeps the recent one.
    #[tokio::test]
    async fn the_producer_expiry_sweeps_the_log_of_every_hosted_partition() {
        const TOPICS: [&str; 2] = ["first", "second"];
        let partitions = PartitionRegistry::new();
        let mut dirs = Vec::new();
        for topic in TOPICS {
            let (partition, dir) = test_partition(Arc::new(Notify::new()));
            {
                let mut log = partition.log.lock().expect("log lock");
                log.append(&mut idempotent_batch(1, 1_000))
                    .expect("append the idle producer");
                log.append(&mut idempotent_batch(2, 9_500))
                    .expect("append the recent producer");
            }
            partitions.insert(Arc::from(topic), PartitionIndex(0), Arc::new(partition));
            dirs.push(dir);
        }

        expire_log_producers(&partitions, 10_000, krabka_units::millis(1_000)).await;

        let recent = vec![ActiveProducer {
            producer_id: ProducerId(2),
            producer_epoch: 0,
            last_sequence: 0,
            last_timestamp: 9_500,
            coordinator_epoch: -1,
            current_txn_start_offset: None,
        }];
        for topic in TOPICS {
            let partition = partitions
                .get(topic, PartitionIndex(0))
                .expect("hosted partition");
            let active = partition.log.lock().expect("log lock").active_producers();
            assert!(active == recent, "{topic}");
        }
    }
}
