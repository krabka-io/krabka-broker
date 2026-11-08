//! Background maintenance loops: ISR upkeep, the disk-usage scanner, JWKS
//! refresh, auto leader rebalance, reassignment completion, producer-id
//! expiry, the log cleaner and the local-retention sweep. They share no state
//! with each other, so they are grouped here purely as the periodic work the
//! broker spawns at startup.

use {
    crate::{
        blocking::spawn_blocking,
        broker::adapters::ControllerAdapter,
        cleaner::{CleanerConfig, run as run_cleaner},
        config::BrokerConfig,
        controller_endpoint::ControllerDialer,
        disk_scanner::DiskScanner,
        error::BrokerError,
        heartbeat::controller_state::ControllerLivenessState,
        isr_maintenance::{Config, run as run_isr_maintenance},
        leader_rebalance::{AutoRebalanceConfig, ControllerLike, run as run_leader_rebalance},
        log_retention::{LogRetentionConfig, run as run_log_retention},
        metadata_source::MetadataSource,
        metrics::BrokerMetrics,
        network::client::InterBrokerClient,
        partition_registry::PartitionRegistry,
        producer_state::{ProducerState, producer_id_expiration},
        reassignment::{ReassignmentController, run as run_reassignment},
        task_util::run_every,
        time_util::now_ms,
    },
    krabka_metadata::NodeId,
    krabka_units::{Time, convert::TimeExt},
    std::sync::Arc,
    tokio::{
        task::JoinHandle,
        time::{self, Instant},
    },
    tokio_util::sync::CancellationToken,
};
#[cfg(not(target_family = "wasm"))]
use {
    crate::{oauth_jwks::JwksRefresher, time_util::system_timer},
    std::sync::PoisonError,
};

pub(super) fn spawn_storage_security_maintenance(
    config: &BrokerConfig,
    partitions: &Arc<PartitionRegistry>,
    controller: &Arc<dyn MetadataSource>,
    inter_broker_client: &Arc<InterBrokerClient>,
    metrics: &BrokerMetrics,
    shutdown: &CancellationToken,
) -> Result<Option<JoinHandle<()>>, BrokerError> {
    tokio::spawn(run_isr_maintenance(Config {
        // Kafka sends `AlterPartition` to the active controller on its
        // CONTROLLER listener, as it sends `BrokerHeartbeat`. An isolated
        // controller has no broker listener at all.
        dialer: ControllerDialer::from_config(config, inter_broker_client),
        node_id: config.node_id,
        partitions: Arc::clone(partitions),
        controller: Arc::clone(controller),
        replica_lag_time_max: config.replica_lag_time_max,
        default_min_insync_replicas: config.default_min_insync_replicas,
        scan_interval: config.isr_scan_interval,
        broker_id: config.broker_id,
        shutdown: shutdown.child_token(),
        metrics: metrics.clone(),
    }));
    let disk_scanner = (config.partition_disk_scan_interval > <Time as TimeExt>::ZERO).then(|| {
        let scanner = DiskScanner {
            log_dirs: config.all_log_dirs(),
            interval: config.partition_disk_scan_interval,
            metrics: metrics.clone(),
            shutdown: shutdown.child_token(),
        };
        tokio::spawn(scanner.run())
    });
    // `BrokerConfig::validate` refuses a JWKS endpoint on wasm32-wasip1, which
    // has no HTTP client stack.
    #[cfg(target_family = "wasm")]
    if config.oauthbearer_jwks_endpoint.is_some() {
        return Err(BrokerError::Startup(
            "JWKS refresh is unavailable on this platform".to_owned(),
        ));
    }
    #[cfg(not(target_family = "wasm"))]
    if let Some(refresher) = jwks_refresher(config, shutdown)? {
        tokio::spawn(refresher.run());
    }
    Ok(disk_scanner)
}

/// The JWKS refresher a signed validator with an endpoint needs, or `None` for
/// any other validator.
///
/// # Errors
///
/// Returns [`BrokerError::Startup`] when the signed validator's signal
/// receiver is missing or was already taken.
#[cfg(not(target_family = "wasm"))]
fn jwks_refresher(
    config: &BrokerConfig,
    shutdown: &CancellationToken,
) -> Result<Option<JwksRefresher>, BrokerError> {
    let Some(endpoint) = config.oauthbearer_jwks_endpoint.clone() else {
        return Ok(None);
    };
    let Some(handle) = config.oauthbearer_validator.jwks_handle() else {
        return Ok(None);
    };
    let missing_receiver =
        || BrokerError::Startup("signed validator must park its JWKS signal receiver".to_owned());
    let runtime = config
        .oauthbearer_jwks_runtime
        .as_ref()
        .ok_or_else(missing_receiver)?;
    let signal_rx = runtime
        .signal_rx
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
        .ok_or_else(missing_receiver)?;
    Ok(Some(JwksRefresher {
        endpoint,
        handle,
        interval: config.oauthbearer_jwks_refresh_interval,
        shutdown: shutdown.child_token(),
        tls_trust: config.oauthbearer_idp_tls_trust.clone(),
        signal_rx,
        min_on_demand_pause: config.oauthbearer_jwks_min_on_demand_pause,
        http_timeout: config.oauth_jwks_http_timeout,
        last_successful_fetch_ms: Arc::clone(&runtime.snapshot.last_successful_fetch_ms),
        cache_generation: Arc::clone(&runtime.snapshot.generation),
        last_on_demand_refresh_ms: Arc::clone(&runtime.last_on_demand_refresh_ms),
        ignore_key_use: config.features.oauthbearer_jwks_ignore_key_use,
        timer: system_timer(),
    }))
}

/// What the producer-expiry sweep reads on each tick.
struct ProducerExpiry {
    producer_state: Arc<ProducerState>,
    partitions: Arc<PartitionRegistry>,
    controller: Arc<dyn MetadataSource>,
    node_id: NodeId,
    /// The static `producer.id.expiration.ms`, which applies while no dynamic
    /// value does.
    static_expiration: Time,
}

fn spawn_producer_expiry(expiry: ProducerExpiry, scan_interval: Time, shutdown: CancellationToken) {
    tokio::spawn(async move {
        // Kafka's `UnifiedLog` schedules `PeriodicProducerExpirationCheck`
        // with `producer.id.expiration.check.interval.ms` as both the initial
        // delay and the period, so the first sweep runs one interval after
        // start, not at start.
        let period = scan_interval.to_std();
        let tick = time::interval_at(Instant::now() + period, period);
        let expiry = &expiry;
        run_every(tick, &shutdown, move || async move {
            let now_ms = now_ms();
            // `producer.id.expiration.ms` is dynamic: Kafka's
            // `DynamicProducerStateManagerConfig` updates the config each
            // log's `removeExpiredProducers` reads, so each sweep reads the
            // value of the current image.
            let expiration = producer_id_expiration(
                &expiry.controller.current_image(),
                expiry.node_id,
                expiry.static_expiration,
            );
            expiry
                .producer_state
                .expire_older_than(now_ms, expiration)
                .await;
            expire_log_producers(&expiry.partitions, now_ms, expiration).await;
        })
        .await;
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
    let sweep = spawn_blocking(move || {
        for partition in hosted {
            // A poisoned log is left out of this sweep.
            match partition.lock_log() {
                Ok(mut log) => log.remove_expired_producers(now_ms, expiration_ms),
                Err(error) => tracing::warn!(
                    topic = %partition.topic,
                    partition = partition.index.get(),
                    %error,
                    "producer expiry: skipping a partition whose log mutex is poisoned"
                ),
            }
        }
    });
    if let Err(error) = sweep.await {
        tracing::warn!(%error, "producer expiry over the partition logs failed");
    }
}

fn cleaner_config(config: &BrokerConfig) -> CleanerConfig {
    #[cfg(any(test, feature = "test-helpers"))]
    if let Some(interval) = config.cleaner_interval_override {
        return CleanerConfig::system(interval);
    }
    CleanerConfig::system(config.cleaner_interval)
}

pub(super) fn spawn_cluster_data_maintenance(
    config: &BrokerConfig,
    controller: &Arc<dyn MetadataSource>,
    liveness: &Arc<ControllerLivenessState>,
    partitions: &Arc<PartitionRegistry>,
    producer_state: &Arc<ProducerState>,
    metrics: &BrokerMetrics,
    shutdown: &CancellationToken,
) {
    if config.features.auto_leader_rebalance_enable {
        let adapter: Arc<dyn ControllerLike> = Arc::new(ControllerAdapter {
            handle: Arc::clone(controller),
            node_id: config.node_id,
        });
        tokio::spawn(run_leader_rebalance(
            adapter,
            Arc::clone(liveness),
            AutoRebalanceConfig {
                check_interval: config.leader_imbalance_check_interval,
            },
            shutdown.child_token(),
        ));
    }
    let reassignment: Arc<dyn ReassignmentController> = Arc::new(ControllerAdapter {
        handle: Arc::clone(controller),
        node_id: config.node_id,
    });
    tokio::spawn(run_reassignment(
        reassignment,
        Arc::clone(liveness),
        shutdown.child_token(),
    ));
    spawn_producer_expiry(
        ProducerExpiry {
            producer_state: Arc::clone(producer_state),
            partitions: Arc::clone(partitions),
            controller: Arc::clone(controller),
            node_id: config.node_id,
            static_expiration: config.producer_id_expiration,
        },
        config.producer_id_expiration_scan_interval,
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
    tokio::spawn(run_cleaner(
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
    let mut retention = LogRetentionConfig::system(config.log_retention_check_interval);
    retention.metadata = Some(Arc::clone(controller));
    tokio::spawn(run_log_retention(
        Arc::clone(partitions),
        retention,
        shutdown.child_token(),
        metrics.clone(),
    ));
}

#[cfg(test)]
mod tests {
    use {
        super::expire_log_producers,
        crate::{partition::test_support::test_partition, partition_registry::PartitionRegistry},
        assert2::assert,
        bytes::Bytes,
        krabka_ids::PartitionIndex,
        krabka_log::{ActiveProducer, ProducerId},
        krabka_protocol::records::{Record, RecordBatch},
        std::sync::Arc,
        tokio::sync::Notify,
    };
    #[cfg(not(target_family = "wasm"))]
    use {
        super::jwks_refresher,
        crate::{BrokerError, config::BrokerConfig, file_config::FileConfig},
        std::sync::atomic::Ordering,
        tokio_util::sync::CancellationToken,
    };

    /// The broker configuration a `[oauthbearer]` table body applies to.
    #[cfg(not(target_family = "wasm"))]
    fn oauthbearer_config(table: &str) -> BrokerConfig {
        let file: FileConfig = toml::from_str(&format!("[oauthbearer]\n{table}")).unwrap();
        let mut config = BrokerConfig::default();
        file.apply_to(&mut config).unwrap();
        config
    }

    /// A signed validator's refresher publishes into the same generation and
    /// fetch timestamp that the authenticate path's cache guard reads, and the
    /// validator's key handle sees the same timestamp.
    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn signed_config_shares_jwks_publication_with_the_authenticate_path() {
        let config = oauthbearer_config("jwks_endpoint_uri = \"https://idp.example/jwks\"\n");
        let refresher = jwks_refresher(&config, &CancellationToken::new())
            .unwrap()
            .expect("a signed validator gets a refresher");
        let authenticate = config.oauthbearer_jwks_snapshot();
        assert!(Arc::ptr_eq(
            &refresher.cache_generation,
            &authenticate.generation
        ));
        assert!(Arc::ptr_eq(
            &refresher.last_successful_fetch_ms,
            &authenticate.last_successful_fetch_ms
        ));
        refresher
            .last_successful_fetch_ms
            .store(42, Ordering::Release);
        let handle = config.oauthbearer_validator.jwks_handle().unwrap();
        assert!(handle.last_successful_fetch_ms() == 42);
    }

    /// The signal receiver is taken once: a second refresher for the same
    /// validator construction is a startup error.
    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn a_second_jwks_refresher_is_a_startup_error() {
        let config = oauthbearer_config("jwks_endpoint_uri = \"https://idp.example/jwks\"\n");
        let shutdown = CancellationToken::new();
        assert!(let Ok(Some(_)) = jwks_refresher(&config, &shutdown));
        assert!(
            let Err(BrokerError::Startup(message)) = jwks_refresher(&config, &shutdown)
        );
        assert!(message == "signed validator must park its JWKS signal receiver");
    }

    /// Only a signed validator gets a refresher.
    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn an_unsecured_config_gets_no_jwks_refresher() {
        let config = oauthbearer_config("allow_unsecured = true\n");
        assert!(let Ok(None) = jwks_refresher(&config, &CancellationToken::new()));
    }

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
        // The directories live until the end of the test.
        let _dirs: Vec<_> = TOPICS
            .into_iter()
            .map(|topic| {
                let (partition, dir) = test_partition(Arc::new(Notify::new()));
                {
                    let mut log = partition.log.lock().expect("log lock");
                    log.append(&mut idempotent_batch(1, 1_000))
                        .expect("append the idle producer");
                    log.append(&mut idempotent_batch(2, 9_500))
                        .expect("append the recent producer");
                }
                partitions.insert(Arc::from(topic), PartitionIndex(0), Arc::new(partition));
                dir
            })
            .collect();

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
