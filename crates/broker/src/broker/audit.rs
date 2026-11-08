//! `FedRAMP` MLA audit pipeline bring-up: the signing key, the on-disk spool,
//! the writer task that drains audit events into the audit topic, and the
//! metrics poller that reports on it. It lives apart from the rest of startup
//! because it also rewraps the configured authorizer.

use {
    crate::{
        BrokerError,
        audit_authorizer::AuditingAuthorizer,
        audit_recovery::{ChainResume, recover_from_partition_tail},
        audit_sink::KafkaTopicAuditSink,
        broker::Broker,
        config::BrokerConfig,
        coordinator::bootstrap::hosted_audit_partition,
        metadata_source::{MetadataSource, watch_image_loop},
        metrics::BrokerMetrics,
        partition_registry::PartitionRegistry,
        time_util::system_timer,
    },
    krabka_audit::{OcsfVersion, OcsfVersionSwitch},
    krabka_ids::PartitionIndex,
    krabka_metadata::{KrabkaVersion, MetadataImage},
    krabka_units::{
        Time,
        convert::{ByteSizeExt as _, TimeExt as _},
    },
    std::sync::Arc,
    tokio::{sync::watch, task::JoinHandle, time},
    tokio_util::sync::CancellationToken,
};

/// The checkpoint signing key the audit writer signs with.
pub(super) type AuditSigner = Arc<krabka_audit::FileEd25519Signer>;

/// Loads the configured audit checkpoint signing key.
///
/// [`crate::Broker::start`] calls it right after it validates the config and
/// before any side effect. With audit disabled, or with no key configured, it
/// returns `None`: checkpoints are disabled. `BrokerConfig::validate` has
/// already refused a `fail-closed` broker without a key.
///
/// # Errors
///
/// [`BrokerError::AuditSigningKey`] when a configured key does not load, in
/// every audit failure mode, and [`BrokerError::InvalidRuntimeConfig`] when
/// only one of the key path and the key id is set.
pub(super) fn load_audit_signer(config: &BrokerConfig) -> Result<Option<AuditSigner>, BrokerError> {
    if !config.audit_enabled {
        return Ok(None);
    }
    match (&config.audit_signing_key_path, &config.audit_signing_key_id) {
        (None, None) => {
            tracing::info!("no audit signing key configured; checkpoints disabled");
            Ok(None)
        }
        (Some(path), Some(key_id)) => {
            krabka_audit::FileEd25519Signer::from_pkcs8_file(path, key_id.clone())
                .map(|signer| Some(Arc::new(signer)))
                .map_err(|source| BrokerError::AuditSigningKey {
                    path: path.clone(),
                    source,
                })
        }
        _ => Err(BrokerError::InvalidRuntimeConfig(
            "audit_signing_key_path and audit_signing_key_id must be set together".to_owned(),
        )),
    }
}

fn open_audit_spool(
    config: &BrokerConfig,
) -> Result<krabka_audit::Spool, krabka_audit::AuditError> {
    let directory = if config.audit_spool_dir.is_absolute() {
        config.audit_spool_dir.clone()
    } else {
        config.log_dir.join(&config.audit_spool_dir)
    };
    krabka_audit::Spool::open_with_sync_every(
        &directory,
        config.audit_spool_max,
        config.audit_spool_sync_every_n,
    )
}

/// Sets `switch` to the OCSF record version of the finalized `krabka.version`
/// of every metadata image `images` publishes, until `shutdown` or until the
/// channel closes.
///
/// The audit writer reads `switch` each time it serializes an event, so a
/// record written after the operator finalizes `krabka.version` 3 carries the
/// version 2 OCSF ids, and every earlier one keeps the krabka 1.0.0 record.
async fn follow_ocsf_version(
    images: watch::Receiver<Arc<MetadataImage>>,
    switch: OcsfVersionSwitch,
    shutdown: CancellationToken,
) {
    watch_image_loop(images, "audit OCSF version", shutdown, |image| {
        switch.set(OcsfVersion::at(KrabkaVersion::finalized(image)));
    })
    .await;
}

fn spawn_audit_metrics(
    stats: Arc<krabka_audit::AuditStats>,
    log: Arc<krabka_audit::AuditLog>,
    metrics: BrokerMetrics,
    poll_interval: Time,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut previous = (0, 0, 0);
        let mut tick = time::interval(poll_interval.to_std());
        loop {
            if !super::tick_or_shutdown(&mut tick, &shutdown).await {
                return;
            }
            let current = (
                stats.spooled(),
                stats.replayed(),
                stats.dropped() + log.dropped(),
            );
            metrics
                .audit_records_spooled_total
                .inc_by(current.0 - previous.0);
            metrics
                .audit_records_replayed_total
                .inc_by(current.1 - previous.1);
            metrics
                .audit_records_dropped_total
                .inc_by(current.2 - previous.2);
            previous = current;
            metrics
                .audit_spool_depth
                .set(i64::try_from(stats.depth()).unwrap_or(i64::MAX));
            metrics
                .audit_spool_bytes
                .set(stats.spool_bytes().bytes_i64());
        }
    });
}

/// What the audit pipeline hands to the rest of startup.
pub(super) struct AuditPipeline {
    /// The audit partition this broker hosts, if it hosts one. Each audit
    /// partition has one replica, and the broker it is placed on writes it.
    pub(super) led_partition: Option<PartitionIndex>,
    pub(super) log: Arc<krabka_audit::AuditLog>,
    /// The writer task, absent when no writer runs.
    pub(super) writer_handle: Option<JoinHandle<()>>,
}

pub(super) fn start_audit_pipeline(
    config: &mut BrokerConfig,
    signer: Option<AuditSigner>,
    controller: &dyn MetadataSource,
    partitions: &Arc<PartitionRegistry>,
    metrics: &BrokerMetrics,
    supervisor_shutdown: &CancellationToken,
) -> AuditPipeline {
    // The decorator counts every Deny, so it goes on whether or not audit is
    // enabled: with audit off the log below is the disabled one and drops the
    // emit, leaving `authorization_denied_total` as the only denial record.
    let install_auditing_authorizer =
        |config: &mut BrokerConfig, log: &Arc<krabka_audit::AuditLog>| {
            config.authorizer = Arc::new(AuditingAuthorizer::new(
                Arc::clone(&config.authorizer),
                Arc::clone(log),
                metrics.clone(),
            ));
        };
    if !config.audit_enabled {
        let log = krabka_audit::AuditLog::disabled();
        install_auditing_authorizer(config, &log);
        return AuditPipeline {
            led_partition: None,
            log,
            writer_handle: None,
        };
    }
    let led_partition = hosted_audit_partition(
        &controller.current_image(),
        &config.audit_topic,
        config.node_id,
    );
    // The sink refuses a write until this broker leads the partition, and
    // the spool keeps the records meanwhile.
    let (spool, mut replay_poisoned) = if led_partition.is_some() {
        match open_audit_spool(config) {
            Ok(spool) => (Some(spool), false),
            Err(error @ krabka_audit::AuditError::Poisoned(_)) => {
                tracing::error!(%error, "audit writer disabled pending explicit spool recovery");
                (None, true)
            }
            Err(error) => {
                tracing::error!(%error, "failed to open audit spool; spooling disabled");
                (None, false)
            }
        }
    } else {
        (None, false)
    };
    let spool_resume = if replay_poisoned {
        None
    } else {
        match spool.as_ref().map(krabka_audit::Spool::resume_point) {
            Some(Ok(resume)) => resume.map(|(next_seq, head)| ChainResume { next_seq, head }),
            Some(Err(error)) => {
                tracing::error!(%error, "audit writer disabled pending explicit spool recovery");
                replay_poisoned = true;
                None
            }
            None => None,
        }
    };
    let (log, receiver) = spool.as_ref().map_or_else(
        || {
            krabka_audit::AuditLog::new_with_mode(
                config.audit_event_queue_capacity,
                config.audit_failure_mode,
            )
        },
        |spool| {
            krabka_audit::AuditLog::new_with_mode_and_spool(
                config.audit_event_queue_capacity,
                config.audit_failure_mode,
                spool,
            )
        },
    );
    let writer_handle = if replay_poisoned {
        drop(receiver);
        None
    } else if let Some(partition_index) = led_partition {
        let sink = Arc::new(KafkaTopicAuditSink::new(
            Arc::clone(partitions),
            config.audit_topic.clone(),
            partition_index,
            config.node_id,
            metrics.clone(),
        ));
        let resume = spool_resume.or_else(|| {
            let partition = partitions.get(&config.audit_topic, partition_index)?;
            recover_from_partition_tail(
                &partition,
                config.audit_tail_window_offsets,
                config.audit_tail_read_max,
            )
        });
        let chain = resume.map_or_else(
            krabka_audit::ChainState::new,
            |ChainResume { next_seq, head }| krabka_audit::ChainState::resume(next_seq, head),
        );
        // Seed the switch from the current image, so an event emitted before
        // the follower task first runs is already in the right version.
        let ocsf_version = OcsfVersionSwitch::new(OcsfVersion::at(KrabkaVersion::finalized(
            &controller.current_image(),
        )));
        tokio::spawn(follow_ocsf_version(
            controller.watch_image(),
            ocsf_version.clone(),
            supervisor_shutdown.child_token(),
        ));
        let stats = Arc::new(krabka_audit::AuditStats::new());
        let writer = krabka_audit::AuditWriter::new(receiver, krabka_audit::AuditWriterParams {
            sink,
            product: Broker::audit_product(),
            ocsf_version,
            signer,
            checkpoint_every_n: config.audit_checkpoint_every_n,
            checkpoint_every: config.audit_checkpoint_every,
            chain,
            spool,
            stats: Arc::clone(&stats),
            replay_every: config.audit_spool_replay_interval,
            timer: system_timer(),
        });
        let writer_handle = tokio::spawn(writer.run());
        spawn_audit_metrics(
            stats,
            Arc::clone(&log),
            metrics.clone(),
            config.audit_stats_poll_interval,
            supervisor_shutdown.child_token(),
        );
        Some(writer_handle)
    } else {
        tracing::warn!("no audit partition is placed on this broker; audit records will drop");
        None
    };
    install_auditing_authorizer(config, &log);
    AuditPipeline {
        led_partition,
        log,
        writer_handle,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            file_config::{FileConfig, FileConfigError},
            test_support::image_at_krabka_version,
        },
        assert2::assert,
        krabka_units::minutes,
        ring::{rand::SystemRandom, signature::Ed25519KeyPair},
        std::{fs, path::PathBuf, time::Duration},
        tempfile::TempDir,
        tokio::{task, time},
    };

    /// Yields until `switch` reads `expected`, or fails after a second.
    async fn await_version(switch: &OcsfVersionSwitch, expected: OcsfVersion) {
        time::timeout(Duration::from_secs(1), async {
            while switch.get() != expected {
                task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the audit OCSF version never became {expected:?}"));
    }

    #[tokio::test]
    async fn ocsf_version_follows_each_finalized_krabka_version() {
        let (images, receiver) = watch::channel(image_at_krabka_version(2));
        let switch = OcsfVersionSwitch::new(OcsfVersion::V2);
        let shutdown = CancellationToken::new();
        let follower = tokio::spawn(follow_ocsf_version(
            receiver,
            switch.clone(),
            shutdown.child_token(),
        ));

        await_version(&switch, OcsfVersion::V1).await;
        images.send(image_at_krabka_version(3)).unwrap();
        await_version(&switch, OcsfVersion::V2).await;
        images.send(image_at_krabka_version(2)).unwrap();
        await_version(&switch, OcsfVersion::V1).await;

        shutdown.cancel();
        time::timeout(Duration::from_secs(1), follower)
            .await
            .expect("the follower stops on shutdown")
            .unwrap();
    }

    #[tokio::test]
    async fn ocsf_version_follower_stops_when_the_image_channel_closes() {
        let (images, receiver) = watch::channel(image_at_krabka_version(0));
        let follower = tokio::spawn(follow_ocsf_version(
            receiver,
            OcsfVersionSwitch::new(OcsfVersion::V1),
            CancellationToken::new(),
        ));
        drop(images);

        time::timeout(Duration::from_secs(1), follower)
            .await
            .expect("the follower stops once the channel closes")
            .unwrap();
    }

    #[tokio::test]
    async fn audit_metrics_cancellation_releases_log_before_next_poll() {
        let stats = Arc::new(krabka_audit::AuditStats::new());
        let (log, _receiver) = krabka_audit::AuditLog::new(1);
        let weak_log = Arc::downgrade(&log);
        let shutdown = CancellationToken::new();
        spawn_audit_metrics(
            stats,
            Arc::clone(&log),
            BrokerMetrics::new(),
            minutes(1),
            shutdown.child_token(),
        );
        drop(log);

        shutdown.cancel();
        time::timeout(Duration::from_secs(1), async {
            while weak_log.upgrade().is_some() {
                task::yield_now().await;
            }
        })
        .await
        .expect("audit metrics task releases log after cancellation");

        assert!(weak_log.upgrade().is_none());
    }

    /// The audit failure mode a scenario's broker runs in.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, krabka_bdd::StepParam)]
    enum FailureMode {
        FailOpen,
        FailClosed,
    }

    impl FailureMode {
        const fn toml(self) -> &'static str {
            match self {
                Self::FailOpen => "fail-open",
                Self::FailClosed => "fail-closed",
            }
        }
    }

    /// Why startup refused, from the config file or from loading the key.
    #[derive(Debug)]
    enum StartupRefusal {
        File(FileConfigError),
        Broker(BrokerError),
    }

    /// Whether the broker that started signs its checkpoints.
    type StartupAttempt = krabka_bdd::Attempt<bool, StartupRefusal>;

    struct AuditStartupScenario {
        dir: TempDir,
        mode: krabka_bdd::Given<FailureMode>,
        key_path: Option<PathBuf>,
    }

    #[rstest::fixture]
    fn audit_startup() -> AuditStartupScenario {
        AuditStartupScenario {
            dir: TempDir::new().expect("scenario directory"),
            mode: krabka_bdd::Given::new("mode", "Given the broker audits in <mode> mode"),
            key_path: None,
        }
    }

    #[rstest::fixture]
    fn startup() -> StartupAttempt {
        // The When step replaces it with what the start attempt returned.
        StartupAttempt::NotExecuted
    }

    impl AuditStartupScenario {
        fn key_file(&mut self, contents: Option<&[u8]>) {
            let path = self.dir.path().join("audit.pk8");
            if let Some(contents) = contents {
                fs::write(&path, contents).expect("write the audit signing key file");
            }
            self.key_path = Some(path);
        }

        /// The broker.toml the operator wrote, applied, validated, and its
        /// signing key loaded, the order `Broker::start` runs them in.
        fn start(&self) -> Result<bool, StartupRefusal> {
            let signing = self.key_path.as_ref().map_or_else(String::new, |path| {
                format!(
                    "[audit.signing]\nkey_path = {:?}\nkey_id = \"audit-2026\"\n",
                    path.display().to_string()
                )
            });
            let toml = format!(
                "[audit]\nfailure_mode = \"{}\"\n{signing}",
                self.mode.get().toml()
            );
            let file: FileConfig = toml::from_str(&toml).expect("parse broker.toml");
            let mut config = BrokerConfig::for_tests(self.dir.path().join("log"));
            file.apply_to(&mut config).map_err(StartupRefusal::File)?;
            config.validate().map_err(StartupRefusal::Broker)?;
            load_audit_signer(&config)
                .map(|signer| signer.is_some())
                .map_err(StartupRefusal::Broker)
        }
    }

    #[krabka_bdd::given("the broker audits in {mode} mode")]
    fn audits_in(audit_startup: &mut AuditStartupScenario, mode: FailureMode) {
        audit_startup.mode.set(mode);
    }

    #[krabka_bdd::given("the audit signing key file the broker names does not exist")]
    fn key_file_missing(audit_startup: &mut AuditStartupScenario) { audit_startup.key_file(None); }

    #[krabka_bdd::given("the audit signing key file the broker names holds no key")]
    fn key_file_not_a_key(audit_startup: &mut AuditStartupScenario) {
        audit_startup.key_file(Some(b"not a PKCS#8 key"));
    }

    #[krabka_bdd::given("the broker names an Ed25519 audit signing key")]
    fn key_file_valid(audit_startup: &mut AuditStartupScenario) {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).expect("generate key");
        audit_startup.key_file(Some(pkcs8.as_ref()));
    }

    #[krabka_bdd::when("the broker attempts to start")]
    fn attempts_to_start(
        audit_startup: &AuditStartupScenario,
    ) -> krabka_bdd::Attempt<bool, StartupRefusal> {
        StartupAttempt::of(|| audit_startup.start())
    }

    #[krabka_bdd::then("startup is refused because the audit signing key did not load")]
    fn refused_key_did_not_load(audit_startup: &AuditStartupScenario, startup: &StartupAttempt) {
        let failed = startup.failed();
        assert!(let StartupRefusal::Broker(BrokerError::AuditSigningKey { path, .. }) = failed);
        assert!(Some(path) == audit_startup.key_path.as_ref());
    }

    #[krabka_bdd::then(
        "startup is refused because a fail-closed broker needs an audit signing key"
    )]
    fn refused_key_required(startup: &StartupAttempt) {
        let failed = startup.failed();
        assert!(let StartupRefusal::File(FileConfigError::InvalidConfig(message)) = failed);
        assert!(*message == BrokerError::AuditSigningKeyRequired.to_string());
    }

    #[krabka_bdd::then("the broker starts with checkpoint signing enabled")]
    fn starts_signed(startup: &StartupAttempt) {
        assert!(*startup.succeeded());
    }

    #[krabka_bdd::then("the broker starts with checkpoint signing disabled")]
    fn starts_unsigned(startup: &StartupAttempt) {
        assert!(!*startup.succeeded());
    }

    krabka_bdd::scenarios!(
        "tests/features/audit_signing_startup",
        fixtures = [audit_startup: AuditStartupScenario, startup: StartupAttempt],
    );
}
