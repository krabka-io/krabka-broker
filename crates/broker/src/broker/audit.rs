//! `FedRAMP` MLA audit pipeline bring-up: the signing key, the on-disk spool,
//! the writer task that drains audit events into the audit topic, and the
//! metrics poller that reports on it. It lives apart from the rest of startup
//! because it also rewraps the configured authorizer.

use {
    crate::{
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

fn audit_signer(config: &BrokerConfig) -> Option<Arc<krabka_audit::FileEd25519Signer>> {
    let (Some(path), Some(key_id)) = (&config.audit_signing_key_path, &config.audit_signing_key_id)
    else {
        tracing::info!("no audit signing key configured; checkpoints disabled");
        return None;
    };
    match krabka_audit::FileEd25519Signer::from_pkcs8_file(path, key_id.clone()) {
        Ok(signer) => Some(Arc::new(signer)),
        Err(error) => {
            tracing::error!(%error, "failed to load audit signing key; checkpoints disabled");
            None
        }
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
            signer: audit_signer(config),
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
        assert2::assert,
        krabka_metadata::{
            FeatureLevelRecord, MetadataRecord, krabka_version::KRABKA_VERSION_FEATURE,
        },
        krabka_units::minutes,
        std::time::Duration,
        tokio::{task, time},
        uuid::Uuid,
    };

    /// An image whose finalized `krabka.version` is `level`.
    fn image_at_krabka_version(level: i16) -> Arc<MetadataImage> {
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: KRABKA_VERSION_FEATURE.into(),
            level,
        }));
        Arc::new(image)
    }

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
}
