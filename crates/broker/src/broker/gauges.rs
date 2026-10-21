//! The periodic broker gauge sampler and the stretch-cluster leader-drift
//! count it reports. The sampler walks the metadata image on a timer, so it is
//! kept apart from the event-driven liveness services that share the same
//! startup phase.

use {
    crate::{
        config::BrokerConfig,
        config_keys::{
            node_min_insync_replicas, resolve_broker_witness, resolve_preferred_leader_site,
        },
        elr::state::is_leaderless,
        health::HealthState,
        heartbeat::controller_state::ControllerLivenessState,
        log_dir_status::LogDirRegistry,
        metadata_source::MetadataSource,
        metrics::{BrokerMetrics, DirectoryLabel, RaftStateLabel},
        partition_registry::PartitionRegistry,
    },
    krabka_units::convert::TimeExt as _,
    std::sync::Arc,
    tokio_util::sync::CancellationToken,
};

/// Epoch milliseconds for the KIP-835 time-since-last-applied-record gauge.
fn epoch_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

/// The KIP-835 applied-record gauges one sample publishes. The fields carry
/// Kafka's units and its values before the first applied record, as
/// `BrokerServerMetrics` reads them from `MetadataProvenance.EMPTY`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AppliedRecordGauges {
    /// `last-applied-record-offset`: `-1` before the first record.
    applied_offset: i64,
    /// `LastCommittedRecordOffset`: `-1` before this node has heard of one.
    committed_offset: i64,
    /// `last-applied-record-timestamp`, in Unix epoch milliseconds: `-1`
    /// before the first record.
    applied_timestamp_ms: i64,
    /// `last-applied-record-lag-ms`: now minus the applied timestamp, on
    /// every role, so `now + 1` before the first record, as in Kafka.
    applied_lag_ms: i64,
    /// Committed offset minus applied offset, never below zero.
    lag_records: i64,
}

/// What a metadata source reports about its progress through the log, in
/// the source's own conventions.
#[derive(Debug, Clone, Copy)]
struct ReportedProgress {
    /// [`MetadataSource::current_metadata_offset`].
    applied_offset: i64,
    /// [`MetadataSource::quorum_committed_offset`].
    committed_offset: i64,
    /// [`MetadataSource::last_applied_record_timestamp_ms`]: zero or less
    /// before the source has applied a record.
    applied_timestamp_ms: i64,
}

impl ReportedProgress {
    fn read(source: &dyn MetadataSource) -> Self {
        Self {
            applied_offset: source.current_metadata_offset(),
            committed_offset: source.quorum_committed_offset(),
            applied_timestamp_ms: source.last_applied_record_timestamp_ms(),
        }
    }
}

impl AppliedRecordGauges {
    /// Derive the gauges from what a source reports, sampled at `now_ms`.
    ///
    /// A source reports a timestamp of zero or less before it has applied a
    /// record; Kafka's gauge reads `-1` then, and its lag reads `now + 1`.
    fn derive(reported: ReportedProgress, now_ms: i64) -> Self {
        let applied_timestamp_ms = if reported.applied_timestamp_ms > 0 {
            reported.applied_timestamp_ms
        } else {
            -1
        };
        Self {
            applied_offset: reported.applied_offset,
            committed_offset: reported.committed_offset,
            applied_timestamp_ms,
            applied_lag_ms: now_ms.saturating_sub(applied_timestamp_ms),
            lag_records: reported
                .committed_offset
                .saturating_sub(reported.applied_offset)
                .max(0),
        }
    }

    fn publish(self, metrics: &BrokerMetrics) {
        metrics
            .metadata_last_applied_offset
            .set(self.applied_offset);
        metrics
            .metadata_last_committed_offset
            .set(self.committed_offset);
        metrics
            .metadata_last_applied_timestamp_ms
            .set(self.applied_timestamp_ms);
        metrics
            .metadata_last_applied_lag_ms
            .set(self.applied_lag_ms);
        metrics.metadata_lag_records.set(self.lag_records);
    }
}

/// Counts the partitions `node_id` leads from a site other than the stretch
/// cluster's preferred leader site.
///
/// The count is zero when the cluster pins leadership to no site. A node
/// whose registration names no rack does not sit in the preferred site, so
/// its partitions count as drift. `node_id` leads every partition the count
/// considers, so one rack lookup covers all of them.
fn leader_site_drift_partitions(
    image: &krabka_metadata::MetadataImage,
    node_id: krabka_metadata::NodeId,
) -> usize {
    let Some(preferred) = resolve_preferred_leader_site(image) else {
        return 0;
    };
    let leader_site = image
        .broker(node_id)
        .and_then(|broker| broker.rack.as_deref());
    if leader_site == Some(preferred) {
        return 0;
    }
    image
        .all_partitions()
        .filter(|partition| partition.leader == node_id)
        .count()
}

fn update_metadata_health_metrics(
    metrics: &BrokerMetrics,
    controller: &dyn MetadataSource,
    now_ms: i64,
) {
    if let Some(snap) = controller.quorum_snapshot() {
        for state_label in RaftStateLabel::ALL {
            let is_active = i64::from(state_label.state == snap.current_state);
            metrics
                .raft_current_state
                .get_or_create(&state_label)
                .set(is_active);
        }
        metrics.raft_current_epoch.set(i64::from(snap.leader_epoch));
        metrics.raft_high_watermark.set(snap.high_watermark);
        metrics.raft_log_end_offset.set(snap.log_end_offset);
        metrics
            .raft_voters
            .set(i64::try_from(snap.voters.len()).unwrap_or(i64::MAX));
        metrics
            .raft_observers
            .set(i64::try_from(snap.observers.len()).unwrap_or(i64::MAX));
    }
    AppliedRecordGauges::derive(ReportedProgress::read(controller), now_ms).publish(metrics);
    let (processing_avg_us, processing_max_us) = controller.pending_record_processing_time_us();
    metrics
        .pending_record_processing_time_us_avg
        .set(i64::try_from(processing_avg_us).unwrap_or(i64::MAX));
    metrics
        .pending_record_processing_time_us_max
        .set(i64::try_from(processing_max_us).unwrap_or(i64::MAX));
    metrics.set_metadata_load_error_count(controller.metadata_load_error_count());
}

/// What this broker holds locally: the partitions it hosts and the health of
/// the directories they sit in. They travel together because one gauge pass
/// samples both, and neither is reachable from the metadata image.
pub(super) struct GaugeStorage {
    pub(super) partitions: Arc<PartitionRegistry>,
    pub(super) log_dir_status: LogDirRegistry,
}

/// The registry the gauge sample is written to and the health state the
/// broker-state gauge reads; they travel together because every sample writes
/// the first from the second.
pub(super) struct GaugeObservability {
    pub(super) metrics: BrokerMetrics,
    pub(super) health: Option<HealthState>,
}

/// Spawns the timer that samples the broker's cluster gauges.
pub(super) fn spawn_broker_gauge_updater(
    storage: GaugeStorage,
    controller: Arc<dyn MetadataSource>,
    liveness: Arc<ControllerLivenessState>,
    node_id: krabka_metadata::NodeId,
    observability: GaugeObservability,
    config: &BrokerConfig,
    shutdown: CancellationToken,
) {
    let GaugeStorage {
        partitions,
        log_dir_status,
    } = storage;
    let GaugeObservability { metrics, health } = observability;
    let poll_interval = config.gauge_poll_interval;
    let default_min_insync_replicas = config.default_min_insync_replicas;
    let static_voter_count = config.controller_quorum_voters.len();
    let audit_topic = config.audit_enabled.then(|| config.audit_topic.clone());
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(poll_interval.to_std());
        let mut previous_voted_directory = None;
        loop {
            if !super::tick_or_shutdown(&mut tick, &shutdown).await {
                return;
            }
            let led = partitions
                .arcs()
                .iter()
                .filter(|partition| {
                    partition
                        .current_leader
                        .load(std::sync::atomic::Ordering::Acquire)
                        == node_id
                })
                .count();
            metrics
                .partitions_led
                .set(i64::try_from(led).unwrap_or(i64::MAX));
            metrics
                .partitions_total
                .set(i64::try_from(partitions.len()).unwrap_or(i64::MAX));
            // The registry is the one place that knows a dir went offline: a
            // runtime write or fsync failure flips it there and nothing else
            // publishes the flip. `DescribeLogDirs` reports the same dirs,
            // but only to a client that asks.
            metrics.set_offline_log_dirs(log_dir_status.offline().len());
            let image = controller.current_image();
            let alive = liveness.alive_snapshot().await;
            let is_controller = controller
                .watch_leader()
                .borrow()
                .is_some_and(|leader| leader == node_id);
            let minimum_isr: std::collections::HashMap<&str, i32> = image
                .topics()
                .map(|topic| {
                    let minimum = node_min_insync_replicas(&image, node_id, &topic.name)
                        .unwrap_or(default_min_insync_replicas);
                    (topic.name.as_str(), minimum)
                })
                .collect();
            let mut partition_health = (0_usize, 0_usize, 0_usize);
            let mut offline_audit = 0_usize;
            for partition in image.all_partitions() {
                if partition.leader == node_id {
                    partition_health.0 +=
                        usize::from(partition.isr.len() < partition.replicas.len());
                    let minimum = minimum_isr
                        .get(partition.topic.as_str())
                        .copied()
                        .unwrap_or(default_min_insync_replicas);
                    partition_health.1 += usize::from(
                        i32::try_from(partition.isr.len()).unwrap_or(i32::MAX) < minimum,
                    );
                }
                // Kafka's OfflinePartitionsCount counts every partition with
                // no leader, internal topics included. A partition has none
                // when its recorded leader is gone, or when the controller
                // left it leaderless with the leader still alive, as the
                // KIP-112 offline-dir failover does: krabka's record keeps
                // that leader, and the ELR marker says it no longer leads.
                let offline = is_controller
                    && (!alive.contains(&partition.leader.0) || is_leaderless(&image, partition));
                partition_health.2 += usize::from(offline);
                // The audit share is the part a down broker explains: each
                // audit partition has one replica on its own broker (#1516).
                // A failed dir on a live broker is not down, so the alert
                // still counts it.
                offline_audit += usize::from(
                    offline
                        && audit_topic.as_deref() == Some(partition.topic.as_str())
                        && !partition
                            .replicas
                            .iter()
                            .any(|replica| alive.contains(&replica.0)),
                );
            }
            metrics
                .under_replicated_partitions
                .set(i64::try_from(partition_health.0).unwrap_or(i64::MAX));
            metrics
                .under_min_isr_partition_count
                .set(i64::try_from(partition_health.1).unwrap_or(i64::MAX));
            metrics
                .offline_partitions_count
                .set(i64::try_from(partition_health.2).unwrap_or(i64::MAX));
            metrics
                .offline_audit_partitions
                .set(i64::try_from(offline_audit).unwrap_or(i64::MAX));
            metrics.leader_site_drift_partitions.set(
                i64::try_from(leader_site_drift_partitions(&image, node_id)).unwrap_or(i64::MAX),
            );
            metrics
                .witness_role
                .set(i64::from(u8::from(resolve_broker_witness(&image, node_id))));
            metrics
                .active_controller
                .set(i64::from(u8::from(is_controller)));

            // Cluster state metrics published by the active controller (#390)
            if is_controller {
                let active = image
                    .brokers()
                    .filter(|b| alive.contains(&b.node_id.0))
                    .count();
                let fenced = image
                    .brokers()
                    .filter(|b| !alive.contains(&b.node_id.0))
                    .count();
                metrics
                    .active_brokers
                    .set(i64::try_from(active).unwrap_or(i64::MAX));
                metrics
                    .fenced_brokers
                    .set(i64::try_from(fenced).unwrap_or(i64::MAX));
                metrics
                    .global_topics
                    .set(i64::try_from(image.topics().count()).unwrap_or(i64::MAX));
                let mut total_partitions = 0_usize;
                let mut at_min_isr = 0_usize;
                let mut reassigning = 0_usize;
                let mut preferred_imbalance = 0_usize;
                for partition in image.all_partitions() {
                    total_partitions += 1;
                    let minimum = minimum_isr
                        .get(partition.topic.as_str())
                        .copied()
                        .unwrap_or(default_min_insync_replicas);
                    if i32::try_from(partition.isr.len()).unwrap_or(i32::MAX) == minimum {
                        at_min_isr += 1;
                    }
                    if !partition.adding_replicas.is_empty()
                        || !partition.removing_replicas.is_empty()
                    {
                        reassigning += 1;
                    }
                    if partition
                        .replicas
                        .first()
                        .is_some_and(|pref| *pref != partition.leader)
                    {
                        preferred_imbalance += 1;
                    }
                }
                metrics
                    .global_partitions
                    .set(i64::try_from(total_partitions).unwrap_or(i64::MAX));
                metrics
                    .at_min_isr_partition_count
                    .set(i64::try_from(at_min_isr).unwrap_or(i64::MAX));
                metrics
                    .reassigning_partitions
                    .set(i64::try_from(reassigning).unwrap_or(i64::MAX));
                metrics
                    .preferred_replica_imbalance
                    .set(i64::try_from(preferred_imbalance).unwrap_or(i64::MAX));
            } else {
                metrics.active_brokers.set(0);
                metrics.fenced_brokers.set(0);
                metrics.global_topics.set(0);
                metrics.global_partitions.set(0);
                metrics.at_min_isr_partition_count.set(0);
                metrics.reassigning_partitions.set(0);
                metrics.preferred_replica_imbalance.set(0);
            }

            update_metadata_health_metrics(&metrics, controller.as_ref(), epoch_now_ms());

            // Broker lifecycle state (#390)
            if let Some(h) = &health {
                metrics.broker_state.set(h.broker_state());
            }

            let ignored_static_voters =
                usize::from(image.kraft_version() >= 1).saturating_mul(static_voter_count);
            metrics
                .ignored_static_voters
                .set(i64::try_from(ignored_static_voters).unwrap_or(i64::MAX));
            let voted_directory = controller.voted_directory_id();
            if voted_directory != previous_voted_directory {
                if let Some(directory_id) = previous_voted_directory {
                    metrics
                        .voted_directory
                        .get_or_create(&DirectoryLabel {
                            directory_id: directory_id.to_string(),
                        })
                        .set(0);
                }
                if let Some(directory_id) = voted_directory {
                    metrics
                        .voted_directory
                        .get_or_create(&DirectoryLabel {
                            directory_id: directory_id.to_string(),
                        })
                        .set(1);
                }
                previous_voted_directory = voted_directory;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            broker::test_support::fake_source,
            config::DEFAULT_AUDIT_TOPIC,
            handlers::test_support::replicated_partition,
            metadata_source::test_support::{StartedController, start_controller, topic_record},
            metrics::BrokerMetrics,
            test_support::{broker_registration, single_partition_topic},
        },
        assert2::{assert, check},
        krabka_metadata::{MetadataImage, MetadataRecord, NodeId, PartitionElrRecord},
        krabka_units::{millis, secs},
    };

    /// One KIP-835 sample: what the source reports, when the gauge samples
    /// it, and the gauges Kafka would show.
    struct AppliedRecordCase {
        name: &'static str,
        reported: ReportedProgress,
        now_ms: i64,
        expected: AppliedRecordGauges,
    }

    #[test]
    fn applied_record_gauges_follow_kafka_units_and_initial_values() {
        const APPENDED_MS: i64 = 1_700_000_000_000;
        let cases = [
            AppliedRecordCase {
                name: "nothing applied: Kafka's MetadataProvenance.EMPTY",
                reported: ReportedProgress {
                    applied_offset: -1,
                    committed_offset: -1,
                    applied_timestamp_ms: 0,
                },
                now_ms: APPENDED_MS,
                expected: AppliedRecordGauges {
                    applied_offset: -1,
                    committed_offset: -1,
                    applied_timestamp_ms: -1,
                    applied_lag_ms: APPENDED_MS + 1,
                    lag_records: 0,
                },
            },
            AppliedRecordCase {
                name: "caught up, sampled 250 ms after the last append",
                reported: ReportedProgress {
                    applied_offset: 41,
                    committed_offset: 41,
                    applied_timestamp_ms: APPENDED_MS,
                },
                now_ms: APPENDED_MS + 250,
                expected: AppliedRecordGauges {
                    applied_offset: 41,
                    committed_offset: 41,
                    applied_timestamp_ms: APPENDED_MS,
                    applied_lag_ms: 250,
                    lag_records: 0,
                },
            },
            AppliedRecordCase {
                name: "behind the quorum by seven records and a minute",
                reported: ReportedProgress {
                    applied_offset: 100,
                    committed_offset: 107,
                    applied_timestamp_ms: APPENDED_MS,
                },
                now_ms: APPENDED_MS + 60_000,
                expected: AppliedRecordGauges {
                    applied_offset: 100,
                    committed_offset: 107,
                    applied_timestamp_ms: APPENDED_MS,
                    applied_lag_ms: 60_000,
                    lag_records: 7,
                },
            },
            AppliedRecordCase {
                name: "a record stamped ahead of the local clock lags negatively, unclamped",
                reported: ReportedProgress {
                    applied_offset: 5,
                    committed_offset: 5,
                    applied_timestamp_ms: APPENDED_MS + 30,
                },
                now_ms: APPENDED_MS,
                expected: AppliedRecordGauges {
                    applied_offset: 5,
                    committed_offset: 5,
                    applied_timestamp_ms: APPENDED_MS + 30,
                    applied_lag_ms: -30,
                    lag_records: 0,
                },
            },
        ];
        for case in cases {
            check!(
                AppliedRecordGauges::derive(case.reported, case.now_ms) == case.expected,
                "{}",
                case.name
            );
        }
    }

    /// Reads the KIP-835 applied-record gauges back from the registry.
    fn published_applied_record_gauges(metrics: &BrokerMetrics) -> AppliedRecordGauges {
        AppliedRecordGauges {
            applied_offset: metrics.metadata_last_applied_offset.get(),
            committed_offset: metrics.metadata_last_committed_offset.get(),
            applied_timestamp_ms: metrics.metadata_last_applied_timestamp_ms.get(),
            applied_lag_ms: metrics.metadata_last_applied_lag_ms.get(),
            lag_records: metrics.metadata_lag_records.get(),
        }
    }

    /// The active controller applies its own records, and its lag is the
    /// sample time minus their append time, as Kafka's
    /// `LastAppliedRecordLagMs` is: a leader is not exempt.
    #[tokio::test]
    async fn active_controller_lag_is_sample_time_minus_the_applied_append_time() {
        let StartedController {
            dir: _dir,
            controller,
        } = start_controller().await;
        controller
            .submit_change(vec![topic_record("kip835-lag")])
            .await
            .expect("the single voter commits the topic");
        let applied = ReportedProgress::read(&controller);
        assert!(applied.applied_timestamp_ms > 0);

        let metrics = BrokerMetrics::new();
        for elapsed_ms in [0, 250, 60_000] {
            update_metadata_health_metrics(
                &metrics,
                &controller,
                applied.applied_timestamp_ms + elapsed_ms,
            );
            check!(
                published_applied_record_gauges(&metrics)
                    == AppliedRecordGauges {
                        applied_offset: applied.applied_offset,
                        committed_offset: applied.applied_offset,
                        applied_timestamp_ms: applied.applied_timestamp_ms,
                        applied_lag_ms: elapsed_ms,
                        lag_records: 0,
                    },
                "sampled {elapsed_ms} ms after the append"
            );
        }
        controller.shutdown().await;
    }

    #[tokio::test]
    async fn broker_gauge_uses_configured_default_min_isr() {
        let node_id = krabka_metadata::NodeId(1);
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        image.apply(&krabka_metadata::MetadataRecord::V1Topic(
            single_partition_topic("gauge-topic", uuid::Uuid::nil()),
        ));
        image.apply(&krabka_metadata::MetadataRecord::V1Partition(
            krabka_metadata::PartitionRecord {
                topic: "gauge-topic".into(),
                partition: 0,
                leader: node_id,
                replicas: vec![node_id],
                isr: vec![node_id],
                leader_epoch: krabka_metadata::LeaderEpoch(0),
                adding_replicas: Vec::new(),
                removing_replicas: Vec::new(),
                directories: Vec::new(),
                partition_epoch: 0,
            },
        ));
        let gauges = spawn_test_gauges()
            .image(image)
            .node_id(node_id)
            .default_min_insync_replicas(2)
            .call();

        gauges
            .wait_until("the gauge observes the configured minimum ISR", |metrics| {
                metrics.under_min_isr_partition_count.get() == 1
            })
            .await;
        assert!(
            gauges.metrics.offline_partitions_count.get() == 0,
            "a non-controller has no cluster liveness view and must not invent an offline partition"
        );
    }

    /// One offline-partition sample on the active controller of a three-broker
    /// cluster: which brokers are alive, which extra partition sits on
    /// broker 3, whether audit is on, and the two gauges it should publish.
    struct OfflineCase {
        name: &'static str,
        alive: &'static [u64],
        audit_enabled: bool,
        single_replica_user_partition_on_broker_3: bool,
        /// A partition, by topic and index, that the controller left without
        /// a leader while its recorded leader stays alive, as the KIP-112
        /// offline-dir failover does.
        leaderless_marker: Option<(&'static str, i32)>,
        expected_offline: i64,
        expected_offline_audit: i64,
    }

    /// The `deploy.md` example: three brokers, an RF=3 user topic, and the
    /// audit topic with one RF=1 partition on each broker. Optionally a user
    /// topic with one RF=1 partition on broker 3, and one partition that
    /// carries the ELR leaderless marker: its last-known ELR names only its
    /// recorded leader.
    #[bon::builder]
    fn three_broker_image(
        #[builder(default)] single_replica_user_partition_on_broker_3: bool,
        leaderless_marker: Option<(&'static str, i32)>,
    ) -> MetadataImage {
        let brokers = [NodeId(1), NodeId(2), NodeId(3)];
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for node_id in brokers {
            image.apply(&MetadataRecord::V1BrokerRegistration(broker_registration(
                node_id,
            )));
        }
        image.apply(&topic_record("orders"));
        image.apply(&MetadataRecord::V1Partition(
            replicated_partition().replicas(&brokers).call(),
        ));
        image.apply(&topic_record(DEFAULT_AUDIT_TOPIC));
        for (index, node_id) in (0..).zip(brokers) {
            image.apply(&MetadataRecord::V1Partition(
                replicated_partition()
                    .topic(DEFAULT_AUDIT_TOPIC)
                    .partition(index)
                    .leader(node_id)
                    .replicas(&[node_id])
                    .call(),
            ));
        }
        if single_replica_user_partition_on_broker_3 {
            image.apply(&topic_record("ledger"));
            image.apply(&MetadataRecord::V1Partition(
                replicated_partition()
                    .topic("ledger")
                    .leader(NodeId(3))
                    .replicas(&[NodeId(3)])
                    .call(),
            ));
        }
        if let Some((topic, partition)) = leaderless_marker {
            let leader = image
                .partition(topic, partition)
                .expect("marked partition")
                .leader;
            image.apply(&MetadataRecord::V1PartitionElr(PartitionElrRecord {
                topic: topic.into(),
                partition,
                eligible_leader_replicas: Vec::new(),
                last_known_elr: vec![leader],
            }));
        }
        image
    }

    /// #1516: a down broker leaves its RF=1 audit partition leaderless. The
    /// Kafka-named gauge counts it, as Kafka's `OfflinePartitionsCount`
    /// counts every leaderless partition, and the audit gauge says how much
    /// of that count is audit, so the alert can leave it out.
    #[tokio::test]
    async fn controller_separates_offline_audit_partitions_from_the_offline_count() {
        let cases = [
            OfflineCase {
                name: "every broker alive",
                alive: &[1, 2, 3],
                audit_enabled: true,
                single_replica_user_partition_on_broker_3: false,
                leaderless_marker: None,
                expected_offline: 0,
                expected_offline_audit: 0,
            },
            OfflineCase {
                name: "broker 3 down: only its audit partition is leaderless",
                alive: &[1, 2],
                audit_enabled: true,
                single_replica_user_partition_on_broker_3: false,
                leaderless_marker: None,
                expected_offline: 1,
                expected_offline_audit: 1,
            },
            OfflineCase {
                name: "broker 3 down with a user partition only it holds",
                alive: &[1, 2],
                audit_enabled: true,
                single_replica_user_partition_on_broker_3: true,
                leaderless_marker: None,
                expected_offline: 2,
                expected_offline_audit: 1,
            },
            OfflineCase {
                name: "audit disabled: a topic with the audit name is a user topic",
                alive: &[1, 2],
                audit_enabled: false,
                single_replica_user_partition_on_broker_3: false,
                leaderless_marker: None,
                expected_offline: 1,
                expected_offline_audit: 0,
            },
            OfflineCase {
                name: "every broker alive, a user partition leaderless after a dir failure",
                alive: &[1, 2, 3],
                audit_enabled: true,
                single_replica_user_partition_on_broker_3: false,
                leaderless_marker: Some(("orders", 0)),
                expected_offline: 1,
                expected_offline_audit: 0,
            },
            OfflineCase {
                name: "every broker alive, an audit partition leaderless after a dir failure",
                alive: &[1, 2, 3],
                audit_enabled: true,
                single_replica_user_partition_on_broker_3: false,
                leaderless_marker: Some((DEFAULT_AUDIT_TOPIC, 0)),
                expected_offline: 1,
                expected_offline_audit: 0,
            },
        ];
        for case in cases {
            let image = three_broker_image()
                .single_replica_user_partition_on_broker_3(
                    case.single_replica_user_partition_on_broker_3,
                )
                .maybe_leaderless_marker(case.leaderless_marker)
                .call();
            let total_partitions = i64::try_from(image.all_partitions().count()).expect("count");
            let liveness = Arc::new(ControllerLivenessState::new(secs(10)));
            for node_id in case.alive {
                liveness.record_heartbeat(*node_id).await;
            }
            let gauges = spawn_test_gauges()
                .image(image)
                .leader(krabka_raft::NodeId(1))
                .liveness(liveness)
                .audit_enabled(case.audit_enabled)
                .call();

            // The controller-only cluster gauges are written in the same pass,
            // after the offline counts, so a full partition count means the
            // pass that set the offline counts has run.
            gauges
                .wait_until("the controller publishes a gauge pass", |metrics| {
                    metrics.global_partitions.get() == total_partitions
                })
                .await;
            let metrics = &gauges.metrics;
            check!(
                (
                    metrics.offline_partitions_count.get(),
                    metrics.offline_audit_partitions.get()
                ) == (case.expected_offline, case.expected_offline_audit),
                "{}",
                case.name
            );
        }
    }

    /// A gauge sampler a test started: the registry it writes, and the token
    /// that stops it when the handle drops.
    struct RunningGauges {
        metrics: BrokerMetrics,
        shutdown: CancellationToken,
    }

    impl RunningGauges {
        /// Wait up to a second for the published gauges to satisfy `done`.
        async fn wait_until(&self, what: &str, done: impl Fn(&BrokerMetrics) -> bool) {
            let waited = tokio::time::timeout(std::time::Duration::from_secs(1), async {
                while !done(&self.metrics) {
                    tokio::task::yield_now().await;
                }
            })
            .await;
            assert!(waited.is_ok(), "timed out waiting until {what}");
        }
    }

    impl Drop for RunningGauges {
        fn drop(&mut self) { self.shutdown.cancel(); }
    }

    /// Start the gauge sampler over `image` with no local partitions, on a
    /// test config that polls every millisecond. `leader` names the quorum
    /// leader the source reports; the node is the active controller when it
    /// names `node_id`. The two config overrides keep the test defaults when
    /// unset.
    #[bon::builder]
    fn spawn_test_gauges(
        image: MetadataImage,
        leader: Option<krabka_raft::NodeId>,
        #[builder(default = Arc::new(ControllerLivenessState::new(secs(10))))] liveness: Arc<
            ControllerLivenessState,
        >,
        #[builder(default)] log_dirs: LogDirRegistry,
        #[builder(default = NodeId(1))] node_id: NodeId,
        audit_enabled: Option<bool>,
        default_min_insync_replicas: Option<i32>,
    ) -> RunningGauges {
        let mut config = BrokerConfig::for_tests(std::path::PathBuf::new());
        config.gauge_poll_interval = millis(1);
        config.audit_enabled = audit_enabled.unwrap_or(config.audit_enabled);
        config.default_min_insync_replicas =
            default_min_insync_replicas.unwrap_or(config.default_min_insync_replicas);
        let gauges = RunningGauges {
            metrics: BrokerMetrics::new(),
            shutdown: CancellationToken::new(),
        };
        spawn_broker_gauge_updater(
            GaugeStorage {
                partitions: Arc::new(PartitionRegistry::new()),
                log_dir_status: log_dirs,
            },
            Arc::new(fake_source(image, leader)),
            liveness,
            node_id,
            GaugeObservability {
                metrics: gauges.metrics.clone(),
                health: None,
            },
            &config,
            gauges.shutdown.child_token(),
        );
        gauges
    }

    /// A dir that goes offline under live traffic has to reach the scrape.
    /// The registry is the only thing that knows about the flip, and
    /// `DescribeLogDirs` reports it only to a client that asks.
    #[tokio::test]
    async fn broker_gauge_publishes_the_offline_log_dir_count() {
        let dir = tempfile::tempdir().expect("log root");
        let log_dirs = LogDirRegistry::probe(&[dir.path().to_path_buf()]);
        let gauges = spawn_test_gauges()
            .image(MetadataImage::new(uuid::Uuid::nil()))
            .log_dirs(log_dirs.clone())
            .call();

        // A healthy dir publishes the series at zero, so an operator can tell
        // "no dir is offline" apart from "this broker does not report".
        gauges
            .wait_until("the sampler ran at least once", |metrics| {
                metrics.partitions_total.get() == 0 && metrics.offline_log_dirs.get() == 0
            })
            .await;

        log_dirs.mark_offline(dir.path(), "partition write/fsync failed: synthetic EIO");
        gauges
            .wait_until("the flip reaches the gauge without a restart", |metrics| {
                metrics.offline_log_dirs.get() == 1
            })
            .await;
    }

    /// An image where node 1 sits in `rack` and leads `led` partitions of one
    /// topic, and node 2 sits in `dc-a` and leads one more.
    fn stretch_image(rack: &str, led: i32) -> krabka_metadata::MetadataImage {
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        for (node_id, node_rack) in [(1_u64, rack), (2_u64, "dc-a")] {
            image.apply(&krabka_metadata::MetadataRecord::V1BrokerRegistration(
                krabka_metadata::BrokerRegistrationRecord {
                    rack: Some(node_rack.to_string()),
                    ..broker_registration(krabka_raft::NodeId(node_id))
                },
            ));
        }
        image.apply(&krabka_metadata::MetadataRecord::V1BrokerConfig(
            krabka_metadata::BrokerConfigRecord {
                node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: "stretch.preferred.leader.site".into(),
                config_value: Some("dc-a".into()),
            },
        ));
        for partition in 0..=led {
            let leader = if partition < led { 1 } else { 2 };
            image.apply(&krabka_metadata::MetadataRecord::V1Partition(
                replicated_partition()
                    .topic("stretch-topic")
                    .partition(partition)
                    .leader(leader)
                    .call(),
            ));
        }
        image
    }

    #[test]
    fn leader_site_drift_counts_only_partitions_led_outside_the_preferred_site() {
        for (rack, led, expected) in [("dc-b", 2, 2), ("dc-a", 2, 0)] {
            let image = stretch_image(rack, led);

            check!(
                leader_site_drift_partitions(&image, krabka_metadata::NodeId(1)) == expected,
                "rack {rack}"
            );
            // Node 2 leads one partition from the preferred site, so it never
            // drifts whatever node 1 does.
            check!(
                leader_site_drift_partitions(&image, krabka_metadata::NodeId(2)) == 0,
                "rack {rack}"
            );
        }
    }

    #[test]
    fn leader_site_drift_is_zero_when_the_cluster_pins_no_leader_site() {
        let mut image = stretch_image("dc-b", 2);
        image.apply(&krabka_metadata::MetadataRecord::V1BrokerConfig(
            krabka_metadata::BrokerConfigRecord {
                node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: "stretch.preferred.leader.site".into(),
                config_value: None,
            },
        ));

        assert!(leader_site_drift_partitions(&image, krabka_metadata::NodeId(1)) == 0);
    }
}
