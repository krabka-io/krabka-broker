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
    std::{
        collections::{HashMap, HashSet},
        sync::{Arc, atomic},
        time::{SystemTime, UNIX_EPOCH},
    },
    tokio::time,
    tokio_util::sync::CancellationToken,
};

/// Epoch milliseconds for the KIP-835 time-since-last-applied-record gauge.
fn epoch_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

/// Converts a count to the `i64` a gauge holds, saturating at `i64::MAX`.
fn gauge_count(count: usize) -> i64 { i64::try_from(count).unwrap_or(i64::MAX) }

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
            .set(gauge_count(snap.voters.len()));
        metrics
            .raft_observers
            .set(gauge_count(snap.observers.len()));
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

/// The `min.insync.replicas` each topic in an image resolves to on one node,
/// and the broker default for a topic the image does not name.
struct MinimumIsr<'a> {
    by_topic: HashMap<&'a str, i32>,
    default: i32,
}

impl<'a> MinimumIsr<'a> {
    fn resolve(
        image: &'a krabka_metadata::MetadataImage,
        node_id: krabka_metadata::NodeId,
        default: i32,
    ) -> Self {
        let by_topic = image
            .topics()
            .map(|topic| {
                let minimum =
                    node_min_insync_replicas(image, node_id, &topic.name).unwrap_or(default);
                (topic.name.as_str(), minimum)
            })
            .collect();
        Self { by_topic, default }
    }

    fn for_topic(&self, topic: &str) -> i32 {
        self.by_topic.get(topic).copied().unwrap_or(self.default)
    }
}

/// The ISR size compared against a topic's minimum, saturating at `i32::MAX`.
fn isr_size(partition: &krabka_metadata::PartitionRecord) -> i32 {
    i32::try_from(partition.isr.len()).unwrap_or(i32::MAX)
}

/// One sample's view of the cluster, which the partition counts read.
struct GaugeInputs<'a> {
    image: &'a krabka_metadata::MetadataImage,
    node_id: krabka_metadata::NodeId,
    /// The brokers the controller's liveness view holds alive.
    alive: &'a HashSet<u64>,
    /// Whether this node is the active controller, the one node whose
    /// liveness view can say a partition is offline.
    is_controller: bool,
    minimum_isr: MinimumIsr<'a>,
    /// The audit topic, when audit is enabled.
    audit_topic: Option<&'a str>,
}

impl GaugeInputs<'_> {
    /// Kafka's OfflinePartitionsCount counts every partition with no leader,
    /// internal topics included. A partition has none when its recorded
    /// leader is gone, or when the controller left it leaderless with the
    /// leader still alive, as the KIP-112 offline-dir failover does: krabka's
    /// record keeps that leader, and the ELR marker says it no longer leads.
    fn is_offline(&self, partition: &krabka_metadata::PartitionRecord) -> bool {
        self.is_controller
            && (!self.alive.contains(&partition.leader.0) || is_leaderless(self.image, partition))
    }

    /// The audit share is the part a down broker explains: each audit
    /// partition has one replica on its own broker (#1516). A failed dir on a
    /// live broker is not down, so the alert still counts it.
    fn is_audit_partition_without_live_replica(
        &self,
        partition: &krabka_metadata::PartitionRecord,
    ) -> bool {
        self.audit_topic == Some(partition.topic.as_str())
            && !partition
                .replicas
                .iter()
                .any(|replica| self.alive.contains(&replica.0))
    }
}

/// The partition-health gauges every broker publishes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct LocalPartitionHealth {
    /// Partitions this node leads whose ISR is smaller than the replica set.
    under_replicated: usize,
    /// Partitions this node leads whose ISR is below the topic's minimum.
    under_min_isr: usize,
    /// Partitions with no leader; only the active controller counts any.
    offline: usize,
    /// The audit partitions among `offline` that have no live replica.
    offline_audit: usize,
}

impl LocalPartitionHealth {
    fn count(inputs: &GaugeInputs<'_>) -> Self {
        let mut health = Self::default();
        for partition in inputs.image.all_partitions() {
            if partition.leader == inputs.node_id {
                health.under_replicated +=
                    usize::from(partition.isr.len() < partition.replicas.len());
                health.under_min_isr += usize::from(
                    isr_size(partition) < inputs.minimum_isr.for_topic(&partition.topic),
                );
            }
            let offline = inputs.is_offline(partition);
            health.offline += usize::from(offline);
            health.offline_audit += usize::from(
                offline && inputs.is_audit_partition_without_live_replica(partition),
            );
        }
        health
    }

    fn publish(self, metrics: &BrokerMetrics) {
        metrics
            .under_replicated_partitions
            .set(gauge_count(self.under_replicated));
        metrics
            .under_min_isr_partition_count
            .set(gauge_count(self.under_min_isr));
        metrics
            .offline_partitions_count
            .set(gauge_count(self.offline));
        metrics
            .offline_audit_partitions
            .set(gauge_count(self.offline_audit));
    }
}

/// The cluster-wide gauges the active controller publishes (#390).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct ClusterCounts {
    /// Registered brokers the liveness view holds alive.
    active_brokers: usize,
    /// Registered brokers the liveness view does not hold alive.
    fenced_brokers: usize,
    topics: usize,
    /// Every partition in the image.
    partitions: usize,
    /// Partitions whose ISR size equals the topic's minimum.
    at_min_isr: usize,
    /// Partitions with replicas being added or removed.
    reassigning: usize,
    /// Partitions not led by their first, preferred, replica.
    preferred_imbalance: usize,
}

impl ClusterCounts {
    fn count(inputs: &GaugeInputs<'_>) -> Self {
        let image = inputs.image;
        let mut counts = Self {
            active_brokers: image
                .brokers()
                .filter(|broker| inputs.alive.contains(&broker.node_id.0))
                .count(),
            fenced_brokers: image
                .brokers()
                .filter(|broker| !inputs.alive.contains(&broker.node_id.0))
                .count(),
            topics: image.topics().count(),
            ..Self::default()
        };
        for partition in image.all_partitions() {
            counts.partitions += 1;
            counts.at_min_isr += usize::from(
                isr_size(partition) == inputs.minimum_isr.for_topic(&partition.topic),
            );
            counts.reassigning += usize::from(
                !partition.adding_replicas.is_empty() || !partition.removing_replicas.is_empty(),
            );
            counts.preferred_imbalance += usize::from(
                partition
                    .replicas
                    .first()
                    .is_some_and(|preferred| *preferred != partition.leader),
            );
        }
        counts
    }

    fn publish(self, metrics: &BrokerMetrics) {
        metrics.active_brokers.set(gauge_count(self.active_brokers));
        metrics.fenced_brokers.set(gauge_count(self.fenced_brokers));
        metrics.global_topics.set(gauge_count(self.topics));
        metrics.global_partitions.set(gauge_count(self.partitions));
        metrics
            .at_min_isr_partition_count
            .set(gauge_count(self.at_min_isr));
        metrics
            .reassigning_partitions
            .set(gauge_count(self.reassigning));
        metrics
            .preferred_replica_imbalance
            .set(gauge_count(self.preferred_imbalance));
    }
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
        let mut tick = time::interval(poll_interval.to_std());
        let mut previous_voted_directory = None;
        loop {
            if !super::tick_or_shutdown(&mut tick, &shutdown).await {
                return;
            }
            let led = partitions
                .arcs()
                .iter()
                .filter(|partition| {
                    partition.current_leader.load(atomic::Ordering::Acquire) == node_id
                })
                .count();
            metrics
                .partitions_led
                .set(gauge_count(led));
            metrics
                .partitions_total
                .set(gauge_count(partitions.len()));
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
            let inputs = GaugeInputs {
                image: &image,
                node_id,
                alive: &alive,
                is_controller,
                minimum_isr: MinimumIsr::resolve(&image, node_id, default_min_insync_replicas),
                audit_topic: audit_topic.as_deref(),
            };
            LocalPartitionHealth::count(&inputs).publish(&metrics);
            metrics.leader_site_drift_partitions.set(
                gauge_count(leader_site_drift_partitions(&image, node_id)),
            );
            metrics
                .witness_role
                .set(i64::from(u8::from(resolve_broker_witness(&image, node_id))));
            metrics
                .active_controller
                .set(i64::from(u8::from(is_controller)));

            // Cluster state metrics published by the active controller (#390);
            // every other node publishes zeros.
            let cluster = if is_controller {
                ClusterCounts::count(&inputs)
            } else {
                ClusterCounts::default()
            };
            cluster.publish(&metrics);

            update_metadata_health_metrics(&metrics, controller.as_ref(), epoch_now_ms());

            // Broker lifecycle state (#390)
            if let Some(h) = &health {
                metrics.broker_state.set(h.broker_state());
            }

            let ignored_static_voters =
                usize::from(image.kraft_version() >= 1).saturating_mul(static_voter_count);
            metrics
                .ignored_static_voters
                .set(gauge_count(ignored_static_voters));
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
        std::{collections::BTreeMap, path::PathBuf, time::Duration},
        tokio::{task, time},
    };

    #[derive(Default)]
    struct MetadataGaugeSample {
        now: Option<i64>,
        reported: Option<ReportedProgress>,
        sampled: Option<AppliedRecordGauges>,
    }

    #[rstest::fixture]
    fn metadata_gauge_sample() -> MetadataGaugeSample {
        // Sampling each source observation starts without prior gauge values.
        MetadataGaugeSample::default()
    }

    #[rstest_bdd_macros::given("metadata progress and sampling time are JSON {progress}")]
    fn supplied_metadata_progress(
        metadata_gauge_sample: &mut MetadataGaugeSample,
        progress: String,
    ) {
        let fields: BTreeMap<String, i64> =
            serde_json::from_str(&progress).expect("explicit metadata source offsets and clock");
        let field = |name: &str| {
            *fields
                .get(name)
                .expect("complete metadata source observation")
        };
        metadata_gauge_sample.reported = Some(ReportedProgress {
            applied_offset: field("applied"),
            committed_offset: field("committed"),
            applied_timestamp_ms: field("append_timestamp"),
        });
        metadata_gauge_sample.now = Some(field("sample_at"));
    }

    #[rstest_bdd_macros::when("the metadata progress gauges are sampled")]
    fn sample_metadata_progress(metadata_gauge_sample: &mut MetadataGaugeSample) {
        metadata_gauge_sample.sampled = Some(AppliedRecordGauges::derive(
            metadata_gauge_sample
                .reported
                .take()
                .expect("source progress was given"),
            metadata_gauge_sample.now.expect("sampling clock was given"),
        ));
    }

    #[rstest_bdd_macros::then("the complete Kafka metadata gauge sample is JSON {gauges}")]
    fn expected_metadata_progress(metadata_gauge_sample: &MetadataGaugeSample, gauges: String) {
        let fields: BTreeMap<String, i64> =
            serde_json::from_str(&gauges).expect("independent complete Kafka gauge sample");
        let field = |name: &str| {
            *fields
                .get(name)
                .expect("complete expected metadata gauge sample")
        };
        check!(
            metadata_gauge_sample.sampled
                == Some(AppliedRecordGauges {
                    applied_offset: field("applied"),
                    committed_offset: field("committed"),
                    applied_timestamp_ms: field("timestamp"),
                    applied_lag_ms: field("lag_ms"),
                    lag_records: field("lag_records")
                })
        );
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/broker/gauges.feature",
        name = "Sample metadata progress using Kafka units and initial values"
    )]
    fn applied_record_gauges_follow_kafka_units_and_initial_values(
        metadata_gauge_sample: MetadataGaugeSample,
        case: String,
        progress: String,
        gauges: String,
    ) {
        let _ = (metadata_gauge_sample, case, progress, gauges);
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
            let waited = time::timeout(Duration::from_secs(1), async {
                while !done(&self.metrics) {
                    task::yield_now().await;
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
        let mut config = BrokerConfig::for_tests(PathBuf::new());
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
                    rack: Some(node_rack.to_owned()),
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
