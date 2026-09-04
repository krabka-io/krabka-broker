//! Release of the per-partition and per-topic series that a reassigned
//! partition or a deleted topic leaves behind.
//!
//! `Family::get_or_create` materialises a series on first produce, fetch,
//! replication or compaction, and `prometheus-client` never releases one on
//! its own. Without this module the `/metrics` body grows for the life of the
//! process on any cluster that reassigns partitions or deletes topics: nine
//! per-partition series and a dozen per-topic series stay in the body for a
//! partition this broker no longer hosts.
//!
//! The rule is the one the metadata image already states. A partition's series
//! live while this broker sits in that partition's replica set, and a topic's
//! series -- along with the series of every partition that topic held -- live
//! while that topic exists, where "that topic" means the incarnation carrying
//! the topic id the image named and not merely the name. The mechanism is the
//! one `share_partition::backlog_poller` uses for the share-group backlog
//! gauge: remember the label sets the last image justified, then remove the
//! ones the next image no longer does.
//!
//! A deleted topic releases the series of partitions this broker never
//! replicated, because a produce or fetch that this broker rejected as
//! misrouted is accounted for under the partition the client asked for. That
//! keeps such a series bounded by the partitions the cluster holds. It leaves
//! one case unbounded: a label set no image ever named, which a client
//! produces by naming a topic or a partition index that does not exist.
//! Releasing those needs eviction driven by series creation rather than by the
//! image diff, which is issue #199.
//!
//! Five further families are keyed by a partition without taking a
//! [`PartitionLabel`], and each is left to a narrower owner that releases it
//! sooner than this diff could. `share_group_backlog` is pruned by
//! `share_partition::backlog_poller` on its own tick. The four diskless WAL
//! gauges -- `diskless_wal_durable_watermark`,
//! `diskless_wal_index_projection_lag`, `diskless_wal_trim_frontier` and
//! `diskless_wal_voter_lag` -- are keyed by topic id and released by
//! `wal::quorum::registry::WalShardRegistry`, whose `replace_placements`
//! reconfigures every live shard engine against the newest image and whose
//! `remove` clears a shard the supervisor tore down. Routing those through
//! [`BrokerMetrics::evict_partition_series`] would be wrong as well as
//! redundant: a shard's voters are selected from the registered brokers rather
//! than from the partition's replica set, so this broker can still vote on --
//! and still report lag for -- a shard whose replicas no longer name it.
//!
//! The two lag families of `metrics::lag` are keyed that way too, and they
//! join in here rather than being left to a narrower owner. Their samplers
//! already release what a pass stops naming, but a reassignment or a topic
//! delete must not wait for the next pass, and a group that leaves this
//! coordinator is never named by a pass again. Each entry point below reaches
//! the families its own rule justifies: the per-partition one covers replica
//! lag, because "this broker left the replica set" is exactly what ends a
//! follower's series, while consumer-group lag follows the group rather than
//! the host and so is reached only by the per-topic entry point and by
//! `evict_group_series`, which gives group removal -- an event no image
//! records -- its own one call to make.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use krabka_metadata::{MetadataImage, NodeId};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{BrokerMetrics, PartitionLabel, QuotaType, SchemaRejectionLabel, TopicLabel};
use crate::schema_validation::RejectReason;

impl BrokerMetrics {
    /// Releases the per-entity throttle series one expired quota bucket
    /// published.
    ///
    /// A bucket materialises its series the first time that entity is
    /// throttled, and `prometheus-client` never releases one on its own. The
    /// quota-expiry sweep calls this so an inactive tenant's label set leaves
    /// the `/metrics` body with its bucket, rather than staying in the body
    /// for the life of the process. The label set is the one
    /// `observe_quota_throttle_for_entity` builds, so a bucket that was never
    /// throttled has no series and the removal is a no-op.
    pub fn evict_quota_entity_series(
        &self,
        quota_type: QuotaType,
        user: Option<String>,
        client_id: Option<String>,
    ) {
        self.quota_entity_throttle_seconds_total
            .remove(&crate::metrics::QuotaEntityLabel {
                quota_type,
                user,
                client_id,
            });
    }


    /// Drop every series that any per-partition family carries for `label`.
    ///
    /// This is the one entry point for partition-series eviction: a caller
    /// that learns a partition left this broker calls it and never has to know
    /// which families take a [`PartitionLabel`].
    pub(crate) fn evict_partition_series(&self, label: &PartitionLabel) {
        for family in [
            &self.partition_bytes_in,
            &self.partition_bytes_out,
            &self.replication_bytes_in,
            &self.replication_bytes_out,
            &self.partition_cpu_micros,
            &self.log_compactions_total,
        ] {
            family.remove(label);
        }
        for family in [
            &self.partition_disk_bytes,
            &self.delivery_watermark,
            &self.delivery_pending_records,
        ] {
            family.remove(label);
        }
        self.evict_partition_lag_series(label);
    }

    /// Drop every series that any per-topic family carries for `topic`.
    ///
    /// The companion of [`BrokerMetrics::evict_partition_series`], and the one
    /// entry point for topic-series eviction. It covers
    /// `schema_validation_rejections` too, whose label set is the topic paired
    /// with each of [`RejectReason::LABELS`].
    pub(crate) fn evict_topic_series(&self, topic: &str) {
        let label = TopicLabel {
            topic: Arc::from(topic),
        };
        for family in [
            &self.topic_bytes_in,
            &self.topic_bytes_out,
            &self.topic_messages_in,
            &self.topic_produce_requests,
            &self.topic_fetch_requests,
            &self.topic_failed_produce_requests,
            &self.topic_failed_fetch_requests,
            &self.produce_message_conversions,
            &self.fetch_message_conversions,
            &self.barrier_markers_written_total,
            &self.topic_freeze_rejections,
            &self.remote_copy_bytes_total,
            &self.remote_fetch_bytes_total,
            &self.remote_copy_requests_total,
            &self.remote_fetch_requests_total,
            &self.remote_delete_requests_total,
            &self.remote_copy_errors_total,
            &self.remote_fetch_errors_total,
            &self.remote_delete_errors_total,
        ] {
            family.remove(&label);
        }
        for family in [
            &self.remote_copy_lag_bytes,
            &self.remote_copy_lag_segments,
            &self.remote_delete_lag_bytes,
            &self.remote_delete_lag_segments,
        ] {
            family.remove(&label);
        }
        for reason in RejectReason::LABELS {
            self.schema_validation_rejections
                .remove(&SchemaRejectionLabel {
                    topic: topic.to_string(),
                    reason: reason.to_string(),
                });
        }
        self.evict_topic_lag_series(topic);
    }
}

/// One topic as the last image described it: which incarnation it was, and
/// which partition indexes it held.
///
/// The id is what separates a topic from a same-named topic created after it
/// was deleted. A `watch` channel publishes only the newest image, so a delete
/// and a recreate that land between two of this evictor's passes arrive as one
/// image in which the name never disappeared; without the id the old
/// incarnation's counters would carry over into the new one.
///
/// The partition indexes are every partition the image named, not only the
/// ones this broker replicates. Produce and fetch account for a partition the
/// broker rejected as well as one it served, so a misrouted request
/// materialises a series for a partition this broker does not host, and the
/// topic going away is what releases it.
#[derive(Debug, PartialEq, Eq)]
struct TrackedTopic {
    id: Uuid,
    partitions: Vec<i32>,
}

/// Reconciles the live metric series against the newest metadata image.
///
/// It holds the `(topic, partition)` pairs whose replica set named this broker
/// in the last image it saw, and the topics that image held. An image that
/// drops one of them is what releases the series, so the evictor keeps no
/// state that the image does not justify.
pub(crate) struct MetricSeriesEvictor {
    node_id: NodeId,
    metrics: BrokerMetrics,
    hosted: HashSet<PartitionLabel>,
    topics: HashMap<String, TrackedTopic>,
}

impl MetricSeriesEvictor {
    /// An evictor that has seen no image yet, so it tracks nothing and has
    /// nothing to release.
    pub(crate) fn new(node_id: NodeId, metrics: BrokerMetrics) -> Self {
        Self {
            node_id,
            metrics,
            hosted: HashSet::new(),
            topics: HashMap::new(),
        }
    }

    /// Evict the series `image` no longer justifies, then track what it does.
    ///
    /// The first call seeds the tracked sets and evicts nothing: a series is
    /// released against an image that once justified it, never against the
    /// first image the broker happens to see.
    pub(crate) fn apply(&mut self, image: &MetadataImage) {
        let hosted: HashSet<PartitionLabel> = image
            .all_partitions()
            .filter(|partition| partition.replicas.contains(&self.node_id))
            .map(|partition| PartitionLabel {
                topic: Arc::from(partition.topic.as_str()),
                partition: partition.partition,
            })
            .collect();
        for label in self.hosted.difference(&hosted) {
            tracing::debug!(
                topic = %label.topic,
                partition = label.partition,
                "evicting partition metric series",
            );
            self.metrics.evict_partition_series(label);
        }
        self.hosted = hosted;

        let topics: HashMap<String, TrackedTopic> = image
            .topics()
            .map(|topic| {
                let partitions = image
                    .partitions_of(&topic.name)
                    .map(|partition| partition.partition)
                    .collect();
                (
                    topic.name.clone(),
                    TrackedTopic {
                        id: topic.topic_id,
                        partitions,
                    },
                )
            })
            .collect();
        for (name, gone) in &self.topics {
            // A name the new image still holds under a different id is a
            // different topic, and the series belong to the incarnation that
            // left.
            if topics.get(name).is_some_and(|live| live.id == gone.id) {
                continue;
            }
            tracing::debug!(topic = name, "evicting topic metric series");
            self.metrics.evict_topic_series(name);
            for partition in &gone.partitions {
                self.metrics.evict_partition_series(&PartitionLabel {
                    topic: Arc::from(name.as_str()),
                    partition: *partition,
                });
            }
        }
        self.topics = topics;
    }
}

/// Run a [`MetricSeriesEvictor`] over every published image until `shutdown`.
///
/// Eviction rides the image watch rather than a timer because the image is the
/// authority the rule is stated against, and a timer would hold released
/// series for up to one tick after the change that released them.
pub(crate) fn spawn_metric_series_evictor(
    images: watch::Receiver<Arc<MetadataImage>>,
    node_id: NodeId,
    metrics: BrokerMetrics,
    shutdown: CancellationToken,
) {
    let mut evictor = MetricSeriesEvictor::new(node_id, metrics);
    // Seed the baseline here rather than leaving it to the loop's own first
    // pass, as `throttle::apply_image` does beside `throttle::run`. The loop
    // reads whatever is current when the task is first polled, so a change
    // published between this call and that poll would otherwise arrive as the
    // baseline, and the series it should have released would stay in the body
    // until the following change.
    evictor.apply(&images.borrow().clone());
    tokio::spawn(crate::metadata_source::watch_image_loop(
        images,
        "metric series eviction",
        shutdown,
        move |image| evictor.apply(image),
    ));
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_metadata::{
        DeleteTopicRecord, LeaderEpoch, MetadataRecord, PartitionRecord, TopicRecord,
    };

    use super::*;
    use crate::{
        metadata_source::MetadataSource as _,
        metrics::{ConsumerGroupLabel, ReplicaLagLabel},
        test_support::FakeMetadataSource,
    };

    const TOPIC: &str = "orders";
    const THIS_BROKER: NodeId = NodeId(1);
    const OTHER_BROKER: NodeId = NodeId(2);

    /// The `/metrics` body the scrape endpoint would serve.
    fn scrape(metrics: &BrokerMetrics) -> String {
        let mut body = String::new();
        let registry = metrics
            .registry
            .try_lock()
            .expect("no scrape holds the registry");
        prometheus_client::encoding::text::encode(&mut body, &registry).expect("encode metrics");
        body
    }

    /// The `{topic=,partition=}` label pair as the `OpenMetrics` body renders
    /// it.
    fn partition_pair(topic: &str, partition: i32) -> String {
        format!("topic=\"{topic}\",partition=\"{partition}\"")
    }

    /// Records for `TOPIC`, one partition per entry of `partitions` and
    /// indexed from zero, each with that entry as its replica set.
    fn topic_records(partitions: &[&[NodeId]]) -> Vec<MetadataRecord> {
        topic_records_with_id(Uuid::from_u128(1), partitions)
    }

    /// [`topic_records`] under an explicit topic id, for the delete-and-
    /// recreate case where the name stays put and only the id changes.
    fn topic_records_with_id(topic_id: Uuid, partitions: &[&[NodeId]]) -> Vec<MetadataRecord> {
        let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
            name: TOPIC.into(),
            topic_id,
            partitions: i32::try_from(partitions.len()).expect("partition count fits"),
            replication_factor: 1,
        })];
        for (index, replicas) in partitions.iter().enumerate() {
            records.push(MetadataRecord::V1Partition(PartitionRecord {
                topic: TOPIC.into(),
                partition: i32::try_from(index).expect("partition index fits"),
                leader: replicas[0],
                replicas: replicas.to_vec(),
                isr: replicas.to_vec(),
                leader_epoch: LeaderEpoch(4),
                ..Default::default()
            }));
        }
        records
    }

    /// Touch every family that carries a [`PartitionLabel`], the way the
    /// produce, fetch, replication, compaction, disk-scan and delivery paths
    /// each do.
    fn create_partition_series(metrics: &BrokerMetrics, topic: &Arc<str>, partition: i32) {
        metrics.record_partition_produce(topic, partition, 512);
        metrics.record_partition_fetch(topic, partition, 256);
        metrics.record_replication_in(topic, partition, 128);
        metrics.record_replication_out(topic, partition, 64);
        metrics.record_partition_cpu_micros(topic, partition, 32);
        metrics.record_compaction(topic, partition);
        metrics.record_delivery_watermark(topic, partition, 7, 3);
        metrics
            .partition_disk_bytes
            .get_or_create(&PartitionLabel {
                topic: Arc::clone(topic),
                partition,
            })
            .set(4_096);
    }

    /// Touch every family that carries a [`TopicLabel`], plus the
    /// topic-and-reason schema-rejection family.
    fn create_topic_series(metrics: &BrokerMetrics, topic: &Arc<str>) {
        metrics.record_produce(topic, 512);
        metrics.record_produce_messages(topic, 4);
        metrics.record_fetch(topic, 256);
        metrics.record_failed_produce(topic);
        metrics.record_failed_fetch(topic);
        metrics.record_produce_message_conversion(topic);
        metrics.record_fetch_message_conversion(topic);
        metrics.record_topic_freeze_rejection(topic);
        metrics
            .barrier_markers_written_total
            .get_or_create(&TopicLabel {
                topic: Arc::clone(topic),
            })
            .inc();
        for reason in RejectReason::LABELS {
            metrics.record_schema_validation_rejection(topic, reason);
        }
    }

    /// Poll `condition` against fresh scrapes until it holds. The evictor runs
    /// in a spawned task, so the test waits on the body rather than on a
    /// sleep.
    async fn scrape_until(metrics: &BrokerMetrics, condition: impl Fn(&str) -> bool) -> String {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let body = scrape(metrics);
                if condition(&body) {
                    return body;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the scrape body never reached the expected state"))
    }

    /// Start an evictor for `THIS_BROKER` over a fake serving `records`.
    fn evicting_source(
        metrics: &BrokerMetrics,
        records: &[MetadataRecord],
        shutdown: &CancellationToken,
    ) -> FakeMetadataSource {
        let source = FakeMetadataSource::builder().records(records).build();
        spawn_metric_series_evictor(
            source.watch_image(),
            THIS_BROKER,
            metrics.clone(),
            shutdown.clone(),
        );
        source
    }

    /// The acceptance case: a reassignment that drops this broker from the
    /// replica set takes the partition's series out of the scrape body, and
    /// leaves the topic's own series alone because the topic still exists.
    #[tokio::test]
    async fn a_metadata_change_removing_this_replica_evicts_the_partition_series() {
        let metrics = BrokerMetrics::new();
        let shutdown = CancellationToken::new();
        let source = evicting_source(&metrics, &topic_records(&[&[THIS_BROKER]]), &shutdown);

        create_partition_series(&metrics, &Arc::from(TOPIC), 0);
        create_topic_series(&metrics, &Arc::from(TOPIC));
        let pair = partition_pair(TOPIC, 0);
        let topic_label = format!("topic=\"{TOPIC}\"}}");
        let before = scrape(&metrics);
        assert!(before.contains(&pair));
        assert!(before.contains(&topic_label));

        // The reassignment: partition 0 still exists, but its replica set now
        // names only the other broker.
        source.set_records(&topic_records(&[&[OTHER_BROKER]]));

        let after = scrape_until(&metrics, |body| !body.contains(&pair)).await;
        check!(!after.contains(&pair));
        check!(after.contains(&topic_label));
        shutdown.cancel();
    }

    /// A topic delete takes the per-topic series with it, and the partition
    /// series of that topic go with the partitions the delete removed.
    #[tokio::test]
    async fn deleting_a_topic_evicts_its_topic_and_partition_series() {
        let metrics = BrokerMetrics::new();
        let shutdown = CancellationToken::new();
        let live = topic_records(&[&[THIS_BROKER]]);
        let source = evicting_source(&metrics, &live, &shutdown);

        create_partition_series(&metrics, &Arc::from(TOPIC), 0);
        create_topic_series(&metrics, &Arc::from(TOPIC));
        let before = scrape(&metrics);
        assert!(before.contains(&partition_pair(TOPIC, 0)));
        assert!(before.contains(&format!("topic=\"{TOPIC}\"")));

        let mut deleted = live;
        deleted.push(MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
            name: TOPIC.into(),
        }));
        source.set_records(&deleted);

        let after = scrape_until(&metrics, |body| !body.contains(TOPIC)).await;
        check!(!after.contains(TOPIC));
        shutdown.cancel();
    }

    /// A partition this broker still hosts keeps its series when a sibling
    /// partition is reassigned away, so eviction is per label set and not per
    /// family.
    #[tokio::test]
    async fn a_partition_this_broker_still_hosts_keeps_its_series() {
        let metrics = BrokerMetrics::new();
        let shutdown = CancellationToken::new();
        let source = evicting_source(
            &metrics,
            &topic_records(&[&[THIS_BROKER], &[THIS_BROKER]]),
            &shutdown,
        );

        create_partition_series(&metrics, &Arc::from(TOPIC), 0);
        create_partition_series(&metrics, &Arc::from(TOPIC), 1);

        source.set_records(&topic_records(&[&[THIS_BROKER], &[OTHER_BROKER]]));

        let evicted = partition_pair(TOPIC, 1);
        let after = scrape_until(&metrics, |body| !body.contains(&evicted)).await;
        check!(after.contains(&partition_pair(TOPIC, 0)));
        check!(!after.contains(&evicted));
        shutdown.cancel();
    }

    /// A misrouted produce or fetch is accounted for under the partition the
    /// client named, so a partition this broker does not replicate still gets
    /// series here. Deleting the topic has to take those with it, or a
    /// deleted topic leaves behind exactly the partitions the eviction rule
    /// never tracked.
    #[tokio::test]
    async fn deleting_a_topic_evicts_partition_series_this_broker_never_hosted() {
        let metrics = BrokerMetrics::new();
        let shutdown = CancellationToken::new();
        // Partition 0 lives here; partition 1 is replicated only on the other
        // broker, and this broker never hosts it.
        let live = topic_records(&[&[THIS_BROKER], &[OTHER_BROKER]]);
        let source = evicting_source(&metrics, &live, &shutdown);

        create_partition_series(&metrics, &Arc::from(TOPIC), 0);
        create_partition_series(&metrics, &Arc::from(TOPIC), 1);
        let unhosted = partition_pair(TOPIC, 1);
        assert!(scrape(&metrics).contains(&unhosted));

        let mut deleted = live;
        deleted.push(MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
            name: TOPIC.into(),
        }));
        source.set_records(&deleted);

        let after = scrape_until(&metrics, |body| !body.contains(TOPIC)).await;
        check!(!after.contains(&partition_pair(TOPIC, 0)));
        check!(!after.contains(&unhosted));
        shutdown.cancel();
    }

    /// A `watch` channel publishes only the newest image, so a delete and a
    /// recreate under the same name can arrive as one image in which the name
    /// never disappeared. The topic id is what makes that a removal, and
    /// without it the new topic would inherit the old one's counters.
    #[test]
    fn a_topic_recreated_under_the_same_name_does_not_inherit_the_old_series() {
        let metrics = BrokerMetrics::new();
        let mut evictor = MetricSeriesEvictor::new(THIS_BROKER, metrics.clone());

        let first = Uuid::from_u128(1);
        evictor.apply(&MetadataImage::from_records(
            Uuid::nil(),
            &topic_records_with_id(first, &[&[THIS_BROKER]]),
        ));
        create_partition_series(&metrics, &Arc::from(TOPIC), 0);
        create_topic_series(&metrics, &Arc::from(TOPIC));
        assert!(scrape(&metrics).contains(&partition_pair(TOPIC, 0)));

        // Same name, same partition, different topic id: a different topic.
        let second = Uuid::from_u128(2);
        evictor.apply(&MetadataImage::from_records(
            Uuid::nil(),
            &topic_records_with_id(second, &[&[THIS_BROKER]]),
        ));

        let after = scrape(&metrics);
        check!(!after.contains(&partition_pair(TOPIC, 0)));
        check!(!after.contains(TOPIC));

        // The new incarnation is now tracked in its own right: series it
        // creates survive an image that repeats it, and go when it goes.
        create_topic_series(&metrics, &Arc::from(TOPIC));
        evictor.apply(&MetadataImage::from_records(
            Uuid::nil(),
            &topic_records_with_id(second, &[&[THIS_BROKER]]),
        ));
        check!(scrape(&metrics).contains(TOPIC));
    }

    /// The first image an evictor sees is a baseline, never a reason to
    /// release anything: a fresh broker whose data path has already recorded a
    /// partition must not lose those series to its own first metadata apply.
    #[test]
    fn the_first_image_seeds_the_baseline_and_evicts_nothing() {
        let metrics = BrokerMetrics::new();
        create_partition_series(&metrics, &Arc::from(TOPIC), 0);
        create_topic_series(&metrics, &Arc::from(TOPIC));

        let mut evictor = MetricSeriesEvictor::new(THIS_BROKER, metrics.clone());
        // An image that names neither the topic nor the partition.
        evictor.apply(&MetadataImage::new(uuid::Uuid::nil()));

        check!(scrape(&metrics).contains(&partition_pair(TOPIC, 0)));
    }

    /// Eviction is exhaustive over the families each entry point owns: after
    /// one call the scrape body carries no sample for that label set at all.
    ///
    /// The two `metrics::lag` families are in here too, and they are what
    /// makes "the families each entry point owns" a narrower claim than "every
    /// family keyed by this label set". Replica lag is owned by both entry
    /// points, because a broker that left the replica set has no follower to
    /// report on. Consumer-group lag is owned only by the per-topic one: a
    /// group's lag on a partition follows this broker coordinating the
    /// *group*, not hosting the *partition*, so it is put on a partition the
    /// per-partition call names and asserted to survive it.
    #[test]
    fn the_eviction_entry_points_clear_every_family_they_own() {
        let metrics = BrokerMetrics::new();
        create_partition_series(&metrics, &Arc::from(TOPIC), 0);
        create_topic_series(&metrics, &Arc::from(TOPIC));
        metrics.publish_replica_lag(&HashMap::from([(
            ReplicaLagLabel {
                topic: TOPIC.into(),
                partition: 0,
                replica: OTHER_BROKER.0,
            },
            12,
        )]));
        metrics.publish_consumer_group_lag(&HashMap::from([(
            ConsumerGroupLabel {
                group_id: "billing".into(),
                topic: TOPIC.into(),
                partition: 0,
            },
            7,
        )]));
        // A registered family keeps its `# HELP` line whether or not it
        // carries a sample, so every check below names a label set rather
        // than a family, and each one is asserted present before it is
        // asserted gone.
        let follower = format!(
            "{},replica=\"{}\"",
            partition_pair(TOPIC, 0),
            OTHER_BROKER.0
        );
        let group = format!("group_id=\"billing\",{}", partition_pair(TOPIC, 0));
        let before = scrape(&metrics);
        assert!(before.contains(TOPIC));
        assert!(before.contains(&follower));
        assert!(before.contains(&group));
        metrics.evict_partition_series(&PartitionLabel {
            topic: TOPIC.into(),
            partition: 0,
        });
        let after_partition = scrape(&metrics);
        check!(!after_partition.contains(&follower));
        check!(after_partition.contains(&group));

        metrics.evict_topic_series(TOPIC);
        check!(!scrape(&metrics).contains(TOPIC));
    }
}
