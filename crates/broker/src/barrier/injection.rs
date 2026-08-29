//! The marker fan-out of one injection.
//!
//! The coordinator freezes the target set, and then writes one marker into
//! every partition of that set. A partition this broker leads takes
//! [`Partition::produce_control_batch`], which applies no compression rewrite
//! and keeps the control batch byte-exact. A partition another broker leads
//! takes the [`RemoteMarkerWriter`] seam.
//!
//! The fan-out collects the offset that each append returned, because those
//! offsets are the cut. It retries the partitions that carry no marker until
//! its deadline runs out. A leader that is down or mid-election is the common
//! failure, and it usually resolves inside the deadline.

use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::Ordering},
};

use async_trait::async_trait;
use krabka_log::Offset;
use krabka_metadata::{MetadataImage, NodeId};
use krabka_units::{Time, convert::TimeExt as _};
use tokio::time::Instant;
use tracing::warn;

use crate::{
    barrier::{
        config::BarrierConfig,
        marker::{BarrierMarker, build_barrier_batch},
        metrics::BarrierMetrics,
        persistence::TopicTarget,
        state::TargetPartition,
    },
    error::BrokerError,
    metadata_source::MetadataSource,
    partition::Partition,
    partition_registry::PartitionRegistry,
};

/// One marker that an append placed, and the offset it took.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MarkerPlacement {
    pub(crate) target: TargetPartition,
    pub(crate) offset: Offset,
}

/// The leg of a marker fan-out that leaves this broker.
///
/// The broker binds this to the `WriteBarrierMarkers` inter-broker request. A
/// coordinator with no binding marks only the partitions it leads, and every
/// other partition of the group lands in the `missing` list of the cut.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub(crate) trait RemoteMarkerWriter: Send + Sync {
    /// Append `marker` into every partition of `targets`, which `leader`
    /// leads.
    ///
    /// The result names the offset of each marker the remote broker placed. A
    /// target that is absent from the result carries no marker, and the
    /// fan-out retries it.
    ///
    /// # Errors
    /// Returns a [`BrokerError`] when the request to `leader` fails. The
    /// fan-out retries every target of that leader.
    async fn write_markers(
        &self,
        leader: NodeId,
        marker: &BarrierMarker,
        targets: &[TargetPartition],
    ) -> Result<Vec<MarkerPlacement>, BrokerError>;
}

/// Freeze the target set of one injection from the metadata image.
///
/// The frozen set names each topic of the group and the partition count the
/// image reported at this instant. A topic that the image does not hold has no
/// partition to mark, so the set leaves it out. An edit to the group's topics
/// and a partition-count change both apply from the next epoch.
pub(crate) fn freeze_targets(topics: &[String], image: &MetadataImage) -> Vec<TopicTarget> {
    let mut out = Vec::with_capacity(topics.len());
    for topic in topics {
        if let Some(record) = image.topic(topic) {
            out.push(TopicTarget {
                topic: topic.clone(),
                partition_count: record.partitions,
            });
        } else {
            warn!(topic, "barrier target topic is not in the metadata image");
        }
    }
    out
}

/// Group the partitions that carry no marker yet by their current leader.
///
/// `leader_of` returns `None` for a partition that the metadata image does not
/// hold. Those partitions group under `None`, and the fan-out leaves them for
/// the next attempt.
pub(crate) fn group_by_leader<F>(
    pending: &[TargetPartition],
    leader_of: F,
) -> BTreeMap<Option<NodeId>, Vec<TargetPartition>>
where
    F: Fn(&TargetPartition) -> Option<NodeId>,
{
    let mut out: BTreeMap<Option<NodeId>, Vec<TargetPartition>> = BTreeMap::new();
    for target in pending {
        out.entry(leader_of(target))
            .or_default()
            .push(target.clone());
    }
    out
}

/// The wait before retry number `attempt`, counted from zero.
///
/// The wait doubles per attempt and stops at `max`.
pub(crate) fn backoff_for(attempt: u32, base: Time, max: Time) -> Time {
    let base_ms = base.millis_i64().max(0);
    let max_ms = max.millis_i64().max(0);
    let factor = 1_i64 << attempt.min(20);
    Time::from_millis(base_ms.saturating_mul(factor).min(max_ms))
}

/// The leader of `target` in `image`.
fn leader_in(image: &MetadataImage, target: &TargetPartition) -> Option<NodeId> {
    image
        .partition(&target.topic, target.partition.get())
        .map(|p| p.leader)
}

/// Everything one marker fan-out needs.
pub(crate) struct MarkerFanout<'a> {
    pub(crate) node_id: NodeId,
    pub(crate) partitions: &'a PartitionRegistry,
    pub(crate) controller: &'a Arc<dyn MetadataSource>,
    pub(crate) remote: Option<&'a Arc<dyn RemoteMarkerWriter>>,
    pub(crate) metrics: &'a dyn BarrierMetrics,
    pub(crate) config: &'a BarrierConfig,
}

impl MarkerFanout<'_> {
    /// Write `marker` into every partition of `targets`, and collect the
    /// offsets.
    ///
    /// The function reads a fresh metadata image per attempt, so a leader that
    /// moves between two attempts still gets the marker. It returns when every
    /// target carries a marker, or when `timeout` passes. The caller publishes
    /// a partial cut for whatever is absent from the result.
    ///
    /// The caller supplies `timeout` rather than the fan-out reading it from
    /// the config, because a `TriggerBarrier` request carries its own bound.
    pub(crate) async fn run(
        &self,
        marker: &BarrierMarker,
        targets: Vec<TargetPartition>,
        timeout: Time,
    ) -> BTreeMap<TargetPartition, Offset> {
        let deadline = Instant::now() + timeout.to_std();
        let mut pending = targets;
        let mut placed: BTreeMap<TargetPartition, Offset> = BTreeMap::new();
        let mut attempt: u32 = 0;

        while !pending.is_empty() {
            let image = self.controller.current_image();
            let plan = group_by_leader(&pending, |target| leader_in(&image, target));
            for (leader, group) in plan {
                match leader {
                    None => {}
                    Some(leader) if leader == self.node_id => {
                        self.place_local(marker, &group, &mut placed).await;
                    }
                    Some(leader) => {
                        self.place_remote(leader, marker, &group, &mut placed).await;
                    }
                }
            }
            pending.retain(|target| !placed.contains_key(target));
            if pending.is_empty() {
                break;
            }

            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let wait = backoff_for(
                attempt,
                self.config.retry_backoff,
                self.config.retry_backoff_max,
            )
            .to_std()
            .min(deadline - now);
            tokio::time::sleep(wait).await;
            attempt = attempt.saturating_add(1);
        }
        placed
    }

    /// Append the marker into every partition this broker leads.
    async fn place_local(
        &self,
        marker: &BarrierMarker,
        targets: &[TargetPartition],
        placed: &mut BTreeMap<TargetPartition, Offset>,
    ) {
        for target in targets {
            let Some(partition) = self.partitions.get(&target.topic, target.partition) else {
                warn!(
                    topic = target.topic,
                    partition = target.partition.get(),
                    "barrier target is led locally but is not open here"
                );
                self.metrics
                    .marker_append_failed(&target.topic, target.partition);
                continue;
            };
            match append_marker(&partition, marker).await {
                Ok(offset) => {
                    self.metrics.marker_written(&target.topic);
                    placed.insert(target.clone(), offset);
                }
                Err(error) => {
                    warn!(
                        topic = target.topic,
                        partition = target.partition.get(),
                        %error,
                        "barrier marker append failed"
                    );
                    self.metrics
                        .marker_append_failed(&target.topic, target.partition);
                }
            }
        }
    }

    /// Ask `leader` to append the marker into every partition it leads.
    async fn place_remote(
        &self,
        leader: NodeId,
        marker: &BarrierMarker,
        targets: &[TargetPartition],
        placed: &mut BTreeMap<TargetPartition, Offset>,
    ) {
        let Some(remote) = self.remote else {
            warn!(
                %leader,
                count = targets.len(),
                "no barrier marker transport; the remote partitions stay unmarked"
            );
            for target in targets {
                self.metrics
                    .marker_append_failed(&target.topic, target.partition);
            }
            return;
        };
        match remote.write_markers(leader, marker, targets).await {
            Ok(placements) => {
                for placement in placements {
                    self.metrics.marker_written(&placement.target.topic);
                    placed.insert(placement.target, placement.offset);
                }
            }
            Err(error) => {
                warn!(%leader, %error, "barrier marker request to the leader failed");
                for target in targets {
                    self.metrics
                        .marker_append_failed(&target.topic, target.partition);
                }
            }
        }
    }
}

/// Append one barrier marker to a local partition, and return its offset.
///
/// The batch carries the partition's current leader epoch, because the writer
/// does not stamp it and a default of zero is a false epoch in the header.
///
/// The `WriteBarrierMarkers` handler appends through this function too, so a
/// marker that a remote coordinator asks for takes the same batch shape as one
/// this broker's own coordinator places.
///
/// # Errors
/// Returns a [`BrokerError`] when the partition writer is gone, or when the
/// log rejects the batch.
pub(crate) async fn append_marker(
    partition: &Partition,
    marker: &BarrierMarker,
) -> Result<Offset, BrokerError> {
    let leader_epoch = partition.current_leader_epoch.load(Ordering::Acquire);
    let batch = build_barrier_batch(marker, partition.log_end_offset(), leader_epoch);
    partition.produce_control_batch(batch).await
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_ids::PartitionIndex;
    use krabka_units::{millis, secs};
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;
    use crate::barrier::{
        marker::parse_barrier_marker,
        metrics::NoBarrierMetrics,
        test_support::{StaticSource, open_partition, topic_records},
    };

    fn at(topic: &str, partition: i32) -> TargetPartition {
        TargetPartition {
            topic: topic.to_owned(),
            partition: PartitionIndex(partition),
        }
    }

    fn marker() -> BarrierMarker {
        BarrierMarker {
            group: "orders-cut".to_owned(),
            epoch: 4,
            triggered_at: 1_724_500_000_000,
        }
    }

    fn fast_config() -> BarrierConfig {
        BarrierConfig {
            injection_timeout: millis(60),
            retry_backoff: millis(1),
            retry_backoff_max: millis(4),
            ..BarrierConfig::default()
        }
    }

    fn source(records: &[krabka_metadata::MetadataRecord]) -> Arc<dyn MetadataSource> {
        Arc::new(StaticSource::new(records))
    }

    #[test]
    fn a_frozen_target_set_takes_the_partition_count_of_the_image() {
        let image = MetadataImage::from_records(
            Uuid::nil(),
            &[
                topic_records("orders", 3, NodeId(1)),
                topic_records("payments", 1, NodeId(1)),
            ]
            .concat(),
        );
        let topics = vec![
            "orders".to_owned(),
            "payments".to_owned(),
            "absent".to_owned(),
        ];
        let expected = vec![
            TopicTarget {
                topic: "orders".to_owned(),
                partition_count: 3,
            },
            TopicTarget {
                topic: "payments".to_owned(),
                partition_count: 1,
            },
        ];
        assert!(freeze_targets(&topics, &image) == expected);
    }

    #[test]
    fn the_fan_out_plan_groups_every_partition_by_its_leader() {
        let pending = vec![at("orders", 0), at("orders", 1), at("payments", 0)];
        let plan = group_by_leader(&pending, |target| match target.partition.get() {
            0 if target.topic == "orders" => Some(NodeId(1)),
            0 => Some(NodeId(2)),
            _ => None,
        });
        let expected = BTreeMap::from([
            (None, vec![at("orders", 1)]),
            (Some(NodeId(1)), vec![at("orders", 0)]),
            (Some(NodeId(2)), vec![at("payments", 0)]),
        ]);
        assert!(plan == expected);
    }

    #[test]
    fn the_backoff_doubles_and_stops_at_the_maximum() {
        let cases: &[(u32, i64)] = &[
            (0, 100),
            (1, 200),
            (2, 400),
            (3, 800),
            (4, 1000),
            (30, 1000),
        ];
        for (attempt, expected) in cases {
            check!(
                backoff_for(*attempt, millis(100), millis(1000)).millis_i64() == *expected,
                "attempt {attempt}"
            );
        }
    }

    #[test]
    fn a_zero_maximum_backoff_waits_no_time() {
        assert!(backoff_for(3, millis(100), Time::ZERO).millis_i64() == 0);
    }

    #[tokio::test]
    async fn a_local_fan_out_marks_every_partition_and_returns_its_offset() {
        let dir = tempdir().expect("tempdir");
        let registry = PartitionRegistry::new();
        for p in 0..2 {
            open_partition(&registry, dir.path(), "orders", p);
        }
        let controller = source(&topic_records("orders", 2, NodeId(1)));
        let metrics = NoBarrierMetrics;
        let config = fast_config();
        let fanout = MarkerFanout {
            node_id: NodeId(1),
            partitions: &registry,
            controller: &controller,
            remote: None,
            metrics: &metrics,
            config: &config,
        };

        let placed = fanout
            .run(
                &marker(),
                vec![at("orders", 0), at("orders", 1)],
                config.injection_timeout,
            )
            .await;
        assert!(placed.len() == 2);
        assert!(placed[&at("orders", 0)] == Offset(0));
        assert!(placed[&at("orders", 1)] == Offset(0));

        // The record at the returned offset is this epoch's marker, and the
        // batch carries the partition's leader epoch rather than a zero.
        for p in 0..2 {
            let partition = registry
                .get("orders", PartitionIndex(p))
                .expect("the partition is open");
            let read = partition
                .read_log(Offset(0), krabka_units::mebibytes(1))
                .expect("read the log back");
            let batch = &read.batches[0];
            check!(batch.attributes.is_control_batch());
            check!(parse_barrier_marker(&batch.records[0]).ok() == Some(marker()));
        }
    }

    #[tokio::test]
    async fn a_partition_that_is_not_open_locally_stays_unmarked() {
        let dir = tempdir().expect("tempdir");
        let registry = PartitionRegistry::new();
        open_partition(&registry, dir.path(), "orders", 0);
        let controller = source(&topic_records("orders", 2, NodeId(1)));
        let metrics = NoBarrierMetrics;
        let config = fast_config();
        let fanout = MarkerFanout {
            node_id: NodeId(1),
            partitions: &registry,
            controller: &controller,
            remote: None,
            metrics: &metrics,
            config: &config,
        };

        let placed = fanout
            .run(
                &marker(),
                vec![at("orders", 0), at("orders", 1)],
                config.injection_timeout,
            )
            .await;
        assert!(placed.keys().cloned().collect::<Vec<_>>() == vec![at("orders", 0)]);
    }

    #[tokio::test]
    async fn a_remote_partition_goes_through_the_transport_seam() {
        let registry = PartitionRegistry::new();
        let controller = source(&topic_records("orders", 1, NodeId(2)));
        let metrics = NoBarrierMetrics;
        let config = fast_config();

        let mut remote = MockRemoteMarkerWriter::new();
        remote
            .expect_write_markers()
            .times(1)
            .returning(|leader, _marker, targets| {
                assert!(leader == NodeId(2));
                Ok(targets
                    .iter()
                    .map(|target| MarkerPlacement {
                        target: target.clone(),
                        offset: Offset(77),
                    })
                    .collect())
            });
        let remote: Arc<dyn RemoteMarkerWriter> = Arc::new(remote);

        let fanout = MarkerFanout {
            node_id: NodeId(1),
            partitions: &registry,
            controller: &controller,
            remote: Some(&remote),
            metrics: &metrics,
            config: &config,
        };
        let placed = fanout
            .run(&marker(), vec![at("orders", 0)], config.injection_timeout)
            .await;
        assert!(placed == BTreeMap::from([(at("orders", 0), Offset(77))]));
    }

    #[tokio::test]
    async fn the_fan_out_retries_a_leader_that_failed_once() {
        let registry = PartitionRegistry::new();
        let controller = source(&topic_records("orders", 1, NodeId(2)));
        let metrics = NoBarrierMetrics;
        let config = BarrierConfig {
            injection_timeout: secs(30),
            retry_backoff: millis(1),
            retry_backoff_max: millis(1),
            ..BarrierConfig::default()
        };

        let mut remote = MockRemoteMarkerWriter::new();
        let mut calls = 0;
        remote
            .expect_write_markers()
            .times(2)
            .returning(move |_leader, _marker, targets| {
                calls += 1;
                if calls == 1 {
                    return Err(BrokerError::Replication("leader is mid-election".into()));
                }
                Ok(targets
                    .iter()
                    .map(|target| MarkerPlacement {
                        target: target.clone(),
                        offset: Offset(12),
                    })
                    .collect())
            });
        let remote: Arc<dyn RemoteMarkerWriter> = Arc::new(remote);

        let fanout = MarkerFanout {
            node_id: NodeId(1),
            partitions: &registry,
            controller: &controller,
            remote: Some(&remote),
            metrics: &metrics,
            config: &config,
        };
        let placed = fanout
            .run(&marker(), vec![at("orders", 0)], config.injection_timeout)
            .await;
        assert!(placed == BTreeMap::from([(at("orders", 0), Offset(12))]));
    }

    #[tokio::test]
    async fn a_deadline_that_runs_out_returns_what_it_placed() {
        let registry = PartitionRegistry::new();
        let controller = source(&topic_records("orders", 1, NodeId(2)));
        let metrics = NoBarrierMetrics;
        let config = fast_config();

        let mut remote = MockRemoteMarkerWriter::new();
        remote
            .expect_write_markers()
            .returning(|_leader, _marker, _targets| {
                Err(BrokerError::Replication("leader is down".into()))
            });
        let remote: Arc<dyn RemoteMarkerWriter> = Arc::new(remote);

        let fanout = MarkerFanout {
            node_id: NodeId(1),
            partitions: &registry,
            controller: &controller,
            remote: Some(&remote),
            metrics: &metrics,
            config: &config,
        };
        let placed = fanout
            .run(&marker(), vec![at("orders", 0)], config.injection_timeout)
            .await;
        assert!(placed.is_empty());
    }
}
