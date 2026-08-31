//! The per-follower replica-lag gauge, the index that makes its series
//! releasable, and the publish entry point its sampler calls.
//!
//! The family is sampled rather than incremented: a pass computes the whole
//! set of label sets it can justify and hands it over at once, so publishing
//! is also what releases the series the pass no longer justifies. That is the
//! same rule `share_partition::backlog_poller` follows for the share-group
//! backlog gauge.
//!
//! The index exists because releasing a series is not always the sampler's
//! job. A topic is deleted, a partition is reassigned away: each of those
//! knows part of a lag label set and neither knows the rest.
//! `prometheus_client::metrics::family::Family` cannot be enumerated, so
//! without a record of the live label sets those callers have no way to reach
//! the series they must release, and the family would keep them until the next
//! pass.

use std::{collections::HashMap, sync::Arc};

use dashmap::DashSet;

use super::{BrokerMetrics, PartitionLabel, ReplicaLagLabel};

/// The label sets the replica-lag family currently carries.
///
/// It is a cache of what is in the family, kept in step by being written only
/// where the family is: [`BrokerMetrics::publish_replica_lag`] and the
/// eviction entry points below. A [`BrokerMetrics`] clone shares one index, as
/// it shares one registry.
#[derive(Clone, Default)]
pub(crate) struct LagSeriesIndex {
    replica: Arc<DashSet<ReplicaLagLabel>>,
}

impl BrokerMetrics {
    /// Replace the replica-lag family with `samples`, and set the max rollup
    /// to the largest value in it.
    ///
    /// A label set the previous pass published and this one does not is
    /// released. That is what takes a follower's series away when the replica
    /// set drops it, and every follower's series away when this broker stops
    /// leading the partition.
    pub(crate) fn publish_replica_lag(&self, samples: &HashMap<ReplicaLagLabel, i64>) {
        self.lag_series.replica.retain(|label| {
            let live = samples.contains_key(label);
            if !live {
                self.replica_lag.remove(label);
            }
            live
        });
        for (label, lag) in samples {
            self.replica_lag.get_or_create(label).set(*lag);
            self.lag_series.replica.insert(label.clone());
        }
        self.replica_lag_max
            .set(samples.values().copied().max().unwrap_or(0));
    }

    /// Drop every replica-lag series the family carries for one partition.
    ///
    /// Called from [`BrokerMetrics::evict_partition_series`], which is where a
    /// reassignment or a topic delete lands: a partition that left this broker
    /// has no follower lag to report.
    pub(super) fn evict_partition_lag_series(&self, partition: &PartitionLabel) {
        self.retain_replica_lag(|label| {
            label.topic != partition.topic || label.partition != partition.partition
        });
    }

    /// Drop every replica-lag series the family carries for one topic.
    ///
    /// Called from [`BrokerMetrics::evict_topic_series`]. A topic delete
    /// removes its partitions from the image too, so this mostly duplicates
    /// the per-partition pass; it is what covers a partition index the image
    /// had already stopped naming when the delete arrived.
    pub(super) fn evict_topic_lag_series(&self, topic: &str) {
        self.retain_replica_lag(|label| label.topic != topic);
    }

    /// Keep the replica-lag series `keep` accepts, and remove the rest from
    /// both the family and the index.
    fn retain_replica_lag(&self, keep: impl Fn(&ReplicaLagLabel) -> bool) {
        self.lag_series.replica.retain(|label| {
            let live = keep(label);
            if !live {
                self.replica_lag.remove(label);
            }
            live
        });
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn replica(topic: &str, partition: i32, node: u64) -> ReplicaLagLabel {
        ReplicaLagLabel {
            topic: topic.to_string(),
            partition,
            replica: node,
        }
    }

    /// The value the family carries for `label`, or `None` when it has no such
    /// series.
    fn replica_lag_of(metrics: &BrokerMetrics, label: &ReplicaLagLabel) -> Option<i64> {
        metrics.replica_lag.get(label).map(|gauge| gauge.get())
    }

    /// Publishing is a replacement, not an accumulation: a pass that no longer
    /// names a follower takes that follower's series with it, and the max
    /// rollup follows the values that remain.
    #[test]
    fn publishing_replica_lag_releases_what_the_pass_no_longer_names() {
        let metrics = BrokerMetrics::new();
        let (slow, fast) = (replica("orders", 0, 2), replica("orders", 0, 3));

        metrics.publish_replica_lag(&HashMap::from([(slow.clone(), 40), (fast.clone(), 5)]));
        check!(replica_lag_of(&metrics, &slow) == Some(40));
        check!(replica_lag_of(&metrics, &fast) == Some(5));
        check!(metrics.replica_lag_max.get() == 40);

        metrics.publish_replica_lag(&HashMap::from([(fast.clone(), 7)]));
        check!(replica_lag_of(&metrics, &slow) == None);
        check!(replica_lag_of(&metrics, &fast) == Some(7));
        check!(metrics.replica_lag_max.get() == 7);

        metrics.publish_replica_lag(&HashMap::new());
        check!(replica_lag_of(&metrics, &fast) == None);
        check!(metrics.replica_lag_max.get() == 0);
    }

    /// Losing a partition releases the series for every follower of it, and
    /// leaves the sibling partition alone.
    #[test]
    fn evicting_a_partition_releases_its_replica_lag_series() {
        let metrics = BrokerMetrics::new();
        let (gone, kept) = (replica("orders", 0, 2), replica("orders", 1, 2));
        metrics.publish_replica_lag(&HashMap::from([(gone.clone(), 9), (kept.clone(), 4)]));

        metrics.evict_partition_series(&PartitionLabel {
            topic: "orders".into(),
            partition: 0,
        });

        check!(replica_lag_of(&metrics, &gone) == None);
        check!(replica_lag_of(&metrics, &kept) == Some(4));
    }

    /// A topic delete takes every replica-lag series the topic carries,
    /// whichever partition it belongs to.
    #[test]
    fn evicting_a_topic_releases_every_replica_lag_series_it_carries() {
        let metrics = BrokerMetrics::new();
        let deleted = replica("orders", 3, 2);
        let survivor = replica("payments", 0, 2);
        metrics.publish_replica_lag(&HashMap::from([
            (deleted.clone(), 11),
            (survivor.clone(), 1),
        ]));
        assert!(replica_lag_of(&metrics, &deleted) == Some(11));

        metrics.evict_topic_series("orders");

        check!(replica_lag_of(&metrics, &deleted) == None);
        check!(replica_lag_of(&metrics, &survivor) == Some(1));
    }

    /// Eviction takes the index with the family, so a later pass that names
    /// the released label set again publishes it afresh rather than treating
    /// it as still live.
    #[test]
    fn a_released_series_is_republished_when_a_later_pass_names_it_again() {
        let metrics = BrokerMetrics::new();
        let label = replica("orders", 0, 2);
        metrics.publish_replica_lag(&HashMap::from([(label.clone(), 12)]));
        metrics.evict_topic_series("orders");

        metrics.publish_replica_lag(&HashMap::from([(label.clone(), 20)]));

        check!(replica_lag_of(&metrics, &label) == Some(20));
    }
}
