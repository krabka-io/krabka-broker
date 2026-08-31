//! Sampling of the replica-lag gauge: how far each follower trails the leader
//! of a partition this broker leads.
//!
//! It is a subtraction the broker can do and no client can. The follower's
//! last-fetched offset is leader-side state that never leaves the leader, so
//! putting the number on the scrape endpoint is what lets an operator alert on
//! "a follower is drifting" without standing a lag exporter beside the
//! cluster.
//!
//! Sampling is a poll rather than an event because lag changes when *nothing*
//! happens on the lagging side: a follower that stops fetching falls further
//! behind on every leader append, and only the leader's own clock can observe
//! that.
//!
//! Each pass publishes the whole set of label sets it can justify, so
//! publishing also releases the series the pass no longer justifies —
//! see [`crate::metrics::lag`].

use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
};

use krabka_metadata::NodeId;
use krabka_units::{Time, convert::TimeExt as _, secs};
use tokio_util::sync::CancellationToken;

use crate::{
    metrics::{BrokerMetrics, ReplicaLagLabel},
    partition_registry::PartitionRegistry,
};

/// How often the lag family is resampled.
///
/// Lag is read at the granularity an operator alerts on, which is minutes of
/// backlog rather than seconds, so the interval is set by what a pass costs
/// rather than by how fresh the number could be. A pass walks this broker's
/// led partitions and takes each one's replica-state lock, and half a minute
/// keeps that off the data path's back while still catching a stalled follower
/// well inside a scrape window.
pub(crate) const LAG_POLL_INTERVAL: Time = secs(30);

/// The periodic sampler behind `replica_lag_records` and
/// `replica_lag_max_records`.
pub(crate) struct LagPoller {
    pub node_id: NodeId,
    pub partitions: Arc<PartitionRegistry>,
    pub period: Time,
    pub metrics: BrokerMetrics,
    pub shutdown: CancellationToken,
}

impl LagPoller {
    /// Run the sampler until `shutdown`, then release every series it holds.
    ///
    /// Shutdown clears the family rather than freezing it: a broker that
    /// stopped sampling knows nothing about the lag, and a stale number is
    /// worse than a missing one for the alert an operator writes on it.
    pub(crate) fn spawn(self) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.period.to_std());
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = self.shutdown.cancelled() => break,
                    _ = interval.tick() => self.sample().await,
                }
            }
            self.metrics.publish_replica_lag(&HashMap::new());
        });
    }

    /// One pass over the family.
    async fn sample(&self) {
        self.metrics
            .publish_replica_lag(&replica_lag_samples(&self.partitions, self.node_id).await);
    }
}

/// Lag of every follower of every partition `node_id` currently leads.
///
/// The follower's tracked offset is what it last told the leader it had
/// persisted, so the difference from the leader's log end offset is the work
/// the follower still owes. It is read per partition rather than accumulated,
/// which is why a follower that stops fetching still shows a climbing value:
/// its tracked offset stands still while the leader's moves.
pub(crate) async fn replica_lag_samples(
    partitions: &PartitionRegistry,
    node_id: NodeId,
) -> HashMap<ReplicaLagLabel, i64> {
    let mut samples = HashMap::new();
    for partition in partitions.arcs() {
        if partition.current_leader.load(Ordering::Acquire) != node_id.0 {
            continue;
        }
        let leader_log_end = partition.log_end_offset().0;
        let state = partition.replica_state.lock().await;
        for (follower, stats) in &state.per_follower {
            samples.insert(
                ReplicaLagLabel {
                    topic: partition.topic.clone(),
                    partition: partition.index.get(),
                    replica: follower.0,
                },
                (leader_log_end - stats.leo.0).max(0),
            );
        }
    }
    samples
}

#[cfg(test)]
mod tests;
