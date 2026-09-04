//! One compaction sweep over the partition registry.
//!
//! The sweep tests every partition for local leadership, for a cleanup policy
//! containing `compact`, for Kafka's cleanable test over the partition's dirty
//! region, and for a KFC-9 write freeze. It dispatches
//! [`Partition::compact_log`] for the partitions that pass all four tests,
//! and it accounts each completed compaction, each failed one, and the sweep
//! itself in the metrics.
//!
//! A failure is accounted rather than logged and forgotten. Compaction and
//! local retention stop together on a compacted topic, so a cleaner that
//! fails every partition is a disk filling with no other signal: the failure
//! counter carries the partition and the reason, the uncleanable set carries
//! the partitions the cleaner has lost across sweeps, and the sweep counter
//! is left where it was so a failing pass never reports success.

use std::{
    collections::BTreeSet,
    sync::{Arc, atomic::Ordering},
};

use krabka_metadata::{MetadataImage, NodeId};
use krabka_verified::FreezeMutationKind;
use tracing::warn;

use crate::{
    error::BrokerError,
    freeze::resolve::{FreezeMutationResolution, resolve_freeze_mutation},
    metrics::{BrokerMetrics, CleanerFailureReason},
    partition::Partition,
    partition_registry::PartitionRegistry,
};

/// The partitions whose most recent compaction attempt failed, carried from
/// one sweep to the next.
///
/// Kafka's `LogCleanerManager` keeps the same set, and for the same reason: a
/// partition that failed a pass stays uncleanable until one succeeds, and a
/// per-sweep count would report zero on the next sweep that finds it
/// ineligible. The set is keyed by the partition's identity rather than by an
/// `Arc<Partition>` so a partition this broker stops leading leaves it.
#[derive(Debug, Default)]
pub(crate) struct UncleanablePartitions {
    inner: BTreeSet<(String, i32)>,
}

impl UncleanablePartitions {
    /// How many partitions the cleaner currently cannot clean.
    fn len(&self) -> usize {
        self.inner.len()
    }

    /// Drop every entry that `swept` does not name, which is what releases a
    /// partition this broker no longer leads or no longer compacts.
    fn retain_swept(&mut self, swept: &BTreeSet<(String, i32)>) {
        self.inner.retain(|key| swept.contains(key));
    }
}

/// How the sweep classifies one failed [`Partition::compact_log`] call.
///
/// The three reasons are what an operator acts on differently: a storage
/// failure is a disk, and the writer arm has already asked the log-dir
/// registry to take that disk offline; a dead writer is a partition whose
/// actor is gone; anything else is the log layer refusing the rewrite.
fn failure_reason(error: &BrokerError) -> CleanerFailureReason {
    match error {
        BrokerError::Log(krabka_log::LogError::Io(_)) => CleanerFailureReason::Io,
        BrokerError::Replication(_) => CleanerFailureReason::Writer,
        _ => CleanerFailureReason::Other,
    }
}

#[cfg(test)]
mod tests;

/// Whether a KFC-9 write freeze stops this sweep from compacting `topic`.
///
/// Compaction removes records, and the KFC's rule refuses every operation that
/// removes data from a frozen topic's log. A disaster-recovery promotion needs
/// the frozen prefix byte-identical between the two sites, and one cleaner run
/// on one side leaves the same offsets holding different bytes.
///
/// `image` is `None` for a sweep with no metadata authority to ask, which
/// resolves no freeze.
fn freeze_stops_compaction(image: Option<&MetadataImage>, topic: &str) -> bool {
    image.is_some_and(|image| {
        matches!(
            resolve_freeze_mutation(image, topic, true, FreezeMutationKind::Compaction),
            FreezeMutationResolution::Frozen(_)
        )
    })
}

pub(crate) async fn tick_all(
    partitions: &PartitionRegistry,
    image: Option<&MetadataImage>,
    node_id: NodeId,
    metrics: &BrokerMetrics,
    uncleanable: &mut UncleanablePartitions,
) {
    // Snapshot first to avoid holding any registry guard across await.
    let snapshot: Vec<Arc<Partition>> = partitions.arcs();
    // Every partition this sweep considered its work, whether or not it was
    // due a pass. What is not in it is not this cleaner's any more, so it
    // leaves the uncleanable set at the end of the sweep.
    let mut swept: BTreeSet<(String, i32)> = BTreeSet::new();
    let mut failed = false;
    for partition in snapshot {
        let leader = partition.current_leader.load(Ordering::Relaxed);
        if leader != node_id {
            continue;
        }
        let (compacted, due) = {
            // Recover the guard if the mutex was poisoned by a panic
            // elsewhere rather than killing the (discarded-JoinHandle)
            // cleaner task. The config snapshot stays readable.
            let log = partition
                .log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // A policy list containing `compact` is what makes a partition the
            // cleaner's work, so `compact,delete` is swept exactly as `compact`
            // is; the delete half is the local-retention sweep's, in
            // `crate::log_retention`, which runs on its own interval and over
            // every hosted log rather than only the led ones.
            let compacted = log.config_snapshot().cleanup_policy.contains_compact();
            (
                compacted,
                // Kafka's `min.cleanable.dirty.ratio`, `min.compaction.lag.ms`
                // and `max.compaction.lag.ms`, which say whether this
                // partition has earned a pass yet.
                compacted && log.compaction_due(std::time::SystemTime::now()),
            )
        };
        if !compacted {
            continue;
        }
        // Leadership and the cleanup policy are what make a partition this
        // cleaner's work. A partition that is merely not due yet keeps the
        // uncleanable mark an earlier failure left on it, because the failure
        // is still there; only a pass that succeeds clears it.
        let key = (partition.topic.clone(), partition.index.get());
        swept.insert(key.clone());
        if !due {
            continue;
        }
        // KFC-9, beside the policy test and skipping in exactly the same way.
        // A skip is the right shape here rather than an error: the cleaner is
        // a background loop with no caller to refuse, so a frozen topic simply
        // has no work for it, and the partition becomes eligible again on the
        // first sweep after the thaw leaves the image, with no operator step.
        if freeze_stops_compaction(image, &partition.topic) {
            continue;
        }
        match partition.compact_log().await {
            Ok(()) => {
                metrics.record_compaction(&partition.topic, partition.index.get());
                uncleanable.inner.remove(&key);
            }
            Err(e) => {
                let reason = failure_reason(&e);
                warn!(
                    topic = %partition.topic,
                    partition_id = partition.index.get(),
                    reason = reason.as_str(),
                    error = %e,
                    "compaction failed for partition",
                );
                metrics.record_cleaner_failure(&partition.topic, partition.index.get(), reason);
                uncleanable.inner.insert(key);
                failed = true;
            }
        }
    }
    // A partition the sweep no longer considers its work — this broker lost
    // the leadership, or the topic stopped being compacted — is no longer
    // uncleanable, so the gauge falls without waiting for a pass to succeed
    // on a partition that will never get one.
    uncleanable.retain_swept(&swept);
    metrics.set_uncleanable_partitions(uncleanable.len());
    if failed {
        // The sweep ran and did not do what it is for. Counting it as a pass
        // is what made a cleaner failing every partition on a dying disk
        // report the same rate as a healthy one.
        return;
    }
    // One increment per clean sweep, whether or not any partition was
    // eligible, so a test that seals a segment can poll this counter to
    // confirm a full pass ran after the seal (see `wait_for_metrics`).
    metrics.record_cleaner_run();
}
