//! One compaction sweep over the partition registry.
//!
//! The sweep tests every partition for local leadership, for the `compact`
//! cleanup policy, and for a KFC-9 write freeze. It dispatches
//! [`Partition::compact_log`] for the partitions that pass all three tests,
//! and it accounts the sweep and each completed compaction in the metrics.

use std::sync::{Arc, atomic::Ordering};

use krabka_metadata::{MetadataImage, NodeId};
use krabka_verified::FreezeMutationKind;
use tracing::warn;

use crate::{
    freeze::resolve::{FreezeMutationResolution, resolve_freeze_mutation},
    metrics::BrokerMetrics,
    partition::Partition,
    partition_registry::PartitionRegistry,
};

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
) {
    // Snapshot first to avoid holding any registry guard across await.
    let snapshot: Vec<Arc<Partition>> = partitions.arcs();
    for partition in snapshot {
        let leader = partition.current_leader.load(Ordering::Relaxed);
        if leader != node_id {
            continue;
        }
        let policy = {
            // Recover the guard if the mutex was poisoned by a panic
            // elsewhere rather than killing the (discarded-JoinHandle)
            // cleaner task. The config snapshot stays readable.
            let log = partition
                .log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            log.config_snapshot().cleanup_policy
        };
        if policy != krabka_log::CleanupPolicy::Compact {
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
            }
            Err(e) => {
                warn!(
                    topic = %partition.topic,
                    partition_id = partition.index.get(),
                    error = %e,
                    "compaction failed for partition",
                );
            }
        }
    }
    // One increment per completed sweep, whether or not any partition was
    // eligible, so a test that seals a segment can poll this counter to
    // confirm a full pass ran after the seal (see `wait_for_metrics`).
    metrics.record_cleaner_run();
}
