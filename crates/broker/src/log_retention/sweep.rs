//! One local-retention sweep over the partition registry.
//!
//! The sweep dispatches [`Partition::retain_log`] for every partition this
//! broker hosts whose topic is not under a KFC-9 write freeze, and it accounts
//! each failure and the sweep itself in the metrics.
//!
//! Two of its rules differ from the cleaner's compaction sweep, and both
//! differences are deliberate.
//!
//! **Leadership.** The cleaner sweeps only the partitions this broker leads,
//! because compaction rewrites a log and Kafka runs it on the leader. Kafka's
//! `LogManager.cleanupLogs` walks `currentLogs` -- every log the broker hosts,
//! leader or follower -- so a follower trims its own replica on its own
//! schedule rather than waiting to be elected. This sweep therefore applies no
//! leadership filter: a follower whose replica is never trimmed accumulates
//! segments for as long as it follows, which is precisely the descriptor climb
//! this loop exists to stop.
//!
//! **Cleanup policy.** There is none to test here either. `Log::tick` reads
//! `retention.ms` and `retention.bytes` out of the partition's own config and
//! decides for itself; a `compact` topic's config leaves both unset, and a
//! `compact,delete` topic is meant to have both halves applied.
//!
//! **KFC-9 write freeze.** Refused, on the same grounds the cleaner refuses
//! compaction on a frozen topic: the freeze rule refuses every operation that
//! removes data from the log, and retention removes data. A disaster-recovery
//! promotion needs the frozen prefix byte-identical between the two sites, and
//! one retention pass on one side leaves the two logs starting at different
//! offsets.

use std::sync::Arc;

use krabka_metadata::MetadataImage;
use krabka_verified::FreezeMutationKind;
use tracing::warn;

use crate::{
    error::BrokerError,
    freeze::resolve::{FreezeMutationResolution, resolve_freeze_mutation},
    metrics::{BrokerMetrics, CleanerFailureReason},
    partition::Partition,
    partition_registry::PartitionRegistry,
};

#[cfg(test)]
mod tests;

/// How the sweep classifies one failed [`Partition::retain_log`] call.
///
/// The same three reasons the cleaner uses, because an operator acts on them
/// the same way: a storage failure is a disk, and the writer arm has already
/// asked the log-dir registry to take that disk offline; a dead writer is a
/// partition whose actor is gone; anything else is the log layer refusing the
/// eviction.
fn failure_reason(error: &BrokerError) -> CleanerFailureReason {
    match error {
        BrokerError::Log(krabka_log::LogError::Io(_)) => CleanerFailureReason::Io,
        BrokerError::Replication(_) => CleanerFailureReason::Writer,
        _ => CleanerFailureReason::Other,
    }
}

/// Whether a KFC-9 write freeze stops this sweep from trimming `topic`.
///
/// [`FreezeMutationKind::Retention`] is the honest fit and already exists: it
/// is the kind the diskless flusher and the remote-log manager resolve for
/// their own retention paths, and `krabka_verified::freeze_refuses` classifies
/// it as refused. Local retention is the third caller of the same rule rather
/// than a new one.
///
/// `image` is `None` for a sweep with no metadata authority to ask, which
/// resolves no freeze.
fn freeze_stops_retention(image: Option<&MetadataImage>, topic: &str) -> bool {
    image.is_some_and(|image| {
        matches!(
            resolve_freeze_mutation(image, topic, true, FreezeMutationKind::Retention),
            FreezeMutationResolution::Frozen(_)
        )
    })
}

pub(crate) async fn tick_all(
    partitions: &PartitionRegistry,
    image: Option<&MetadataImage>,
    metrics: &BrokerMetrics,
) {
    // Snapshot first to avoid holding any registry guard across await.
    let snapshot: Vec<Arc<Partition>> = partitions.arcs();
    let mut failed = false;
    for partition in snapshot {
        // A skip is the right shape here rather than an error: the sweep is a
        // background loop with no caller to refuse, so a frozen topic simply
        // has no work for it, and the partition becomes eligible again on the
        // first sweep after the thaw leaves the image, with no operator step.
        if freeze_stops_retention(image, &partition.topic) {
            continue;
        }
        if let Err(error) = partition.retain_log().await {
            let reason = failure_reason(&error);
            warn!(
                topic = %partition.topic,
                partition_id = partition.index.get(),
                reason = reason.as_str(),
                error = %error,
                "local retention failed for partition",
            );
            metrics.record_retention_failure(&partition.topic, partition.index.get(), reason);
            failed = true;
        }
    }
    if failed {
        // The sweep ran and did not do what it is for. Counting it as a pass
        // is what would let a broker whose every partition fails to unlink a
        // segment report the same pass rate as a healthy one, on the disk
        // where that matters most.
        return;
    }
    // One increment per clean sweep, whether or not any segment was evicted,
    // so a test can poll this counter to confirm a full pass ran.
    metrics.record_retention_run();
}
