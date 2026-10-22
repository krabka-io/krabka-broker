//! The writer's Compact arm.
//!
//! A pass keeps the last record of each active producer, so that the state of
//! the producer survives the rewrite. [`krabka_log::Log::compact`] reads those records
//! from the producer state of the log, which a leader and a follower both
//! update for each batch that they append. This is Kafka's
//! `Cleaner.cleanSegments`, which reads `UnifiedLog.lastRecordsOfActiveProducers`
//! on every replica. The tracker of the produce path does not take part: on a
//! follower it does not hold the replicated data batches.
//!
//! The pass holds the log's mutex only to take its input and to install its
//! output. It reads and rewrites the sealed segments without the mutex, so
//! produce, replica appends and fetches of the partition go on meanwhile, as
//! they do beside Kafka's cleaner.

use {
    super::storage::{LogStorage, MaintenanceAck, lock_log, maintain_shared_log},
    crate::replica_state::ReplicaState,
    krabka_log::{CompactionContext, Log, LogError, Offset},
    std::{sync::Mutex, time::SystemTime},
};

pub(super) async fn handle_compact(
    storage: LogStorage<'_>,
    replica_state: &tokio::sync::Mutex<ReplicaState>,
    ack: MaintenanceAck,
) {
    maintain_shared_log(
        storage,
        replica_state,
        ack,
        "compact task panicked",
        compact_without_holding_the_log,
    )
    .await;
}

/// One compaction pass that holds `log` only to prepare and to finish it.
///
/// The pass is bounded at the last stable offset, not the high watermark:
/// Kafka's `LogCleanerManager.cleanableOffsets` takes `lastStableOffset`,
/// because the records between the LSO and the high watermark belong to
/// transactions with no marker yet. Rewriting them before the commit or
/// abort is known would leave the output segment's aborted-transaction
/// index unable to learn about a marker that arrives later. The LSO is read
/// under the same lock that takes the pass's input, so the input and the
/// bound agree. An append that lands while the pass builds goes past the
/// consumed range. A truncation, retention or `DeleteRecords` that drops a
/// consumed segment meanwhile makes the pass stale, and it installs nothing.
fn compact_without_holding_the_log(
    log: &Mutex<Log>,
    now: SystemTime,
    high_watermark: Offset,
) -> Result<(), LogError> {
    let job = {
        let mut log = lock_log(log);
        let context = CompactionContext {
            now,
            last_stable_offset: log.last_stable_offset(high_watermark),
        };
        log.prepare_compaction(&context)?
    };
    let Some(job) = job else {
        return Ok(());
    };
    let built = job.build();
    lock_log(log).finish_compaction(built).map(drop)
}
