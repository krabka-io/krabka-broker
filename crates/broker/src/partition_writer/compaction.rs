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
//! The writer prepares a pass while it handles the Compact message, and
//! finishes it in a later turn of its loop, once the build has ended. The
//! build reads and rewrites the sealed segments on the blocking pool, holding
//! neither the log's mutex nor the writer. Produces, replica appends, fetches
//! and the writer's other messages go on meanwhile, as they do beside Kafka's
//! cleaner. A message that drops or replaces a segment the pass read makes the
//! pass stale, and it installs nothing.

use {
    super::storage::{
        LogStorage, MaintenanceAck, StorageStatus, flag_storage_failure, lock_log,
        run_log_mutation, storage_failure_error,
    },
    crate::{
        blocking::{BlockingError, BlockingHandle, spawn_blocking},
        error::BrokerError,
        replica_state::ReplicaState,
    },
    krabka_log::{BuiltCompaction, CompactionContext, CompactionJob, Log, LogError, Offset},
    std::{
        sync::{Arc, Mutex},
        time::SystemTime,
    },
};

/// What a failed or panicked compaction task reports.
const PANIC_CONTEXT: &str = "compact task panicked";

/// A compaction pass whose build is running, and the ack it answers.
pub(super) struct BuildingCompaction {
    build: BlockingHandle<BuiltCompaction>,
    ack: MaintenanceAck,
}

impl BuildingCompaction {
    /// Wait for the build to end.
    pub(super) async fn built(&mut self) -> Result<BuiltCompaction, BlockingError> {
        (&mut self.build).await
    }
}

/// Prepare a pass and start its build. Returns the building pass, or `None`
/// once `ack` is answered: there was nothing to compact, or preparing failed.
///
/// The pass is bounded at the last stable offset, not the high watermark:
/// Kafka's `LogCleanerManager.cleanableOffsets` takes `lastStableOffset`,
/// because the records between the LSO and the high watermark belong to
/// transactions with no marker yet. Rewriting them before the commit or
/// abort is known would leave the output segment's aborted-transaction
/// index unable to learn about a marker that arrives later. The LSO is read
/// under the same lock that takes the pass's input, so the input and the
/// bound agree.
pub(super) async fn start_compaction(
    (log, log_dir, registry): LogStorage<'_>,
    replica_state: &tokio::sync::Mutex<ReplicaState>,
    ack: MaintenanceAck,
) -> Option<BuildingCompaction> {
    let high_watermark = replica_state.lock().await.hw;
    let now = SystemTime::now();
    let shared = Arc::clone(log);
    let prepared = run_log_mutation(
        move || prepare(&shared, now, high_watermark).map_err(BrokerError::from),
        PANIC_CONTEXT,
        StorageStatus { log_dir, registry },
    )
    .await;
    match prepared {
        Ok(Some(job)) => Some(BuildingCompaction {
            build: spawn_blocking(move || job.build()),
            ack,
        }),
        Ok(None) => {
            let _ = ack.send(Ok(()));
            None
        }
        Err(error) => {
            let _ = ack.send(Err(error));
            None
        }
    }
}

/// Install a pass whose build has ended, and answer its ack.
pub(super) async fn finish_compaction(
    (log, log_dir, registry): LogStorage<'_>,
    BuildingCompaction { ack, .. }: BuildingCompaction,
    built: Result<BuiltCompaction, BlockingError>,
) {
    let status = StorageStatus { log_dir, registry };
    let result = match built {
        Ok(built) => {
            let shared = Arc::clone(log);
            run_log_mutation(
                move || {
                    lock_log(&shared)
                        .finish_compaction(built)
                        .map(drop)
                        .map_err(BrokerError::from)
                },
                PANIC_CONTEXT,
                status,
            )
            .await
        }
        Err(panic) => {
            let error = storage_failure_error(PANIC_CONTEXT, panic);
            flag_storage_failure(&error, log_dir, registry);
            Err(error)
        }
    };
    let _ = ack.send(result);
}

fn prepare(
    log: &Mutex<Log>,
    now: SystemTime,
    high_watermark: Offset,
) -> Result<Option<CompactionJob>, LogError> {
    let mut log = lock_log(log);
    let context = CompactionContext {
        now,
        last_stable_offset: log.last_stable_offset(high_watermark),
    };
    log.prepare_compaction(&context)
}
