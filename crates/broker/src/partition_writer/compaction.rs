//! The writer's Compact arm.
//!
//! A pass keeps the last record of each active producer, so that the state of
//! the producer survives the rewrite. [`Log::compact`] reads those records
//! from the producer state of the log, which a leader and a follower both
//! update for each batch that they append. This is Kafka's
//! `Cleaner.cleanSegments`, which reads `UnifiedLog.lastRecordsOfActiveProducers`
//! on every replica. The tracker of the produce path does not take part: on a
//! follower it does not hold the replicated data batches.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;
use krabka_log::Log;

use super::storage::{flag_storage_failure, lock_log, storage_failure_error};
use crate::{log_dir_status::LogDirRegistry, replica_state::ReplicaState};

pub(super) async fn handle_compact(
    storage: (&Arc<Mutex<Log>>, &Arc<ArcSwap<PathBuf>>, &LogDirRegistry),
    replica_state: &tokio::sync::Mutex<ReplicaState>,
    ack: tokio::sync::oneshot::Sender<Result<(), crate::error::BrokerError>>,
) {
    let (log, log_dir, log_dir_status) = storage;
    // The pass is bounded at the last stable offset, not the high watermark:
    // Kafka's `LogCleanerManager.cleanableOffsets` takes `lastStableOffset`,
    // because the records between the LSO and the high watermark belong to
    // transactions with no marker yet. Rewriting them before the commit or
    // abort is known would leave the output segment's aborted-transaction
    // index unable to learn about a marker that arrives later. The watermark
    // is read inside the writer actor, which is the only task that moves the
    // log, so no append or truncation can land between the read and the
    // rewrite.
    let high_watermark = replica_state.lock().await.hw;
    let now = std::time::SystemTime::now();
    let log_for_blocking = Arc::clone(log);
    let join = crate::blocking::spawn_blocking(move || {
        let mut log = lock_log(&log_for_blocking);
        let last_stable_offset = log.last_stable_offset(high_watermark);
        let context = krabka_log::CompactionContext {
            now,
            last_stable_offset,
        };
        log.compact(&context)
            .map_err(crate::error::BrokerError::from)
    });
    let result = match join.await {
        Ok(value) => value,
        Err(join_err) => Err(storage_failure_error("compact task panicked", join_err)),
    };
    if let Err(err) = &result {
        flag_storage_failure(err, log_dir, log_dir_status);
    }
    let _ = ack.send(result);
}
