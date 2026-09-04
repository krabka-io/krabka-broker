//! The writer's Retain arm: one local-retention pass over the partition log.
//!
//! `Log::tick` does the time-based segment roll and then the `retention.ms` /
//! `retention.bytes` eviction of sealed segments. Both mutate the log, so the
//! pass runs on the writer actor rather than under a lock taken by the
//! background sweep, exactly as [`super::compaction::handle_compact`] does.
//!
//! Routing the error through `flag_storage_failure` is the point of putting
//! this arm here rather than calling `Log::tick` from the sweep: it is what
//! makes the deletion error that `retention::delete_segment_files` now
//! propagates (#470) reachable in production instead of dying in a background
//! task, and it extends the offline-log-dir escalation of #471 to cover
//! retention as well as compaction -- an `unlink` that fails with `EIO` takes
//! the log directory offline just as a failed compaction rewrite does.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;
use krabka_log::Log;

use super::storage::{lock_log, run_log_mutation};
use crate::log_dir_status::LogDirRegistry;

pub(super) async fn handle_retention(
    storage: (&Arc<Mutex<Log>>, &Arc<ArcSwap<PathBuf>>, &LogDirRegistry),
    ack: tokio::sync::oneshot::Sender<Result<(), crate::error::BrokerError>>,
) {
    let (log, log_dir, log_dir_status) = storage;
    let now = std::time::SystemTime::now();
    let log_for_blocking = Arc::clone(log);
    let result = run_log_mutation(
        move || {
            lock_log(&log_for_blocking)
                .tick(now)
                .map_err(crate::error::BrokerError::from)
        },
        "retention task panicked",
        (log_dir, log_dir_status),
    )
    .await;
    let _ = ack.send(result);
}
