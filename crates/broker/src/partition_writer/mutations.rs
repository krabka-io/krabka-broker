//! The writer's non-produce log mutations: replicate, truncate, reset, and
//! trim.
//!
//! Each of these arms runs one blocking log call, acks it, and then repairs
//! whatever derived state the call moved, so they share a module and the
//! blocking-pool wrapper they all call.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;
use krabka_log::{Log, Offset};
use tokio::sync::Notify;

use super::storage::{flag_storage_failure, lock_log, run_log_mutation};
use crate::{log_dir_status::LogDirRegistry, replica_state::ReplicaState};

pub(super) async fn handle_replicate(
    log: &Arc<Mutex<Log>>,
    log_dir: &Arc<ArcSwap<PathBuf>>,
    log_dir_status: &LogDirRegistry,
    mut batch: krabka_protocol::records::RecordBatch,
    ack: tokio::sync::oneshot::Sender<Result<(), crate::error::BrokerError>>,
    append_notify: &Notify,
) {
    let offset = batch.base_offset;
    let log_for_blocking = Arc::clone(log);
    let result = run_log_mutation(
        move || {
            lock_log(&log_for_blocking)
                .append_at(&mut batch, Offset(offset))
                .map_err(crate::error::BrokerError::from)
        },
        "replicate task panicked",
        (log_dir, log_dir_status),
    )
    .await;
    let succeeded = result.is_ok();
    let _ = ack.send(result);
    if succeeded {
        append_notify.notify_waiters();
    }
}

pub(super) async fn handle_truncate(
    log: &Arc<Mutex<Log>>,
    storage_status: (&Arc<ArcSwap<PathBuf>>, &LogDirRegistry),
    replica_state: &tokio::sync::Mutex<ReplicaState>,
    wal: Option<&crate::wal::SharedWal>,
    offset: Offset,
    ack: tokio::sync::oneshot::Sender<Result<(), crate::error::BrokerError>>,
) {
    let log_for_blocking = Arc::clone(log);
    let result = run_log_mutation(
        move || {
            lock_log(&log_for_blocking)
                .truncate_to(offset)
                .map_err(crate::error::BrokerError::from)
        },
        "truncate task panicked",
        storage_status,
    )
    .await;
    let succeeded = result.is_ok();
    if succeeded {
        if let Some(wal) = wal {
            wal.invalidate_hot_tail();
        }
        let new_leo = lock_log(log).log_end_offset();
        replica_state
            .lock()
            .await
            .recompute_hw_for_leader_append(new_leo);
    }
    let _ = ack.send(result);
}

pub(super) async fn handle_reset(
    log: &Arc<Mutex<Log>>,
    storage_status: (&Arc<ArcSwap<PathBuf>>, &LogDirRegistry),
    replica_state: &tokio::sync::Mutex<ReplicaState>,
    wal: Option<&crate::wal::SharedWal>,
    new_base: Offset,
    ack: tokio::sync::oneshot::Sender<Result<(), crate::error::BrokerError>>,
) {
    let log_for_blocking = Arc::clone(log);
    let result = run_log_mutation(
        move || {
            lock_log(&log_for_blocking)
                .reset_to(new_base)
                .map_err(crate::error::BrokerError::from)
        },
        "reset_to task panicked",
        storage_status,
    )
    .await;
    let succeeded = result.is_ok();
    if succeeded {
        if let Some(wal) = wal {
            wal.invalidate_hot_tail();
        }
        let new_leo = lock_log(log).log_end_offset();
        replica_state
            .lock()
            .await
            .recompute_hw_for_leader_append(new_leo);
    }
    let _ = ack.send(result);
}

pub(super) async fn handle_trim(
    log: &Arc<Mutex<Log>>,
    storage_status: (&Arc<ArcSwap<PathBuf>>, &LogDirRegistry),
    wal: Option<&crate::wal::SharedWal>,
    new_start: Offset,
    ack: tokio::sync::oneshot::Sender<Result<Offset, crate::error::BrokerError>>,
) {
    let result = if let Some(wal) = wal {
        let result = wal.trim_to_offset(new_start).await;
        if let Err(error) = &result {
            flag_storage_failure(error, storage_status.0, storage_status.1);
        }
        result
    } else {
        let log_for_blocking = Arc::clone(log);
        run_log_mutation(
            move || {
                lock_log(&log_for_blocking)
                    .trim_to_offset(new_start)
                    .map_err(crate::error::BrokerError::from)
            },
            "trim_to_offset task panicked",
            storage_status,
        )
        .await
    };
    let _ = ack.send(result);
}
