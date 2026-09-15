//! The writer's Produce arm: group drain, append, ack, and high-watermark
//! advance.
//!
//! This is the only arm that batches several queued messages into one log
//! mutation and the only one that has to reconcile a diskless offset
//! assignment with the durable watermark, so it gets its own module.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;
use krabka_ids::PartitionIndex;
use krabka_log::Log;
use tokio::sync::{Notify, mpsc};

use super::{
    append::{run_produce_append_batch, run_produce_append_batch_at},
    storage::{flag_storage_failure, storage_failure_error},
};
use crate::{
    log_dir_status::LogDirRegistry,
    partition::{ProduceData, ProduceJob, WriterMessage},
    producer_state::ProducerState,
    replica_state::ReplicaState,
};

pub(super) async fn handle_produce(
    identity: (&str, PartitionIndex),
    group: (ProduceJob, usize),
    rx: &mut mpsc::Receiver<WriterMessage>,
    pending: &mut Option<WriterMessage>,
    storage: (
        &Arc<Mutex<Log>>,
        &Arc<ArcSwap<PathBuf>>,
        &LogDirRegistry,
        &ProducerState,
    ),
    signals: (
        &Arc<Notify>,
        &Arc<tokio::sync::Mutex<ReplicaState>>,
        &Arc<Notify>,
    ),
    diskless: (
        Option<&crate::wal::SharedWal>,
        Option<&Arc<dyn crate::wal::OffsetSequencer>>,
    ),
) {
    let (first, max_produce_group) = group;
    let (wal, sequencer) = diskless;
    let (log, log_dir, log_dir_status, producer_state) = storage;
    let (append_notify, replica_state, hw_advance_notify) = signals;
    let mut jobs = vec![first];
    while jobs.len() < max_produce_group {
        match rx.try_recv() {
            Ok(WriterMessage::Produce(job)) => jobs.push(job),
            Ok(other) => {
                *pending = Some(other);
                break;
            }
            Err(_) => break,
        }
    }

    let mut acks = Vec::with_capacity(jobs.len());
    let mut datas = Vec::with_capacity(jobs.len());
    for ProduceJob { data, ack } in jobs {
        acks.push(ack);
        datas.push(data);
    }

    // A transaction marker adds a complete transaction that holds the last
    // stable offset until the high watermark passes the marker. Release the
    // ones the high watermark already passed before the group appends, so the
    // set stays bounded on a partition that no reader fetches from.
    let high_watermark = if datas
        .iter()
        .any(|data| data.control_producer_id().is_some())
    {
        Some(replica_state.lock().await.hw)
    } else {
        None
    };

    let append_result = if wal.is_some() {
        let Some(sequencer) = sequencer else {
            for ack in acks {
                let _ = ack.send(Err(storage_failure_error(
                    "diskless append missing offset sequencer",
                    "no sequencer configured",
                )));
            }
            return;
        };
        let count = datas.iter().map(ProduceData::record_count).sum();
        match sequencer.assign(identity.0, identity.1, count).await {
            Ok(base) => {
                run_produce_append_batch_at(Arc::clone(log), base, high_watermark, datas).await
            }
            Err(error) => {
                for ack in acks {
                    let _ = ack.send(Err(storage_failure_error(
                        "offset assignment failed",
                        &error,
                    )));
                }
                return;
            }
        }
    } else {
        run_produce_append_batch(Arc::clone(log), high_watermark, datas).await
    };
    let (results, leo, control_entries) = match append_result {
        Ok(value) => value,
        Err(err) => {
            flag_storage_failure(&err, log_dir, log_dir_status);
            for ack in acks {
                let _ = ack.send(Err(storage_failure_error(
                    "append task panicked",
                    "group append panic",
                )));
            }
            return;
        }
    };

    // A transaction marker changes the producer state in the log. Without
    // this copy the tracker keeps the state from before the marker until a
    // restart rebuilds it from the log, and a produce gets a different
    // answer before and after that restart. `control_entries` comes from the
    // same lock acquisition the append itself already ran under
    // `block_in_place`/`spawn_blocking` (see `append_produce_batch`'s doc
    // comment): mirroring here takes no further `std::sync::Mutex` lock on
    // this async task, only the `ProducerState` per-partition `tokio::sync::
    // Mutex` this call already awaits cooperatively.
    if !control_entries.is_empty() {
        producer_state
            .mirror_log_entries(identity.0, identity.1, control_entries)
            .await;
    }

    let mut any_ok = false;
    for (ack, result) in acks.into_iter().zip(results) {
        match &result {
            Ok(_) => any_ok = true,
            Err(err) => {
                flag_storage_failure(err, log_dir, log_dir_status);
            }
        }
        let _ = ack.send(result);
    }

    if any_ok {
        append_notify.notify_waiters();
        let advanced = if let Some(wal) = wal {
            match wal.sync_durable(leo).await {
                Ok(durable) => {
                    let mut state = replica_state.lock().await;
                    let previous = state.hw;
                    state.recompute_hw_for_wal_durable(durable) > previous
                }
                Err(error) => {
                    flag_storage_failure(&error, log_dir, log_dir_status);
                    false
                }
            }
        } else {
            let mut state = replica_state.lock().await;
            let previous = state.hw;
            state.recompute_hw_for_leader_append(leo) > previous
        };
        if advanced {
            hw_advance_notify.notify_waiters();
        }
    }
}
